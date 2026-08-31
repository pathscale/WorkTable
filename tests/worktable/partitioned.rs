//! `partition_by`: one table type, many instances routed by an integer key.

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: Price,
    partition_by: symbol_id: u16,
    columns: {
        exchange_id: u8 primary_key,
        bid: f64,
        ask: f64
    }
);

// Partitioning composes with the rest of the grammar: indexes, queries and
// config are untouched by it.
worktable!(
    name: Quote,
    persist: false,
    partition_by: venue: u32,
    columns: {
        id: u64 primary_key autoincrement,
        tag: u32,
        px: f64
    },
    indexes: { tag_idx: tag unique },
    config: { page_size: 1024 }
);

fn row(exchange_id: u8, bid: f64) -> PriceRow {
    PriceRow {
        exchange_id,
        bid,
        ask: bid + 1.0,
    }
}

#[test]
fn partitions_are_independent_tables() {
    let prices = PricePartitions::new();
    assert!(prices.is_empty());
    assert!(prices.partition(7).is_none(), "reading must not create");

    let btc = prices.partition_or_create(7).unwrap();
    let eth = prices.partition_or_create(9).unwrap();
    btc.insert(row(1, 100.0)).unwrap();
    eth.insert(row(1, 200.0)).unwrap();

    // The same primary key in two partitions is two different rows. This is
    // the semantic change partitioning makes, so it is asserted rather than
    // assumed.
    assert_eq!(prices.partition(7).unwrap().select(1).unwrap().bid, 100.0);
    assert_eq!(prices.partition(9).unwrap().select(1).unwrap().bid, 200.0);
    assert_eq!(prices.len(), 2);
    assert_eq!(prices.keys(), vec![7u16, 9]);
}

#[test]
fn a_key_maps_to_one_table_however_often_it_is_asked_for() {
    let prices = PricePartitions::new();
    let a = prices.partition_or_create(3).unwrap();
    a.insert(row(0, 1.0)).unwrap();
    let b = prices.partition_or_create(3).unwrap();
    // Same table, so the row inserted through `a` is visible through `b`.
    assert_eq!(b.select(0).unwrap().bid, 1.0);
    assert_eq!(prices.len(), 1);
}

#[test]
fn keys_are_typed_and_span_chunk_boundaries() {
    let prices = PricePartitions::new();
    for k in [5000u16, 0, 1024, 1023, 2048] {
        prices.partition_or_create(k).unwrap();
    }
    assert_eq!(prices.keys(), vec![0u16, 1023, 1024, 2048, 5000]);
    for (k, table) in prices.iter() {
        table.insert(row(0, k as f64)).unwrap();
        assert_eq!(table.select(0).unwrap().bid, k as f64);
    }
}

#[test]
fn removing_a_partition_leaves_held_handles_alive() {
    let prices = PricePartitions::new();
    let held = prices.partition_or_create(4).unwrap();
    held.insert(row(2, 9.0)).unwrap();

    assert!(prices.remove(4).is_some());
    assert_eq!(prices.len(), 0);
    assert!(prices.partition(4).is_none());
    // A reader mid-query keeps working.
    assert_eq!(held.select(2).unwrap().bid, 9.0);
    assert!(prices.remove(4).is_none());
}

#[test]
fn insert_with_a_custom_initialiser_runs_once_per_key() {
    let prices = PricePartitions::new();
    let seeded = prices
        .partition_or_insert_with(11, || {
            let t = PriceWorkTable::default();
            for e in 0..3u8 {
                t.insert(row(e, e as f64)).unwrap();
            }
            t
        })
        .unwrap();
    assert_eq!(seeded.select(2).unwrap().bid, 2.0);
    // Second call must not re-run the initialiser or replace the table.
    let again = prices
        .partition_or_insert_with(11, || panic!("initialiser ran twice"))
        .unwrap();
    assert_eq!(again.select(2).unwrap().bid, 2.0);
}

#[tokio::test]
async fn partitioning_composes_with_indexes_and_queries() {
    let quotes = QuotePartitions::new();
    let a = quotes.partition_or_create(100).unwrap();
    let b = quotes.partition_or_create(200).unwrap();

    a.insert(QuoteRow {
        id: a.get_next_pk().0,
        tag: 1,
        px: 10.0,
    })
    .unwrap();
    b.insert(QuoteRow {
        id: b.get_next_pk().0,
        tag: 1,
        px: 20.0,
    })
    .unwrap();

    // `tag` is a unique index, and tag 1 exists in both partitions, because
    // uniqueness is per partition.
    let from_a = a.select_by_tag(1).expect("tag 1 exists in partition 100");
    let from_b = b.select_by_tag(1).expect("tag 1 exists in partition 200");
    assert_eq!(from_a.px, 10.0);
    assert_eq!(from_b.px, 20.0);

    // autoincrement counts per partition, so both start from the same place.
    assert_eq!(from_a.id, from_b.id);
}

#[test]
fn memory_and_rows_are_reported_per_key() {
    let prices = PricePartitions::new();
    assert_eq!(prices.memory_total(), 0);
    for k in [1u16, 2, 3] {
        let t = prices.partition_or_create(k).unwrap();
        for e in 0..(k as u8) {
            t.insert(row(e, 1.0)).unwrap();
        }
    }
    let by_key = prices.memory_by_key();
    assert_eq!(by_key.iter().map(|(k, _)| *k).collect::<Vec<_>>(), vec![1, 2, 3]);
    assert_eq!(prices.memory_total(), by_key.iter().map(|(_, b)| *b).sum::<u64>());
    assert!(prices.memory_total() > 0);

    // Row counts are the thing a residency budget would weigh on.
    assert_eq!(prices.rows_by_key(), vec![(1u16, 1), (2, 2), (3, 3)]);
}

#[test]
fn concurrent_creation_and_reading_is_sound() {
    use std::sync::Arc;
    let prices = Arc::new(PricePartitions::new());
    let mut handles = Vec::new();
    for t in 0..8u16 {
        let prices = prices.clone();
        handles.push(std::thread::spawn(move || {
            for k in 0..128u16 {
                let table = prices.partition_or_create(k).unwrap();
                // Every thread writes the same row for a key, so whichever
                // wins the insert the value must match the key.
                let _ = table.insert(row(0, k as f64));
                let got = prices.partition(k).unwrap().select(0).unwrap();
                assert_eq!(got.bid, k as f64, "thread {t} saw a torn partition at {k}");
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(prices.len(), 128);
}

// ---------------------------------------------------------------------------
// Isolation. Partitioning only pays for itself if a write in one partition is
// invisible to every other, so each mutating path is asserted rather than
// assumed to inherit isolation from the storage.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn updates_are_scoped_to_one_partition() {
    let prices = PricePartitions::new();
    let a = prices.partition_or_create(1).unwrap();
    let b = prices.partition_or_create(2).unwrap();
    a.insert(row(7, 100.0)).unwrap();
    b.insert(row(7, 200.0)).unwrap();

    a.update(PriceRow {
        exchange_id: 7,
        bid: 999.0,
        ask: 1000.0,
    })
    .await
    .unwrap();

    assert_eq!(a.select(7).unwrap().bid, 999.0);
    assert_eq!(
        b.select(7).unwrap().bid,
        200.0,
        "an update in partition 1 reached partition 2"
    );
}

#[tokio::test]
async fn deletes_are_scoped_to_one_partition() {
    let prices = PricePartitions::new();
    let a = prices.partition_or_create(1).unwrap();
    let b = prices.partition_or_create(2).unwrap();
    let pk = a.insert(row(7, 100.0)).unwrap();
    b.insert(row(7, 200.0)).unwrap();

    a.delete(pk).await.unwrap();

    assert!(a.select(7).is_none());
    assert_eq!(
        b.select(7).unwrap().bid,
        200.0,
        "a delete in partition 1 reached partition 2"
    );
    assert_eq!(prices.rows_by_key(), vec![(1u16, 0), (2, 1)]);
}

#[tokio::test]
async fn a_unique_index_collides_only_inside_its_own_partition() {
    let quotes = QuotePartitions::new();
    let a = quotes.partition_or_create(1).unwrap();
    let b = quotes.partition_or_create(2).unwrap();

    a.insert(QuoteRow {
        id: a.get_next_pk().0,
        tag: 42,
        px: 1.0,
    })
    .unwrap();
    // The same tag in a sibling partition is fine.
    b.insert(QuoteRow {
        id: b.get_next_pk().0,
        tag: 42,
        px: 2.0,
    })
    .unwrap();
    // The same tag again in the same partition is not.
    let dup = a.insert(QuoteRow {
        id: a.get_next_pk().0,
        tag: 42,
        px: 3.0,
    });
    assert!(dup.is_err(), "a unique index must still be unique within its partition");

    assert_eq!(a.select_by_tag(42).unwrap().px, 1.0);
    assert_eq!(b.select_by_tag(42).unwrap().px, 2.0);
}

#[test]
fn autoincrement_counts_independently_in_each_partition() {
    let quotes = QuotePartitions::new();
    let a = quotes.partition_or_create(1).unwrap();
    let b = quotes.partition_or_create(2).unwrap();

    for i in 0..5u32 {
        a.insert(QuoteRow {
            id: a.get_next_pk().0,
            tag: i,
            px: 1.0,
        })
        .unwrap();
    }
    // `b` has had no inserts, so its counter has not moved.
    let first_in_b = b.get_next_pk().0;
    b.insert(QuoteRow {
        id: first_in_b,
        tag: 100,
        px: 2.0,
    })
    .unwrap();

    assert_eq!(
        first_in_b, 0,
        "autoincrement leaked across partitions: b started at {first_in_b}"
    );
    assert_eq!(a.select_by_tag(4).unwrap().id, 4);
    assert_eq!(quotes.rows_by_key(), vec![(1u32, 5), (2, 1)]);
}

// ---------------------------------------------------------------------------
// Reclamation through the generated facade.
// ---------------------------------------------------------------------------

#[test]
fn a_removed_partition_is_reclaimed_only_by_gc() {
    let mut prices = PricePartitions::new();
    let held = prices.partition_or_create(4).unwrap();
    held.insert(row(2, 9.0)).unwrap();

    let taken = prices.remove(4).expect("was present");
    assert_eq!(prices.len(), 0);
    assert_eq!(prices.retired_len(), 1, "removal must retire, not free");

    // Both handles still work: this is the reader-mid-query case.
    assert_eq!(held.select(2).unwrap().bid, 9.0);
    assert_eq!(taken.select(2).unwrap().bid, 9.0);
    drop(taken);
    drop(held);

    assert_eq!(prices.gc(), 1);
    assert_eq!(prices.retired_len(), 0);
    assert_eq!(prices.gc(), 0, "gc must be idempotent");
}

#[test]
fn removing_every_partition_empties_the_set() {
    let prices = PricePartitions::new();
    for k in 0..16u16 {
        prices.partition_or_create(k).unwrap();
    }
    assert_eq!(prices.len(), 16);
    for k in 0..16u16 {
        assert!(prices.remove(k).is_some());
    }
    assert!(prices.is_empty());
    assert_eq!(prices.keys(), Vec::<u16>::new());
    assert_eq!(prices.iter().len(), 0);
    assert_eq!(prices.memory_total(), 0);
    assert_eq!(prices.rows_by_key(), Vec::<(u16, usize)>::new());
}

// ---------------------------------------------------------------------------
// Key range. A key type wider than the spine can address has to fail as an
// error rather than wrap onto some other partition.
// ---------------------------------------------------------------------------

#[test]
fn a_key_beyond_the_spine_is_an_error_not_a_wrap() {
    let quotes = QuotePartitions::new();
    let inside = worktable::partition::MAX_PARTITIONS as u32 - 1;
    let outside = worktable::partition::MAX_PARTITIONS as u32;

    quotes.partition_or_create(inside).unwrap();
    assert!(quotes.partition(inside).is_some());

    assert!(
        quotes.partition_or_create(outside).is_err(),
        "an unroutable key must be refused"
    );
    assert!(quotes.partition(outside).is_none());
    assert!(!quotes.contains(outside));
    assert!(quotes.remove(outside).is_none());
    assert!(quotes.partition_or_create(u32::MAX).is_err());

    // The refusals must not have disturbed the one real partition.
    assert_eq!(quotes.len(), 1);
    assert_eq!(quotes.keys(), vec![inside]);
}

#[test]
fn the_full_range_of_a_u16_key_is_routable() {
    // A u16 key can address 65,536 partitions and the spine holds exactly
    // that, so both ends of the type must work.
    let prices = PricePartitions::new();
    for k in [0u16, 1, u16::MAX - 1, u16::MAX] {
        prices.partition_or_create(k).unwrap();
        assert!(prices.contains(k), "key {k} did not stick");
    }
    assert_eq!(prices.keys(), vec![0, 1, u16::MAX - 1, u16::MAX]);
}

// ---------------------------------------------------------------------------
// Concurrency through the generated facade, with real tables rather than the
// unit tests' trivial payload.
// ---------------------------------------------------------------------------

#[test]
fn concurrent_writers_on_disjoint_partitions_do_not_interfere() {
    use std::sync::Arc;
    const THREADS: u16 = 8;
    const ROWS: u8 = 32;

    let prices = Arc::new(PricePartitions::new());
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let prices = prices.clone();
            std::thread::spawn(move || {
                let table = prices.partition_or_create(t).unwrap();
                for e in 0..ROWS {
                    table.insert(row(e, t as f64 * 1000.0 + e as f64)).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(prices.len(), THREADS as usize);
    for t in 0..THREADS {
        let table = prices.partition(t).expect("partition {t} vanished");
        for e in 0..ROWS {
            assert_eq!(
                table.select(e).unwrap().bid,
                t as f64 * 1000.0 + e as f64,
                "partition {t} row {e} was written by the wrong thread"
            );
        }
    }
    assert_eq!(
        prices.rows_by_key(),
        (0..THREADS).map(|t| (t, ROWS as usize)).collect::<Vec<_>>()
    );
}

#[test]
fn readers_survive_partitions_being_removed_under_them() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    const KEYS: u16 = 24;
    const ROUNDS: u32 = 150;

    let prices = Arc::new(PricePartitions::new());
    let stop = Arc::new(AtomicBool::new(false));

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let prices = prices.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    for k in 0..KEYS {
                        // Reading through a handle that a churn thread may be
                        // removing right now is the case that would fault on a
                        // use-after-free.
                        if let Some(r) = prices.partition(k).and_then(|t| t.select(0)) {
                            assert_eq!(r.bid, k as f64, "partition {k} was torn");
                        }
                    }
                }
            })
        })
        .collect();

    for _ in 0..ROUNDS {
        for k in 0..KEYS {
            let t = prices
                .partition_or_insert_with(k, || {
                    let t = PriceWorkTable::default();
                    t.insert(row(0, k as f64)).unwrap();
                    t
                })
                .unwrap();
            let _ = t;
        }
        for k in 0..KEYS {
            prices.remove(k);
        }
    }
    stop.store(true, Ordering::Relaxed);
    for h in readers {
        h.join().unwrap();
    }

    assert!(prices.is_empty());
    assert_eq!(prices.retired_len(), ROUNDS as usize * KEYS as usize);
    let mut prices = Arc::try_unwrap(prices).expect("readers have exited");
    assert_eq!(prices.gc(), ROUNDS as usize * KEYS as usize);
}
