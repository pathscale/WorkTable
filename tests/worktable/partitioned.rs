//! `partition_by`: one table type, many instances routed by an integer key.

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: Price,
    partition_by: symbol_id: u16,
    partition_max_size: u64,
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
    partition_max_size: u64,
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

#[tokio::test]
async fn partitions_are_independent_tables() {
    let prices = PricePartitions::new();
    assert!(prices.is_empty());
    assert!(prices.partition(7).is_none(), "reading must not create");

    let btc = prices.partition_or_create(7).unwrap();
    let eth = prices.partition_or_create(9).unwrap();
    btc.insert(row(1, 100.0)).await.unwrap();
    eth.insert(row(1, 200.0)).await.unwrap();

    // The same primary key in two partitions is two different rows. This is
    // the semantic change partitioning makes, so it is asserted rather than
    // assumed.
    assert_eq!(prices.partition(7).unwrap().select(1).unwrap().bid, 100.0);
    assert_eq!(prices.partition(9).unwrap().select(1).unwrap().bid, 200.0);
    assert_eq!(prices.len(), 2);
    assert_eq!(prices.keys(), vec![7u16, 9]);
}

#[tokio::test]
async fn a_key_maps_to_one_table_however_often_it_is_asked_for() {
    let prices = PricePartitions::new();
    let a = prices.partition_or_create(3).unwrap();
    a.insert(row(0, 1.0)).await.unwrap();
    let b = prices.partition_or_create(3).unwrap();
    // Same table, so the row inserted through `a` is visible through `b`.
    assert_eq!(b.select(0).unwrap().bid, 1.0);
    assert_eq!(prices.len(), 1);
}

#[tokio::test]
async fn keys_are_typed_and_span_chunk_boundaries() {
    let prices = PricePartitions::new();
    for k in [5000u16, 0, 1024, 1023, 2048] {
        prices.partition_or_create(k).unwrap();
    }
    assert_eq!(prices.keys(), vec![0u16, 1023, 1024, 2048, 5000]);
    for (k, table) in prices.iter() {
        table.insert(row(0, k as f64)).await.unwrap();
        assert_eq!(table.select(0).unwrap().bid, k as f64);
    }
}

#[tokio::test]
async fn removing_a_partition_leaves_held_handles_alive() {
    let prices = PricePartitions::new();
    let held = prices.partition_or_create(4).unwrap();
    held.insert(row(2, 9.0)).await.unwrap();

    assert!(prices.remove(4).is_some());
    assert_eq!(prices.len(), 0);
    assert!(prices.partition(4).is_none());
    // A reader mid-query keeps working.
    assert_eq!(held.select(2).unwrap().bid, 9.0);
    assert!(prices.remove(4).is_none());
}

#[tokio::test]
async fn insert_with_a_custom_initialiser_runs_once_per_key() {
    let prices = PricePartitions::new();
    let seeded = prices
        .partition_or_insert_with(11, || {
            let t = PriceWorkTable::default();
            for e in 0..3u8 {
                nagoya::block_on(t.insert(row(e, e as f64))).unwrap();
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
    .await
    .unwrap();
    b.insert(QuoteRow {
        id: b.get_next_pk().0,
        tag: 1,
        px: 20.0,
    })
    .await
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

#[tokio::test]
async fn memory_and_rows_are_reported_per_key() {
    let prices = PricePartitions::new();
    assert_eq!(prices.memory_total(), 0);
    for k in [1u16, 2, 3] {
        let t = prices.partition_or_create(k).unwrap();
        for e in 0..(k as u8) {
            t.insert(row(e, 1.0)).await.unwrap();
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
                let _ = nagoya::block_on(table.insert(row(0, k as f64)));
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
    a.insert(row(7, 100.0)).await.unwrap();
    b.insert(row(7, 200.0)).await.unwrap();

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
    let pk = a.insert(row(7, 100.0)).await.unwrap();
    b.insert(row(7, 200.0)).await.unwrap();

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
    .await
    .unwrap();
    // The same tag in a sibling partition is fine.
    b.insert(QuoteRow {
        id: b.get_next_pk().0,
        tag: 42,
        px: 2.0,
    })
    .await
    .unwrap();
    // The same tag again in the same partition is not.
    let dup = a
        .insert(QuoteRow {
            id: a.get_next_pk().0,
            tag: 42,
            px: 3.0,
        })
        .await;
    assert!(dup.is_err(), "a unique index must still be unique within its partition");

    assert_eq!(a.select_by_tag(42).unwrap().px, 1.0);
    assert_eq!(b.select_by_tag(42).unwrap().px, 2.0);
}

#[tokio::test]
async fn autoincrement_counts_independently_in_each_partition() {
    let quotes = QuotePartitions::new();
    let a = quotes.partition_or_create(1).unwrap();
    let b = quotes.partition_or_create(2).unwrap();

    for i in 0..5u32 {
        a.insert(QuoteRow {
            id: a.get_next_pk().0,
            tag: i,
            px: 1.0,
        })
        .await
        .unwrap();
    }
    // `b` has had no inserts, so its counter has not moved.
    let first_in_b = b.get_next_pk().0;
    b.insert(QuoteRow {
        id: first_in_b,
        tag: 100,
        px: 2.0,
    })
    .await
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

#[tokio::test]
async fn a_removed_partition_waits_out_its_readers_then_frees_through_a_shared_handle() {
    let prices = PricePartitions::new();
    let held = prices.partition_or_create(4).unwrap();
    held.insert(row(2, 9.0)).await.unwrap();

    // A pinned borrow models the reader that resolved the partition before
    // the removal; its grace period keeps the removal retired.
    let reader = prices.partition_ref(4).expect("present");
    let taken = prices.remove(4).expect("was present");
    assert_eq!(prices.len(), 0);
    assert_eq!(
        prices.retired_len(),
        1,
        "removal must wait out the reader's grace period"
    );

    // All three handles still work: this is the reader-mid-query case.
    assert_eq!(reader.select(2).unwrap().bid, 9.0);
    assert_eq!(held.select(2).unwrap().bid, 9.0);
    assert_eq!(taken.select(2).unwrap().bid, 9.0);
    drop(taken);
    drop(reader);

    // Reclamation works through `&self` once the reader has left: no `&mut`,
    // which a router shared behind an `Arc` could never produce.
    let mut freed = 0;
    for _ in 0..16 {
        freed += prices.collect();
        if freed > 0 {
            break;
        }
    }
    assert_eq!(freed, 1, "collect must report what it reclaimed");
    assert_eq!(prices.retired_len(), 0);
    assert_eq!(prices.collect(), 0, "collect must be idempotent");

    // The strong handle keeps the table alive independently of the router.
    assert_eq!(held.select(2).unwrap().bid, 9.0);
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

#[tokio::test]
async fn concurrent_writers_on_disjoint_partitions_do_not_interfere() {
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
                    nagoya::block_on(table.insert(row(e, t as f64 * 1000.0 + e as f64))).unwrap();
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

#[tokio::test]
async fn readers_survive_partitions_being_removed_under_them() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    const KEYS: u16 = 24;
    const ROUNDS: u32 = 150;

    let prices = Arc::new(PricePartitions::new());
    let stop = Arc::new(AtomicBool::new(false));

    let readers: Vec<_> = (0..2)
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
                    // Yield rather than spin. These readers share a test binary
                    // with the persistence suite, and a tight spin on every core
                    // starves it into failing.
                    std::thread::yield_now();
                }
            })
        })
        .collect();

    for _ in 0..ROUNDS {
        for k in 0..KEYS {
            let t = prices
                .partition_or_insert_with(k, || {
                    let t = PriceWorkTable::default();
                    nagoya::block_on(t.insert(row(0, k as f64))).unwrap();
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

    // Reclamation happened through the shared `Arc` while readers were
    // running; drain whatever grace period is still open the same way.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while prices.retired_len() > 0 && std::time::Instant::now() < deadline {
        prices.collect();
    }
    assert_eq!(
        prices.retired_len(),
        0,
        "a shared router must reclaim every removed partition"
    );
}

// ---------------------------------------------------------------------------
// Accounting. Raised in review: a total that counts only live partitions falls
// after a removal that freed nothing, which is exactly backwards for a
// residency budget.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn retired_bytes_accounts_for_what_removal_has_not_freed_yet() {
    let prices = PricePartitions::new();
    for k in 0..4u16 {
        let t = prices.partition_or_create(k).unwrap();
        for e in 0..(k as u8 + 1) {
            t.insert(row(e, 1.0)).await.unwrap();
        }
    }
    let live_before = prices.memory_total();
    assert!(live_before > 0);
    assert_eq!(prices.retired_bytes(), 0);
    assert_eq!(prices.retired_len(), 0);

    let removed_rows = prices.rows_by_key().iter().find(|(k, _)| *k == 2).unwrap().1;
    // The pinned borrow holds the removal's grace period open, modelling the
    // reader that is still mid-query on the partition being removed.
    let reader = prices.partition_ref(2).expect("present");
    prices.remove(2);

    // The live total drops, because the partition is no longer live.
    assert!(
        prices.memory_total() < live_before,
        "memory_total must count only live partitions"
    );
    // But nothing was freed yet, and this is the number that says so.
    assert!(
        prices.retired_bytes() > 0,
        "a retired partition still occupies memory and must be reported"
    );
    assert_eq!(prices.retired_len(), 1);
    assert_eq!(removed_rows, 3);

    // Once the reader leaves, collect makes the retired bytes real.
    drop(reader);
    let mut freed = 0;
    for _ in 0..16 {
        freed += prices.collect();
        if freed > 0 {
            break;
        }
    }
    assert_eq!(freed, 1);
    assert_eq!(prices.retired_bytes(), 0);
    assert_eq!(prices.retired_len(), 0);
}

#[tokio::test]
async fn metrics_agree_with_each_other() {
    let prices = PricePartitions::new();
    for k in [1u16, 7, 2048] {
        let t = prices.partition_or_create(k).unwrap();
        for e in 0..(k as u8 % 5 + 1) {
            t.insert(row(e, 1.0)).await.unwrap();
        }
    }

    let by_key = prices.memory_by_key();
    let rows = prices.rows_by_key();
    let keys = prices.keys();

    // All three walk the same slots and must report the same keys in the same
    // ascending order: they no longer share a code path, so this is asserted.
    assert_eq!(by_key.iter().map(|(k, _)| *k).collect::<Vec<_>>(), keys);
    assert_eq!(rows.iter().map(|(k, _)| *k).collect::<Vec<_>>(), keys);
    assert_eq!(
        prices.memory_total(),
        by_key.iter().map(|(_, b)| *b).sum::<u64>(),
        "memory_total folds directly and must still match memory_by_key"
    );
    for (k, count) in rows {
        assert_eq!(count, prices.partition_ref(k).unwrap().row_count());
    }
}

// ---------------------------------------------------------------------------
// The pinned scope. A pin ends in a fence that the slot loads wait on, so
// pinning per lookup costs five times what pinning per batch does. These
// assert the batch form behaves identically to the per-call one.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_pinned_scope_sees_what_partition_ref_sees() {
    let prices = PricePartitions::new();
    for k in [0u16, 7, 1023, 1024, 5000] {
        let t = prices.partition_or_create(k).unwrap();
        t.insert(row(1, k as f64)).await.unwrap();
    }

    let pinned = prices.pinned();
    for k in [0u16, 7, 1023, 1024, 5000] {
        assert!(pinned.contains(k), "key {k} missing from the pinned scope");
        assert_eq!(
            pinned.get(k).unwrap().select(1).unwrap().bid,
            k as f64,
            "key {k} routed wrong inside the pinned scope"
        );
    }
    // Absent and out-of-range keys behave as everywhere else.
    assert!(pinned.get(9).is_none());
    assert!(!pinned.contains(9));
    assert!(pinned.get(u16::MAX).is_none());
}

#[tokio::test]
async fn a_borrow_taken_in_a_pinned_scope_survives_removal() {
    let prices = PricePartitions::new();
    let created = prices.partition_or_create(3).unwrap();
    created.insert(row(1, 30.0)).await.unwrap();
    drop(created);

    let pinned = prices.pinned();
    // Taken *before* the removal. This is what the pin protects.
    let borrowed = pinned.get(3).expect("present");
    assert_eq!(borrowed.select(1).unwrap().bid, 30.0);

    // Creation and removal both proceed while the scope is open.
    prices.partition_or_create(4).unwrap();
    assert!(prices.remove(3).is_some());

    // The borrow stays readable although the partition was removed and every
    // owning handle is gone: reclamation cannot run under the pin.
    assert_eq!(
        borrowed.select(1).unwrap().bid,
        30.0,
        "a borrow taken before the removal must stay valid"
    );

    // A fresh lookup of a removed key correctly finds nothing: the pin keeps
    // an existing borrow alive, it does not resurrect a cleared slot.
    assert!(pinned.get(3).is_none());
    // A partition created after the scope opened is still routable.
    assert!(pinned.contains(4));

    drop(pinned);
    assert!(prices.partition(3).is_none());
}

#[tokio::test]
async fn pinned_scopes_work_from_several_threads_at_once() {
    use std::sync::Arc;
    const KEYS: u16 = 64;

    let prices = Arc::new(PricePartitions::new());
    for k in 0..KEYS {
        let t = prices.partition_or_create(k).unwrap();
        t.insert(row(1, k as f64)).await.unwrap();
    }

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let prices = prices.clone();
            std::thread::spawn(move || {
                // One pin for the whole batch, which is the shape the docs
                // recommend and the benchmark measures.
                let pinned = prices.pinned();
                for _ in 0..200 {
                    for k in 0..KEYS {
                        let t = pinned.get(k).expect("partition vanished");
                        assert_eq!(t.select(1).unwrap().bid, k as f64);
                    }
                }
            })
        })
        .collect();
    for r in readers {
        r.join().unwrap();
    }
}

// A `Vec`-backed table is a legal partition payload.
//
// This was refused, on the grounds that "`vec: true` is one contiguous `Vec`
// and has nothing to partition". That reads the relationship backwards.
// Partitioning is what makes the `Vec` shape correct: a `Vec` table is
// single-writer and grows linearly, and cutting the data into many small
// independent ones is exactly how you keep both of those from mattering.
worktable!(
    name: Book,
    vec: true,
    partition_by: symbol_id: u16,
    partition_max_size: u64,
    columns: {
        exchange_id: u8 primary_key,
        bid: f64,
        ask: f64
    }
);

#[test]
fn a_vec_table_can_be_partitioned() {
    let books = BookPartitions::new();

    // `insert` on a `vec: true` table takes `&mut self`, and the router hands
    // out `Arc`, so a partition is populated before it is handed over rather
    // than after. That is the shape the callers wanting this already have:
    // every row of a book is known when the book is created.
    for symbol in 0u16..4 {
        let mut book = BookWorkTable::with_capacity(3);
        for exchange_id in 0u8..3 {
            book.insert(BookRow {
                exchange_id,
                bid: f64::from(symbol) + f64::from(exchange_id) / 10.0,
                ask: 0.0,
            })
            .expect("fresh key");
        }
        books
            .partition_or_insert_with(symbol, move || book)
            .expect("a fresh partition");
    }

    assert_eq!(books.len(), 4);

    let book = books.partition(2).expect("declared above");
    assert_eq!(book.len(), 3);
    assert_eq!(book.select(&1).expect("present").bid, 2.1);

    // The keys are per partition, not global: every book has an exchange 0.
    for symbol in 0u16..4 {
        let book = books.partition(symbol).expect("declared above");
        assert!(book.select(&0).is_some(), "symbol {symbol} has no exchange 0");
    }

    // `used_bytes` is what the router totals, so a Vec payload has to answer
    // it. Rows alone are 3 * size_of::<BookRow>() per partition, and the index
    // is on top, so the total must exceed the rows and be finite.
    let rows_only = 4 * 3 * core::mem::size_of::<BookRow>() as u64;
    let total = books.memory_total();
    assert!(total > rows_only, "{total} should exceed the {rows_only} bytes of rows");

    let by_key = books.memory_by_key();
    assert_eq!(by_key.len(), 4);
    assert_eq!(by_key.iter().map(|(_, bytes)| bytes).sum::<u64>(), total);
}

// A narrow `partition_max_size` generates a table with no index at all.
//
// This is the shape the key exists to make declarable: `exchange_id: u8` is not
// looked up, it *is* the row's position, so there is no tree to descend and
// nothing to hash. The router is unchanged; only its payload is.
worktable!(
    name: Tick,
    partition_by: symbol_id: u16,
    partition_max_size: u8,
    columns: {
        exchange_id: u8 primary_key,
        bid: f64,
        ask: f64
    }
);

#[test]
fn a_narrow_width_generates_a_dense_payload() {
    let ticks = TickPartitions::new();

    // `&self`, straight through the `Arc` the router hands out. This is the
    // difference from a `vec: true` payload, whose `insert` needs `&mut self`
    // and so has to be populated before it is handed over.
    let book = ticks.partition_or_create(7).expect("a fresh partition");
    for exchange_id in 0u8..23 {
        book.insert(TickRow {
            exchange_id,
            bid: f64::from(exchange_id),
            ask: f64::from(exchange_id) + 1.0,
        })
        .expect("fresh key");
    }

    assert_eq!(book.row_count(), 23);
    assert_eq!(book.select(&11).expect("present").bid, 11.0);
    assert_eq!(book.slots(), 23, "grown to the keys used, not to the declared 256");
    assert_eq!(TickDenseTable::MAX_ROWS, 256);
}

#[test]
fn the_declared_width_is_a_bound_at_run_time_too() {
    let ticks = TickPartitions::new();
    let book = ticks.partition_or_create(1).expect("a fresh partition");

    // `exchange_id: u8` counts to 255 and the cap is 256, so nothing a `u8` can
    // hold is out of range. What the cap does reject is a duplicate.
    book.insert(TickRow {
        exchange_id: 3,
        bid: 1.0,
        ask: 2.0,
    })
    .expect("fresh key");
    let again = book
        .insert(TickRow {
            exchange_id: 3,
            bid: 9.0,
            ask: 9.0,
        })
        .expect_err("3 is taken");
    assert_eq!(again, DenseError::Duplicate { key: 3 });
    assert_eq!(
        book.select(&3).expect("present").bid,
        1.0,
        "the refusal changed nothing"
    );
}

#[test]
fn a_column_is_updated_without_cloning_the_row() {
    // The method web3.trading's `update_top_price` wants: touch one field of a
    // wide row rather than reading it out, editing it and writing it back.
    let ticks = TickPartitions::new();
    let book = ticks.partition_or_create(2).expect("a fresh partition");
    book.insert(TickRow {
        exchange_id: 4,
        bid: 1.0,
        ask: 2.0,
    })
    .expect("fresh key");

    assert_eq!(book.update_bid(&4, 1.5), Some(1.0));
    assert_eq!(book.select(&4).expect("present").bid, 1.5);
    assert_eq!(
        book.select(&4).expect("present").ask,
        2.0,
        "the other column is untouched"
    );

    assert_eq!(book.update_bid(&5, 1.0), None, "a key holding no row updates nothing");
}

#[test]
fn a_dense_partition_costs_its_rows_and_nothing_else() {
    // The measurement the whole shape exists for. A full partition of this
    // declaration measured 28,395 bytes empty; this one must be its rows.
    let ticks = TickPartitions::new();
    for symbol in 0u16..4 {
        let book = ticks.partition_or_create(symbol).expect("a fresh partition");
        for exchange_id in 0u8..23 {
            book.insert(TickRow {
                exchange_id,
                bid: 0.0,
                ask: 0.0,
            })
            .expect("fresh key");
        }
    }

    let rows = 4 * 23 * core::mem::size_of::<Option<TickRow>>() as u64;
    assert_eq!(ticks.memory_total(), rows, "there is nothing else to count");
    assert_eq!(ticks.rows_by_key(), (0u16..4).map(|k| (k, 23)).collect::<Vec<_>>());
}

#[test]
fn deleting_does_not_renumber_the_rows_above_it() {
    let ticks = TickPartitions::new();
    let book = ticks.partition_or_create(3).expect("a fresh partition");
    for exchange_id in 0u8..4 {
        book.insert(TickRow {
            exchange_id,
            bid: f64::from(exchange_id),
            ask: 0.0,
        })
        .expect("fresh key");
    }

    assert_eq!(book.delete(&1).expect("present").bid, 1.0);
    assert_eq!(book.select(&1), None);
    assert_eq!(book.select(&2).expect("present").bid, 2.0, "key 2 did not become key 1");
    assert_eq!(book.row_count(), 3);
    assert_eq!(book.select_all().len(), 3, "select_all skips the hole");
}

// The same columns as `Tick`, with the width that keeps the full table, so the
// two shapes can be measured against each other rather than against a
// recollection.
worktable!(
    name: FatTick,
    partition_by: symbol_id: u16,
    partition_max_size: u64,
    columns: {
        exchange_id: u8 primary_key,
        bid: f64,
        ask: f64
    }
);

/// `memory_total` cannot see what the width is worth, and that is worth a test.
///
/// `used_bytes` is row bytes plus index bytes by definition: it excludes the
/// table's fixed floor, its reserved-but-unused page capacity, the router spine
/// and `Arc` overhead. The fixed floor is precisely what a dense partition
/// deletes, so the router's own reporting shows the two shapes as equal while
/// one of them holds 28 KB per partition that the other does not.
///
/// The real comparison is in `tests/dense_partition_memory.rs`, which counts
/// what the allocator was actually asked for. This test exists so nobody
/// reaches for `memory_total` to make the claim and concludes the feature does
/// nothing.
#[tokio::test]
async fn memory_total_reports_rows_and_cannot_see_the_apparatus() {
    const ROWS: u8 = 23;

    let dense = TickPartitions::new();
    let book = dense.partition_or_create(0).expect("a fresh partition");
    for exchange_id in 0..ROWS {
        book.insert(TickRow {
            exchange_id,
            bid: 0.0,
            ask: 0.0,
        })
        .expect("fresh key");
    }

    let full = FatTickPartitions::new();
    let fat = full.partition_or_create(0).expect("a fresh partition");
    for exchange_id in 0..ROWS {
        fat.insert(FatTickRow {
            exchange_id,
            bid: 0.0,
            ask: 0.0,
        })
        .await
        .expect("fresh key");
    }

    let payload = u64::from(ROWS) * core::mem::size_of::<Option<TickRow>>() as u64;
    assert_eq!(
        dense.memory_total(),
        payload,
        "a dense partition is its rows, and `used_bytes` sees all of it"
    );
    assert_eq!(
        full.memory_total(),
        dense.memory_total(),
        "the two shapes report the same used bytes, because the difference between them is \
         entirely in what `used_bytes` excludes. If this ever differs, the definition changed \
         and the note above needs rewriting."
    );

    // Both hold the same rows. The saving is apparatus, not data.
    assert_eq!(dense.rows_by_key(), full.rows_by_key());
}

/// An empty partition is where the cost lived, so it is where to look.
#[test]
fn an_empty_dense_partition_allocates_nothing() {
    let dense = TickPartitions::new();
    dense.partition_or_create(0).expect("a fresh partition");
    assert_eq!(dense.memory_total(), 0, "nothing is allocated until a row arrives");
    assert_eq!(
        dense.partition(0).expect("created above").slots(),
        0,
        "and no slots either: the declared width is a bound, not a reservation"
    );
}

// A dense partition carries `queries:`, keyed by position.
//
// This is what decides whether the shape is adoptable: web3.trading's
// `update_top_price` and `update_full` go through declared update queries, and
// a payload that could not carry them would be a payload they cannot use.
worktable!(
    name: Quoted,
    partition_by: symbol_id: u16,
    partition_max_size: u8,
    columns: {
        exchange_id: u8 primary_key,
        bid: f64,
        ask: f64,
        seq: u64
    },
    queries: {
        update: {
            TopPrice(bid, ask) by exchange_id,
        },
        delete: {
            Stale() by exchange_id,
        }
    }
);

#[test]
fn a_dense_partition_carries_its_update_queries() {
    let quotes = QuotedPartitions::new();
    let book = quotes.partition_or_create(0).expect("a fresh partition");
    book.insert(QuotedRow {
        exchange_id: 2,
        bid: 1.0,
        ask: 2.0,
        seq: 7,
    })
    .expect("fresh key");

    // The same method name and the same query struct the paged table generates,
    // so the call reads the same. What differs is that there is no `.await`.
    assert_eq!(
        book.update_top_price(TopPriceQuery { bid: 9.0, ask: 10.0 }, &2),
        Some(())
    );

    let row = book.select(&2).expect("present");
    assert_eq!((row.bid, row.ask), (9.0, 10.0));
    assert_eq!(row.seq, 7, "a column the query does not name is untouched");

    assert_eq!(
        book.update_top_price(TopPriceQuery { bid: 0.0, ask: 0.0 }, &3),
        None,
        "a key holding no row updates nothing"
    );
}

#[test]
fn a_dense_partition_carries_its_delete_queries() {
    let quotes = QuotedPartitions::new();
    let book = quotes.partition_or_create(0).expect("a fresh partition");
    book.insert(QuotedRow {
        exchange_id: 1,
        bid: 1.0,
        ask: 2.0,
        seq: 1,
    })
    .expect("fresh key");
    book.insert(QuotedRow {
        exchange_id: 2,
        bid: 3.0,
        ask: 4.0,
        seq: 2,
    })
    .expect("fresh key");

    assert_eq!(book.delete_stale(&1).expect("present").seq, 1);
    assert_eq!(book.select(&1), None);
    assert_eq!(book.select(&2).expect("present").seq, 2, "key 2 did not move");
    assert_eq!(book.delete_stale(&1), None, "deleting twice is not an error");
}
