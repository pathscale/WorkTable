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
