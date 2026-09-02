use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: Batch,
    columns: {
        id: u64 primary_key autoincrement,
        unique_value: u64,
        plain: u32,
    },
    indexes: {
        unique_value_idx: unique_value unique,
        plain_idx: plain,
    },
);

fn row(id: u64, unique_value: u64) -> BatchRow {
    BatchRow {
        id,
        unique_value,
        plain: 0,
    }
}

#[test]
fn insert_many_returns_pks_and_reads_see_every_row() {
    let table = BatchWorkTable::default();
    let rows: Vec<_> = (0..10).map(|i| row(i, 100 + i)).collect();

    let pks = table.insert_many(rows).await.unwrap();

    assert_eq!(pks.len(), 10);
    assert_eq!(table.count(), 10);
    for i in 0..10u64 {
        let selected = table.select(i).expect("read-your-writes: row visible after Ok");
        assert_eq!(selected.unique_value, 100 + i);
        let by_unique = table
            .select_by_unique_value(100 + i)
            .expect("unique index resolves after Ok");
        assert_eq!(by_unique.id, i);
    }
}

#[test]
fn insert_many_of_no_rows_is_ok() {
    let table = BatchWorkTable::default();
    assert!(table.insert_many(vec![]).await.unwrap().is_empty());
    assert_eq!(table.count(), 0);
}

fn assert_batch_fully_rejected(table: &BatchWorkTable, batch: &[BatchRow], preexisting: usize) {
    assert_eq!(table.count(), preexisting, "no batch row may survive rejection");
    for row in batch {
        assert!(
            table.select(row.id).is_none(),
            "rejected batch row {} is visible",
            row.id
        );
    }
}

fn collision_case(collide_at: usize) {
    let table = BatchWorkTable::default();
    table.insert(row(1000, 999)).await.unwrap();

    let batch: Vec<_> = (0..5)
        .map(|i| {
            let unique = if i == collide_at as u64 { 999 } else { 200 + i };
            row(i, unique)
        })
        .collect();

    let error = table.insert_many(batch.clone()).await.unwrap_err();
    match error {
        BatchInsertError::Row { row_index, source } => {
            assert_eq!(row_index, collide_at, "the offending row must be named");
            match source {
                WorkTableError::AlreadyExists(index_name) => {
                    assert_eq!(
                        index_name, "UniqueValueIdx",
                        "the colliding index must be named, got `{index_name}`"
                    );
                }
                other => panic!("expected AlreadyExists, got {other:?}"),
            }
        }
        other => panic!("expected a row-level rejection, got {other:?}"),
    }

    assert_batch_fully_rejected(&table, &batch, 1);
    for i in 0..5u64 {
        if i != collide_at as u64 {
            assert!(
                table.select_by_unique_value(200 + i).is_none(),
                "unique entry of rejected row {i} survived"
            );
        }
    }
    // The pre-existing row is untouched.
    assert_eq!(table.select_by_unique_value(999).unwrap().id, 1000);

    // The table stays fully usable after the rollback.
    table.insert_many((0..5).map(|i| row(i, 200 + i)).collect()).await.unwrap();
    assert_eq!(table.count(), 6);
}

#[test]
fn unique_collision_at_first_row_rejects_the_batch() {
    collision_case(0);
}

#[test]
fn unique_collision_at_middle_row_rejects_the_batch() {
    collision_case(2);
}

#[test]
fn unique_collision_at_last_row_rejects_the_batch() {
    collision_case(4);
}

#[test]
fn unique_collision_between_two_batch_rows_rejects_the_batch() {
    let table = BatchWorkTable::default();

    let mut batch: Vec<_> = (0..5).map(|i| row(i, 300 + i)).collect();
    // Rows 1 and 4 collide with each other inside the batch.
    batch[4].unique_value = batch[1].unique_value;

    let error = table.insert_many(batch.clone()).await.unwrap_err();
    match error {
        BatchInsertError::Row { row_index, source } => {
            assert_eq!(row_index, 4, "the later of the two colliding rows is the offender");
            assert!(matches!(source, WorkTableError::AlreadyExists(_)));
        }
        other => panic!("expected a row-level rejection, got {other:?}"),
    }
    assert_eq!(table.count(), 0);
    for row in &batch {
        assert!(table.select(row.id).is_none());
        assert!(table.select_by_unique_value(row.unique_value).is_none());
    }
}

#[test]
fn duplicate_primary_key_inside_the_batch_rejects_the_batch() {
    let table = BatchWorkTable::default();

    let mut batch: Vec<_> = (0..4).map(|i| row(i, 400 + i)).collect();
    batch[3].id = batch[1].id;

    let error = table.insert_many(batch).await.unwrap_err();
    match error {
        BatchInsertError::Row { row_index, source } => {
            assert_eq!(row_index, 3);
            assert!(matches!(source, WorkTableError::PrimaryAlreadyExists));
        }
        other => panic!("expected a row-level rejection, got {other:?}"),
    }
    assert_eq!(table.count(), 0);
}

#[test]
fn duplicate_primary_key_with_an_existing_row_rejects_the_batch() {
    let table = BatchWorkTable::default();
    table.insert(row(7, 500)).await.unwrap();

    let batch = vec![row(20, 501), row(21, 502), row(7, 503)];
    let error = table.insert_many(batch).await.unwrap_err();
    match error {
        BatchInsertError::Row { row_index, source } => {
            assert_eq!(row_index, 2);
            assert!(matches!(source, WorkTableError::PrimaryAlreadyExists));
        }
        other => panic!("expected a row-level rejection, got {other:?}"),
    }
    assert_eq!(table.count(), 1);
    assert_eq!(table.select(7).unwrap().unique_value, 500);
}

#[test]
fn reserve_pks_hands_out_contiguous_keys_that_interleave_with_get_next_pk() {
    let table = BatchWorkTable::default();

    let first: u64 = table.get_next_pk().into();
    let range = table.reserve_pks(5);
    assert_eq!(range.end - range.start, 5);
    assert_eq!(range.start, first + 1, "reservation starts after handed-out keys");
    let after: u64 = table.get_next_pk().into();
    assert_eq!(after, range.end, "get_next_pk continues after the reservation");

    let rows: Vec<_> = range.clone().map(|id| row(id, 600 + id)).collect();
    let pks = table.insert_many(rows).await.unwrap();
    assert_eq!(pks.len(), 5);
    for id in range {
        assert_eq!(table.select(id).unwrap().unique_value, 600 + id);
    }
}

/// A rejected batch must never be observable: a reader polling the batch's
/// primary keys and unique values while rejected batches are retried in a
/// loop may not see a single row.
#[test]
fn concurrent_reader_never_observes_a_rejected_batch() {
    const BATCH: u64 = 16;
    const ITERATIONS: usize = 200;

    let table = Arc::new(BatchWorkTable::default());
    // The poison row that every batch's last row collides with.
    table.insert(row(5000, 999)).await.unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let reader_table = table.clone();
    let reader_stop = stop.clone();
    let reader = std::thread::spawn(move || {
        let mut observed = Vec::new();
        while !reader_stop.load(Ordering::Acquire) {
            for id in 0..BATCH {
                if let Some(row) = reader_table.select(id) {
                    observed.push(("pk", id, row.unique_value));
                }
                if let Some(row) = reader_table.select_by_unique_value(700 + id) {
                    observed.push(("unique", id, row.id));
                }
            }
        }
        observed
    });

    for _ in 0..ITERATIONS {
        let mut batch: Vec<_> = (0..BATCH).map(|id| row(id, 700 + id)).collect();
        batch.last_mut().unwrap().unique_value = 999;
        let error = table.insert_many(batch).await.unwrap_err();
        assert!(matches!(error, BatchInsertError::Row { row_index, .. } if row_index == BATCH as usize - 1));
    }

    stop.store(true, Ordering::Release);
    let observed = reader.join().unwrap();
    assert!(
        observed.is_empty(),
        "a concurrent reader observed rows of rejected batches: {observed:?}"
    );
    assert_eq!(table.count(), 1);
}

/// Successful batches become visible in row order, so visibility is
/// prefix-monotone: whenever a reader can see row `k` of a batch, every
/// earlier row of that batch is visible too.
#[test]
fn concurrent_reader_sees_successful_batches_in_prefix_order() {
    const BATCH: u64 = 32;
    const BATCHES: u64 = 100;

    let table = Arc::new(BatchWorkTable::default());
    let published_start = Arc::new(AtomicU64::new(u64::MAX));
    let stop = Arc::new(AtomicBool::new(false));

    let reader_table = table.clone();
    let reader_start = published_start.clone();
    let reader_stop = stop.clone();
    let reader = std::thread::spawn(move || {
        while !reader_stop.load(Ordering::Acquire) {
            let start = reader_start.load(Ordering::Acquire);
            if start == u64::MAX {
                continue;
            }
            let mut highest_visible = None;
            for id in (start..start + BATCH).rev() {
                if reader_table.select(id).is_some() {
                    highest_visible = Some(id);
                    break;
                }
            }
            let Some(highest_visible) = highest_visible else {
                continue;
            };
            for id in start..highest_visible {
                assert!(
                    reader_table.select(id).is_some(),
                    "row {id} invisible while later row {highest_visible} of the same batch is visible"
                );
            }
        }
    });

    for batch_index in 0..BATCHES {
        let start = batch_index * BATCH;
        published_start.store(start, Ordering::Release);
        let rows: Vec<_> = (start..start + BATCH).map(|id| row(id, 1_000_000 + id)).collect();
        table.insert_many(rows).await.unwrap();
    }

    stop.store(true, Ordering::Release);
    reader.join().unwrap();
    assert_eq!(table.count(), (BATCH * BATCHES) as usize);
}
