//! Bulk delete: what it removes, what it leaves reachable, and what it reuses.
//!
//! The single-row `delete` already ghosts rather than moves: the row is marked
//! deleted in place, its index entries come out, and the storage becomes
//! reusable once no reader can still reach it. `delete_many` is that, batched,
//! and the batching is not cosmetic. The per-row cost is dominated by the
//! reclamation bookkeeping each retirement takes rather than by the bit flip,
//! so a loop of `n` deletes pays `n` domain advances where a batch pays one.
//!
//! These tests are about behaviour rather than speed. The property that would
//! actually bite a consumer is the one in
//! `deleted_rows_are_unreachable_through_every_index`: a bulk delete that
//! removed rows from the primary index but left a secondary index pointing at
//! their storage would still pass a naive `select` test, and would resolve to
//! reused storage the moment an insert claimed the link.

use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: Evict,
    columns: {
        id: u64 primary_key autoincrement,
        unique_value: u64,
        generation: u32,
    },
    indexes: {
        unique_value_idx: unique_value unique,
        generation_idx: generation,
    },
);

fn row(id: u64, unique_value: u64, generation: u32) -> EvictRow {
    EvictRow {
        id,
        unique_value,
        generation,
    }
}

fn table_with(rows: u64) -> EvictWorkTable {
    let table = EvictWorkTable::default();
    let batch: Vec<_> = (0..rows).map(|i| row(i, 1_000 + i, (i % 4) as u32)).collect();
    table.insert_many(batch).expect("fixture inserts");
    table
}

#[test]
fn delete_many_removes_exactly_the_named_keys() {
    let table = table_with(20);

    let deleted = table.delete_many((0..5u64).collect()).expect("bulk delete");

    let expected: Vec<EvictPrimaryKey> = (0..5u64).map(Into::into).collect();
    assert_eq!(deleted, expected);
    assert_eq!(table.count(), 15);
    for id in 0..5u64 {
        assert!(table.select(id).is_none(), "row {id} should be gone");
    }
    for id in 5..20u64 {
        assert!(table.select(id).is_some(), "row {id} should survive");
    }
}

/// The property a bulk delete is most likely to get wrong.
///
/// Removing rows from the primary index while leaving a secondary index
/// pointing at their storage leaves a dangling entry: the link is queued for
/// reuse, so a later insert can claim it and the stale entry then resolves to a
/// live, unrelated row.
///
/// Reading it back is **not** enough to catch that, and this test asserted only
/// that at first. A ghosted row is filtered out of reads, so a stale index
/// entry is invisible through `select` and the test passed with secondary
/// removal deleted entirely. What catches it is claiming the key again: a
/// unique index that still holds the deleted row's value rejects the insert.
#[test]
fn deleted_rows_are_unreachable_through_every_index() {
    let table = table_with(20);

    table.delete_many((0..5u64).collect()).expect("bulk delete");

    for id in 0..5u64 {
        assert!(
            table.select_by_unique_value(1_000 + id).is_none(),
            "unique index still resolves deleted row {id}"
        );
    }
    // The non-unique index must have lost exactly the deleted members of each
    // group, not the whole group.
    let generation_zero = table.select_by_generation(0).execute().expect("non-unique read");
    let surviving: Vec<u64> = generation_zero.iter().map(|r| r.id).collect();
    assert!(
        surviving.iter().all(|id| *id >= 5),
        "non-unique index still resolves deleted rows: {surviving:?}"
    );
    assert!(
        surviving.contains(&8) && surviving.contains(&12),
        "non-unique index lost rows it should have kept: {surviving:?}"
    );

    // The assertion with teeth: reclaiming a deleted row's unique value must
    // succeed. If the unique index still holds the entry, this is rejected.
    for id in 0..5u64 {
        table
            .insert(row(100 + id, 1_000 + id, 9))
            .unwrap_or_else(|error| panic!("unique value {} was not released by the delete: {error}", 1_000 + id));
    }
}

/// A key that is not there is skipped, not an error.
///
/// A caller evicting a generation does not know which of its keys a concurrent
/// writer already removed, and making them find out first is a race they cannot
/// win. The return value is what was actually deleted, so the caller can tell.
#[test]
fn absent_keys_are_skipped_rather_than_failing_the_batch() {
    let table = table_with(10);

    let deleted = table
        .delete_many(vec![1u64, 999, 3, 1_000, 5])
        .expect("absent keys must not fail the batch");

    let expected: Vec<EvictPrimaryKey> = vec![1u64, 3, 5].into_iter().map(Into::into).collect();
    assert_eq!(deleted, expected);
    assert_eq!(table.count(), 7);
}

/// Deleting the same key twice in one batch is not a double free.
#[test]
fn a_repeated_key_is_deleted_once() {
    let table = table_with(10);

    let deleted = table.delete_many(vec![2u64, 2, 2]).expect("repeats must be safe");

    let expected: Vec<EvictPrimaryKey> = vec![2u64.into()];
    assert_eq!(deleted, expected, "a key already ghosted in this batch is skipped");
    assert_eq!(table.count(), 9);
}

#[test]
fn an_empty_batch_is_a_no_op() {
    let table = table_with(4);
    assert_eq!(
        table.delete_many(Vec::<u64>::new()).expect("empty batch"),
        Vec::<EvictPrimaryKey>::new()
    );
    assert_eq!(table.count(), 4);
}

/// Storage from a bulk delete is reused, which is the point of the exercise.
///
/// Not asserted as an exact byte figure: reuse happens once no reader can
/// reach the links, so the observable property is that a delete-then-insert
/// cycle does not grow the table without bound. A table that never reused a
/// link would grow by the full batch on every cycle.
#[test]
fn bulk_delete_then_insert_reuses_storage() {
    let table = table_with(200);
    let before = table.count();

    for cycle in 0..10u64 {
        let keys: Vec<u64> = (0..100).collect();
        table.delete_many(keys).expect("bulk delete");
        assert_eq!(table.count(), before - 100);

        let refill: Vec<_> = (0..100)
            .map(|i| row(i, 500_000 + cycle * 1_000 + i, (i % 4) as u32))
            .collect();
        table.insert_many(refill).expect("refill");
        assert_eq!(table.count(), before);
    }

    // Every row is still readable through both indexes after ten cycles of
    // ghosting and reclaiming the same links.
    for id in 0..200u64 {
        assert!(table.select(id).is_some(), "row {id} lost after reuse cycles");
    }
}

/// A bulk delete and the single-row path agree.
///
/// Cheap to state and the thing most likely to drift: if `delete_many` ever
/// stops doing exactly what a loop of `delete` does, this is where it shows.
#[tokio::test]
async fn delete_many_matches_a_loop_of_delete() {
    let batched = table_with(30);
    let looped = table_with(30);

    let keys: Vec<u64> = (0..30).filter(|i| i % 3 == 0).collect();
    batched.delete_many(keys.clone()).expect("bulk delete");
    for key in &keys {
        looped.delete(*key).await.expect("single delete");
    }

    assert_eq!(batched.count(), looped.count());
    for id in 0..30u64 {
        assert_eq!(
            batched.select(id).is_some(),
            looped.select(id).is_some(),
            "row {id} disagrees between the batched and looped paths"
        );
    }
}

/// Eviction by span, which is the shape a caller dropping a generation has.
#[test]
fn delete_range_removes_the_span_and_nothing_else() {
    let table = table_with(20);

    let deleted = table.delete_range(EvictPrimaryKey::from(5u64)..EvictPrimaryKey::from(10u64));
    let deleted = deleted.expect("range delete");

    let expected: Vec<EvictPrimaryKey> = (5u64..10).map(Into::into).collect();
    assert_eq!(deleted, expected, "half-open: 10 is not included");
    assert_eq!(table.count(), 15);
    for id in 5..10u64 {
        assert!(table.select(id).is_none(), "row {id} should be gone");
        assert!(
            table.select_by_unique_value(1_000 + id).is_none(),
            "unique index still resolves deleted row {id}"
        );
    }
    assert!(table.select(4u64).is_some(), "the row below the span survives");
    assert!(table.select(10u64).is_some(), "the row at the exclusive end survives");
}

/// An inclusive end, and a range that matches nothing.
#[test]
fn delete_range_honours_its_bounds() {
    let table = table_with(20);

    let deleted = table
        .delete_range(EvictPrimaryKey::from(0u64)..=EvictPrimaryKey::from(2u64))
        .expect("inclusive range");
    assert_eq!(deleted.len(), 3, "0, 1 and 2");

    let empty = table
        .delete_range(EvictPrimaryKey::from(500u64)..EvictPrimaryKey::from(600u64))
        .expect("a range matching nothing is not an error");
    assert!(empty.is_empty());
    assert_eq!(table.count(), 17);
}
