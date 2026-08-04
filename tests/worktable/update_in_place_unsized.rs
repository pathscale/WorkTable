//! Regression (KNOWN BUG, currently #[ignore]): updating a variable-length
//! (unsized / `String`) row to a value of the SAME serialized length still does
//! a full delete-and-reinsert instead of an in-place mutation.
//!
//! ## Why this matters
//! Reinsert allocates a fresh page slot (the row's `Link` changes), re-serializes
//! the whole row, and rebuilds every secondary index. On a `String`-bearing
//! table this makes UPDATE ~9x slower than an in-place field write — WorkTable
//! leads insert and point_read in the KV benchmark but is dead last on overwrite
//! purely because of this path.
//!
//! ## Root cause
//! `codegen/src/generators/in_memory/queries/update.rs`, custom-update size
//! check (`gen_size_check`): `let mut need_to_reinsert = true;` then
//! `need_to_reinsert |= <size changed>`. Initialized to `true`, the `|=` can
//! never clear it, so EVERY update reinserts regardless of whether any unsized
//! field changed size.
//!
//! ## Why the obvious fix is not enough (do not just flip the initializer)
//! Setting the initializer to `false` correctly lets same-length updates skip
//! reinsert — but the in-place archived write in this custom-update path then
//! CORRUPTS variable-length rows in existing tests
//! (`worktable::unsized_::update_parallel_more_strings`, `update_many_times`,
//! `in_place::test_update_in_place_and_update_unsized_multithread`): reads come
//! back as raw archived bytes. So a real fix must make the in-place write of an
//! (even equal-length) archived `String` field safe in this path, not merely
//! change when the fast path is taken. That is a storage/codegen change beyond a
//! one-liner; tracked here so the fix has a proof.
//!
//! The observable is the row's physical `Link`: an in-place update keeps it, a
//! reinsert changes it. Remove `#[ignore]` when the in-place path is fixed.

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: UnsizedUpdate,
    columns: {
        id: u64 primary_key,
        payload: String,
    },
    queries: {
        update: {
            Payload(payload) by id,
        }
    }
);

/// Read the current physical link for a primary key.
fn link_of(table: &UnsizedUpdateWorkTable, pk: u64) -> Link {
    table
        .0
        .primary_index
        .pk_map
        .get_value(&UnsizedUpdatePrimaryKey::from(pk))
        .map(Into::into)
        .expect("row must exist")
}

#[tokio::test]
#[ignore = "known bug: same-length unsized update reinserts; in-place write of a String field corrupts the row — needs a storage-path fix"]
async fn same_length_update_stays_in_place() {
    let table = UnsizedUpdateWorkTable::default();
    table
        .insert(UnsizedUpdateRow {
            id: 1,
            payload: "abcdefgh".to_string(), // 8 bytes
        })
        .unwrap();

    let before = link_of(&table, 1);

    table
        .update_payload(
            PayloadQuery {
                payload: "12345678".to_string(), // 8 bytes — same length
            },
            1,
        )
        .await
        .unwrap();

    let after = link_of(&table, 1);

    assert_eq!(table.select(1).unwrap().payload, "12345678");
    assert_eq!(
        before, after,
        "same-length unsized update must not reinsert (link changed: {before:?} -> {after:?})"
    );
}

/// This one already holds on master and must keep holding through any fix:
/// a length change round-trips correctly (via reinsert).
#[tokio::test]
async fn different_length_update_is_correct() {
    let table = UnsizedUpdateWorkTable::default();
    table
        .insert(UnsizedUpdateRow {
            id: 1,
            payload: "abcdefghij".to_string(),
        })
        .unwrap();

    table
        .update_payload(PayloadQuery { payload: "xy".to_string() }, 1)
        .await
        .unwrap();
    assert_eq!(table.select(1).unwrap().payload, "xy");

    table
        .update_payload(
            PayloadQuery {
                payload: "much longer payload".to_string(),
            },
            1,
        )
        .await
        .unwrap();
    assert_eq!(table.select(1).unwrap().payload, "much longer payload");
}
