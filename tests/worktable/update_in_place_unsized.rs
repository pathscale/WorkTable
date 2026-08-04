//! Regression: updating a variable-length (unsized) row to a value that still
//! fits its current slot must be an IN-PLACE mutation, not a full
//! delete-and-reinsert. Reinsert allocates a fresh page slot (so the row's
//! `Link` changes), re-serializes the whole row, and rebuilds every secondary
//! index — making updates on `String`-bearing tables an order of magnitude
//! slower than in-place field writes, even when the payload length is unchanged.
//!
//! The observable is the row's physical `Link`: an in-place update keeps it,
//! a reinsert changes it. This test fails on the unconditional-reinsert path
//! and passes once the same-or-smaller-length update mutates in place.

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
async fn same_length_update_stays_in_place() {
    let table = UnsizedUpdateWorkTable::default();
    table
        .insert(UnsizedUpdateRow {
            id: 1,
            payload: "abcdefgh".to_string(), // 8 bytes
        })
        .unwrap();

    let before = link_of(&table, 1);

    // Update to a DIFFERENT value of the SAME length — must fit the slot.
    table
        .update_payload(
            PayloadQuery {
                payload: "12345678".to_string(), // 8 bytes
            },
            1,
        )
        .await
        .unwrap();

    let after = link_of(&table, 1);

    // Value updated...
    assert_eq!(table.select(1).unwrap().payload, "12345678");
    // ...and the row did NOT move: same-length update is in place, not a reinsert.
    assert_eq!(
        before, after,
        "same-length unsized update must not reinsert (link changed: {before:?} -> {after:?})"
    );
}

#[tokio::test]
async fn shorter_update_stays_in_place() {
    let table = UnsizedUpdateWorkTable::default();
    table
        .insert(UnsizedUpdateRow {
            id: 1,
            payload: "abcdefghij".to_string(), // 10 bytes
        })
        .unwrap();
    let before = link_of(&table, 1);

    table
        .update_payload(PayloadQuery { payload: "xy".to_string() }, 1) // 2 bytes, fits
        .await
        .unwrap();

    let after = link_of(&table, 1);
    assert_eq!(table.select(1).unwrap().payload, "xy");
    assert_eq!(
        before, after,
        "shorter unsized update must not reinsert (link changed: {before:?} -> {after:?})"
    );
}

#[tokio::test]
async fn repeated_same_length_updates_do_not_grow_storage() {
    // A tight loop of same-length updates on one key must not keep allocating
    // fresh slots. Correctness proxy: the value is always current and the row
    // never moves after the first settle.
    let table = UnsizedUpdateWorkTable::default();
    table
        .insert(UnsizedUpdateRow {
            id: 7,
            payload: "0000".to_string(),
        })
        .unwrap();

    let anchor = link_of(&table, 7);
    for i in 0..1000u32 {
        let p = format!("{:04}", i % 10000); // always 4 bytes
        table.update_payload(PayloadQuery { payload: p.clone() }, 7).await.unwrap();
        assert_eq!(table.select(7).unwrap().payload, p);
        assert_eq!(link_of(&table, 7), anchor, "row moved on iteration {i}");
    }
}
