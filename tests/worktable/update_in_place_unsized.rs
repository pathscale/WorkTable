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
//! Setting the initializer to `false` lets same-length updates skip reinsert —
//! but the in-place write then CORRUPTS long strings. The generated field write
//! is `mem::swap(&mut archived.inner.<field>, &mut archived_row.<field>)`.
//! `ArchivedString` is a union: short strings (<= rkyv INLINE_CAPACITY) are
//! inline, so the swap is self-contained; LONG strings are out-of-line — a
//! relative pointer + length whose characters live in `archived_row`'s buffer.
//! Swapping only the pointer into the slot leaves it pointing at bytes that were
//! never written to the slot → reads come back as raw archived bytes (see
//! `worktable::unsized_::update_parallel_more_strings`, `update_many_times`,
//! `in_place::test_update_in_place_and_update_unsized_multithread`).
//!
//! A real fix must overwrite the existing out-of-line byte region in place
//! (e.g. `ArchivedStringRepr::as_bytes_seal`) when the new value fits, reinserting
//! only when it doesn't — preserving field-level semantics. Unsafe archived-memory
//! work; a subtle error is silent corruption. See docs/pr46-review-findings.md (F4).
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
        .update_payload(
            PayloadQuery {
                payload: "xy".to_string(),
            },
            1,
        )
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

/// F5 (review): a reader resolving key K concurrently with same-size in-place
/// updates must always observe a VALID payload — one of the values written,
/// never a torn/partial read. The in-place path keeps the same slot and
/// republishes via `PublishedRow::replace` (whole version swapped under a lock),
/// so every `select` should return a well-formed, expected-length string.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reads_during_in_place_update_never_tear() {
    use std::sync::Arc;

    let table = Arc::new(UnsizedUpdateWorkTable::default());
    // 4-digit payloads: every same-length update takes the in-place path.
    table
        .insert(UnsizedUpdateRow {
            id: 1,
            payload: "0000".to_string(),
        })
        .unwrap();

    let writer = {
        let table = table.clone();
        tokio::spawn(async move {
            for i in 0..20_000u64 {
                table
                    .update_payload(
                        PayloadQuery {
                            payload: format!("{:04}", i % 10000),
                        },
                        1,
                    )
                    .await
                    .unwrap();
            }
        })
    };

    let mut readers = Vec::new();
    for _ in 0..3 {
        let table = table.clone();
        readers.push(tokio::spawn(async move {
            for _ in 0..50_000u64 {
                if let Some(row) = table.select(1) {
                    // Any observed value must be a valid 4-char ASCII-digit
                    // string — never garbage bytes from a torn read.
                    assert_eq!(row.payload.len(), 4, "torn read: payload {:?}", row.payload);
                    assert!(
                        row.payload.bytes().all(|b| b.is_ascii_digit()),
                        "torn read: non-digit payload {:?}",
                        row.payload
                    );
                }
            }
        }));
    }

    writer.await.unwrap();
    for r in readers {
        r.await.unwrap();
    }

    // Final value is the writer's last write.
    assert_eq!(
        table.select(1).unwrap().payload,
        format!("{:04}", (20_000u64 - 1) % 10000)
    );
}
