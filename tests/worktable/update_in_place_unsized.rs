//! Regression coverage for the unsized (variable-length / `String`) in-place
//! update path, run across ALL THREE primary-index backends selectable through
//! the `using` keyword — WorkTablesIndex (the default), Congee, and Arctic.
//!
//! ## What this guards
//! A same-length update to an unsized field must mutate the row in place (its
//! physical `Link` is preserved) rather than delete-and-reinsert; a
//! length-changing update must still round-trip correctly (via reinsert); and a
//! reader racing same-length in-place updates must never observe a torn value.
//!
//! ## Why in-place is subtle (do not "just flip the initializer")
//! The naive fix — letting same-length updates skip reinsert — corrupts long
//! strings unless the out-of-line byte region is overwritten in place. The
//! generated field write is
//! `mem::swap(&mut archived.inner.<field>, &mut archived_row.<field>)`.
//! `ArchivedString` is a union: short strings (<= rkyv INLINE_CAPACITY) are
//! inline, so the swap is self-contained; LONG strings are out-of-line — a
//! relative pointer + length whose characters live in `archived_row`'s buffer.
//! Swapping only the pointer into the slot leaves it pointing at bytes never
//! written to the slot. See `codegen/.../queries/update.rs` (`gen_size_check`)
//! and `docs/pr46-review-findings.md` (F4).
//!
//! ## Why run it on every backend
//! The in-place fast path re-resolves the row's current `Link` and republishes
//! through the primary index; a backend whose link lookup or publication path
//! differs could keep the wrong slot or tear a read. Parametrizing over the
//! backends turns any such divergence into a test failure rather than a silent
//! corruption on one index type.

/// Generates the full unsized in-place update suite for one primary-index
/// backend. Each backend gets its own module so the generated
/// `UnsizedUpdateRow` / `PayloadQuery` / `UnsizedUpdateWorkTable` idents do not
/// collide. `persist: false` is explicit because Congee and Arctic require a
/// persistence choice (WorkTablesIndex accepts it too).
macro_rules! unsized_in_place_suite {
    ($module:ident, $using:ident) => {
        mod $module {
            use worktable::prelude::*;
            use worktable::worktable;

            worktable!(
                name: UnsizedUpdate,
                persist: false,
                columns: {
                    id: u64 primary_key using $using,
                    payload: String,
                    balance: f64,
                },
                queries: {
                    update: {
                        Payload(payload) by id,
                        Balance(balance) by id,
                    }
                }
            );

            /// Read the current physical link for a primary key. `get_value` is
            /// a `TableIndex` trait method implemented by every backend's pk_map,
            /// so this observability check is backend-agnostic.
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
                        balance: 1.0,
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

            /// A length change must round-trip correctly (via reinsert), in both
            /// directions (shrink then grow).
            #[tokio::test]
            async fn different_length_update_is_correct() {
                let table = UnsizedUpdateWorkTable::default();
                table
                    .insert(UnsizedUpdateRow {
                        id: 1,
                        payload: "abcdefghij".to_string(),
                        balance: 1.0,
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

            /// A reader resolving key K concurrently with same-size in-place
            /// updates must always observe a VALID payload — one of the values
            /// written, never a torn/partial read. The in-place path keeps the
            /// same slot and republishes via `PublishedRow::replace` (whole
            /// version swapped under a lock).
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn concurrent_reads_during_in_place_update_never_tear() {
                use std::sync::Arc;

                let table = Arc::new(UnsizedUpdateWorkTable::default());
                // 4-digit payloads: every same-length update takes the in-place path.
                table
                    .insert(UnsizedUpdateRow {
                        id: 1,
                        payload: "0000".to_string(),
                        balance: 1.0,
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
                                // Any observed value must be a valid 4-char
                                // ASCII-digit string — never garbage bytes.
                                assert_eq!(
                                    row.payload.len(),
                                    4,
                                    "torn read: payload {:?}",
                                    row.payload
                                );
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

            #[tokio::test]
            async fn fixed_width_update_on_unsized_row_stays_in_place() {
                let table = UnsizedUpdateWorkTable::default();
                table
                    .insert(UnsizedUpdateRow {
                        id: 1,
                        payload: "out-of-line payload that must remain unchanged".to_string(),
                        balance: 1.0,
                    })
                    .unwrap();
                let before = link_of(&table, 1);

                table
                    .update_balance(BalanceQuery { balance: 42.5 }, 1)
                    .await
                    .unwrap();

                let after = link_of(&table, 1);
                let row = table.select(1).unwrap();
                assert_eq!(before, after);
                assert_eq!(row.balance, 42.5);
                assert_eq!(
                    row.payload,
                    "out-of-line payload that must remain unchanged"
                );
            }
        }
    };
}

unsized_in_place_suite!(wti, worktables_index);
unsized_in_place_suite!(congee, congee);
unsized_in_place_suite!(arctic, arctic);
