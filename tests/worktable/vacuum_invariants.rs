//! The invariants vacuum must not break, checked under concurrent mutation.
//!
//! Four defects were fixed in reclamation this week and written out they are
//! one defect: an index entry resolving to storage that now holds a different
//! row. A page and its inner links handed to two allocators; `value_idx[792]`
//! returning a row holding `2703`; `upsert` returning `PrimaryUpdateTry`; a
//! page reclaimed with a live row still on it. Each was found by a test that
//! happened to notice the damage downstream, and each needed its own
//! investigation to get back to the cause.
//!
//! This asserts the property instead of the symptom, so the next one in that
//! class fails here rather than as a wrong row somewhere far away:
//!
//! 1. every primary entry resolves to a row carrying that key
//! 2. every secondary entry resolves to a row carrying that key
//! 3. the forward and reverse primary indexes agree
//! 4. no two primary keys name the same storage
//!
//! Run across all three primary-index backends, because they do not have the
//! same shape: arctic boxes every value, congee holds no non-unique index, and
//! WTI is the default. A reclamation bug that only reproduces on one of them
//! is the kind that ships.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use worktable::prelude::*;
use worktable::vacuum::{VacuumManager, VacuumManagerConfig};
use worktable::worktable;

/// Rows the storm starts from.
const SEED_ROWS: u64 = 1_500;
/// How long the mutation storm runs.
///
/// Duration rather than a count, because what matters is how many vacuum
/// passes the mutations overlap. A fixed count finished in 200ms against a 5ms
/// check interval, which is far too few passes to catch a reclamation race:
/// the first version of this test passed with the page-reclaim fix reverted.
const STORM: Duration = Duration::from_millis(1_500);

macro_rules! vacuum_invariant_suite {
    ($module:ident, $backend:ident) => {
        mod $module {
            use super::*;

            worktable!(
                name: VacInv,
                persist: false,
                columns: {
                    id: u64 primary_key autoincrement using $backend,
                    uniq: u64,
                    tag: u32,
                    payload: String
                },
                indexes: {
                    // The unique secondary follows the primary's backend, so
                    // each arm exercises one implementation end to end. Only
                    // parameterising the primary left every arm resolving its
                    // secondaries through the same map, which is the one that
                    // measured slowest and is not what a congee or arctic
                    // table would be doing.
                    uniq_idx: uniq unique using $backend,
                    // Non-unique stays general purpose: congee holds no
                    // non-unique index at all, so this cannot follow.
                    tag_idx: tag,
                }
            );

            fn row(id: u64, uniq: u64) -> VacInvRow {
                VacInvRow {
                    id,
                    uniq,
                    tag: (id % 8) as u32,
                    payload: format!("payload-{id}"),
                }
            }

            /// Everything the four bugs violated, in one place.
            ///
            /// Reads through the raw link deliberately: going through `select`
            /// would hide exactly the failure being looked for, because it
            /// revalidates and retries.
            fn assert_indexes_resolve_to_their_own_rows(table: &VacInvWorkTable, phase: &str) {
                // Enumerated through the reverse index, which is always the
                // general-purpose map whatever the primary backend is. Arctic
                // and congee expose no iterator, which is the three-backend
                // difference in miniature: a check written against one of them
                // does not compile against the others.
                let mut seen_links: HashMap<u64, u64> = HashMap::new();

                for (link, pk) in table.0.primary_index.reverse_pk_map.iter() {
                    let link: Link = link.into();
                    let key: u64 = pk.clone().into();
                    let row = table
                        .0
                        .data
                        .select_non_ghosted(link)
                        .unwrap_or_else(|e| panic!("{phase}: reverse entry {key} points at unreadable storage: {e:?}"));
                    assert_eq!(
                        row.id, key,
                        "{phase}: reverse entry {key} resolves to a row whose id is {}",
                        row.id
                    );

                    // Forward and reverse must agree, and must agree on the
                    // same storage. Vacuum plans from the reverse map, so a
                    // disagreement is a page drained against a stale picture.
                    let forward: Option<Link> = table
                        .0
                        .primary_index
                        .pk_map
                        .get_value(&pk)
                        .map(Into::into);
                    assert_eq!(
                        forward,
                        Some(link),
                        "{phase}: key {key} is at {link:?} in the reverse index and {forward:?} in the forward one"
                    );

                    let packed = (u32::from(link.page_id) as u64) << 32 | link.offset as u64;
                    if let Some(other) = seen_links.insert(packed, key) {
                        panic!("{phase}: keys {other} and {key} name the same storage");
                    }
                }

                // Iterated directly on every backend. Arctic and congee
                // exposed no inherent `iter` until this suite needed one, so
                // the first version of this check probed by key to stay
                // portable. The alias makes the backends interchangeable here,
                // which is the point: a check that only reads one of them is
                // a check that only guards one of them.
                for (uniq, link) in table.0.indexes.uniq_idx.iter() {
                    let link: Link = link.into();
                    let row = table
                        .0
                        .data
                        .select_non_ghosted(link)
                        .unwrap_or_else(|e| panic!("{phase}: uniq_idx[{uniq}] points at unreadable storage: {e:?}"));
                    assert_eq!(
                        row.uniq, uniq,
                        "{phase}: uniq_idx[{uniq}] resolves to a row holding {}",
                        row.uniq
                    );
                }
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn vacuum_never_leaves_an_index_pointing_at_another_row() {
                let manager = Arc::new(VacuumManager::with_config(VacuumManagerConfig {
                    // Aggressive on purpose: the defects only appear when
                    // reclamation overlaps mutation.
        // No interval any more; wake on any freed byte instead.
        wake_threshold_bytes: 1,
                    ..Default::default()
                }));
                let table = Arc::new(VacInvWorkTable::default());

                for id in 0..SEED_ROWS {
                    table.insert(row(id, 1_000_000 + id)).await.expect("seed");
                }
                assert_indexes_resolve_to_their_own_rows(&table, "seed");

                manager.register(table.vacuum());
                let vacuum_task = Arc::clone(&manager).run_vacuum_task();

                // Deletes free the space, inserts take it, vacuum reclaims
                // underneath. All three are required: with any one missing the
                // class does not reproduce.
                let deadline = tokio::time::Instant::now() + STORM;

                // Deletes free the space, inserts take it, vacuum reclaims
                // underneath. All three are required: with any one missing the
                // class does not reproduce.
                let deleter = {
                    let table = Arc::clone(&table);
                    tokio::spawn(async move {
                        let mut id = 0u64;
                        while tokio::time::Instant::now() < deadline {
                            let _ = table.delete(id % SEED_ROWS).await;
                            id += 2;
                            if id % 64 == 0 {
                                tokio::task::yield_now().await;
                            }
                        }
                    })
                };
                let inserter = {
                    let table = Arc::clone(&table);
                    tokio::spawn(async move {
                        let mut n = 0u64;
                        while tokio::time::Instant::now() < deadline {
                            let id = SEED_ROWS + n;
                            let _ = table.insert(row(id, 1_000_000 + id)).await;
                            n += 1;
                            if n % 64 == 0 {
                                tokio::task::yield_now().await;
                            }
                        }
                    })
                };
                let upserter = {
                    let table = Arc::clone(&table);
                    tokio::spawn(async move {
                        let mut n = 0u64;
                        while tokio::time::Instant::now() < deadline {
                            let id = (n * 7) % SEED_ROWS;
                            // A distinct unique value, so a rejected write is
                            // never what this measures.
                            let _ = table.upsert(row(id, 5_000_000 + n)).await;
                            n += 1;
                            if n % 64 == 0 {
                                tokio::task::yield_now().await;
                            }
                        }
                    })
                };

                deleter.await.expect("deleter");
                inserter.await.expect("inserter");
                upserter.await.expect("upserter");

                // Let vacuum run once more against the wreckage, then stop it
                // so the check reads a still table.
                tokio::time::sleep(Duration::from_millis(60)).await;
                vacuum_task.abort();
                tokio::time::sleep(Duration::from_millis(20)).await;

                assert_indexes_resolve_to_their_own_rows(&table, "after churn");
            }
        }
    };
}

vacuum_invariant_suite!(wti, worktables_index);
vacuum_invariant_suite!(arctic, arctic);
vacuum_invariant_suite!(congee, congee);
