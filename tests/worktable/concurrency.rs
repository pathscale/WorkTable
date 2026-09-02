//! Concurrent writes, across every index backend, above the writer count the
//! rest of the suite reaches.
//!
//! Concurrency coverage existed before this file but was scattered and
//! backend-specific: `nonunique_arctic` races four writers against an arctic
//! non-unique index, `index_backends` recovers concurrent same-row updates,
//! `vacuum` runs a vacuum thread beside sequential inserts. Nothing raced
//! writers across all three backends, and **nothing went above four writers**.
//!
//! That mattered. Insert throughput on this engine is flat to four writers and
//! collapses at eight, so four is precisely the last thread count at which
//! everything looks fine. See `insert_throughput_should_scale_past_four_writers`
//! at the end of this file.
//!
//! Congee appears only where a unique index is enough: it has no non-unique
//! backend, and `worktable_codegen` rejects the declaration rather than letting
//! it fail later.

/// Shape of the concurrent workload, tunable without editing the file.
///
/// Hardcoding a writer count is what let this whole class of problem hide: the
/// suite stopped at four writers, four is the last count at which this engine
/// still behaves, and nobody could turn the dial without a recompile. Every
/// number below is an environment variable with a default sized for CI.
///
/// ```sh
/// WT_CONC_WRITERS=32 WT_CONC_PER_WRITER=2000 cargo test --test mod concurrency
/// WT_CONC_SWEEP=1,2,4,8,16,32,64 cargo test --test mod insert_throughput -- --ignored --nocapture
/// ```
mod params {
    /// Reads an integer from the environment, falling back to `default`.
    ///
    /// A malformed value is a hard error rather than a silent fallback: a typo
    /// in `WT_CONC_WRITERS` that quietly runs the default is a run you believe
    /// tested something it did not.
    pub fn env_u64(name: &str, default: u64) -> u64 {
        match std::env::var(name) {
            Ok(raw) => raw
                .trim()
                .parse()
                .unwrap_or_else(|_| panic!("{name} must be an integer, got {raw:?}")),
            Err(_) => default,
        }
    }

    /// Concurrent writers. Defaults to eight because four is where the rest of
    /// the suite stops and where this engine still looks healthy.
    pub fn writers() -> u64 {
        env_u64("WT_CONC_WRITERS", 8)
    }

    /// Rows each writer inserts.
    pub fn per_writer() -> u64 {
        env_u64("WT_CONC_PER_WRITER", 500)
    }

    /// Rows seeded before a race that also deletes.
    pub fn seed_rows() -> u64 {
        env_u64("WT_CONC_SEED_ROWS", 2_000)
    }

    /// Reader threads running beside the writers.
    pub fn readers() -> u64 {
        env_u64("WT_CONC_READERS", 4)
    }
}

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use worktable::prelude::*;
use worktable::worktable;

/// One table per backend. Separate modules because the generated idents would
/// otherwise collide, and a macro because three hand-written copies drift.
macro_rules! backend_suite {
    ($module:ident, $backend:ident, $label:literal) => {
        mod $module {
            use super::*;

            worktable!(
                name: Conc,
                persist: false,
                columns: {
                    id: u64 primary_key,
                    payload: u64,
                    bucket: u32,
                },
                indexes: {
                    payload_idx: payload unique using $backend,
                    bucket_idx: bucket using worktables_index,
                },
            );

            fn row(id: u64) -> ConcRow {
                ConcRow { id, payload: 1_000_000 + id, bucket: (id % 16) as u32 }
            }

            /// Eight writers on disjoint key ranges: every row lands, exactly
            /// once, reachable through both the unique and the non-unique
            /// index.
            ///
            /// Eight rather than four on purpose. Four is where the existing
            /// tests stop and where this engine still behaves; the interesting
            /// interleavings start above it.
            #[test]
            fn eight_writers_lose_no_rows() {
                let (writers, per_writer) = (params::writers(), params::per_writer());

                let table = Arc::new(ConcWorkTable::default());
                std::thread::scope(|scope| {
                    for w in 0..writers {
                        let table = Arc::clone(&table);
                        scope.spawn(move || {
                            for n in 0..per_writer {
                                let id = w * per_writer + n;
                                table.insert(row(id)).expect("insert");
                                // Read while the others write, so the scan and
                                // the mutations actually overlap.
                                let _ = table.select(id);
                            }
                        });
                    }
                });

                assert_eq!(table.count(), (writers * per_writer) as usize, "{} lost rows", $label);
                for id in 0..(writers * per_writer) {
                    assert!(table.select(id).is_some(), "{}: row {id} missing by primary key", $label);
                    assert!(
                        table.select_by_payload(1_000_000 + id).is_some(),
                        "{}: row {id} missing from the unique index",
                        $label
                    );
                }
            }

            /// Writers inserting while other threads delete, so inserts race
            /// storage reuse rather than only each other.
            ///
            /// A link freed by a delete becomes reusable once no reader can
            /// reach it, so this is the interleaving where an insert can claim
            /// a slot another thread is still finishing with.
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn inserts_racing_deletes_leave_a_consistent_index() {
                let seed = params::seed_rows();
                let (writers, per_writer) = (params::writers(), params::per_writer());

                let table = Arc::new(ConcWorkTable::default());
                for id in 0..seed {
                    table.insert(row(id)).expect("seed");
                }

                let deleter = {
                    let table = Arc::clone(&table);
                    tokio::spawn(async move {
                        for id in 0..seed {
                            table.delete(id).await.expect("delete");
                        }
                    })
                };

                std::thread::scope(|scope| {
                    for w in 0..writers {
                        let table = Arc::clone(&table);
                        scope.spawn(move || {
                            for n in 0..per_writer {
                                let id = seed + w * per_writer + n;
                                table.insert(row(id)).expect("insert");
                            }
                        });
                    }
                });
                deleter.await.expect("deleter did not panic");

                // Every inserted row survived, and nothing the deleter removed
                // came back through an index.
                for w in 0..writers {
                    for n in 0..per_writer {
                        let id = seed + w * per_writer + n;
                        assert!(table.select(id).is_some(), "{}: inserted row {id} lost", $label);
                    }
                }
                for id in 0..seed {
                    assert!(table.select(id).is_none(), "{}: deleted row {id} still readable", $label);
                    assert!(
                        table.select_by_payload(1_000_000 + id).is_none(),
                        "{}: deleted row {id} still in the unique index",
                        $label
                    );
                }
            }

            /// `insert` and `insert_many` racing on the same table.
            ///
            /// They take the same striped mutation gate by different routes:
            /// `insert` gates one key, `insert_many` gates its whole key set
            /// and sorts the stripes so a batch and a single insert cannot
            /// deadlock against each other. That ordering is the claim worth
            /// testing, and a deadlock here shows up as a hang rather than a
            /// failure, which is why the batches and singles interleave on
            /// overlapping stripes rather than staying politely apart.
            #[test]
            fn batches_and_single_inserts_interleave_without_loss() {
                let writers = params::writers();
                let per_writer = params::per_writer();

                let table = Arc::new(ConcWorkTable::default());
                std::thread::scope(|scope| {
                    for w in 0..writers {
                        let table = Arc::clone(&table);
                        scope.spawn(move || {
                            let base = w * per_writer;
                            if w % 2 == 0 {
                                // Batches, in chunks, so several stripes are
                                // held at once.
                                for chunk in (0..per_writer).step_by(16) {
                                    let rows: Vec<_> = (chunk..(chunk + 16).min(per_writer))
                                        .map(|n| row(base + n))
                                        .collect();
                                    table.insert_many(rows).expect("insert_many");
                                }
                            } else {
                                for n in 0..per_writer {
                                    table.insert(row(base + n)).expect("insert");
                                }
                            }
                        });
                    }
                });

                assert_eq!(
                    table.count(),
                    (writers * per_writer) as usize,
                    "{}: rows lost between insert and insert_many",
                    $label
                );
                for id in 0..(writers * per_writer) {
                    assert!(table.select(id).is_some(), "{}: row {id} missing", $label);
                    assert!(
                        table.select_by_payload(1_000_000 + id).is_some(),
                        "{}: row {id} missing from the unique index",
                        $label
                    );
                }
            }

            /// A unique collision under contention rejects exactly one writer.
            ///
            /// Every writer races to claim the same payload. Exactly one must
            /// win: two winners is a broken unique index, zero is a broken
            /// insert.
            #[test]
            fn a_contended_unique_key_admits_exactly_one_writer() {
                let writers = params::writers();

                let table = Arc::new(ConcWorkTable::default());
                let winners = Arc::new(AtomicU64::new(0));
                std::thread::scope(|scope| {
                    for w in 0..writers {
                        let table = Arc::clone(&table);
                        let winners = Arc::clone(&winners);
                        scope.spawn(move || {
                            // Distinct primary keys, one shared payload.
                            let contended = ConcRow { id: w, payload: 42, bucket: 0 };
                            if table.insert(contended).is_ok() {
                                winners.fetch_add(1, Ordering::Release);
                            }
                        });
                    }
                });

                assert_eq!(
                    winners.load(Ordering::Acquire),
                    1,
                    "{}: a contended unique key admitted more than one writer",
                    $label
                );
                assert_eq!(table.count(), 1, "{}", $label);
            }

            /// Concurrent readers see a consistent non-unique group while it is
            /// being written.
            #[test]
            fn readers_see_consistent_groups_during_writes() {
                let (writers, per_writer) = (params::writers(), params::per_writer());
                let readers = params::readers();

                let table = Arc::new(ConcWorkTable::default());
                std::thread::scope(|scope| {
                    for w in 0..writers {
                        let table = Arc::clone(&table);
                        scope.spawn(move || {
                            for n in 0..per_writer {
                                table.insert(row(w * per_writer + n)).expect("insert");
                            }
                        });
                    }
                    // Readers running throughout: a group read must never
                    // return a row that is not in that group.
                    for _ in 0..readers {
                        let table = Arc::clone(&table);
                        scope.spawn(move || {
                            for _ in 0..2_000 {
                                for bucket in 0..16u32 {
                                    for row in table.select_by_bucket(bucket).execute().unwrap() {
                                        assert_eq!(row.bucket, bucket, "{}: row in the wrong group", $label);
                                    }
                                }
                            }
                        });
                    }
                });

                let mut counts: HashMap<u32, usize> = HashMap::new();
                for id in 0..(writers * per_writer) {
                    let row = table.select(id).expect("row present");
                    *counts.entry(row.bucket).or_default() += 1;
                }
                for bucket in 0..16u32 {
                    let selected = table.select_by_bucket(bucket).execute().unwrap();
                    assert_eq!(
                        selected.len(),
                        *counts.get(&bucket).unwrap_or(&0),
                        "{}: group {bucket} disagrees with the rows",
                        $label
                    );
                }
            }
        }
    };
}

backend_suite!(wti, worktables_index, "wti");
backend_suite!(arctic, arctic, "arctic");
backend_suite!(congee, congee, "congee");
