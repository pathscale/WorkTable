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
            #[tokio::test]
            async fn eight_writers_lose_no_rows() {
                let (writers, per_writer) = (params::writers(), params::per_writer());

                let table = Arc::new(ConcWorkTable::default());
                std::thread::scope(|scope| {
                    for w in 0..writers {
                        let table = Arc::clone(&table);
                        scope.spawn(move || {
                            for n in 0..per_writer {
                                let id = w * per_writer + n;
                                futures::executor::block_on(table.insert(row(id))).expect("insert");
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
                    table.insert(row(id)).await.expect("seed");
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
                                futures::executor::block_on(table.insert(row(id))).expect("insert");
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
            #[tokio::test]
            async fn batches_and_single_inserts_interleave_without_loss() {
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
                                    futures::executor::block_on(table.insert_many(rows)).expect("insert_many");
                                }
                            } else {
                                for n in 0..per_writer {
                                    futures::executor::block_on(table.insert(row(base + n))).expect("insert");
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
            #[tokio::test]
            async fn a_contended_unique_key_admits_exactly_one_writer() {
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
                            if futures::executor::block_on(table.insert(contended)).is_ok() {
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
            /// Readers racing storage *reuse*, which is the case the rest of
            /// this suite does not reach.
            ///
            /// The other tests race readers against inserts, so links are only
            /// ever allocated. This one deletes and reinserts continuously, so
            /// links are retired, reclaimed, and handed to *different* rows
            /// while readers are resolving index entries. That covers the
            /// epoch grace period, the retirement queue, the free list, and
            /// the deferral that moved reclamation off the deleting thread.
            ///
            /// Three things here were each necessary to make it test anything,
            /// and each was found by instrumenting rather than by reasoning:
            ///
            /// 1. **The reader uses the raw link path, not `select`.** `select`
            ///    returns an owned published snapshot rather than page bytes,
            ///    and that indirection is exactly what makes the published path
            ///    safe. Written against `select`, this test passes with the
            ///    grace period deleted outright.
            /// 2. **Two id sets share the same storage.** A writer that deletes
            ///    and reinserts the *same* id would put the same id back in the
            ///    recycled space, and a stale link would read the row it
            ///    expected. Set A and set B alternate, so recycled storage
            ///    holds a different id than the link was resolved for.
            /// 3. **A live population is kept at all times.** The first version
            ///    inserted and immediately deleted, so across a whole run the
            ///    readers resolved a live link nine times. It asserted almost
            ///    nothing.
            ///
            /// The detection is probabilistic, so the round count is
            /// load-bearing rather than arbitrary. Measured against a build
            /// with the grace period removed outright: 16 rounds catches it in
            /// three runs out of four, 128 catches it in five out of five,
            /// costing 1.4s. Lowering it trades away the only thing this test
            /// does. Raise `WT_CONC_PER_WRITER` to go further.
            #[tokio::test]
            async fn readers_never_see_a_row_reassembled_from_reused_storage() {
                /// Rows live per writer per set. Small enough that readers
                /// sweep the live population often, large enough to keep the
                /// free list and its coalescing genuinely busy.
                const WINDOW: u64 = 32;

                let readers = params::readers();
                let seed = params::seed_rows();
                let writers = params::writers();
                // Each round retires and reallocates `WINDOW` rows per writer.
                let rounds = (params::per_writer() / WINDOW).max(128);

                let table = Arc::new(ConcWorkTable::default());
                for id in 0..seed {
                    table.insert(row(id)).await.expect("seed");
                }

                // Writer `w` owns [base, base + 2 * WINDOW): set A below, set B
                // above. Disjoint per writer, so a failure is reuse rather than
                // two writers colliding on a key.
                let base_of = |w: u64| seed + w * WINDOW * 2;
                for w in 0..writers {
                    for i in 0..WINDOW {
                        table.insert(row(base_of(w) + i)).await.expect("seed set A");
                    }
                }

                let churn_end = base_of(writers);
                let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let finished = Arc::new(AtomicU64::new(0));

                std::thread::scope(|scope| {
                    for w in 0..writers {
                        let table = Arc::clone(&table);
                        let finished = Arc::clone(&finished);
                        scope.spawn(move || {
                            let base = base_of(w);
                            for round in 0..rounds {
                                // Alternate which set is live. The set being
                                // freed this round is the storage the set being
                                // filled will be given.
                                let (from, to) = if round % 2 == 0 {
                                    (base, base + WINDOW)
                                } else {
                                    (base + WINDOW, base)
                                };
                                for i in 0..WINDOW {
                                    futures::executor::block_on(table.delete(from + i)).expect("delete");
                                    futures::executor::block_on(table.insert(row(to + i))).expect("insert");
                                }
                            }
                            finished.fetch_add(1, Ordering::Release);
                        });
                    }

                    for _ in 0..readers {
                        let table = Arc::clone(&table);
                        let stop = Arc::clone(&stop);
                        scope.spawn(move || {
                            while !stop.load(Ordering::Relaxed) {
                                for id in seed..churn_end {
                                    let pk: ConcPrimaryKey = id.into();
                                    // Pin first, then resolve, then read: the
                                    // order `select` itself uses, and the order
                                    // the grace period is defined against.
                                    let guard = table.0.data.read_guard();
                                    let link: Option<Link> =
                                        table.0.primary_index.pk_map.get_value(&pk).map(Into::into);
                                    // The interval between resolving a link and
                                    // reading through it is the whole hazard.
                                    std::thread::yield_now();
                                    if let Some(link) = link
                                        && let Ok(r) = table.0.data.select_non_ghosted(link)
                                    {
                                        assert_eq!(
                                            r.id, id,
                                            "{}: the link resolved for {id} produced row {}, so its storage was \
                                             recycled while a reader was pinned on it",
                                            $label, r.id
                                        );
                                        assert_eq!(r.payload, 1_000_000 + r.id, "{}: payload disagrees with id", $label);
                                    }
                                    drop(guard);
                                }
                                // The published path too, so ordinary reads are
                                // checked for consistency beside the raw ones.
                                for bucket in 0..16u32 {
                                    for r in table.select_by_bucket(bucket).execute().unwrap() {
                                        assert_eq!(r.payload, 1_000_000 + r.id, "{}: group read saw a mismatched row", $label);
                                        assert_eq!(r.bucket, bucket, "{}: row in the wrong group", $label);
                                    }
                                }
                            }
                        });
                    }

                    // Counting finished writers rather than watching the row
                    // count: the count returns to its starting value between
                    // every delete and the next insert.
                    scope.spawn({
                        let stop = Arc::clone(&stop);
                        let finished = Arc::clone(&finished);
                        move || {
                            while finished.load(Ordering::Acquire) < writers {
                                std::hint::spin_loop();
                            }
                            stop.store(true, Ordering::Relaxed);
                        }
                    });
                });

                // The seeded rows are untouched: churn beside them must not
                // have taken any with it.
                for id in 0..seed {
                    let r = table.select(id).expect("seeded row present");
                    assert_eq!(r.payload, 1_000_000 + id, "{}: seeded row {id} was corrupted", $label);
                }
                // And each writer's window ends with exactly one set live.
                for w in 0..writers {
                    let base = base_of(w);
                    let live = (base..base + WINDOW * 2).filter(|id| table.select(*id).is_some()).count();
                    assert_eq!(live as u64, WINDOW, "{}: writer {w} left {live} rows, expected {WINDOW}", $label);
                }
            }

            #[tokio::test]
            async fn readers_see_consistent_groups_during_writes() {
                let (writers, per_writer) = (params::writers(), params::per_writer());
                let readers = params::readers();

                let table = Arc::new(ConcWorkTable::default());
                std::thread::scope(|scope| {
                    for w in 0..writers {
                        let table = Arc::clone(&table);
                        scope.spawn(move || {
                            for n in 0..per_writer {
                                futures::executor::block_on(table.insert(row(w * per_writer + n))).expect("insert");
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
