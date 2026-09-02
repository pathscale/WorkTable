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

    pub fn env_f64(name: &str, default: f64) -> f64 {
        match std::env::var(name) {
            Ok(raw) => raw
                .trim()
                .parse()
                .unwrap_or_else(|_| panic!("{name} must be a number, got {raw:?}")),
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

    /// Rows inserted per arm of the throughput sweep.
    pub fn scale_rows() -> u64 {
        env_u64("WT_SCALE_ROWS", 200_000)
    }

    /// Writer counts the throughput sweep visits.
    pub fn scale_sweep() -> Vec<u64> {
        match std::env::var("WT_SCALE_SWEEP") {
            Ok(raw) => raw
                .split(',')
                .map(|part| {
                    part.trim()
                        .parse()
                        .unwrap_or_else(|_| panic!("WT_SCALE_SWEEP must be comma-separated integers, got {raw:?}"))
                })
                .collect(),
            Err(_) => vec![2, 4, 8, 16],
        }
    }

    /// Share of single-writer throughput a run must keep.
    pub fn scale_floor() -> f64 {
        env_f64("WT_SCALE_FLOOR", 0.6)
    }

    /// Whether this is a release build.
    ///
    /// A function rather than `cfg!(..)` inline, so the assertion that uses it
    /// is not a compile-time constant. `assert!(!cfg!(debug_assertions))` is
    /// rejected by clippy as a constant assertion, and `#[cfg] panic!` makes
    /// the rest of the function unreachable and its imports unused. This keeps
    /// one code path in both profiles.
    pub fn is_release_build() -> bool {
        !cfg!(debug_assertions)
    }
}

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

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

worktable!(
    name: Scale,
    persist: false,
    columns: {
        id: u64 primary_key,
        payload: u64,
    },
    indexes: { payload_idx: payload unique },
);

/// Insert throughput must not collapse as writers are added.
///
/// **Ignored because it fails on current code, deliberately.** It records a
/// defect rather than guarding against one, in the same way
/// `worktable_codegen`'s `generator_determinism` does. Run it with
/// `cargo test --test mod insert_throughput -- --ignored --nocapture`.
///
/// Measured on an M4 Max, best of three, 200,000 inserts:
///
/// | writers | throughput | vs 1 writer |
/// | ---: | ---: | ---: |
/// | 1 | 1.20 M/s | 1.00x |
/// | 2 | 1.20 M/s | 1.00x |
/// | 4 | 1.11 M/s | 0.92x |
/// | 8 | 297 K/s | **0.25x** |
/// | 16 | 266 K/s | 0.22x |
///
/// Eight concurrent writers are four times slower **in aggregate** than one.
/// It is not the index: all three backends collapse to the same ~300 K/s, and
/// arctic and congee are 1.3x faster than WTI single-threaded before hitting
/// the identical wall. A `sample` of the eight-writer run puts the time in
/// `parking_lot::RawRwLock::lock_exclusive_slow` and `DataPages`, which is
/// `pages.rs`: every insert takes an exclusive write lock on the *one* page
/// named by `current_page_id`, so appends serialise by construction, and
/// `EmptyLinkRegistry::pop_max` takes a global mutex on every insert even when
/// the free list is empty.
///
/// The threshold is 0.6x rather than 1.0x: some loss is expected from cache
/// traffic and allocation, and a benchmark-shaped assertion on a shared machine
/// has to leave room. At 0.25x this is not a threshold question.
#[test]
#[ignore = "records the concurrent-insert collapse; fails until pages.rs stops serialising appends"]
fn insert_throughput_should_scale_past_four_writers() {
    // A debug build makes this test lie, and lie reassuringly. Per-operation
    // overhead swamps the lock contention, so the collapse disappears and the
    // sweep reports 8 writers as *faster* than 1 (measured: 2.79x). A
    // throughput assertion that passes for the wrong reason is worse than none,
    // so refuse rather than mislead.
    assert!(
        params::is_release_build(),
        "run this in release: `cargo test --release --test mod insert_throughput -- --ignored --nocapture`. \
         In a debug build the per-operation overhead hides the contention and this test passes for the wrong reason."
    );

    let n = params::scale_rows();
    let floor = params::scale_floor();

    let throughput = |writers: u64| -> f64 {
        let mut best = f64::MAX;
        for _ in 0..3 {
            let table = Arc::new(ScaleWorkTable::default());
            let per = n / writers;
            let start = Instant::now();
            std::thread::scope(|scope| {
                for w in 0..writers {
                    let table = Arc::clone(&table);
                    scope.spawn(move || {
                        for i in (w * per)..((w + 1) * per) {
                            let _ = table.insert(ScaleRow {
                                id: i,
                                payload: 1_000_000 + i,
                            });
                        }
                    });
                }
            });
            let ns = start.elapsed().as_nanos() as f64 / n as f64;
            if ns < best {
                best = ns;
            }
        }
        1e9 / best
    };

    let single = throughput(1);
    println!("  1 writer : {single:>12.0}/s  1.00x (baseline)");
    for writers in params::scale_sweep() {
        let scaled = throughput(writers);
        println!("{writers:>3} writers: {scaled:>12.0}/s  {:.2}x", scaled / single);
        assert!(
            scaled / single >= floor,
            "{writers} writers reached {:.2}x of single-writer throughput, below the {floor:.2}x floor",
            scaled / single
        );
    }
}
