//! What "this runtime backend is supported" is allowed to mean.
//!
//! The support policy is one sentence: every backend listed compiles, the
//! library and integration suites pass against it, and a persisted table
//! survives open, write, close and reload. Performance belongs to the backend.
//! This file is the half of that sentence a test can hold. It runs one body
//! against `nagoya(locality)`, `nagoya(spread)`, `nagoya(throughput)` and
//! `tokio`, the same way `base_backend_suite!` and `vacuum_backend_suite!` run
//! one body across the index backends.
//!
//! The body covers four things, and each one is here because leaving it out
//! would let a broken backend pass:
//!
//! 1. `insert` / `select` / `update` / `delete` / `in_place`, so a backend that
//!    compiles but cannot drive a mutation is caught.
//! 2. A persisted table opened, written, closed and reloaded, so a backend
//!    whose spawn or timer never reaches the persistence worker is caught.
//! 3. Concurrent tasks, so the backend's sync primitives are exercised rather
//!    than merely named. See the note on runtime flavors below: this is the
//!    part that is easiest to write and hardest to write honestly.
//! 4. `wait_for_ops` and `close` bounded by a timeout. A shutdown that does not
//!    flush tears the data file, that has happened here before, and swapping
//!    the runtime under the persistence worker is exactly the change that would
//!    bring it back. A hang is reported as a failed assertion, not as a test
//!    that never returns.
//!
//! ## The runtime the harness runs on is not the runtime under test
//!
//! `#[tokio::test]` with no arguments builds a **current-thread** runtime. Tasks
//! spawned inside it interleave only at await points on one thread, so two
//! writers never actually overlap and a data race between them cannot be
//! observed. Any test here that means to exercise concurrency therefore says
//! `flavor = "multi_thread"` explicitly.
//!
//! This is not a hypothetical. Commit 2702c06 on this branch records two tests
//! that shared one table directory and only began failing once the persistence
//! worker moved off `tokio::spawn`: while the worker ran on the test's own
//! current-thread runtime, the two tables' writes never overlapped in time and
//! the corruption stayed hidden. The single-threaded harness was concealing a
//! real defect.
//!
//! Note that the harness runtime and the table's declared runtime are separate
//! things. `tokio::spawn` below drives the *test*; the table drives its own
//! internals on whatever `runtime:` selected. Once the four-backend arms are
//! live, that separation is what lets one body test four backends.
//!
//! ## Directories
//!
//! Every arm gets its own data directory and every test within an arm gets its
//! own subdirectory below that, both derived from the arm's label. Two tests
//! sharing one table directory is the defect 2702c06 fixed: the harness runs
//! tests on parallel threads, so two tables attached to one set of files and
//! filled them at once, and each table's event ids start at zero, so one saw
//! the other's events as corruption. This suite multiplies every test by four
//! backends, which would make that a four-way collision. Deriving the path from
//! the label also means a failure names the arm that failed.
//!
//! ## Status
//!
//! The four-backend arms are behind the `runtime-backends` feature, off by
//! default, because the DSL `runtime:` keyword and the `Runtime` trait are
//! landing separately. The `hardcoded_default` arm has no `runtime:` at all and
//! runs today against the runtime the engine currently hardcodes. That arm is
//! the pre-merge baseline: it proves the body is correct before the body is
//! asked to tell four backends apart.

/// One arm of the matrix.
///
/// `$label` names the arm and supplies its data directory, so a failure says
/// which backend failed. The runtime spec is optional and, when present, is
/// re-emitted verbatim into the table declaration. Omitting it is not the same
/// as writing `runtime: nagoya`: it declares nothing, which is what the arm
/// that runs today needs.
macro_rules! runtime_backend_suite {
    ($module:ident, $label:literal $(, runtime: $backend:tt $(($flavor:tt))?)?) => {
        mod $module {
            use std::collections::BTreeSet;
            use std::sync::Arc;
            use std::time::Duration;

            // The watchdog is deliberately the harness's clock, not the
            // table's. A backend whose own timers are broken must not be able
            // to break the timeout that is supposed to catch it, so this stays
            // `tokio::time` even on the nagoya arms and is aliased so it is
            // not confused with `worktable::prelude::timeout`.
            use tokio::time::timeout as harness_timeout;
            use worktable::prelude::PersistedWorkTable;
            use worktable::prelude::*;
            use worktable::worktable;

            use crate::remove_dir_if_exists;

            // The in-memory table. Carries an indexed column so `update` and
            // `delete` have index maintenance to do, and a plain one so
            // `in_place` has somewhere to write that no index watches.
            worktable!(
                name: RuntimeMatrix,
                persist: false,
                $(runtime: $backend $(($flavor))?,)?
                columns: {
                    id: u64 primary_key autoincrement,
                    counter: u64,
                    bucket: u64,
                    note: String,
                },
                indexes: {
                    bucket_idx: bucket,
                },
                queries: {
                    update: {
                        BucketById(bucket) by id,
                    },
                    delete: {
                        ByBucket() by bucket,
                    },
                    in_place: {
                        CounterById(counter) by id,
                    }
                }
            );

            // The persisted table. Same shape, no autoincrement, because a
            // reload has to compare against keys the test chose rather than
            // keys a generator handed out. Its queries carry a `Persist`
            // prefix because `worktable!` puts the generated query types at
            // module scope, so two tables in one module cannot share a query
            // name.
            worktable!(
                name: RuntimeMatrixPersist,
                persist: true,
                $(runtime: $backend $(($flavor))?,)?
                columns: {
                    id: u64 primary_key,
                    counter: u64,
                    bucket: u64,
                },
                indexes: {
                    bucket_idx: bucket,
                },
                queries: {
                    update: {
                        PersistBucketById(bucket) by id,
                    },
                    in_place: {
                        PersistCounterById(counter) by id,
                    }
                }
            );

            /// Names this arm. Used for the data directory, so a torn store
            /// says which backend tore it.
            const LABEL: &str = $label;

            /// Bounds every drain and shutdown in this file. A backend whose
            /// `close` never returns must fail the test, not stall the suite
            /// until CI's own timeout kills the run with no attribution.
            const SHUTDOWN_BUDGET: Duration = Duration::from_secs(5);

            /// Concurrent writers. Four is enough to have two of them actually
            /// running at once on the four-worker harness runtime, and small
            /// enough that four arms of this suite stay cheap.
            const WRITERS: u64 = 4;

            /// Rows each writer inserts.
            const PER_WRITER: u64 = 250;

            /// One directory per test per arm. Never share.
            fn data_dir(test: &str) -> String {
                format!("tests/data/runtime_backends/{LABEL}/{test}")
            }

            /// Bytes the arm actually put on disk. A reload that "survived"
            /// without the store growing would mean the assertions below were
            /// reading something other than the file, so this is the check
            /// that keeps the persistence tests honest.
            fn data_file_len(dir: &str) -> u64 {
                let path = format!(
                    "{dir}/{}/{WT_DATA_EXTENSION}",
                    RuntimeMatrixPersistWorkTable::name_snake_case()
                );
                std::fs::metadata(&path)
                    .unwrap_or_else(|error| panic!("{LABEL}: no store at {path}: {error}"))
                    .len()
            }

            fn config(dir: &str) -> DiskConfig {
                DiskConfig::new_with_table_name(
                    dir,
                    RuntimeMatrixPersistWorkTable::name_snake_case(),
                    RuntimeMatrixPersistWorkTable::version(),
                )
            }

            async fn open(dir: &str) -> RuntimeMatrixPersistWorkTable {
                let engine = RuntimeMatrixPersistPersistenceEngine::new(config(dir)).await.unwrap();
                RuntimeMatrixPersistWorkTable::load(engine).await.unwrap()
            }

            fn row(id: u64) -> RuntimeMatrixPersistRow {
                RuntimeMatrixPersistRow {
                    id,
                    counter: id * 10,
                    bucket: id % 4,
                }
            }

            /// Every mutation the table exposes, in one pass, on the runtime
            /// the arm declares. The point is coverage of the surface rather
            /// than depth in any one operation: a runtime that cannot drive a
            /// delete is not supported, whatever else it does well.
            #[tokio::test]
            async fn every_mutation_runs() {
                let table = RuntimeMatrixWorkTable::default();

                // Raw keys rather than the returned primary-key newtype: the
                // newtype is not `Copy`, and every assertion below reuses the
                // same key more than once.
                let mut ids: Vec<u64> = Vec::new();
                for i in 0..16u64 {
                    let id: u64 = table.get_next_pk().into();
                    table
                        .insert(RuntimeMatrixRow {
                            id,
                            counter: 0,
                            bucket: i % 4,
                            note: format!("{LABEL}-{i}"),
                        })
                        .await
                        .unwrap();
                    ids.push(id);
                }

                // select
                let first = table.select(ids[0]).expect("the inserted row must be selectable");
                assert_eq!(first.bucket, 0);
                assert_eq!(first.counter, 0);

                // select through a secondary index
                let bucket_zero = table.select_by_bucket(0).execute().unwrap();
                assert_eq!(bucket_zero.len(), 4, "{LABEL}: four of sixteen rows are in bucket 0");

                // update, which also has to move the row between index buckets
                table
                    .update_bucket_by_id(BucketByIdQuery { bucket: 3 }, ids[0])
                    .await
                    .unwrap();
                assert_eq!(table.select(ids[0]).unwrap().bucket, 3);
                assert_eq!(
                    table.select_by_bucket(0).execute().unwrap().len(),
                    3,
                    "{LABEL}: the update must leave the old index bucket"
                );

                // in_place, which mutates the page bytes rather than
                // republishing the row
                for _ in 0..64 {
                    table
                        .update_counter_by_id_in_place(|counter| *counter += 1u64, ids[1])
                        .await
                        .unwrap();
                }
                assert_eq!(table.select(ids[1]).unwrap().counter, 64);

                // delete, by a secondary index rather than by primary key
                table.delete_by_bucket(1).await.unwrap();
                assert!(
                    table.select_by_bucket(1).execute().unwrap().is_empty(),
                    "{LABEL}: bucket 1 must be empty after the delete"
                );

                // and delete by primary key
                table.delete(ids[0]).await.unwrap();
                assert!(table.select(ids[0]).is_none());
            }

            /// Writers and readers at once, on a genuinely multi-threaded
            /// harness. Under the default `#[tokio::test]` this test would
            /// pass without two tasks ever overlapping, which is to say it
            /// would prove nothing about the backend's sync primitives.
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn concurrent_tasks_reach_a_consistent_table() {
                let table = Arc::new(RuntimeMatrixWorkTable::default());

                let mut writers = Vec::new();
                for writer in 0..WRITERS {
                    let table = table.clone();
                    writers.push(tokio::spawn(async move {
                        for i in 0..PER_WRITER {
                            table
                                .insert(RuntimeMatrixRow {
                                    id: table.get_next_pk().into(),
                                    counter: writer,
                                    bucket: i % 4,
                                    note: format!("{LABEL}-{writer}-{i}"),
                                })
                                .await
                                .unwrap();
                        }
                    }));
                }

                // Readers run beside the writers rather than after them. A
                // select that observes a half-published row is the failure
                // this is looking for, and it cannot happen once the writers
                // have joined.
                let mut readers = Vec::new();
                for _ in 0..2 {
                    let table = table.clone();
                    readers.push(tokio::spawn(async move {
                        for _ in 0..PER_WRITER {
                            let rows = table.select_by_bucket(0).execute().unwrap();
                            for row in rows {
                                assert!(
                                    row.counter < WRITERS,
                                    "{LABEL}: a concurrent select observed a row that no writer wrote"
                                );
                            }
                            worktable::prelude::yield_now().await;
                        }
                    }));
                }

                for writer in writers {
                    writer.await.unwrap();
                }
                for reader in readers {
                    reader.await.unwrap();
                }

                let expected = WRITERS * PER_WRITER;
                let ids: BTreeSet<_> = table.select_all().execute().unwrap().into_iter().map(|r| r.id).collect();
                assert_eq!(
                    ids.len() as u64,
                    expected,
                    "{LABEL}: {expected} concurrent inserts must produce {expected} distinct rows"
                );
            }

            /// Open, write, close, reload, and the data is still there. This is
            /// the clause of the support policy that a backend cannot fake:
            /// the persistence worker has to have been spawned, driven and
            /// drained on the declared runtime.
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn a_persisted_table_survives_a_reload() {
                let dir = data_dir("survives_reload");
                remove_dir_if_exists(dir.clone()).await;

                {
                    let table = open(&dir).await;
                    for id in 1..=64u64 {
                        table.insert(row(id)).await.unwrap();
                    }
                    table
                        .update_persist_bucket_by_id(PersistBucketByIdQuery { bucket: 3 }, 1)
                        .await
                        .unwrap();
                    table
                        .update_persist_counter_by_id_in_place(|counter| *counter = 4_242u64.into(), 2)
                        .await
                        .unwrap();

                    harness_timeout(SHUTDOWN_BUDGET, table.wait_for_ops())
                        .await
                        .expect("wait_for_ops must not hang")
                        .unwrap();
                    harness_timeout(SHUTDOWN_BUDGET, table.close())
                        .await
                        .expect("close must not hang")
                        .unwrap();
                }

                assert!(
                    data_file_len(&dir) > 0,
                    "{LABEL}: the closed table left an empty store"
                );

                {
                    let table = open(&dir).await;
                    for id in 1..=64u64 {
                        let reloaded = table
                            .select(id)
                            .unwrap_or_else(|| panic!("{LABEL}: row {id} did not survive the reload"));
                        let expected_bucket = if id == 1 { 3 } else { row(id).bucket };
                        assert_eq!(reloaded.bucket, expected_bucket, "{LABEL}: row {id} reloaded wrong");
                        let expected_counter = if id == 2 { 4_242 } else { row(id).counter };
                        assert_eq!(
                            reloaded.counter, expected_counter,
                            "{LABEL}: row {id} lost its counter across the reload"
                        );
                    }

                    // The secondary index has to come back too, not just the
                    // rows: a reload that rebuilt the data and dropped the
                    // index would pass every check above.
                    let bucket_three: BTreeSet<_> = table
                        .select_by_bucket(3)
                        .execute()
                        .unwrap()
                        .into_iter()
                        .map(|r| r.id)
                        .collect();
                    let expected: BTreeSet<_> = (1..=64u64).filter(|id| *id == 1 || id % 4 == 3).collect();
                    assert_eq!(bucket_three, expected, "{LABEL}: the secondary index did not survive");

                    harness_timeout(SHUTDOWN_BUDGET, table.close())
                        .await
                        .expect("close must not hang")
                        .unwrap();
                }

                remove_dir_if_exists(dir).await;
            }

            /// Concurrent writers into a persisted table, then a drain and a
            /// shutdown, then a reload that has to find every row.
            ///
            /// This is the one that matters. A shutdown that returns before the
            /// persistence worker has flushed leaves a torn `.wt.data`, that
            /// has happened in this repo, and moving the worker onto a
            /// different runtime is precisely the change that could reintroduce
            /// it. Concurrency is here rather than in a separate test because a
            /// single-writer drain is the case that works even when the flush
            /// is broken.
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn a_concurrent_shutdown_flushes_rather_than_hangs() {
                let dir = data_dir("concurrent_shutdown");
                remove_dir_if_exists(dir.clone()).await;

                let written = WRITERS * PER_WRITER;
                {
                    let table = Arc::new(open(&dir).await);

                    let mut writers = Vec::new();
                    for writer in 0..WRITERS {
                        let table = table.clone();
                        writers.push(tokio::spawn(async move {
                            for i in 0..PER_WRITER {
                                table.insert(row(writer * PER_WRITER + i + 1)).await.unwrap();
                            }
                        }));
                    }
                    for writer in writers {
                        writer.await.unwrap();
                    }

                    harness_timeout(SHUTDOWN_BUDGET, table.wait_for_ops())
                        .await
                        .expect("wait_for_ops must not hang after concurrent writes")
                        .unwrap();

                    // `close` consumes the table, so the writers' clones have
                    // to be gone first. If they are not, that is a leaked
                    // handle and worth failing on rather than working around.
                    let table = Arc::try_unwrap(table)
                        .unwrap_or_else(|arc| panic!("{LABEL}: {} table lease(s) outlived the writers", Arc::strong_count(&arc) - 1));
                    harness_timeout(SHUTDOWN_BUDGET, table.close())
                        .await
                        .expect("close must not hang after concurrent writes")
                        .unwrap();
                }

                assert!(
                    data_file_len(&dir) > 0,
                    "{LABEL}: the shutdown left an empty store"
                );

                {
                    let table = open(&dir).await;
                    let ids: BTreeSet<_> = table.select_all().execute().unwrap().into_iter().map(|r| r.id).collect();
                    let expected: BTreeSet<_> = (1..=written).collect();
                    assert_eq!(
                        ids, expected,
                        "{LABEL}: the shutdown did not flush every concurrent write"
                    );
                    harness_timeout(SHUTDOWN_BUDGET, table.close())
                        .await
                        .expect("close must not hang")
                        .unwrap();
                }

                remove_dir_if_exists(dir).await;
            }
        }
    };
}

// The arm that runs today. No `runtime:` is declared, so the table takes the
// runtime the engine currently hardcodes. This is the pre-merge baseline: it
// proves the body before the body is asked to discriminate between backends.
runtime_backend_suite!(hardcoded_default, "hardcoded_default");

// The matrix. Off by default until the DSL `runtime:` keyword and the `Runtime`
// trait land; `cargo test --features runtime-backends` is what turns it on.
#[cfg(feature = "runtime-backends")]
runtime_backend_suite!(nagoya_locality, "nagoya_locality", runtime: nagoya(locality));
#[cfg(feature = "runtime-backends")]
runtime_backend_suite!(nagoya_spread, "nagoya_spread", runtime: nagoya(spread));
#[cfg(feature = "runtime-backends")]
runtime_backend_suite!(nagoya_throughput, "nagoya_throughput", runtime: nagoya(throughput));
#[cfg(feature = "runtime-backends")]
runtime_backend_suite!(tokio_rt, "tokio", runtime: tokio);
