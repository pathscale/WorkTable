use std::collections::BTreeSet;
use std::time::Duration;

use tokio::time::timeout;

use crate::remove_dir_if_exists;
use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable::worktable;

/*
 * Regression test: a store torn by an abrupt process death must fail CLEANLY
 * at the next load.
 *
 * The persistence engine writes pages in place and multi-step operations are
 * not atomic (a data page can be half-written while its index events are
 * abandoned — the Drop impl on PersistenceTask says as much). A process that
 * dies mid-write (SIGKILL, crash, quit without wait_for_ops) therefore
 * leaves torn bytes on disk. Today those bytes are read back through
 * `rkyv::access_unchecked`, so a torn store does not fail at load: it loads
 * as garbage, and the garbage's wild relative pointers blow up later as a
 * SIGBUS in whatever operation happens to walk them. On 2026-08-01 that
 * pattern killed a production store four times: each session ended abruptly,
 * each next session opened "fine" and died mid-append, and each death tore
 * the store further.
 *
 * The test kills a writer child mid-write repeatedly and then loads the
 * store. Acceptable outcomes at load or scan: Ok with a consistent prefix of
 * the data, or Err naming corruption. Unacceptable: SIGBUS / UB-check abort,
 * which is what `access_unchecked` turns torn bytes into.
 */
worktable!(
    name: TornShutdown,
    persist: true,
    columns: {
        id: String primary_key,
        project_id: String,
        body: String,
    },
    indexes: {
        project_idx: project_id,
    },
);

const DIR: &str = "tests/data/torn_shutdown/persisted";
pub const WRITER_ENV: &str = "WT_TORN_SHUTDOWN_WRITER";
static TORN_STORE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn lock_torn_store_tests() -> std::sync::MutexGuard<'static, ()> {
    // These process-level tests intentionally mutate the same persisted store.
    // Cargo runs tests concurrently, so serialize the parent processes while
    // still allowing each test's writer child to operate on that store.
    TORN_STORE_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn key(i: u64) -> String {
    format!("msg-00000000-0000-4000-8000-{i:012}")
}

fn row(i: u64) -> TornShutdownRow {
    TornShutdownRow {
        id: key(i),
        project_id: format!("proj-{:02}", i % 3),
        body: "x".repeat(1_000),
    }
}

async fn try_open_table() -> eyre::Result<TornShutdownWorkTable> {
    let config = DiskConfig::new_with_table_name(
        DIR,
        TornShutdownWorkTable::name_snake_case(),
        TornShutdownWorkTable::version(),
    );
    let engine = TornShutdownPersistenceEngine::new(config).await?;
    TornShutdownWorkTable::load(engine).await
}

async fn open_table() -> TornShutdownWorkTable {
    try_open_table().await.unwrap()
}

/// The writer half, run as a child process: appends rows forever without
/// ever draining, so a SIGKILL lands mid-write with high probability. Not a
/// test of anything by itself; the env gate keeps it inert in normal runs.
#[test]
fn torn_shutdown_writer() {
    let Ok(start) = std::env::var(WRITER_ENV) else {
        return; // Normal test run: nothing to do.
    };
    let start: u64 = start.parse().unwrap();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let table = open_table().await;
        for i in start.. {
            // Errors are tolerated, aborts are not: the parent only checks
            // how this process DIES, and it must die by the parent's signal,
            // not by its own reading of what the last kill left behind.
            let _ = table.insert(row(i)).await;
            if i % 64 == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    });
}

/// Build a base store, then run a writer child and kill it mid-write, five
/// rounds. Each round loads whatever the previous kill left. A child that
/// dies on its own must have died NAMING corruption ("torn or corrupt"), not
/// of a signal: a named refusal is containment working, a signal is the
/// disease.
fn tear_the_store_repeatedly() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists(DIR.to_string()).await;

        // A clean base, so the writers start from a real store rather than
        // an empty directory.
        {
            let table = open_table().await;
            for i in 0..200 {
                table.insert(row(i)).await.unwrap();
            }
            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence stalled building the base store")
                .expect("persistence engine failed");
        }
    });

    let exe = std::env::current_exe().unwrap();
    for round in 0..5u64 {
        let mut child = std::process::Command::new(&exe)
            .arg("--exact")
            .arg("persistence::torn_shutdown::torn_shutdown_writer")
            .arg("--nocapture")
            .env(WRITER_ENV, (1_000 + round * 10_000).to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        // Long enough to be mid-write, short enough to stay a unit test.
        std::thread::sleep(Duration::from_millis(700));
        let status = match child.try_wait().unwrap() {
            // Still writing, as intended: kill it mid-flight.
            None => {
                child.kill().unwrap();
                child.wait().unwrap()
            }
            Some(status) => {
                let mut stderr = String::new();
                use std::io::Read;
                if let Some(mut pipe) = child.stderr.take() {
                    let _ = pipe.read_to_string(&mut stderr);
                }
                /*
                 * `code()` is None exactly when a signal killed it: SIGBUS,
                 * SIGSEGV, SIGABRT from the UB check. Any actual exit code
                 * means the writer refused cleanly with an error of its own,
                 * which is containment working.
                 */
                assert!(
                    status.code().is_some(),
                    "writer round {round} was killed by a signal ({status}): the \
                     tear was read as data instead of refused. Child stderr:\n{stderr}"
                );
                assert!(
                    stderr.contains("torn or corrupt"),
                    "writer round {round} refused the store without the typed corruption message. \
                     Child stderr:\n{stderr}"
                );
                continue;
            }
        };
        assert!(
            !status.success(),
            "the writer exited cleanly; it is meant to write until killed"
        );
    }
}

/// The bar validated page reads meet TODAY: a store torn by mid-write kills
/// never takes a process down with a signal. Every load either succeeds or
/// refuses naming corruption, in the writer children and in this process.
/// What this bar does NOT include is row fidelity — the full Option B load
/// contract is exercised by the next test.
#[test]
fn test_torn_store_fails_clean_never_by_signal() {
    let _test_guard = lock_torn_store_tests();
    tear_the_store_repeatedly();

    let outcome = std::panic::catch_unwind(|| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let table = open_table().await;
            let _ = table.select_all().execute().unwrap();
        });
    });
    /*
     * A caught panic is containment working: the torn store was refused with
     * a message instead of taking the process down. The invariant this test
     * holds is narrower and absolute — reaching this line at all means no
     * signal killed the process. The full-bar test below additionally
     * demands row fidelity.
     */
    drop(outcome);
}

/// Option B's load boundary: persistence is best-effort, so a SIGKILL may
/// lose acknowledged rows. The next load must nevertheless do exactly one of
/// two things: return a validated state containing no phantom rows, or return
/// the typed `PersistenceLoadError` that directs the caller to restore or
/// rebuild. A torn store must never become a live table with invented data.
#[test]
fn test_store_survives_torn_shutdowns() {
    let _test_guard = lock_torn_store_tests();
    tear_the_store_repeatedly();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        match try_open_table().await {
            Ok(table) => {
                let rows = table.select_all().execute().unwrap();
                let legal_projects: BTreeSet<String> = (0..3).map(|p| format!("proj-{p:02}")).collect();
                for row in &rows {
                    assert!(
                        row.id.starts_with("msg-00000000-0000-4000-8000-"),
                        "scan returned a row no writer ever inserted (id {:?}): torn bytes \
                     were read as data",
                        &row.id[..row.id.len().min(60)]
                    );
                    assert!(
                        legal_projects.contains(&row.project_id),
                        "row {} carries project {:?}, which no writer ever wrote",
                        row.id,
                        row.project_id
                    );
                }
                // And the survivor must still accept writes and a drain.
                table.insert(row(9_000_000)).await.unwrap();
                timeout(Duration::from_secs(30), table.wait_for_ops())
                    .await
                    .expect("persistence stalled appending to the survivor store")
                    .expect("persistence engine failed");
            }
            Err(error) => assert!(
                error.downcast_ref::<PersistenceLoadError>().is_some(),
                "torn store was refused with an untyped error: {error:#}"
            ),
        }
    });
}

#[test]
fn corrupted_row_is_refused_with_typed_load_error() {
    let _test_guard = lock_torn_store_tests();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    let link = runtime.block_on(async {
        remove_dir_if_exists(DIR.to_string()).await;
        let table = open_table().await;
        let primary_key = table.insert(row(7)).await.unwrap();
        let link = table.0.primary_index.pk_map.get_value(&primary_key).unwrap().0;
        table.close().await.unwrap();
        link
    });

    let data_path = format!(
        "{DIR}/{}/{}",
        TornShutdownWorkTable::name_snake_case(),
        WT_DATA_EXTENSION
    );
    let page_id: u32 = link.page_id.into();
    let byte_offset = u64::from(page_id) * PAGE_SIZE as u64 + GENERAL_HEADER_SIZE as u64 + u64::from(link.offset);
    {
        use std::io::{Seek, SeekFrom, Write};

        let mut data_file = std::fs::OpenOptions::new().write(true).open(data_path).unwrap();
        data_file.seek(SeekFrom::Start(byte_offset)).unwrap();
        data_file.write_all(&vec![0; link.length as usize]).unwrap();
        data_file.sync_all().unwrap();
    }

    runtime.block_on(async {
        let error = match try_open_table().await {
            Ok(_) => panic!("corrupted row was exposed as a live table"),
            Err(error) => error,
        };
        let typed = error
            .downcast_ref::<PersistenceLoadError>()
            .expect("corrupt persisted row must return PersistenceLoadError");
        assert_eq!(typed.path(), std::path::Path::new(&format!("{DIR}/torn_shutdown")));
        assert!(!typed.reason().is_empty());
    });
}

#[test]
fn incomplete_secondary_index_is_refused_with_typed_load_error() {
    let _test_guard = lock_torn_store_tests();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists(DIR.to_string()).await;
        let table = open_table().await;
        table.insert(row(11)).await.unwrap();
        table.close().await.unwrap();
    });

    let secondary_path = format!(
        "{DIR}/{}/project_idx{}",
        TornShutdownWorkTable::name_snake_case(),
        WT_INDEX_EXTENSION
    );
    {
        let secondary_file = std::fs::OpenOptions::new().write(true).open(secondary_path).unwrap();
        secondary_file.set_len(PAGE_SIZE as u64).unwrap();
        secondary_file.sync_all().unwrap();
    }

    runtime.block_on(async {
        let error = match try_open_table().await {
            Ok(_) => panic!("incomplete secondary index was exposed as a live table"),
            Err(error) => error,
        };
        let typed = error
            .downcast_ref::<PersistenceLoadError>()
            .expect("incomplete secondary index must return PersistenceLoadError");
        assert!(
            typed.reason().contains("project_idx"),
            "unexpected reason: {}",
            typed.reason()
        );
    });
}

/// The clean-shutdown sibling: many short load-append-drain-close sessions,
/// no kill anywhere, then one full scan. The production table that died had
/// lived exactly this life — dozens of small sessions, each ended with a
/// drained quit — so if this fails, the corruption needs no crash at all:
/// the load-append path drifts on its own, one generation at a time.
#[test]
fn test_many_clean_sessions_stay_readable() {
    const DIR: &str = "tests/data/torn_shutdown/clean_sessions";
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists(DIR.to_string()).await;
        let open = || async {
            let config = DiskConfig::new_with_table_name(
                DIR,
                TornShutdownWorkTable::name_snake_case(),
                TornShutdownWorkTable::version(),
            );
            let engine = TornShutdownPersistenceEngine::new(config).await.unwrap();
            TornShutdownWorkTable::load(engine).await.unwrap()
        };

        // Forty generations of a handful of appends each: the shape of a
        // long-lived store that is opened, written a little, and closed.
        let mut next_id = 0u64;
        for session in 0..40u64 {
            let table = open().await;
            for _ in 0..8 {
                table
                    .insert(row(next_id))
                    .unwrap_or_else(|error| panic!("session {session}: insert {next_id} refused: {error:?}"));
                next_id += 1;
            }
            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .unwrap_or_else(|_| panic!("session {session}: drain stalled"))
                .expect("persistence engine failed");
        }

        let table = open().await;
        let got: BTreeSet<String> = table
            .select_all()
            .execute()
            .unwrap()
            .into_iter()
            .map(|r| r.id)
            .collect();
        let expected: BTreeSet<String> = (0..next_id).map(key).collect();
        assert_eq!(
            got, expected,
            "rows lost, duplicated, or invented across clean load-append-close generations"
        );
    })
}
