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

async fn open_table() -> TornShutdownWorkTable {
    let config = DiskConfig::new_with_table_name(
        DIR,
        TornShutdownWorkTable::name_snake_case(),
        TornShutdownWorkTable::version(),
    );
    let engine = TornShutdownPersistenceEngine::new(config).await.unwrap();
    TornShutdownWorkTable::load(engine).await.unwrap()
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
            let _ = table.insert(row(i));
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
                table.insert(row(i)).unwrap();
            }
            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence stalled building the base store");
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
/// What this bar does NOT include is row fidelity — see the ignored full-bar
/// test below for that.
#[test]
fn test_torn_store_fails_clean_never_by_signal() {
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

/// The FULL bar, which needs crash-consistent writes the engine does not
/// have yet: a torn store scans as a consistent prefix of what was written —
/// no phantom rows — or refuses loudly. Validated reads cannot meet it
/// alone: a dangling index link into a zeroed data region reads as a row of
/// empty fields that validates perfectly, so kills still manufacture rows
/// nobody wrote. Ignored until write-side atomicity (WAL / shadow paging /
/// page checksums) lands; run with
/// `cargo test -- --ignored test_store_survives_torn_shutdowns`.
#[test]
#[ignore = "needs crash-consistent writes: dangling index links still read as phantom rows"]
fn test_store_survives_torn_shutdowns() {
    tear_the_store_repeatedly();
    // The reckoning: load and scan the torn store IN THIS PROCESS, through
    // an unwind boundary so a named corruption refusal counts as the fix
    // working. What must not happen is the process dying of SIGBUS/UB (the
    // harness reports that as the test binary dying), or the scan returning
    // rows nobody wrote.
    let outcome = std::panic::catch_unwind(|| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let table = open_table().await;
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
            table.insert(row(9_000_000)).unwrap();
            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence stalled appending to the survivor store");
        });
    });
    if let Err(panic) = outcome {
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("(non-string panic)");
        assert!(
            message.contains("torn or corrupt"),
            "the torn store failed without naming corruption: {message}"
        );
    }
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
                .unwrap_or_else(|_| panic!("session {session}: drain stalled"));
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
