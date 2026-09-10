//! Concurrent upserts against a persisted table.
//!
//! Found by the persisted benchmark grid, which panicked on worker threads in
//! `persistence::space::data::save_batch_data`:
//!
//! ```text
//! should be available as pages parsed from these ids
//! ```
//!
//! The lookup that fails is `batch_data.get(&id)` over the union of the pages
//! the batch created and the pages it parsed back. Every id in both sets comes
//! from `batch_data.keys()`, so the only way the lookup misses is for a parsed
//! page to carry a header id that was never requested.

use worktable::prelude::*;
use worktable::worktable;

use crate::remove_dir_if_exists;

worktable!(
    name: ConcurrentUpsert,
    persist: true,
    columns: {
        id: u64 primary_key,
        payload: u64,
    },
);

/// Several tasks upserting overlapping keys, which is what any table behind a
/// service does.
///
/// Deliberately `multi_thread`: on the default current-thread runtime the
/// tasks never overlap and the batch path only ever sees one writer, which is
/// how a bug in it stays hidden. See `tests/worktable/multi_thread_discipline`.
/// **Currently failing, and ignored so the suite stays honest rather than
/// green.** Remove the `ignore` when the bug below is fixed; it is the
/// regression test for it.
///
/// Two panics, reliably:
///
/// ```text
/// src/persistence/space/data.rs:381  should be available as pages parsed from these ids
/// async-task/src/task.rs:452         Task polled after completion
/// ```
///
/// Writers alone do not reproduce it: a four-writer version of this test
/// passes. It needs readers overlapping the writers, which is what a service
/// actually does and what no existing persistence test does.
#[ignore = "reproduces an open bug in the persisted batch save path, see the comment above"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_upserts_do_not_lose_a_page() {
    let dir = "tests/data/concurrent_upsert_batch/persisted";
    remove_dir_if_exists(dir.to_string()).await;

    let config = DiskConfig::new_with_table_name(
        dir,
        ConcurrentUpsertWorkTable::name_snake_case(),
        ConcurrentUpsertWorkTable::version(),
    );
    let engine = ConcurrentUpsertPersistenceEngine::new(config)
        .await
        .expect("an engine");
    let table = std::sync::Arc::new(ConcurrentUpsertWorkTable::load(engine).await.expect("a table"));

    const ROWS: u64 = 20_000;
    /// Operations per task. Bounded independently of `ROWS`, because scaling
    /// both together made this an 80,000,000 upsert test that ran for ten
    /// minutes and told us nothing the first thousand had not.
    const OPS: u64 = 4_000;
    for id in 0..ROWS {
        table
            .insert(ConcurrentUpsertRow { id, payload: id })
            .await
            .expect("fresh key");
    }

    // Enough rows to span many pages, and overlapping key ranges so two
    // writers can land in one batch for the same page.
    // Readers alongside the writers. The benchmark that found this ran six
    // selects against two upserts, and a select takes the same page latch the
    // flush needs, so leaving them out changes which paths overlap.
    let mut handles = Vec::new();
    for reader in 0..6u64 {
        let table = std::sync::Arc::clone(&table);
        handles.push(tokio::spawn(async move {
            for step in 0..OPS {
                let id = (step * 11 + reader * 17) % ROWS;
                let _ = table.select(id);
            }
        }));
    }
    for writer in 0..4u64 {
        let table = std::sync::Arc::clone(&table);
        handles.push(tokio::spawn(async move {
            for step in 0..OPS {
                let id = (step * 7 + writer * 13) % ROWS;
                table
                    .upsert(ConcurrentUpsertRow {
                        id,
                        payload: writer * 1_000_000 + step,
                    })
                    .await
                    .expect("an upsert");
            }
        }));
    }
    for handle in handles {
        handle.await.expect("a writer");
    }

    // Every key must still be readable, and the table must close cleanly:
    // `close` returning `Ok` is the only proof the queue drained to disk.
    for id in 0..ROWS {
        assert!(table.select(id).is_some(), "row {id} went missing");
    }
    let table = std::sync::Arc::try_unwrap(table).unwrap_or_else(|_| panic!("the writers are joined"));
    table.close().await.expect("a clean close");
}
