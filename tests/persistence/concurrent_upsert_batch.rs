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
///
/// It used to panic twice, reliably:
///
/// ```text
/// src/persistence/space/data.rs  should be available as pages parsed from these ids
/// async-task/src/task.rs:452     Task polled after completion
/// ```
///
/// The cause was a gap in the page sequence, reduced to two calls in
/// `SpaceData::create_pages_up_to`'s test. Writers alone do not reproduce it:
/// a four-writer version of this test passes. It needs readers overlapping the
/// writers, because that is what makes two writers allocate pages at once.
///
/// The size matters and is not arbitrary. At 5,000 rows this passed even with
/// the bug, and the wall time was the tell rather than the panic: 0.43s
/// passing at 5,000, 158s failing at 10,000, 0.89s passing at 10,000 once
/// fixed. The minutes were the panic's aftermath, not the cost of persisting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_upserts_do_not_lose_a_page() {
    let dir = "tests/data/concurrent_upsert_batch/persisted";
    remove_dir_if_exists(dir.to_string()).await;

    let config = DiskConfig::new_with_table_name(
        dir,
        ConcurrentUpsertWorkTable::name_snake_case(),
        ConcurrentUpsertWorkTable::version(),
    );
    let engine = ConcurrentUpsertPersistenceEngine::new(config).await.expect("an engine");
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
    // Bounded, because the failure mode this test guards against is a drain
    // that takes minutes rather than one that returns an error. An unbounded
    // `close` turns that regression into a hung suite instead of a red test.
    // The isolated test takes about two seconds, but the all-features suite
    // runs many persistence and CPU-heavy tests concurrently. Five seconds
    // repeatedly expires under that contention despite a successful isolated
    // run. This is a deadlock watchdog; the persistence benchmark measures
    // latency. Keep it below the historical multi-minute failure mode.
    tokio::time::timeout(std::time::Duration::from_secs(30), table.close())
        .await
        .expect("close must drain in seconds, not minutes")
        .expect("a clean close");
}
