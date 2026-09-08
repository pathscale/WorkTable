//! Is the persistence path waiting, or working?
//!
//! Bulk load persists at about 310 MB/s while the disk under it does gigabytes
//! and DataBucket's own write path does 500+ MB/s single threaded. So something
//! between them is the limit. CPU time against wall time says which kind of
//! limit it is: near or above wall means it is computing, well under means it
//! is waiting.

use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable_codegen::worktable;

use crate::remove_dir_if_exists;

worktable!(
    name: PersistShape,
    persist: true,
    columns: { id: u64 primary_key, payload: String }
);

#[test]
#[ignore = "a measurement, not an assertion"]
fn is_persistence_waiting_or_working() {
    let dir = "tests/data/persistence_is_what";
    let config = DiskConfig::new_with_table_name(
        dir,
        PersistShapeWorkTable::name_snake_case(),
        PersistShapeWorkTable::version(),
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists(dir.to_string()).await;
        let engine = PersistShapePersistenceEngine::new(config).await.unwrap();
        let table = PersistShapeWorkTable::load(engine).await.unwrap();
        let payload = "x".repeat(4096);

        // Marked so the times either side can be attributed to this and not to
        // building the table or tearing it down.
        println!("MARK begin");
        let at = std::time::Instant::now();
        for id in 0..25_000u64 {
            table
                .insert(PersistShapeRow {
                    id,
                    payload: payload.clone(),
                })
                .await
                .unwrap();
        }
        table.wait_for_ops().await.expect("the queue drains");
        println!("MARK end {:.1} ms", at.elapsed().as_secs_f64() * 1e3);

        remove_dir_if_exists(dir.to_string()).await;
    });
}
