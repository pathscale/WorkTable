use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable_codegen::worktable;

use crate::remove_dir_if_exists;

worktable!(
    name: BulkDeleteDurability,
    persist: true,
    columns: {
        id: u64 primary_key,
        tag: u64,
        note: String,
    },
    indexes: {
        tag_idx: tag,
    }
);

fn row(id: u64) -> BulkDeleteDurabilityRow {
    BulkDeleteDurabilityRow {
        id,
        tag: id % 4,
        note: format!("row-{id}"),
    }
}

/// A bulk delete has to reach the disk, not just memory.
///
/// The generated persisted `delete_many` and `delete_range` used to call
/// straight through to the in-memory batch. That returned success and the rows
/// vanished from memory, but no persistence operation was ever produced, so
/// the durable table was untouched and every deleted row came back on the next
/// load. Nothing caught it: the bulk-delete tests declared their table without
/// `persist: true`, so they only ever exercised the in-memory generator.
///
/// This is why it is a restart test rather than an assertion about operations:
/// what a caller cares about is that a row it deleted stays deleted across a
/// reopen, through the primary index *and* the secondary one.
#[test]
fn bulk_deletes_survive_reload() {
    let dir = "tests/data/bulk_delete_durability";
    let config = DiskConfig::new_with_table_name(
        dir,
        BulkDeleteDurabilityWorkTable::name_snake_case(),
        BulkDeleteDurabilityWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists(dir.to_string()).await;

        {
            let engine = BulkDeleteDurabilityPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = BulkDeleteDurabilityWorkTable::load(engine).await.unwrap();
            for id in 0..20u64 {
                table.insert(row(id)).unwrap();
            }
            table.wait_for_ops().await.unwrap();

            // A key list, and a span. Both paths, because they generate
            // separately.
            let by_key = table.delete_many((0..5u64).collect()).await.unwrap();
            assert_eq!(by_key.len(), 5);
            let lo: BulkDeleteDurabilityPrimaryKey = 10u64.into();
            let hi: BulkDeleteDurabilityPrimaryKey = 15u64.into();
            let by_range = table.delete_range(lo..hi).await.unwrap();
            assert_eq!(by_range.len(), 5);

            assert_eq!(table.count(), 10);
            table.wait_for_ops().await.unwrap();
        }

        {
            let engine = BulkDeleteDurabilityPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = BulkDeleteDurabilityWorkTable::load(engine).await.unwrap();

            assert_eq!(table.count(), 10, "deleted rows came back on reload");

            for id in (0..5u64).chain(10..15u64) {
                assert!(
                    table.select(id).is_none(),
                    "row {id} was deleted before the reload and is present after it"
                );
            }
            for id in (5..10u64).chain(15..20u64) {
                assert!(table.select(id).is_some(), "row {id} was never deleted but is missing");
            }

            // The secondary index has to agree. A row removed from the primary
            // index but left in a secondary one is still reachable, and points
            // at storage that is free to be reused.
            for tag in 0..4u64 {
                for found in table.select_by_tag(tag).execute().unwrap() {
                    assert!(
                        !(found.id < 5 || (10..15).contains(&found.id)),
                        "deleted row {} is still reachable through tag_idx after reload",
                        found.id
                    );
                }
            }
        }

        remove_dir_if_exists(dir.to_string()).await;
    })
}
