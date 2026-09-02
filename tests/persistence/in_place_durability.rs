use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable_codegen::worktable;

use crate::remove_dir_if_exists;

worktable!(
    name: InPlaceDurability,
    persist: true,
    columns: {
        id: u64 primary_key,
        counter: u64,
        note: String,
    },
    queries: {
        in_place: {
            CounterById(counter) by id,
        }
    }
);

/// An in-place update mutates the page bytes directly; it must still emit a
/// persistence operation, otherwise the write silently reverts on restart.
///
/// NOTE: this test depends on data-only operations (no index events)
/// surviving batch validation in src/persistence/operation/batch.rs, which is
/// being fixed on a separate branch. If it fails on this branch, that is the
/// missing integration, not a regression here.
#[test]
fn in_place_update_survives_reload() {
    let dir = "tests/data/in_place_durability";
    let config = DiskConfig::new_with_table_name(
        dir,
        InPlaceDurabilityWorkTable::name_snake_case(),
        InPlaceDurabilityWorkTable::version(),
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
            let engine = InPlaceDurabilityPersistenceEngine::new(config.clone()).await.unwrap();
            let table = InPlaceDurabilityWorkTable::load(engine).await.unwrap();
            table
                .insert(InPlaceDurabilityRow {
                    id: 1,
                    counter: 10,
                    note: "row".to_string(),
                })
                .await
                .unwrap();
            table
                .update_counter_by_id_in_place(|counter| *counter = 42u64.into(), 1)
                .await
                .unwrap();
            table.wait_for_ops().await.unwrap();
        }
        {
            let engine = InPlaceDurabilityPersistenceEngine::new(config.clone()).await.unwrap();
            let table = InPlaceDurabilityWorkTable::load(engine).await.unwrap();
            assert_eq!(
                table.select(1).expect("the row must survive reload").counter,
                42,
                "the in-place update was not persisted"
            );
        }

        remove_dir_if_exists(dir.to_string()).await;
    })
}
