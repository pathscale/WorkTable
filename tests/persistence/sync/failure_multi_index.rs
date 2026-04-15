/// Test for multi-index persistence hang bug
///
/// This test detects a bug where:
/// 1. Insert succeeds on Index A (CDC event generated)
/// 2. Insert fails on Index B (duplicate value - must be unique index)
/// 3. Rollback removes Index A entry in memory
/// 4. CDC system may have queued an event for the partial insert that never gets cleaned up
/// 5. This leaves the persistence queue/state corrupted, blocking future operations
use crate::remove_dir_if_exists;
use std::time::Duration;
use tokio::time::timeout;
use worktable::prelude::*;
use worktable::worktable;


// Table with TWO unique indexes to trigger the bug scenario
worktable!(
    name: MultiUniqueIdx,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        unique_a: u64,
        unique_b: u64,
    },
    indexes: {
        unique_a_idx: unique_a unique,
        unique_b_idx: unique_b unique,
    },
    queries: {}
);

#[test]
fn test_multi_index_insert_failure_doesnt_corrupt_persistence() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/sync/failure_multi_index_insert",
        MultiUniqueIdxWorkTable::name_snake_case(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/sync/failure_multi_index_insert".to_string()).await;

        // Phase 1: Insert initial rows to populate indexes
        let pk = {
            let engine = MultiUniqueIdxPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = MultiUniqueIdxWorkTable::load(engine).await.unwrap();

            let row = MultiUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 0,
                unique_b: 0,
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.id
        };

        // Phase 2: The critical test - failed insert followed by valid insert
        // This tests if the persistence system gets stuck after a failed insert
        let valid_insert_pk = {
            let engine = MultiUniqueIdxPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = MultiUniqueIdxWorkTable::load(engine).await.unwrap();

            let valid_row = MultiUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 9999,
                unique_b: 999,
            };
            table.insert(valid_row).unwrap();

            let valid_row = MultiUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 1,
                unique_b: 1,
            };
            table.insert(valid_row).unwrap();

            tokio::time::sleep(Duration::from_millis(500)).await;

            let failing_row = MultiUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 99,
                unique_b: 0, // This already exists
            };

            let result = table.insert(failing_row);
            assert!(result.is_err());
            assert!(matches!(
                result.unwrap_err(),
                WorkTableError::AlreadyExists(_)
            ));

            let failing_row = MultiUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 99,
                unique_b: 0, // This already exists
            };

            let result = table.insert(failing_row);
            assert!(result.is_err());
            assert!(matches!(
                result.unwrap_err(),
                WorkTableError::AlreadyExists(_)
            ));

            let valid_row = MultiUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 999,
                unique_b: 99,
            };
            let valid_pk = valid_row.id;
            table.insert(valid_row).unwrap();

            // Use timeout to detect if persistence is stuck
            // If this hangs, the bug exists - CDC queue is blocked
            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;

            if wait_result.is_err() {
                panic!(
                    "BUG DETECTED: Persistence system is stuck! \
                     wait_for_ops() timed out after 10 seconds. "
                );
            }

            valid_pk
        };

        // Phase 3: Reload from disk and verify
        {
            let engine = MultiUniqueIdxPersistenceEngine::new(config).await.unwrap();
            let table = MultiUniqueIdxWorkTable::load(engine).await.unwrap();

            let original_row = table.select(pk).unwrap();
            assert_eq!(original_row.unique_a, 0);
            assert_eq!(original_row.unique_b, 0);

            let persisted_row = table.select(valid_insert_pk).unwrap();
            assert_eq!(persisted_row.unique_a, 999);
            assert_eq!(persisted_row.unique_b, 99);

            let row_with_99 = MultiUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 99,
                unique_b: 100,
            };
            let result = table.insert(row_with_99.clone());
            assert!(
                result.is_ok(),
                "BUG DETECTED: unique_a_idx has orphaned entry for unique_a=99 \
                 from the failed insert. This indicates CDC event loss during rollback."
            );
            assert!(table.select(row_with_99.id).is_some());
        }
    });
}
