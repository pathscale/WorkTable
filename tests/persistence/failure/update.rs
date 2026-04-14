/// Update failure tests - testing CDC event gaps during update operations
use super::*;
use crate::remove_dir_if_exists;

#[test]
fn test_update_unique_secondary_conflict() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/update_unique_conflict",
        TwoUniqueIdxWorkTable::name_snake_case(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/update_unique_conflict".to_string()).await;

        // Phase 1: Setup
        let row1_pk = {
            let engine = TwoUniqueIdxPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = TwoUniqueIdxWorkTable::load(engine).await.unwrap();

            let row1 = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 100,
                unique_b: 200,
            };
            table.insert(row1.clone()).unwrap();

            let row2 = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 300,
                unique_b: 400,
            };
            table.insert(row2.clone()).unwrap();
            table.wait_for_ops().await;
            row1.id
        };

        // Phase 2: 2 valid inserts -> failure update -> valid insert -> wait_for_ops
        {
            let engine = TwoUniqueIdxPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = TwoUniqueIdxWorkTable::load(engine).await.unwrap();

            let valid_row1 = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 1000,
                unique_b: 1001,
            };
            table.insert(valid_row1).unwrap();

            let valid_row2 = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 2000,
                unique_b: 2001,
            };
            table.insert(valid_row2).unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;

            let updated_row = TwoUniqueIdxRow {
                id: row1_pk,
                unique_a: 300,
                unique_b: 500,
            };

            let result = table.update(updated_row).await;
            assert!(result.is_err());

            let valid_row3 = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 3000,
                unique_b: 3001,
            };
            table.insert(valid_row3).unwrap();

            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;
            assert!(wait_result.is_ok(), "BUG: persistence blocked!");
        }

        // Phase 3: Verify
        {
            let engine = TwoUniqueIdxPersistenceEngine::new(config).await.unwrap();
            let table = TwoUniqueIdxWorkTable::load(engine).await.unwrap();

            let row1 = table.select(row1_pk).unwrap();
            assert_eq!(row1.unique_a, 100);
            assert_eq!(row1.unique_b, 200);
            table.wait_for_ops().await;
        }
    });
}

#[test]
fn test_update_pk_based_success() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/update_pk_success",
        TwoUniqueIdxWorkTable::name_snake_case(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/update_pk_success".to_string()).await;

        // Phase 1: Setup
        let row1_pk = {
            let engine = TwoUniqueIdxPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = TwoUniqueIdxWorkTable::load(engine).await.unwrap();

            let row1 = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 100,
                unique_b: 200,
            };
            table.insert(row1.clone()).unwrap();
            table.wait_for_ops().await;
            row1.id
        };

        // Phase 2: 2 valid inserts -> success update -> valid insert -> wait_for_ops
        {
            let engine = TwoUniqueIdxPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = TwoUniqueIdxWorkTable::load(engine).await.unwrap();

            let valid_row1 = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 1000,
                unique_b: 1001,
            };
            table.insert(valid_row1).unwrap();

            let valid_row2 = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 2000,
                unique_b: 2001,
            };
            table.insert(valid_row2).unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;

            let updated_row = TwoUniqueIdxRow {
                id: row1_pk,
                unique_a: 150,
                unique_b: 250,
            };

            let result = table.update(updated_row).await;
            assert!(result.is_ok());

            let valid_row3 = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 3000,
                unique_b: 3001,
            };
            table.insert(valid_row3).unwrap();

            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;
            assert!(wait_result.is_ok());
        }

        // Phase 3: Verify
        {
            let engine = TwoUniqueIdxPersistenceEngine::new(config).await.unwrap();
            let table = TwoUniqueIdxWorkTable::load(engine).await.unwrap();

            let row1 = table.select(row1_pk).unwrap();
            assert_eq!(row1.unique_a, 150);
            assert_eq!(row1.unique_b, 250);
            table.wait_for_ops().await;
        }
    });
}