/// Non-unique index update failure tests
use super::*;
use crate::remove_dir_if_exists;

#[test]
fn test_update_non_unique_middle_fail() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/update_non_unique_middle",
        MixedIdxWorkTable::name_snake_case(),
        MixedIdxWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/update_non_unique_middle".to_string()).await;

        // Phase 1: Setup
        let (row1_pk, row2_pk, row3_pk) = {
            let engine = MixedIdxPersistenceEngine::new(config.clone()).await.unwrap();
            let table = MixedIdxWorkTable::load(engine).await.unwrap();

            let row1 = MixedIdxRow {
                id: table.get_next_pk().0,
                category: 1,
                unique_value: 10,
                data: 100,
            };
            table.insert(row1.clone()).unwrap();

            let row2 = MixedIdxRow {
                id: table.get_next_pk().0,
                category: 1,
                unique_value: 20,
                data: 200,
            };
            table.insert(row2.clone()).unwrap();

            let row3 = MixedIdxRow {
                id: table.get_next_pk().0,
                category: 1,
                unique_value: 30,
                data: 300,
            };
            table.insert(row3.clone()).unwrap();
            table.wait_for_ops().await;

            (row1.id, row2.id, row3.id)
        };

        // Phase 2: 2 valid inserts -> failure bulk update -> valid insert -> wait_for_ops
        {
            let engine = MixedIdxPersistenceEngine::new(config.clone()).await.unwrap();
            let table = MixedIdxWorkTable::load(engine).await.unwrap();

            let valid_row1 = MixedIdxRow {
                id: table.get_next_pk().0,
                category: 100,
                unique_value: 1000,
                data: 1000,
            };
            table.insert(valid_row1).unwrap();

            let valid_row2 = MixedIdxRow {
                id: table.get_next_pk().0,
                category: 200,
                unique_value: 2000,
                data: 2000,
            };
            table.insert(valid_row2).unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;

            let query = UniqueValueByCategoryQuery { unique_value: 99 };
            let result = table.update_unique_value_by_category(query, 1).await;
            assert!(result.is_err());

            let valid_row3 = MixedIdxRow {
                id: table.get_next_pk().0,
                category: 300,
                unique_value: 3000,
                data: 3000,
            };
            table.insert(valid_row3).unwrap();

            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;
            assert!(wait_result.is_ok(), "BUG: persistence blocked!");
        }

        // Phase 3: Verify
        {
            let engine = MixedIdxPersistenceEngine::new(config).await.unwrap();
            let table = MixedIdxWorkTable::load(engine).await.unwrap();

            assert!(table.select(row1_pk).is_some());
            assert!(table.select(row2_pk).is_some());
            assert!(table.select(row3_pk).is_some());
            table.wait_for_ops().await;
        }
    });
}

#[test]
fn test_update_non_unique_last_fail() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/update_non_unique_last",
        MixedIdxWorkTable::name_snake_case(),
        MixedIdxWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/update_non_unique_last".to_string()).await;

        // Phase 1: Setup
        let conflict_pk = {
            let engine = MixedIdxPersistenceEngine::new(config.clone()).await.unwrap();
            let table = MixedIdxWorkTable::load(engine).await.unwrap();

            let conflict_row = MixedIdxRow {
                id: table.get_next_pk().0,
                category: 100,
                unique_value: 99,
                data: 0,
            };
            table.insert(conflict_row.clone()).unwrap();

            let row1 = MixedIdxRow {
                id: table.get_next_pk().0,
                category: 1,
                unique_value: 10,
                data: 100,
            };
            table.insert(row1.clone()).unwrap();

            let row2 = MixedIdxRow {
                id: table.get_next_pk().0,
                category: 1,
                unique_value: 20,
                data: 200,
            };
            table.insert(row2.clone()).unwrap();

            let row3 = MixedIdxRow {
                id: table.get_next_pk().0,
                category: 1,
                unique_value: 30,
                data: 300,
            };
            table.insert(row3.clone()).unwrap();
            table.wait_for_ops().await;

            conflict_row.id
        };

        // Phase 2: 2 valid inserts -> failure bulk update -> valid insert -> wait_for_ops
        {
            let engine = MixedIdxPersistenceEngine::new(config.clone()).await.unwrap();
            let table = MixedIdxWorkTable::load(engine).await.unwrap();

            let valid_row1 = MixedIdxRow {
                id: table.get_next_pk().0,
                category: 200,
                unique_value: 1000,
                data: 1000,
            };
            table.insert(valid_row1).unwrap();

            let valid_row2 = MixedIdxRow {
                id: table.get_next_pk().0,
                category: 300,
                unique_value: 2000,
                data: 2000,
            };
            table.insert(valid_row2).unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;

            let query = UniqueValueByCategoryQuery { unique_value: 99 };
            let result = table.update_unique_value_by_category(query, 1).await;
            assert!(result.is_err());

            let valid_row3 = MixedIdxRow {
                id: table.get_next_pk().0,
                category: 400,
                unique_value: 3000,
                data: 3000,
            };
            table.insert(valid_row3).unwrap();

            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;
            assert!(wait_result.is_ok(), "BUG: persistence blocked!");
        }

        // Phase 3: Verify
        {
            let engine = MixedIdxPersistenceEngine::new(config).await.unwrap();
            let table = MixedIdxWorkTable::load(engine).await.unwrap();

            let conflict = table.select(conflict_pk).unwrap();
            assert_eq!(conflict.unique_value, 99);
            table.wait_for_ops().await;
        }
    });
}
