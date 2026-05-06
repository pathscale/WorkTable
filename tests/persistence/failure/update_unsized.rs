/// Unsized field update failure tests
use super::*;
use crate::remove_dir_if_exists;

#[test]
fn test_update_unsized_same_size() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/update_unsized_same_size",
        NonUniqueUnsizedWorkTable::name_snake_case(),
        NonUniqueUnsizedWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/update_unsized_same_size".to_string()).await;

        // Phase 1: Setup
        let (row1_pk, row2_pk, row3_pk) = {
            let engine = NonUniqueUnsizedPersistenceEngine::new(config.clone()).await.unwrap();
            let table = NonUniqueUnsizedWorkTable::load(engine).await.unwrap();

            let row1 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 1,
                unique_value: 10,
                name: "aaa".to_string(),
            };
            table.insert(row1.clone()).unwrap();

            let row2 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 1,
                unique_value: 20,
                name: "bbb".to_string(),
            };
            table.insert(row2.clone()).unwrap();

            let row3 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 1,
                unique_value: 30,
                name: "ccc".to_string(),
            };
            table.insert(row3.clone()).unwrap();
            table.wait_for_ops().await;

            (row1.id, row2.id, row3.id)
        };

        // Phase 2: 2 valid inserts -> failure update -> valid insert -> wait_for_ops
        {
            let engine = NonUniqueUnsizedPersistenceEngine::new(config.clone()).await.unwrap();
            let table = NonUniqueUnsizedWorkTable::load(engine).await.unwrap();

            let valid_row1 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 100,
                unique_value: 1000,
                name: "xxx".to_string(),
            };
            table.insert(valid_row1).unwrap();

            let valid_row2 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 200,
                unique_value: 2000,
                name: "yyy".to_string(),
            };
            table.insert(valid_row2).unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;

            let query = NameAndValueByCategoryQuery {
                name: "xxx".to_string(),
                unique_value: 99,
            };
            let result = table.update_name_and_value_by_category(query, 1).await;
            assert!(result.is_err());

            let valid_row3 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 300,
                unique_value: 3000,
                name: "zzz".to_string(),
            };
            table.insert(valid_row3).unwrap();

            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;
            assert!(wait_result.is_ok(), "BUG: persistence blocked!");
        }

        // Phase 3: Verify
        {
            let engine = NonUniqueUnsizedPersistenceEngine::new(config).await.unwrap();
            let table = NonUniqueUnsizedWorkTable::load(engine).await.unwrap();

            assert!(table.select(row1_pk).is_some());
            assert!(table.select(row2_pk).is_some());
            assert!(table.select(row3_pk).is_some());
            table.wait_for_ops().await;
        }
    });
}

#[test]
fn test_update_unsized_larger_all_success() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/update_unsized_larger_success",
        NonUniqueUnsizedWorkTable::name_snake_case(),
        NonUniqueUnsizedWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/update_unsized_larger_success".to_string()).await;

        // Phase 1: Setup
        let row_pk = {
            let engine = NonUniqueUnsizedPersistenceEngine::new(config.clone()).await.unwrap();
            let table = NonUniqueUnsizedWorkTable::load(engine).await.unwrap();

            let row = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 1,
                unique_value: 10,
                name: "a".to_string(),
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.id
        };

        // Phase 2: 2 valid inserts -> success update -> valid insert -> wait_for_ops
        {
            let engine = NonUniqueUnsizedPersistenceEngine::new(config.clone()).await.unwrap();
            let table = NonUniqueUnsizedWorkTable::load(engine).await.unwrap();

            let valid_row1 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 100,
                unique_value: 1000,
                name: "xxx".to_string(),
            };
            table.insert(valid_row1).unwrap();

            let valid_row2 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 200,
                unique_value: 2000,
                name: "yyy".to_string(),
            };
            table.insert(valid_row2).unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;

            let query = NameAndValueByCategoryQuery {
                name: "larger_name".to_string(),
                unique_value: 20,
            };

            let result = table.update_name_and_value_by_category(query, 1).await;
            assert!(result.is_ok());

            let valid_row3 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 300,
                unique_value: 3000,
                name: "zzz".to_string(),
            };
            table.insert(valid_row3).unwrap();

            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;
            assert!(wait_result.is_ok());
        }

        // Phase 3: Verify
        {
            let engine = NonUniqueUnsizedPersistenceEngine::new(config).await.unwrap();
            let table = NonUniqueUnsizedWorkTable::load(engine).await.unwrap();

            let row = table.select(row_pk).unwrap();
            assert_eq!(row.unique_value, 20);
            table.wait_for_ops().await;
        }
    });
}

#[test]
fn test_update_unsized_larger_middle_fail() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/update_unsized_larger_middle",
        NonUniqueUnsizedWorkTable::name_snake_case(),
        NonUniqueUnsizedWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/update_unsized_larger_middle".to_string()).await;

        // Phase 1: Setup
        let (conflict_pk, row2_pk, row3_pk) = {
            let engine = NonUniqueUnsizedPersistenceEngine::new(config.clone()).await.unwrap();
            let table = NonUniqueUnsizedWorkTable::load(engine).await.unwrap();

            let conflict = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 100,
                unique_value: 99,
                name: "x".to_string(),
            };
            table.insert(conflict.clone()).unwrap();

            let row1 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 1,
                unique_value: 10,
                name: "a".to_string(),
            };
            table.insert(row1.clone()).unwrap();

            let row2 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 1,
                unique_value: 20,
                name: "b".to_string(),
            };
            table.insert(row2.clone()).unwrap();

            let row3 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 1,
                unique_value: 30,
                name: "c".to_string(),
            };
            table.insert(row3.clone()).unwrap();
            table.wait_for_ops().await;

            (conflict.id, row2.id, row3.id)
        };

        // Phase 2: 2 valid inserts -> failure update -> valid insert -> wait_for_ops
        {
            let engine = NonUniqueUnsizedPersistenceEngine::new(config.clone()).await.unwrap();
            let table = NonUniqueUnsizedWorkTable::load(engine).await.unwrap();

            let valid_row1 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 200,
                unique_value: 1000,
                name: "xxx".to_string(),
            };
            table.insert(valid_row1).unwrap();

            let valid_row2 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 300,
                unique_value: 2000,
                name: "yyy".to_string(),
            };
            table.insert(valid_row2).unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;

            let query = NameAndValueByCategoryQuery {
                name: "larger_name".to_string(),
                unique_value: 99,
            };

            let result = table.update_name_and_value_by_category(query, 1).await;
            assert!(result.is_err());

            let valid_row3 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 400,
                unique_value: 3000,
                name: "zzz".to_string(),
            };
            table.insert(valid_row3).unwrap();

            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;
            assert!(wait_result.is_ok(), "BUG: persistence blocked!");
        }

        // Phase 3: Verify
        {
            let engine = NonUniqueUnsizedPersistenceEngine::new(config).await.unwrap();
            let table = NonUniqueUnsizedWorkTable::load(engine).await.unwrap();

            let row2 = table.select(row2_pk).unwrap();
            assert_eq!(row2.name, "b".to_string());
            assert_eq!(row2.unique_value, 20);

            let row3 = table.select(row3_pk).unwrap();
            assert_eq!(row3.name, "c".to_string());
            assert_eq!(row3.unique_value, 30);

            let conflict = table.select(conflict_pk).unwrap();
            assert_eq!(conflict.unique_value, 99);
            table.wait_for_ops().await;
        }
    });
}

#[test]
fn test_update_unsized_larger_last_fail() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/update_unsized_larger_last",
        NonUniqueUnsizedWorkTable::name_snake_case(),
        NonUniqueUnsizedWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/update_unsized_larger_last".to_string()).await;

        // Phase 1: Setup
        let (row1_pk, row2_pk) = {
            let engine = NonUniqueUnsizedPersistenceEngine::new(config.clone()).await.unwrap();
            let table = NonUniqueUnsizedWorkTable::load(engine).await.unwrap();

            let row1 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 1,
                unique_value: 10,
                name: "a".to_string(),
            };
            table.insert(row1.clone()).unwrap();

            let row2 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 1,
                unique_value: 20,
                name: "b".to_string(),
            };
            table.insert(row2.clone()).unwrap();
            table.wait_for_ops().await;

            (row1.id, row2.id)
        };

        // Phase 2: 2 valid inserts -> failure update -> valid insert -> wait_for_ops
        {
            let engine = NonUniqueUnsizedPersistenceEngine::new(config.clone()).await.unwrap();
            let table = NonUniqueUnsizedWorkTable::load(engine).await.unwrap();

            let valid_row1 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 100,
                unique_value: 1000,
                name: "xxx".to_string(),
            };
            table.insert(valid_row1).unwrap();

            let valid_row2 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 200,
                unique_value: 2000,
                name: "yyy".to_string(),
            };
            table.insert(valid_row2).unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;

            let query = NameAndValueByCategoryQuery {
                name: "larger".to_string(),
                unique_value: 99,
            };

            let result = table.update_name_and_value_by_category(query, 1).await;
            assert!(result.is_err());

            let valid_row3 = NonUniqueUnsizedRow {
                id: table.get_next_pk().0,
                category: 300,
                unique_value: 3000,
                name: "zzz".to_string(),
            };
            table.insert(valid_row3).unwrap();

            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;
            assert!(wait_result.is_ok(), "BUG: persistence blocked!");
        }

        // Phase 3: Verify
        {
            let engine = NonUniqueUnsizedPersistenceEngine::new(config).await.unwrap();
            let table = NonUniqueUnsizedWorkTable::load(engine).await.unwrap();

            // Row2: unchanged (failed to update)
            let row2 = table.select(row2_pk).unwrap();
            assert_eq!(row2.name, "b".to_string());
            assert_eq!(row2.unique_value, 20);

            // Row1: was updated before row2 failed
            let row1 = table.select(row1_pk).unwrap();
            assert_eq!(row1.name, "larger".to_string());
            assert_eq!(row1.unique_value, 99);
            table.wait_for_ops().await;
        }
    });
}
