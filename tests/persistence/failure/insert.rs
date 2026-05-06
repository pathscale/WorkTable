use super::*;
use crate::remove_dir_if_exists;

#[test]
fn test_insert_two_indexes_first_fail() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/insert_two_first",
        TwoUniqueIdxWorkTable::name_snake_case(),
        TwoUniqueIdxWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/insert_two_first".to_string()).await;

        // Phase 1: Setup - insert a row to populate first unique index
        let existing_a = {
            let engine = TwoUniqueIdxPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TwoUniqueIdxWorkTable::load(engine).await.unwrap();

            let row = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 100,
                unique_b: 200,
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.unique_a
        };

        // Phase 2: 2 valid inserts -> failure -> valid insert -> wait_for_ops
        {
            let engine = TwoUniqueIdxPersistenceEngine::new(config.clone()).await.unwrap();
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

            let failing_row = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: existing_a,
                unique_b: 300,
            };

            let result = table.insert(failing_row);
            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), WorkTableError::AlreadyExists(_)));

            let valid_row3 = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 3000,
                unique_b: 3001,
            };
            table.insert(valid_row3).unwrap();

            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;
            assert!(
                wait_result.is_ok(),
                "BUG: persistence blocked after insert failure on first index!"
            );
        }

        // Phase 3: Verify state
        {
            let engine = TwoUniqueIdxPersistenceEngine::new(config).await.unwrap();
            let table = TwoUniqueIdxWorkTable::load(engine).await.unwrap();

            // Can insert new valid row
            let new_row = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 4000,
                unique_b: 4001,
            };
            assert!(table.insert(new_row).is_ok());
            table.wait_for_ops().await;
        }
    });
}

#[test]
fn test_insert_two_indexes_second_fail() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/insert_two_second",
        TwoUniqueIdxWorkTable::name_snake_case(),
        TwoUniqueIdxWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/insert_two_second".to_string()).await;

        // Phase 1: Setup - insert a row to populate second unique index
        let existing_b = {
            let engine = TwoUniqueIdxPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TwoUniqueIdxWorkTable::load(engine).await.unwrap();

            let row = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 100,
                unique_b: 200,
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.unique_b
        };

        // Phase 2: 2 valid inserts -> failure -> valid insert -> wait_for_ops
        {
            let engine = TwoUniqueIdxPersistenceEngine::new(config.clone()).await.unwrap();
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

            let failing_row = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 300,
                unique_b: existing_b,
            };

            let result = table.insert(failing_row);
            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), WorkTableError::AlreadyExists(_)));

            let valid_row3 = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 3000,
                unique_b: 3001,
            };
            table.insert(valid_row3).unwrap();

            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;
            assert!(
                wait_result.is_ok(),
                "BUG: persistence blocked after insert failure on second index!"
            );
        }

        // Phase 3: Verify state - check rollback worked
        {
            let engine = TwoUniqueIdxPersistenceEngine::new(config).await.unwrap();
            let table = TwoUniqueIdxWorkTable::load(engine).await.unwrap();

            let new_row = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 300,
                unique_b: 301,
            };
            assert!(table.insert(new_row).is_ok(), "BUG: orphaned entry in unique_a_idx!");
            table.wait_for_ops().await;
        }
    });
}

#[test]
fn test_insert_three_indexes_first_fail() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/insert_three_first",
        ThreeUniqueIdxWorkTable::name_snake_case(),
        TwoUniqueIdxWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/insert_three_first".to_string()).await;

        // Phase 1: Setup
        let existing_a = {
            let engine = ThreeUniqueIdxPersistenceEngine::new(config.clone()).await.unwrap();
            let table = ThreeUniqueIdxWorkTable::load(engine).await.unwrap();

            let row = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 100,
                unique_b: 200,
                unique_c: 300,
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.unique_a
        };

        // Phase 2: 2 valid inserts -> failure -> valid insert -> wait_for_ops
        {
            let engine = ThreeUniqueIdxPersistenceEngine::new(config.clone()).await.unwrap();
            let table = ThreeUniqueIdxWorkTable::load(engine).await.unwrap();

            let valid_row1 = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 1000,
                unique_b: 1001,
                unique_c: 1002,
            };
            table.insert(valid_row1).unwrap();

            let valid_row2 = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 2000,
                unique_b: 2001,
                unique_c: 2002,
            };
            table.insert(valid_row2).unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;

            let failing_row = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: existing_a,
                unique_b: 400,
                unique_c: 500,
            };

            let result = table.insert(failing_row);
            assert!(result.is_err());

            let valid_row3 = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 3000,
                unique_b: 3001,
                unique_c: 3002,
            };
            table.insert(valid_row3).unwrap();

            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;
            assert!(wait_result.is_ok(), "BUG: persistence blocked!");
        }

        // Phase 3: Verify
        {
            let engine = ThreeUniqueIdxPersistenceEngine::new(config).await.unwrap();
            let table = ThreeUniqueIdxWorkTable::load(engine).await.unwrap();

            let new_row = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 4000,
                unique_b: 4001,
                unique_c: 4002,
            };
            assert!(table.insert(new_row).is_ok());
            table.wait_for_ops().await;
        }
    });
}

#[test]
fn test_insert_three_indexes_middle_fail() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/insert_three_middle",
        ThreeUniqueIdxWorkTable::name_snake_case(),
        TwoUniqueIdxWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/insert_three_middle".to_string()).await;

        // Phase 1: Setup
        let existing_b = {
            let engine = ThreeUniqueIdxPersistenceEngine::new(config.clone()).await.unwrap();
            let table = ThreeUniqueIdxWorkTable::load(engine).await.unwrap();

            let row = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 100,
                unique_b: 200,
                unique_c: 300,
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.unique_b
        };

        // Phase 2: 2 valid inserts -> failure -> valid insert -> wait_for_ops
        {
            let engine = ThreeUniqueIdxPersistenceEngine::new(config.clone()).await.unwrap();
            let table = ThreeUniqueIdxWorkTable::load(engine).await.unwrap();

            let valid_row1 = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 1000,
                unique_b: 1001,
                unique_c: 1002,
            };
            table.insert(valid_row1).unwrap();

            let valid_row2 = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 2000,
                unique_b: 2001,
                unique_c: 2002,
            };
            table.insert(valid_row2).unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;

            let failing_row = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 400,
                unique_b: existing_b,
                unique_c: 500,
            };

            let result = table.insert(failing_row);
            assert!(result.is_err());

            let valid_row3 = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 3000,
                unique_b: 3001,
                unique_c: 3002,
            };
            table.insert(valid_row3).unwrap();

            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;
            assert!(wait_result.is_ok(), "BUG: persistence blocked!");
        }

        // Phase 3: Verify - first index value should be rolled back
        {
            let engine = ThreeUniqueIdxPersistenceEngine::new(config).await.unwrap();
            let table = ThreeUniqueIdxWorkTable::load(engine).await.unwrap();

            let new_row = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 400,
                unique_b: 201,
                unique_c: 500,
            };
            assert!(table.insert(new_row).is_ok(), "BUG: orphaned entry in unique_a_idx!");
            table.wait_for_ops().await;
        }
    });
}

#[test]
fn test_insert_three_indexes_last_fail() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/insert_three_last",
        ThreeUniqueIdxWorkTable::name_snake_case(),
        TwoUniqueIdxWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/insert_three_last".to_string()).await;

        // Phase 1: Setup
        let existing_c = {
            let engine = ThreeUniqueIdxPersistenceEngine::new(config.clone()).await.unwrap();
            let table = ThreeUniqueIdxWorkTable::load(engine).await.unwrap();

            let row = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 100,
                unique_b: 200,
                unique_c: 300,
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.unique_c
        };

        // Phase 2: 2 valid inserts -> failure -> valid insert -> wait_for_ops
        {
            let engine = ThreeUniqueIdxPersistenceEngine::new(config.clone()).await.unwrap();
            let table = ThreeUniqueIdxWorkTable::load(engine).await.unwrap();

            let valid_row1 = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 1000,
                unique_b: 1001,
                unique_c: 1002,
            };
            table.insert(valid_row1).unwrap();

            let valid_row2 = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 2000,
                unique_b: 2001,
                unique_c: 2002,
            };
            table.insert(valid_row2).unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;

            let failing_row = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 400,
                unique_b: 500,
                unique_c: existing_c,
            };

            let result = table.insert(failing_row);
            assert!(result.is_err());

            let valid_row3 = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 3000,
                unique_b: 3001,
                unique_c: 3002,
            };
            table.insert(valid_row3).unwrap();

            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;
            assert!(wait_result.is_ok(), "BUG: persistence blocked!");
        }

        // Phase 3: Verify - first two index values should be rolled back
        {
            let engine = ThreeUniqueIdxPersistenceEngine::new(config).await.unwrap();
            let table = ThreeUniqueIdxWorkTable::load(engine).await.unwrap();

            let new_row = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 400,
                unique_b: 500,
                unique_c: 301,
            };
            assert!(table.insert(new_row).is_ok(), "BUG: orphaned entries in indexes!");
            table.wait_for_ops().await;
        }
    });
}

#[test]
fn test_insert_primary_duplicate() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/insert_primary_dup",
        PrimaryOnlyWorkTable::name_snake_case(),
        TwoUniqueIdxWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/insert_primary_dup".to_string()).await;

        // Phase 1: Setup
        let existing_pk = {
            let engine = PrimaryOnlyPersistenceEngine::new(config.clone()).await.unwrap();
            let table = PrimaryOnlyWorkTable::load(engine).await.unwrap();

            let row = PrimaryOnlyRow {
                id: table.get_next_pk().0,
                data: 100,
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.id
        };

        // Phase 2: 2 valid inserts -> failure -> valid insert -> wait_for_ops
        {
            let engine = PrimaryOnlyPersistenceEngine::new(config.clone()).await.unwrap();
            let table = PrimaryOnlyWorkTable::load(engine).await.unwrap();

            let valid_row1 = PrimaryOnlyRow {
                id: table.get_next_pk().0,
                data: 1000,
            };
            table.insert(valid_row1).unwrap();

            let valid_row2 = PrimaryOnlyRow {
                id: table.get_next_pk().0,
                data: 2000,
            };
            table.insert(valid_row2).unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;

            let failing_row = PrimaryOnlyRow {
                id: existing_pk, // Duplicate PK
                data: 200,
            };

            let result = table.insert(failing_row);
            assert!(result.is_err());

            let valid_row3 = PrimaryOnlyRow {
                id: table.get_next_pk().0,
                data: 3000,
            };
            table.insert(valid_row3).unwrap();

            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;
            assert!(wait_result.is_ok());
        }

        // Phase 3: Verify
        {
            let engine = PrimaryOnlyPersistenceEngine::new(config).await.unwrap();
            let table = PrimaryOnlyWorkTable::load(engine).await.unwrap();

            let original = table.select(existing_pk).unwrap();
            assert_eq!(original.data, 100);
            table.wait_for_ops().await;
        }
    });
}
