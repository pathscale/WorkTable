use super::*;
use crate::remove_dir_if_exists;

#[test]
fn test_reinsert_pk_mismatch() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/reinsert_pk_mismatch",
        TwoUniqueIdxWorkTable::name_snake_case(),
        TwoUniqueIdxWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/reinsert_pk_mismatch".to_string()).await;

        // Phase 1: Setup
        let existing_pk = {
            let engine = TwoUniqueIdxPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TwoUniqueIdxWorkTable::load(engine).await.unwrap();

            let row = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 100,
                unique_b: 200,
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.id
        };

        // Phase 2: 2 valid inserts -> failure update -> valid insert -> wait_for_ops
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

            let new_row = TwoUniqueIdxRow {
                id: existing_pk + 1,
                unique_a: 100,
                unique_b: 200,
            };

            let result = table.update(new_row).await;
            assert!(result.is_err());

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

            let original = table.select(existing_pk).unwrap();
            assert_eq!(original.unique_a, 100);
            assert_eq!(original.unique_b, 200);
            table.wait_for_ops().await;
        }
    });
}

#[test]
fn test_reinsert_two_indexes_first_fail() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/reinsert_two_first",
        TwoUniqueIdxWorkTable::name_snake_case(),
        TwoUniqueIdxWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/reinsert_two_first".to_string()).await;

        // Phase 1: Setup - insert two rows
        let (row1_pk, row2_pk, conflict_a) = {
            let engine = TwoUniqueIdxPersistenceEngine::new(config.clone()).await.unwrap();
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

            (row1.id, row2.id, row1.unique_a)
        };

        // Phase 2: 2 valid inserts -> failure update -> valid insert -> wait_for_ops
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

            let updated_row = TwoUniqueIdxRow {
                id: row2_pk,
                unique_a: conflict_a,
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
            assert!(wait_result.is_ok(), "BUG: persistence blocked after reinsert failure!");
        }

        // Phase 3: Verify
        {
            let engine = TwoUniqueIdxPersistenceEngine::new(config).await.unwrap();
            let table = TwoUniqueIdxWorkTable::load(engine).await.unwrap();

            // Both rows unchanged
            let row1 = table.select(row1_pk).unwrap();
            assert_eq!(row1.unique_a, 100);
            assert_eq!(row1.unique_b, 200);

            let row2 = table.select(row2_pk).unwrap();
            assert_eq!(row2.unique_a, 300);
            assert_eq!(row2.unique_b, 400);

            table.wait_for_ops().await;
        }
    });
}

#[test]
fn test_reinsert_two_indexes_second_fail() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/reinsert_two_second",
        TwoUniqueIdxWorkTable::name_snake_case(),
        TwoUniqueIdxWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/reinsert_two_second".to_string()).await;

        // Phase 1: Setup
        let conflict_b = {
            let engine = TwoUniqueIdxPersistenceEngine::new(config.clone()).await.unwrap();
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

            row1.unique_b
        };

        // Phase 2: 2 valid inserts -> failure update -> valid insert -> wait_for_ops
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

            let row2 = table.select_by_unique_a(300).unwrap();

            let updated_row = TwoUniqueIdxRow {
                id: row2.id,
                unique_a: 500,
                unique_b: conflict_b,
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

        // Phase 3: Verify rollback worked
        {
            let engine = TwoUniqueIdxPersistenceEngine::new(config).await.unwrap();
            let table = TwoUniqueIdxWorkTable::load(engine).await.unwrap();

            let new_row = TwoUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 500,
                unique_b: 600,
            };
            assert!(table.insert(new_row).is_ok(), "BUG: orphaned entry in unique_a_idx!");
            table.wait_for_ops().await;
        }
    });
}

#[test]
fn test_reinsert_three_indexes_first_fail() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/reinsert_three_first",
        ThreeUniqueIdxWorkTable::name_snake_case(),
        TwoUniqueIdxWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/reinsert_three_first".to_string()).await;

        // Phase 1: Setup
        let conflict_a = {
            let engine = ThreeUniqueIdxPersistenceEngine::new(config.clone()).await.unwrap();
            let table = ThreeUniqueIdxWorkTable::load(engine).await.unwrap();

            let row1 = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 100,
                unique_b: 200,
                unique_c: 300,
            };
            table.insert(row1.clone()).unwrap();

            let row2 = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 400,
                unique_b: 500,
                unique_c: 600,
            };
            table.insert(row2.clone()).unwrap();
            table.wait_for_ops().await;

            row1.unique_a
        };

        // Phase 2: 2 valid inserts -> failure update -> valid insert -> wait_for_ops
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

            let row2 = table.select_by_unique_a(400).unwrap();

            let updated_row = ThreeUniqueIdxRow {
                id: row2.id,
                unique_a: conflict_a,
                unique_b: 700,
                unique_c: 800,
            };

            let result = table.update(updated_row).await;
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

            let row2 = table.select_by_unique_a(400).unwrap();
            assert_eq!(row2.unique_a, 400);
            assert_eq!(row2.unique_b, 500);
            assert_eq!(row2.unique_c, 600);
            table.wait_for_ops().await;
        }
    });
}

#[test]
fn test_reinsert_three_indexes_middle_fail() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/reinsert_three_middle",
        ThreeUniqueIdxWorkTable::name_snake_case(),
        TwoUniqueIdxWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/reinsert_three_middle".to_string()).await;

        // Phase 1: Setup
        let conflict_b = {
            let engine = ThreeUniqueIdxPersistenceEngine::new(config.clone()).await.unwrap();
            let table = ThreeUniqueIdxWorkTable::load(engine).await.unwrap();

            let row1 = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 100,
                unique_b: 200,
                unique_c: 300,
            };
            table.insert(row1.clone()).unwrap();

            let row2 = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 400,
                unique_b: 500,
                unique_c: 600,
            };
            table.insert(row2.clone()).unwrap();
            table.wait_for_ops().await;

            row1.unique_b
        };

        // Phase 2: 2 valid inserts -> failure update -> valid insert -> wait_for_ops
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

            let row2 = table.select_by_unique_a(400).unwrap();

            let updated_row = ThreeUniqueIdxRow {
                id: row2.id,
                unique_a: 700,
                unique_b: conflict_b,
                unique_c: 800,
            };

            let result = table.update(updated_row).await;
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

        // Phase 3: Verify rollback
        {
            let engine = ThreeUniqueIdxPersistenceEngine::new(config).await.unwrap();
            let table = ThreeUniqueIdxWorkTable::load(engine).await.unwrap();

            let new_row = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 700,
                unique_b: 900,
                unique_c: 1000,
            };
            assert!(table.insert(new_row).is_ok(), "BUG: orphaned entry!");
            table.wait_for_ops().await;
        }
    });
}

#[test]
fn test_reinsert_three_indexes_last_fail() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/failure/reinsert_three_last",
        ThreeUniqueIdxWorkTable::name_snake_case(),
        TwoUniqueIdxWorkTable::version(),
    );

    let runtime = get_runtime();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/failure/reinsert_three_last".to_string()).await;

        // Phase 1: Setup
        let conflict_c = {
            let engine = ThreeUniqueIdxPersistenceEngine::new(config.clone()).await.unwrap();
            let table = ThreeUniqueIdxWorkTable::load(engine).await.unwrap();

            let row1 = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 100,
                unique_b: 200,
                unique_c: 300,
            };
            table.insert(row1.clone()).unwrap();

            let row2 = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 400,
                unique_b: 500,
                unique_c: 600,
            };
            table.insert(row2.clone()).unwrap();
            table.wait_for_ops().await;

            row1.unique_c
        };

        // Phase 2: 2 valid inserts -> failure update -> valid insert -> wait_for_ops
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

            let row2 = table.select_by_unique_a(400).unwrap();

            let updated_row = ThreeUniqueIdxRow {
                id: row2.id,
                unique_a: 700,
                unique_b: 800,
                unique_c: conflict_c,
            };

            let result = table.update(updated_row).await;
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

        // Phase 3: Verify rollback
        {
            let engine = ThreeUniqueIdxPersistenceEngine::new(config).await.unwrap();
            let table = ThreeUniqueIdxWorkTable::load(engine).await.unwrap();

            let new_row = ThreeUniqueIdxRow {
                id: table.get_next_pk().0,
                unique_a: 700,
                unique_b: 800,
                unique_c: 900,
            };
            assert!(table.insert(new_row).is_ok(), "BUG: orphaned entries!");
            table.wait_for_ops().await;
        }
    });
}
