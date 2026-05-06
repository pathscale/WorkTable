use crate::remove_dir_if_exists;
use std::time::Duration;
use tokio::time::timeout;

use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable_codegen::worktable;

worktable!(
    name: StringReRead,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        first: String,
        second: String,
        third: String,
        last: String,
    },
    indexes: {
        first_idx: first,
        second_idx: second unique,
    },
    queries: {
        delete: {
            BySecond() by second,
            ByFirst() by first,
        }
    }
);

#[test]
fn test_key() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/key/key",
        StringReReadWorkTable::name_snake_case(),
        StringReReadWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/key/key".to_string()).await;

        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third".to_string(),
                    second: "second".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();
            table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third_again".to_string(),
                    second: "second_again".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();

            table.wait_for_ops().await
        }
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            table
                .insert(StringReReadRow {
                    first: "first_last".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third_last".to_string(),
                    second: "second_last".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();
            table.wait_for_ops().await
        }
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            assert_eq!(table.select_all().execute().unwrap().len(), 3);
        }
    })
}

#[test]
fn test_key_delete_scenario() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/key/delete_scenario",
        StringReReadWorkTable::name_snake_case(),
        StringReReadWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/key/delete_scenario".to_string()).await;

        let (pk0, pk) = {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            let pk0 = table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third".to_string(),
                    second: "second".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();
            let pk = table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third_again".to_string(),
                    second: "second_again".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();

            table.wait_for_ops().await;
            (pk0, pk)
        };
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            table.delete(pk.clone()).await.unwrap();

            table.wait_for_ops().await
        }
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            assert_eq!(table.select_all().execute().unwrap().len(), 1);

            assert!(table.select(pk).is_none());
            assert_eq!(table.select_by_first("first".to_string()).execute().unwrap().len(), 1);
            assert!(table.select_by_second("second_again".to_string()).is_none());
            table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third_again".to_string(),
                    second: "second_again".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();

            table.wait_for_ops().await
        }
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            table.delete(pk0.clone()).await.unwrap();

            table.wait_for_ops().await
        }
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            assert_eq!(table.select_all().execute().unwrap().len(), 1);

            assert!(table.select(pk0).is_none());
            assert_eq!(table.select_by_first("first".to_string()).execute().unwrap().len(), 1);
            assert!(table.select_by_second("second".to_string()).is_none());
        }
    })
}

// #[test]
// fn test_key_delete_scenario_multiple() {
//     for _ in 0..100 {
//         test_key_delete_by_unique()
//     }
// }

#[test]
fn test_key_delete() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/key/delete",
        StringReReadWorkTable::name_snake_case(),
        StringReReadWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/key/delete".to_string()).await;

        let pk = {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third".to_string(),
                    second: "second".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();
            let pk = table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third_again".to_string(),
                    second: "second_again".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();

            table.wait_for_ops().await;
            pk
        };
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            table.delete(pk.clone()).await.unwrap();

            table.wait_for_ops().await
        }
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            assert_eq!(table.select_all().execute().unwrap().len(), 1);

            assert!(table.select(pk).is_none());
            assert_eq!(table.select_by_first("first".to_string()).execute().unwrap().len(), 1);
            assert!(table.select_by_second("second_again".to_string()).is_none())
        }
    })
}

#[test]
fn test_key_delete_all() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/key/delete_all",
        StringReReadWorkTable::name_snake_case(),
        StringReReadWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/key/delete_all".to_string()).await;

        let (pk0, pk1) = {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            let pk0 = table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third".to_string(),
                    second: "second".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();
            let pk1 = table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third_again".to_string(),
                    second: "second_again".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();

            table.wait_for_ops().await;
            (pk0, pk1)
        };
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            table.delete(pk0.clone()).await.unwrap();
            table.delete(pk1.clone()).await.unwrap();

            table.wait_for_ops().await
        }
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            assert_eq!(table.select_all().execute().unwrap().len(), 0);

            assert!(table.select(pk0).is_none());
            assert!(table.select(pk1).is_none());
            assert_eq!(table.select_by_first("first".to_string()).execute().unwrap().len(), 0);
            assert!(table.select_by_second("second_again".to_string()).is_none());
            assert!(table.select_by_second("second".to_string()).is_none())
        }
    })
}

#[test]
fn test_key_delete_all_and_insert() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/key/delete_all_and_insert",
        StringReReadWorkTable::name_snake_case(),
        StringReReadWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/key/delete_all_and_insert".to_string()).await;

        let (pk0, pk1) = {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            let pk0 = table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third".to_string(),
                    second: "second".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();
            let pk1 = table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third_again".to_string(),
                    second: "second_again".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();

            table.wait_for_ops().await;
            (pk0, pk1)
        };
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            table.delete(pk0.clone()).await.unwrap();
            table.delete(pk1.clone()).await.unwrap();

            table.wait_for_ops().await
        }
        let pk = {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            assert_eq!(table.select_all().execute().unwrap().len(), 0);

            let pk = table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third_again".to_string(),
                    second: "second".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();

            table.wait_for_ops().await;
            pk
        };
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();

            assert_eq!(table.select_all().execute().unwrap().len(), 1);

            assert!(table.select(pk).is_some());
            assert_eq!(table.select_by_first("first".to_string()).execute().unwrap().len(), 1);
            assert!(table.select_by_second("second".to_string()).is_some())
        }
    })
}

#[test]
fn test_key_delete_by_unique() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/key/delete_unique",
        StringReReadWorkTable::name_snake_case(),
        StringReReadWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/key/delete_unique".to_string()).await;

        let pk = {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third".to_string(),
                    second: "second".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();
            let pk = table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third_again".to_string(),
                    second: "second_again".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();

            table.wait_for_ops().await;
            pk
        };
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            table.delete_by_second("second_again".to_string()).await.unwrap();

            table.wait_for_ops().await
        }
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            assert_eq!(table.select_all().execute().unwrap().len(), 1);

            assert!(table.select(pk).is_none());
            assert_eq!(table.select_by_first("first".to_string()).execute().unwrap().len(), 1);
            assert!(table.select_by_second("second_again".to_string()).is_none())
        }
    })
}

#[test]
fn test_key_delete_by_non_unique() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/key/delete_non_unique",
        StringReReadWorkTable::name_snake_case(),
        StringReReadWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/key/delete_non_unique".to_string()).await;

        let (pk0, pk1) = {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            let pk0 = table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third".to_string(),
                    second: "second".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();
            let pk1 = table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third_again".to_string(),
                    second: "second_again".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();

            table.wait_for_ops().await;
            (pk0, pk1)
        };
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            table.delete_by_first("first".to_string()).await.unwrap();

            table.wait_for_ops().await
        }
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            assert_eq!(table.select_all().execute().unwrap().len(), 0);

            assert!(table.select(pk0).is_none());
            assert!(table.select(pk1).is_none());
            assert_eq!(table.select_by_first("first".to_string()).execute().unwrap().len(), 0);
            assert!(table.select_by_second("second".to_string()).is_none());
            assert!(table.select_by_second("second_again".to_string()).is_none())
        }
    })
}

#[test]
fn test_toc_not_updated_when_index_value_same_but_link_changes() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/key/toc_link_bug",
        StringReReadWorkTable::name_snake_case(),
        StringReReadWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/key/toc_link_bug".to_string()).await;

        let pk1 = {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();

            let pk1 = table
                .insert(StringReReadRow {
                    first: "same_first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third_1".to_string(),
                    second: "second_1".to_string(),
                    last: "last_1".to_string(),
                })
                .unwrap();

            table
                .insert(StringReReadRow {
                    first: "same_first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third_2".to_string(),
                    second: "second_2".to_string(),
                    last: "last_2".to_string(),
                })
                .unwrap();

            table.wait_for_ops().await;
            pk1
        };

        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();

            table
                .update(StringReReadRow {
                    first: "same_first".to_string(),
                    id: pk1.into(),
                    third: "third_updated".to_string(),
                    second: "second_1".to_string(),
                    last: "last_updated".to_string(),
                })
                .await
                .unwrap();

            table.wait_for_ops().await;
        }

        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();

            let result = table.insert(StringReReadRow {
                first: "same_first".to_string(),
                id: table.get_next_pk().into(),
                third: "third_3".to_string(),
                second: "second_3".to_string(),
                last: "last_3".to_string(),
            });

            assert!(result.is_ok(), "TOC entry is stale after update with same index value");

            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;
            if wait_result.is_err() {
                panic!("BUG DETECTED: Persistence system is stuck - wait_for_ops() timed out");
            }
        }

        {
            let engine = StringReReadPersistenceEngine::new(config).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();

            assert_eq!(table.select_all().execute().unwrap().len(), 3);
            assert_eq!(
                table.select_by_first("same_first".to_string()).execute().unwrap().len(),
                3
            );
        }
    });
}

#[test]
fn test_big_amount_reread() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/key/big_amount",
        StringReReadWorkTable::name_snake_case(),
        StringReReadWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/key/big_amount".to_string()).await;

        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            for i in 0..1000 {
                table
                    .insert(StringReReadRow {
                        first: format!("first_{}", i % 100),
                        id: table.get_next_pk().into(),
                        third: format!("third_{i}"),
                        second: format!("second_{i}"),
                        last: format!("_________________________last_____________________{i}"),
                    })
                    .unwrap();
            }

            table.wait_for_ops().await
        }
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            table
                .insert(StringReReadRow {
                    first: "first_last".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third_last".to_string(),
                    second: "second_last".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();

            table.wait_for_ops().await
        }
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();
            assert_eq!(table.select_all().execute().unwrap().len(), 1001);
            assert!(table.select_by_second("second_last".to_string()).is_some());
        }
    })
}

#[test]
fn test_unique_index_same_value_link_changes() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/key/unique_link_change",
        StringReReadWorkTable::name_snake_case(),
        StringReReadWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/key/unique_link_change".to_string()).await;

        // Phase 1: Insert initial value
        let pk1 = {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();

            let pk1 = table
                .insert(StringReReadRow {
                    first: "first_1".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third_1".to_string(),
                    second: "unique_second".to_string(),
                    last: "last_1".to_string(),
                })
                .unwrap();

            table.wait_for_ops().await;
            pk1
        };

        // Phase 2: Update (same unique value) + Insert new (same block)
        {
            let engine = StringReReadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();

            // Update: same second value, other fields change
            table
                .update(StringReReadRow {
                    first: "first_updated".to_string(),
                    id: pk1.into(),
                    third: "third_updated".to_string(),
                    second: "unique_second".to_string(), // SAME unique index value
                    last: "last_updated".to_string(),
                })
                .await
                .unwrap();

            // Insert new row with different unique value
            let result = table.insert(StringReReadRow {
                first: "first_2".to_string(),
                id: table.get_next_pk().into(),
                third: "third_2".to_string(),
                second: "unique_second_2".to_string(),
                last: "last_2".to_string(),
            });

            assert!(
                result.is_ok(),
                "Insert should succeed after update with same unique index value"
            );

            // Timeout check for stuck persistence
            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;
            assert!(
                wait_result.is_ok(),
                "BUG: persistence blocked after unique index update"
            );
        }

        // Phase 3: Load and verify all data
        {
            let engine = StringReReadPersistenceEngine::new(config).await.unwrap();
            let table = StringReReadWorkTable::load(engine).await.unwrap();

            assert_eq!(table.select_all().execute().unwrap().len(), 2);
            assert!(table.select_by_second("unique_second".to_string()).is_some());
            assert!(table.select_by_second("unique_second_2".to_string()).is_some());

            let row1 = table.select_by_second("unique_second".to_string()).unwrap();
            assert_eq!(row1.first, "first_updated");
        }
    });
}
