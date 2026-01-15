use worktable::prelude::*;
use worktable_codegen::worktable;

use crate::remove_dir_if_exists;

worktable! (
    name: TestOptionSync,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        test: u64 optional,
        another: u64,
        exchange: i32,
    },
    indexes: {
        another_idx: another unique,
        exchnage_idx: exchange,
    },
    queries: {
        update: {
            TestById(test) by id,
            TestByAnother(test) by another,
            TestByExchange(test) by exchange,
        }
    }
);

#[test]
fn test_option_insert_none_sync() {
    let config = PersistenceConfig::new(
        "tests/data/option_sync/insert_none",
        "tests/data/option_sync/insert_none",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/option_sync/insert_none".to_string()).await;

        let pk = {
            let table = TestOptionSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestOptionSyncRow {
                id: table.get_next_pk().0,
                test: None,
                another: 1,
                exchange: 1,
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.id
        };

        {
            let table = TestOptionSyncWorkTable::load_from_file(config)
                .await
                .unwrap();
            let selected = table.select(pk).unwrap();
            assert_eq!(selected.test, None);
            assert_eq!(table.0.pk_gen.get_state(), pk + 1);
        }
    });
}

#[test]
fn test_option_insert_some_sync() {
    let config = PersistenceConfig::new(
        "tests/data/option_sync/insert_some",
        "tests/data/option_sync/insert_some",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/option_sync/insert_some".to_string()).await;

        let pk = {
            let table = TestOptionSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestOptionSyncRow {
                id: table.get_next_pk().0,
                test: Some(42),
                another: 1,
                exchange: 1,
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.id
        };

        {
            let table = TestOptionSyncWorkTable::load_from_file(config)
                .await
                .unwrap();
            let selected = table.select(pk).unwrap();
            assert_eq!(selected.test, Some(42));
            assert_eq!(table.0.pk_gen.get_state(), pk + 1);
        }
    });
}

#[test]
fn test_option_update_full_sync() {
    let config = PersistenceConfig::new(
        "tests/data/option_sync/update_full",
        "tests/data/option_sync/update_full",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/option_sync/update_full".to_string()).await;

        let pk = {
            let table = TestOptionSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestOptionSyncRow {
                id: table.get_next_pk().0,
                test: None,
                another: 1,
                exchange: 1,
            };
            table.insert(row.clone()).unwrap();

            table
                .update(TestOptionSyncRow {
                    id: row.id,
                    test: Some(100),
                    another: 1,
                    exchange: 1,
                })
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };

        {
            let table = TestOptionSyncWorkTable::load_from_file(config)
                .await
                .unwrap();
            let selected = table.select(pk).unwrap();
            assert_eq!(selected.test, Some(100));
            assert_eq!(table.0.pk_gen.get_state(), pk + 1);
        }
    });
}

#[test]
fn test_option_update_by_id_sync() {
    let config = PersistenceConfig::new(
        "tests/data/option_sync/update_by_id",
        "tests/data/option_sync/update_by_id",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/option_sync/update_by_id".to_string()).await;

        let pk = {
            let table = TestOptionSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestOptionSyncRow {
                id: table.get_next_pk().0,
                test: None,
                another: 1,
                exchange: 1,
            };
            table.insert(row.clone()).unwrap();

            table
                .update_test_by_id(TestByIdQuery { test: Some(42) }, row.id)
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };

        {
            let table = TestOptionSyncWorkTable::load_from_file(config)
                .await
                .unwrap();
            let selected = table.select(pk).unwrap();
            assert_eq!(selected.test, Some(42));
            assert_eq!(table.0.pk_gen.get_state(), pk + 1);
        }
    });
}

#[test]
fn test_option_update_none_to_some_sync() {
    let config = PersistenceConfig::new(
        "tests/data/option_sync/none_to_some",
        "tests/data/option_sync/none_to_some",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/option_sync/none_to_some".to_string()).await;

        let pk = {
            let table = TestOptionSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestOptionSyncRow {
                id: table.get_next_pk().0,
                test: None,
                another: 1,
                exchange: 1,
            };
            table.insert(row.clone()).unwrap();

            table
                .update_test_by_id(TestByIdQuery { test: Some(55) }, row.id)
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };

        {
            let table = TestOptionSyncWorkTable::load_from_file(config)
                .await
                .unwrap();
            let selected = table.select(pk).unwrap();
            assert_eq!(selected.test, Some(55));
            assert_eq!(table.0.pk_gen.get_state(), pk + 1);
        }
    });
}

#[test]
fn test_option_update_some_to_none_sync() {
    let config = PersistenceConfig::new(
        "tests/data/option_sync/some_to_none",
        "tests/data/option_sync/some_to_none",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/option_sync/some_to_none".to_string()).await;

        let pk = {
            let table = TestOptionSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestOptionSyncRow {
                id: table.get_next_pk().0,
                test: Some(100),
                another: 1,
                exchange: 1,
            };
            table.insert(row.clone()).unwrap();

            table
                .update_test_by_id(TestByIdQuery { test: None }, row.id)
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };

        {
            let table = TestOptionSyncWorkTable::load_from_file(config)
                .await
                .unwrap();
            let selected = table.select(pk).unwrap();
            assert_eq!(selected.test, None);
            assert_eq!(table.0.pk_gen.get_state(), pk + 1);
        }
    });
}

#[test]
fn test_option_update_by_another_sync() {
    let config = PersistenceConfig::new(
        "tests/data/option_sync/update_by_another",
        "tests/data/option_sync/update_by_another",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/option_sync/update_by_another".to_string()).await;

        let pk = {
            let table = TestOptionSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestOptionSyncRow {
                id: table.get_next_pk().0,
                test: None,
                another: 123,
                exchange: 1,
            };
            table.insert(row.clone()).unwrap();

            table
                .update_test_by_another(TestByAnotherQuery { test: Some(77) }, 123)
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };

        {
            let table = TestOptionSyncWorkTable::load_from_file(config)
                .await
                .unwrap();
            let selected = table.select(pk).unwrap();
            assert_eq!(selected.test, Some(77));
            assert_eq!(table.0.pk_gen.get_state(), pk + 1);
        }
    });
}

#[test]
fn test_option_update_by_exchange_sync() {
    let config = PersistenceConfig::new(
        "tests/data/option_sync/update_by_exchange",
        "tests/data/option_sync/update_by_exchange",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/option_sync/update_by_exchange".to_string()).await;

        let pk = {
            let table = TestOptionSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestOptionSyncRow {
                id: table.get_next_pk().0,
                test: None,
                another: 1,
                exchange: 456,
            };
            table.insert(row.clone()).unwrap();

            table
                .update_test_by_exchange(TestByExchangeQuery { test: Some(88) }, 456)
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };

        {
            let table = TestOptionSyncWorkTable::load_from_file(config)
                .await
                .unwrap();
            let selected = table.select(pk).unwrap();
            assert_eq!(selected.test, Some(88));
            assert_eq!(table.0.pk_gen.get_state(), pk + 1);
        }
    });
}

#[test]
fn test_option_multiple_rows_sync() {
    let config = PersistenceConfig::new(
        "tests/data/option_sync/multiple_rows",
        "tests/data/option_sync/multiple_rows",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/option_sync/multiple_rows".to_string()).await;

        let (pk1, pk2) = {
            let table = TestOptionSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();

            let row1 = TestOptionSyncRow {
                id: table.get_next_pk().0,
                test: Some(10),
                another: 1,
                exchange: 1,
            };
            let pk1 = table.insert(row1).unwrap();

            let row2 = TestOptionSyncRow {
                id: table.get_next_pk().0,
                test: Some(20),
                another: 2,
                exchange: 2,
            };
            let pk2 = table.insert(row2).unwrap();

            table
                .update_test_by_id(TestByIdQuery { test: Some(30) }, pk1.clone())
                .await
                .unwrap();

            table.wait_for_ops().await;
            (pk1, pk2)
        };

        {
            let table = TestOptionSyncWorkTable::load_from_file(config)
                .await
                .unwrap();
            assert_eq!(table.select(pk1).unwrap().test, Some(30));
            assert_eq!(table.select(pk2).unwrap().test, Some(20));
        }
    });
}

worktable! (
    name: TestOptionSyncIndex,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        test: u64 optional,
        another: u64,
        exchange: i32,
    },
    indexes: {
        another_idx: another unique,
        test_idx: test,
        exchnage_idx: exchange,
    },
    queries: {
        update: {
            IndexTestById(test) by id,
            IndexTestByAnother(test) by another,
            IndexTestByExchange(test) by exchange,
        }
    }
);

#[test]
fn test_option_indexed_insert_none_sync() {
    let config = PersistenceConfig::new(
        "tests/data/option_sync/indexed_insert_none",
        "tests/data/option_sync/indexed_insert_none",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/option_sync/indexed_insert_none".to_string()).await;

        let pk = {
            let table = TestOptionSyncIndexWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestOptionSyncIndexRow {
                id: table.get_next_pk().0,
                test: None,
                another: 1,
                exchange: 1,
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.id
        };

        {
            let table = TestOptionSyncIndexWorkTable::load_from_file(config)
                .await
                .unwrap();
            let selected = table.select(pk).unwrap();
            assert_eq!(selected.test, None);
            assert_eq!(table.0.pk_gen.get_state(), pk + 1);
        }
    });
}

#[test]
fn test_option_indexed_insert_some_sync() {
    let config = PersistenceConfig::new(
        "tests/data/option_sync/indexed_insert_some",
        "tests/data/option_sync/indexed_insert_some",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/option_sync/indexed_insert_some".to_string()).await;

        let pk = {
            let table = TestOptionSyncIndexWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestOptionSyncIndexRow {
                id: table.get_next_pk().0,
                test: Some(42),
                another: 1,
                exchange: 1,
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.id
        };

        {
            let table = TestOptionSyncIndexWorkTable::load_from_file(config)
                .await
                .unwrap();
            let selected = table.select(pk).unwrap();
            assert_eq!(selected.test, Some(42));
            assert_eq!(table.0.pk_gen.get_state(), pk + 1);
        }
    });
}

#[test]
fn test_option_indexed_update_none_to_some_by_id_sync() {
    let config = PersistenceConfig::new(
        "tests/data/option_sync/indexed_none_to_some",
        "tests/data/option_sync/indexed_none_to_some",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/option_sync/indexed_none_to_some".to_string()).await;

        let pk = {
            let table = TestOptionSyncIndexWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestOptionSyncIndexRow {
                id: table.get_next_pk().0,
                test: None,
                another: 1,
                exchange: 1,
            };
            table.insert(row.clone()).unwrap();

            table
                .update_index_test_by_id(IndexTestByIdQuery { test: Some(55) }, row.id)
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };

        {
            let table = TestOptionSyncIndexWorkTable::load_from_file(config)
                .await
                .unwrap();
            let selected = table.select(pk).unwrap();
            assert_eq!(selected.test, Some(55));
            assert_eq!(table.0.pk_gen.get_state(), pk + 1);
        }
    });
}

#[test]
fn test_option_indexed_update_some_to_none_by_id_sync() {
    let config = PersistenceConfig::new(
        "tests/data/option_sync/indexed_some_to_none",
        "tests/data/option_sync/indexed_some_to_none",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/option_sync/indexed_some_to_none".to_string()).await;

        let pk = {
            let table = TestOptionSyncIndexWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestOptionSyncIndexRow {
                id: table.get_next_pk().0,
                test: Some(100),
                another: 1,
                exchange: 1,
            };
            table.insert(row.clone()).unwrap();

            table
                .update_index_test_by_id(IndexTestByIdQuery { test: None }, row.id)
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };

        {
            let table = TestOptionSyncIndexWorkTable::load_from_file(config)
                .await
                .unwrap();
            let selected = table.select(pk).unwrap();
            assert_eq!(selected.test, None);
            assert_eq!(table.0.pk_gen.get_state(), pk + 1);
        }
    });
}

#[test]
fn test_option_indexed_update_by_another_sync() {
    let config = PersistenceConfig::new(
        "tests/data/option_sync/indexed_update_by_another",
        "tests/data/option_sync/indexed_update_by_another",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/option_sync/indexed_update_by_another".to_string()).await;

        let pk = {
            let table = TestOptionSyncIndexWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestOptionSyncIndexRow {
                id: table.get_next_pk().0,
                test: None,
                another: 123,
                exchange: 1,
            };
            table.insert(row.clone()).unwrap();

            table
                .update_index_test_by_another(IndexTestByAnotherQuery { test: Some(77) }, 123)
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };

        {
            let table = TestOptionSyncIndexWorkTable::load_from_file(config)
                .await
                .unwrap();
            let selected = table.select(pk).unwrap();
            assert_eq!(selected.test, Some(77));
            assert_eq!(table.0.pk_gen.get_state(), pk + 1);
        }
    });
}

#[test]
fn test_option_indexed_multiple_rows_sync() {
    let config = PersistenceConfig::new(
        "tests/data/option_sync/indexed_multiple_rows",
        "tests/data/option_sync/indexed_multiple_rows",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/option_sync/indexed_multiple_rows".to_string()).await;

        let (pk1, pk2, pk3) = {
            let table = TestOptionSyncIndexWorkTable::load_from_file(config.clone())
                .await
                .unwrap();

            let row1 = TestOptionSyncIndexRow {
                id: table.get_next_pk().0,
                test: Some(10),
                another: 1,
                exchange: 1,
            };
            let pk1 = table.insert(row1).unwrap();

            let row2 = TestOptionSyncIndexRow {
                id: table.get_next_pk().0,
                test: None,
                another: 2,
                exchange: 2,
            };
            let pk2 = table.insert(row2).unwrap();

            let row3 = TestOptionSyncIndexRow {
                id: table.get_next_pk().0,
                test: Some(30),
                another: 3,
                exchange: 3,
            };
            let pk3 = table.insert(row3).unwrap();

            table
                .update_index_test_by_id(IndexTestByIdQuery { test: Some(40) }, pk1.clone())
                .await
                .unwrap();

            table
                .update_index_test_by_id(IndexTestByIdQuery { test: Some(50) }, pk2.clone())
                .await
                .unwrap();

            table.wait_for_ops().await;
            (pk1, pk2, pk3)
        };

        {
            let table = TestOptionSyncIndexWorkTable::load_from_file(config)
                .await
                .unwrap();
            assert_eq!(table.select(pk1).unwrap().test, Some(40));
            assert_eq!(table.select(pk2).unwrap().test, Some(50));
            assert_eq!(table.select(pk3).unwrap().test, Some(30));
        }
    });
}

#[test]
fn test_option_indexed_full_row_update_sync() {
    let config = PersistenceConfig::new(
        "tests/data/option_sync/indexed_full_update",
        "tests/data/option_sync/indexed_full_update",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/option_sync/indexed_full_update".to_string()).await;

        let pk = {
            let table = TestOptionSyncIndexWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestOptionSyncIndexRow {
                id: table.get_next_pk().0,
                test: None,
                another: 100,
                exchange: 200,
            };
            table.insert(row.clone()).unwrap();

            table
                .update(TestOptionSyncIndexRow {
                    id: row.id,
                    test: Some(99),
                    another: 100,
                    exchange: 200,
                })
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };

        {
            let table = TestOptionSyncIndexWorkTable::load_from_file(config)
                .await
                .unwrap();
            let selected = table.select(pk).unwrap();
            assert_eq!(selected.test, Some(99));
            assert_eq!(selected.another, 100);
            assert_eq!(selected.exchange, 200);
            assert_eq!(table.0.pk_gen.get_state(), pk + 1);
        }
    });
}
