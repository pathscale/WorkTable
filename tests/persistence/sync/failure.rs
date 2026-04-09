use crate::remove_dir_if_exists;
use worktable::prelude::*;

use super::{
    AnotherByIdQuery, FieldByAnotherQuery, TestSyncPersistenceEngine, TestSyncRow,
    TestSyncWorkTable,
};

#[test]
fn test_failed_update_by_pk_doesnt_corrupt_persistence() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/sync/failure_update_pk",
        TestSyncWorkTable::name_snake_case(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/sync/failure_update_pk".to_string()).await;

        let pks = {
            let engine = TestSyncPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            let mut pks = vec![];
            for i in 0..100 {
                let row = TestSyncRow {
                    id: table.get_next_pk().0,
                    another: i,
                    non_unique: 0,
                    field: i as f64,
                };
                table.insert(row.clone()).unwrap();
                pks.push(row.id);
            }
            table.wait_for_ops().await;
            pks
        };

        {
            let engine = TestSyncPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();

            let result = table
                .update_another_by_id(AnotherByIdQuery { another: 9999 }, 9999)
                .await;
            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), WorkTableError::NotFound));

            for (i, pk) in pks.iter().enumerate() {
                table
                    .update_another_by_id(
                        AnotherByIdQuery {
                            another: i as u64 + 1000,
                        },
                        *pk,
                    )
                    .await
                    .unwrap();
            }
            table.wait_for_ops().await;
        }

        {
            let engine = TestSyncPersistenceEngine::new(config).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            for (i, pk) in pks.iter().enumerate() {
                let row = table.select(*pk).unwrap();
                assert_eq!(row.another, i as u64 + 1000);
            }
            let last_pk = *pks.last().unwrap();
            assert_eq!(table.0.pk_gen.get_state(), last_pk + 1);
        }
    });
}

#[test]
fn test_failed_update_by_unique_index_doesnt_corrupt_persistence() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/sync/failure_update_unique",
        TestSyncWorkTable::name_snake_case(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/sync/failure_update_unique".to_string()).await;

        let pks = {
            let engine = TestSyncPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            let mut pks = vec![];
            for i in 0..100 {
                let row = TestSyncRow {
                    id: table.get_next_pk().0,
                    another: i,
                    non_unique: 0,
                    field: i as f64,
                };
                table.insert(row.clone()).unwrap();
                pks.push(row.id);
            }
            table.wait_for_ops().await;
            pks
        };

        {
            let engine = TestSyncPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();

            let result = table
                .update_field_by_another(FieldByAnotherQuery { field: 9999.0 }, 9999)
                .await;
            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), WorkTableError::NotFound));

            for (i, _pk) in pks.iter().enumerate() {
                table
                    .update_field_by_another(
                        FieldByAnotherQuery {
                            field: i as f64 + 1000.0,
                        },
                        i as u64,
                    )
                    .await
                    .unwrap();
            }
            table.wait_for_ops().await;
        }

        {
            let engine = TestSyncPersistenceEngine::new(config).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            for (i, pk) in pks.iter().enumerate() {
                let row = table.select(*pk).unwrap();
                assert_eq!(row.field, i as f64 + 1000.0);
            }
            let last_pk = *pks.last().unwrap();
            assert_eq!(table.0.pk_gen.get_state(), last_pk + 1);
        }
    });
}

#[test]
fn test_failed_delete_by_pk_doesnt_corrupt_persistence() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/sync/failure_delete_pk",
        TestSyncWorkTable::name_snake_case(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/sync/failure_delete_pk".to_string()).await;

        let pks = {
            let engine = TestSyncPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            let mut pks = vec![];
            for i in 0..100 {
                let row = TestSyncRow {
                    id: table.get_next_pk().0,
                    another: i,
                    non_unique: 0,
                    field: i as f64,
                };
                table.insert(row.clone()).unwrap();
                pks.push(row.id);
            }
            table.wait_for_ops().await;
            pks
        };

        {
            let engine = TestSyncPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();

            let result = table.delete(9999).await;
            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), WorkTableError::NotFound));

            table.wait_for_ops().await;
        }

        {
            let engine = TestSyncPersistenceEngine::new(config).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            for pk in &pks {
                let row = table.select(*pk).unwrap();
                assert_eq!(
                    row.another,
                    pks.iter().position(|p| p == pk).unwrap() as u64
                );
            }
            let last_pk = *pks.last().unwrap();
            assert_eq!(table.0.pk_gen.get_state(), last_pk + 1);
        }
    });
}
