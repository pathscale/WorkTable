use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable_codegen::worktable;

use crate::remove_dir_if_exists;

worktable! (
    name: TestSync,
    persist: true,
    columns: {
        id: String primary_key,
        field: String,
        another: u64,
    },
    queries: {
        update: {
            FieldAnotherById(field, another) by id,
        },
    }
);

#[test]
fn test_space_update_query_pk_sync() {
    let config = DiskConfig::new_with_table_name("tests/data/unsized_primary_and_other_sync/update_query_pk", TestSyncWorkTable::name_snake_case());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists(
            "tests/data/unsized_primary_and_other_sync/update_query_pk".to_string(),
        )
        .await;

        let pk = {
            let engine = DiskPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            let row = TestSyncRow {
                another: 42,
                field: "".to_string(),
                id: "Some string before".to_string(),
            };
            table.insert(row.clone()).unwrap();
            let row = TestSyncRow {
                another: 43,
                field: "".to_string(),
                id: "Some string before 2".to_string(),
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let engine = DiskPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            assert!(table.select(pk.clone()).is_some());
            assert_eq!(table.select(pk.clone()).unwrap().another, 43);
            let q = FieldAnotherByIdQuery {
                field: "Some field value".to_string(),
                another: 0,
            };
            table
                .update_field_another_by_id(q, pk.clone())
                .await
                .unwrap();
            table.wait_for_ops().await;
        }
        {
            let engine = DiskPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            assert!(table.select(pk.clone()).is_some());
            assert_eq!(table.select(pk.clone()).unwrap().another, 0);
            assert_eq!(
                table.select(pk).unwrap().field,
                "Some field value".to_string()
            );
        }
    });
}

#[test]
fn test_space_update_query_pk_many_times_sync() {
    let config = DiskConfig::new_with_table_name("tests/data/unsized_primary_and_other_sync/update_query_pk_many", TestSyncWorkTable::name_snake_case());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists(
            "tests/data/unsized_primary_and_other_sync/update_query_pk_many".to_string(),
        )
        .await;

        let pk = {
            let engine = DiskPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            let row = TestSyncRow {
                another: 42,
                field: "".to_string(),
                id: "Some string before".to_string(),
            };
            table.insert(row.clone()).unwrap();
            let row = TestSyncRow {
                another: 43,
                field: "".to_string(),
                id: "Some string before 2".to_string(),
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let engine = DiskPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            assert!(table.select(pk.clone()).is_some());
            assert_eq!(table.select(pk.clone()).unwrap().another, 43);
            for i in 0..512 {
                let q = FieldAnotherByIdQuery {
                    field: "Some field value".to_string(),
                    another: i,
                };
                table
                    .update_field_another_by_id(q, pk.clone())
                    .await
                    .unwrap();
            }

            table.wait_for_ops().await;
        }
        {
            let engine = DiskPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            assert!(table.select(pk.clone()).is_some());
            assert_eq!(table.select(pk.clone()).unwrap().another, 511);
            assert_eq!(
                table.select(pk).unwrap().field,
                "Some field value".to_string()
            );
        }
    });
}
