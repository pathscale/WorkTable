use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable_codegen::worktable;

use crate::remove_dir_if_exists;

worktable! (
    name: TestSync,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        another: String,
        non_unique: u32,
        field: f64,
    },
    indexes: {
        another_idx: another unique,
        non_unique_idx: non_unique
    },
    queries: {
        update: {
            AnotherById(another) by id,
            FieldByAnother(field) by another,
            AnotherByNonUnique(another) by non_unique
        },
        delete: {
             ByAnother() by another,
        }
    }
);

worktable! (
    name: FragmentedStringSecondary,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        project_id: String,
    },
    indexes: {
        project_idx: project_id,
    },
);

#[test]
fn fragmented_string_index_compacts_after_restart_before_appending() {
    let path = "tests/data/unsized_secondary_sync/fragmented_restart";
    let config = DiskConfig::new_with_table_name(
        path,
        FragmentedStringSecondaryWorkTable::name_snake_case(),
        FragmentedStringSecondaryWorkTable::version(),
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists(path.to_string()).await;
        let project_id = "proj-fdfb706b-72bc-4b0c-a96e-6b6235be6fb4".to_string();
        let mut inserted_ids = Vec::new();

        {
            let engine = FragmentedStringSecondaryPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = FragmentedStringSecondaryWorkTable::load(engine).await.unwrap();
            for _ in 0..211 {
                let row = FragmentedStringSecondaryRow {
                    id: table.get_next_pk().0,
                    project_id: project_id.clone(),
                };
                inserted_ids.push(row.id);
                table.insert(row).await.unwrap();
            }
            table.wait_for_ops().await.unwrap();
            for id in inserted_ids.iter().take(37).copied() {
                table.delete(id).await.unwrap();
            }
            table.wait_for_ops().await.unwrap();
        }

        {
            let engine = FragmentedStringSecondaryPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = FragmentedStringSecondaryWorkTable::load(engine).await.unwrap();
            for _ in 0..20 {
                let row = FragmentedStringSecondaryRow {
                    id: table.get_next_pk().0,
                    project_id: project_id.clone(),
                };
                table.insert(row).await.unwrap();
            }
            table.wait_for_ops().await.unwrap();
        }

        {
            let engine = FragmentedStringSecondaryPersistenceEngine::new(config).await.unwrap();
            let table = FragmentedStringSecondaryWorkTable::load(engine).await.unwrap();
            assert_eq!(table.select_all().execute().unwrap().len(), 194);
            assert_eq!(table.select_by_project_id(project_id).execute().unwrap().len(), 194);
        }

        let index_path = format!("{path}/fragmented_string_secondary/project_idx.wt.idx");
        let mut index_file = tokio::fs::File::open(index_path).await.unwrap();
        let page = parse_page::<UnsizedIndexPage<String, { INNER_PAGE_SIZE as u32 }>, { INNER_PAGE_SIZE as u32 }>(
            &mut index_file,
            2,
        )
        .await
        .unwrap();
        let utility_size = worktable::data_bucket::UnsizedIndexPageUtility::<String>::persisted_size(
            page.inner.slots_size as usize,
            page.inner.node_id_size as usize,
        );
        assert!(
            utility_size + page.inner.last_value_offset as usize <= INNER_PAGE_SIZE,
            "slot directory must not overlap values stored at the page tail"
        );

        remove_dir_if_exists(path.to_string()).await;
    });
}

#[test]
fn test_space_insert_sync() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/unsized_secondary_sync/insert",
        TestSyncWorkTable::name_snake_case(),
        TestSyncWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_secondary_sync/insert".to_string()).await;

        let pk = {
            let engine = TestSyncPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            let row = TestSyncRow {
                another: "Some string to test".to_string(),
                non_unique: 0,
                field: 0.234,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).await.unwrap();
            table.wait_for_ops().await.unwrap();
            row.id
        };
        {
            let engine = TestSyncPersistenceEngine::new(config).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            assert!(table.select(pk).is_some());
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

#[test]
fn test_space_insert_many_sync() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/unsized_secondary_sync/insert_many",
        TestSyncWorkTable::name_snake_case(),
        TestSyncWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_secondary_sync/insert_many".to_string()).await;

        let mut pks = vec![];
        {
            let engine = TestSyncPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            for i in 0..20 {
                let pk = {
                    let row = TestSyncRow {
                        another: format!("Some string to test number {i}"),
                        non_unique: (i % 4) as u32,
                        field: i as f64 / 100.0,
                        id: table.get_next_pk().0,
                    };
                    table.insert(row.clone()).await.unwrap();
                    row.id
                };
                pks.push(pk);
            }
            table.wait_for_ops().await.unwrap();
        }

        {
            let engine = TestSyncPersistenceEngine::new(config).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            let last = *pks.last().unwrap();
            for pk in pks {
                assert!(table.select(pk).is_some());
            }
            assert_eq!(table.0.pk_gen.get_state(), last + 1)
        }
    });
}

#[test]
fn test_space_update_full_sync() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/unsized_secondary_sync/update_full",
        TestSyncWorkTable::name_snake_case(),
        TestSyncWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_secondary_sync/update_full".to_string()).await;

        let pk = {
            let engine = TestSyncPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            let row = TestSyncRow {
                another: "Some string before".to_string(),
                non_unique: 0,
                field: 0.0,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).await.unwrap();
            table
                .update(TestSyncRow {
                    another: "Some string to test updated".to_string(),
                    non_unique: 0,
                    field: 0.0,
                    id: row.id,
                })
                .await
                .unwrap();
            table.wait_for_ops().await.unwrap();
            assert_eq!(
                table.select(row.id).unwrap().another,
                "Some string to test updated".to_string()
            );
            row.id
        };
        {
            let engine = TestSyncPersistenceEngine::new(config).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            assert!(table.select(pk).is_some());
            assert_eq!(
                table.select(pk).unwrap().another,
                "Some string to test updated".to_string()
            );
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

#[test]
fn test_space_update_query_pk_sync() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/unsized_secondary_sync/update_query_pk",
        TestSyncWorkTable::name_snake_case(),
        TestSyncWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_secondary_sync/update_query_pk".to_string()).await;

        let pk = {
            let engine = TestSyncPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            let row = TestSyncRow {
                another: "Some string before".to_string(),
                non_unique: 0,
                field: 0.0,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).await.unwrap();
            table
                .update_another_by_id(
                    AnotherByIdQuery {
                        another: "Some string to test updated".to_string(),
                    },
                    row.id,
                )
                .await
                .unwrap();
            table.wait_for_ops().await.unwrap();
            row.id
        };
        {
            let engine = TestSyncPersistenceEngine::new(config).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            assert!(table.select(pk).is_some());
            assert_eq!(
                table.select(pk).unwrap().another,
                "Some string to test updated".to_string()
            );
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

#[test]
fn test_space_update_query_unique_sync() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/unsized_secondary_sync/update_query_unique",
        TestSyncWorkTable::name_snake_case(),
        TestSyncWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_secondary_sync/update_query_unique".to_string()).await;

        let pk = {
            let engine = TestSyncPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            let row = TestSyncRow {
                another: "Some string before".to_string(),
                non_unique: 0,
                field: 0.0,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).await.unwrap();
            table
                .update_field_by_another(FieldByAnotherQuery { field: 1.0 }, "Some string before".to_string())
                .await
                .unwrap();
            table.wait_for_ops().await.unwrap();
            row.id
        };
        {
            let engine = TestSyncPersistenceEngine::new(config).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            assert!(table.select(pk).is_some());
            assert_eq!(table.select(pk).unwrap().field, 1.0);
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

#[test]
fn test_space_update_query_non_unique_sync() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/unsized_secondary_sync/update_query_non_unique",
        TestSyncWorkTable::name_snake_case(),
        TestSyncWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_secondary_sync/update_query_non_unique".to_string()).await;

        let pk = {
            let engine = TestSyncPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            let row = TestSyncRow {
                another: "Some string before".to_string(),
                non_unique: 10,
                field: 0.0,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).await.unwrap();
            table
                .update_another_by_non_unique(
                    AnotherByNonUniqueQuery {
                        another: "Some string to test updated".to_string(),
                    },
                    10,
                )
                .await
                .unwrap();
            table.wait_for_ops().await.unwrap();
            row.id
        };
        {
            let engine = TestSyncPersistenceEngine::new(config).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            assert!(table.select(pk).is_some());
            assert_eq!(
                table.select(pk).unwrap().another,
                "Some string to test updated".to_string()
            );
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

#[test]
fn test_space_delete_sync() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/unsized_secondary_sync/delete",
        TestSyncWorkTable::name_snake_case(),
        TestSyncWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_secondary_sync/delete".to_string()).await;

        let pk = {
            let engine = TestSyncPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            let row = TestSyncRow {
                another: "Some string before".to_string(),
                non_unique: 0,
                field: 0.0,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).await.unwrap();
            table.delete(row.id).await.unwrap();
            table.wait_for_ops().await.unwrap();
            row.id
        };
        {
            let engine = TestSyncPersistenceEngine::new(config).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            assert!(table.select(pk).is_none());
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

#[test]
fn test_space_delete_query_sync() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/unsized_secondary_sync/delete_query",
        TestSyncWorkTable::name_snake_case(),
        TestSyncWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_secondary_sync/delete_query".to_string()).await;

        let pk = {
            let engine = TestSyncPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            let row = TestSyncRow {
                another: "Some string before".to_string(),
                non_unique: 0,
                field: 0.0,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).await.unwrap();
            table.delete_by_another(row.another).await.unwrap();
            table.wait_for_ops().await.unwrap();
            row.id
        };
        {
            let engine = TestSyncPersistenceEngine::new(config).await.unwrap();
            let table = TestSyncWorkTable::load(engine).await.unwrap();
            assert!(table.select(pk).is_none());
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

// #[test]
// fn test_space_all_data_is_available() {
//     let config = DiskConfig::new(
//         "tests/data/unsized_secondary_sync/data_is_available",
//         "tests/data/unsized_secondary_sync/data_is_available",
//     );
//
//     let runtime = tokio::runtime::Builder::new_multi_thread()
//         .worker_threads(2)
//         .enable_io()
//         .enable_time()
//         .build()
//         .unwrap();
//
//     runtime.block_on(async {
//         remove_dir_if_exists("tests/data/unsized_secondary_sync/data_is_available".to_string())
//             .await;
//
//         {
//             let table = TestSyncWorkTable::load_from_file(config.clone())
//                 .await
//                 .unwrap();
//             for i in 0..2000 {
//                 let row = TestSyncRow {
//                     another: format!("ValueNumber{i}"),
//                     non_unique: i % 200,
//                     field: 0.0,
//                     id: table.get_next_pk().0,
//                 };
//                 table.insert(row.clone()).await.unwrap();
//             }
//
//             table.wait_for_ops().await.unwrap();
//         };
//         {
//             let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
//             for i in 0..2000 {
//                 assert!(table.select_by_another(format!("ValueNumber{i}")).is_some());
//             }
//             for i in 0..200 {
//                 assert_eq!(table.select_by_non_unique(i).execute().unwrap().len(), 10,);
//             }
//         }
//     });
// }
