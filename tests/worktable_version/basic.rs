use crate::remove_dir_if_exists;

use worktable::prelude::*;
use worktable_codegen::{worktable, worktable_version};

worktable!(
    name: User,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        name: String,
        email: String,
    },
    indexes: {
        name_idx: name,
    },
);

worktable_version!(
    name: UserV1,
    columns: {
        id: u64 primary_key autoincrement,
        name: String,
        email: String,
    },
    indexes: {
        name_idx: name,
    },
);

#[test]
fn test_version_reads_persisted_data() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/version/basic",
        UserWorkTable::name_snake_case(),
        UserWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/version/basic".to_string()).await;

        {
            let engine = UserPersistenceEngine::new(config.clone()).await.unwrap();
            let table = UserWorkTable::load(engine).await.unwrap();

            table
                .insert(UserRow {
                    id: table.get_next_pk().into(),
                    name: "Alice".to_string(),
                    email: "alice@example.com".to_string(),
                })
                .unwrap();

            table
                .insert(UserRow {
                    id: table.get_next_pk().into(),
                    name: "Bob".to_string(),
                    email: "bob@example.com".to_string(),
                })
                .unwrap();

            table.wait_for_ops().await
        }

        {
            let engine = ReadOnlyPersistenceEngine::create(config.clone()).await.unwrap();
            let table = UserV1WorkTable::load(engine).await.unwrap();

            // Verify count
            assert_eq!(table.count(), 2);

            // Verify data via select_all
            let rows = table.select_all().execute().unwrap();
            assert_eq!(rows.len(), 2);

            // Check specific values exist
            let names: Vec<_> = rows.iter().map(|r| r.name.clone()).collect();
            assert!(names.contains(&"Alice".to_string()));
            assert!(names.contains(&"Bob".to_string()));
        }
    });
}
