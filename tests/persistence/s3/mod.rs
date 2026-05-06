use crate::remove_dir_if_exists;
use worktable::prelude::*;
use worktable::s3_sync_persistence;
use worktable::worktable;

worktable!(
    name: TestS3,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        value: u64,
    },
);

s3_sync_persistence!(TestS3WorkTable);

#[test]
#[ignore]
fn test_s3_engine_compiles() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/s3/compile_test".to_string()).await;

        let config = S3DiskConfig {
            disk: DiskConfig::new_with_table_name(
                "tests/data/s3/compile_test",
                TestS3WorkTable::name_snake_case(),
                TestS3WorkTable::version(),
            ),
            s3: S3Config {
                bucket_name: "test".to_string(),
                endpoint: "test".to_string(),
                access_key: "test".to_string(),
                secret_key: "test".to_string(),
                region: None,
                prefix: Some("wt-test".to_string()),
            },
        };

        {
            let engine = TestS3S3SyncPersistenceEngine::new(config).await.unwrap();
            let table = TestS3WorkTable::load(engine).await.unwrap();
            table
                .insert(TestS3Row {
                    id: table.get_next_pk().into(),
                    value: 0,
                })
                .unwrap();
            assert!(!table.select_all().execute().unwrap().is_empty());
            table.wait_for_ops().await;
        }
    });
}
