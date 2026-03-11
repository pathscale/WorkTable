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
            ),
            s3: S3Config {
                bucket_name: "honey-auth".to_string(),
                endpoint: "https://1b7c8f39b677597cd7d2d8740cdf70d0.r2.cloudflarestorage.com"
                    .to_string(),
                access_key: "f0b666f2ae141c91fe621d5d4ae427e0".to_string(),
                secret_key: "f1b639f1d3eab3f0dd2a6077439a45e49ef58c31b22901a177f9d5d49f7bc72e"
                    .to_string(),
                region: None,
                prefix: Some("wt-test".to_string()),
            },
        };

        {
            let engine = TestS3SyncPersistenceEngine::new(config).await.unwrap();
            let table = TestS3WorkTable::load(engine).await.unwrap();
            table
                .insert(TestS3Row {
                    id: table.get_next_pk().into(),
                    value: 0,
                })
                .unwrap();
            assert!(table.select_all().execute().unwrap().len() > 0);
            table.wait_for_ops().await;
        }
    });
}
