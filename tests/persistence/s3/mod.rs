use crate::remove_dir_if_exists;
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: TestS3,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        value: u64,
    },
);

type TestS3SyncPersistenceEngine = S3SyncDiskPersistenceEngine<
    SpaceData<
        <<TestS3PrimaryKey as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
        { TEST_S_3_INNER_SIZE },
        { TEST_S_3_PAGE_SIZE as u32 },
    >,
    SpaceIndex<TestS3PrimaryKey, { TEST_S_3_INNER_SIZE as u32 }>,
    TestS3SpaceSecondaryIndex,
    TestS3PrimaryKey,
    TestS3SpaceSecondaryIndexEvents,
    TestS3AvailableIndexes,
>;

#[test]
fn test_s3_engine_compiles() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
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
                bucket_name: "test-bucket".to_string(),
                endpoint: "test".to_string(),
                access_key: "test".to_string(),
                secret_key: "test".to_string(),
                region: None,
                prefix: None,
            },
        };

        let engine = TestS3SyncPersistenceEngine::new(config).await.unwrap();
        let _table = TestS3WorkTable::load(engine).await.unwrap();
    });
}
