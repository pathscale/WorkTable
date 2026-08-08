use std::time::Duration;

use tokio::time::timeout;
use worktable::prelude::*;
use worktable::worktable;

use crate::remove_dir_if_exists;

worktable!(
    name: StringBlob,
    persist: true,
    columns: {
        key: String primary_key,
        value: String,
        updated_at: String,
    },
);

#[test]
fn repeated_varying_string_upserts_keep_the_worker_healthy() {
    let path = "tests/data/sync/repeated_string_upsert";
    let config = DiskConfig::new_with_table_name(
        path,
        StringBlobWorkTable::name_snake_case(),
        StringBlobWorkTable::version(),
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists(path.to_string()).await;
        {
            let engine = StringBlobPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringBlobWorkTable::load(engine).await.unwrap();

            for index in 0..256 {
                table
                    .insert(StringBlobRow {
                        key: format!("marker:{index:04}"),
                        value: format!("initial-{index}"),
                        updated_at: format!("2026-08-08T00:{:02}:00Z", index % 60),
                    })
                    .unwrap();
            }
            table
                .insert(StringBlobRow {
                    key: "settings".into(),
                    value: "{}".into(),
                    updated_at: "2026-08-08T00:00:00Z".into(),
                })
                .unwrap();
            table.wait_for_ops().await.unwrap();
            table.close().await.unwrap();
        }
        {
            // Loading a multi-segment TOC resets its insertion cursor to the
            // first segment. The next split must carry a new identity through
            // the existing chain instead of dropping it at that full page.
            let engine = StringBlobPersistenceEngine::new(config.clone()).await.unwrap();
            let table = StringBlobWorkTable::load(engine).await.unwrap();

            for revision in 0..1_000 {
                table
                    .upsert(StringBlobRow {
                        key: "settings".into(),
                        value: "x".repeat(64 + revision % 4_096),
                        updated_at: format!("2026-08-08T01:{:02}:{:02}Z", revision % 60, revision % 60),
                    })
                    .await
                    .unwrap();
            }

            timeout(Duration::from_secs(15), table.wait_for_ops())
                .await
                .expect("persistence stalled after repeated string upserts")
                .expect("persistence worker failed after repeated string upserts");
            table.close().await.unwrap();
        }
        {
            let engine = StringBlobPersistenceEngine::new(config).await.unwrap();
            let table = StringBlobWorkTable::load(engine).await.unwrap();
            assert_eq!(table.select("settings".to_string()).unwrap().value.len(), 1_063);
            table.close().await.unwrap();
        }
    });
}
