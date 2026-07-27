use std::time::Duration;

use tokio::time::timeout;

use crate::remove_dir_if_exists;
use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: DuplicateKeyReload,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        score: u64,
        label: String,
    },
    indexes: {
        score_idx: score,
    },
);

/// Reproduction for lossy persistence of duplicate-key secondary indexes.
///
/// A plain bulk insert of 10_000 rows spread over 97 distinct `score` values
/// (~103 duplicates per key) is fully visible through `select_by_score` while
/// the table is in memory, but after wait_for_ops + reopen a chunk of the
/// entries (~12% as of 0.9.0) is no longer reachable through the index, even
/// though `select_all` still returns every row. The loss is baked into the
/// index file at write time: re-reading the same files gives the same wrong
/// counts. Unique-valued secondary indexes survive the same round trip intact.
#[test]
#[ignore = "exposes lossy persistence of duplicate-key secondary indexes (~12% of entries unreachable after reload)"]
fn test_duplicate_key_secondary_index_survives_reload() {
    const ROWS: u64 = 10_000;
    const KEYS: u64 = 97;

    let config = DiskConfig::new_with_table_name(
        "tests/data/duplicate_key_index_reload/persisted",
        DuplicateKeyReloadWorkTable::name_snake_case(),
        DuplicateKeyReloadWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/duplicate_key_index_reload/persisted".to_string()).await;

        {
            let engine = DuplicateKeyReloadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = DuplicateKeyReloadWorkTable::load(engine).await.unwrap();

            for i in 0..ROWS {
                table
                    .insert(DuplicateKeyReloadRow {
                        id: i,
                        score: i % KEYS,
                        label: format!("row-{i}-{}", "x".repeat((i % 50) as usize)),
                    })
                    .unwrap();
            }

            // The in-memory index is complete before shutdown.
            for s in 0..KEYS {
                let expected = ROWS / KEYS + u64::from(s < ROWS % KEYS);
                assert_eq!(
                    table.select_by_score(s).execute().unwrap().len() as u64,
                    expected,
                    "in-memory index incomplete for key {s}"
                );
            }

            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence stalled on bulk insert");
        }
        {
            let engine = DuplicateKeyReloadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = DuplicateKeyReloadWorkTable::load(engine).await.unwrap();

            // Row data survives the round trip...
            assert_eq!(table.select_all().execute().unwrap().len() as u64, ROWS);

            // ...and every row must stay reachable through the secondary index.
            let mut reachable = 0u64;
            for s in 0..KEYS {
                reachable += table.select_by_score(s).execute().unwrap().len() as u64;
            }
            assert_eq!(
                reachable, ROWS,
                "secondary index lost {} of {ROWS} entries across persist+reload",
                ROWS - reachable
            );
        }
    })
}
