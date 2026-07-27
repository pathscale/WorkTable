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

/// Regression test for lossy reconstruction of duplicate-key secondary
/// indexes.
///
/// Index pages store entries in event-arrival order, but `from_persisted`
/// used to treat the last entry of every page as the node's maximum and
/// re-append it with discriminator `u64::MAX - 1`. For pages that were
/// incrementally updated through CDC events (any bulk load), that "maximum"
/// was an arbitrary entry, so the reconstructed in-memory node index
/// registered wrong node maxima and every entry sorting above one became
/// unreachable through `select_by_*` — around 12% of the entries in this
/// workload, although `select_all` still returned every row. Reconstruction
/// now sorts each page, orders nodes by their true maximum, and assigns
/// discriminators that keep growing across node boundaries within one key,
/// which restores the B-tree ordering invariant even when one key's
/// duplicates straddle nodes.
#[test]
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

            // ...and every row stays reachable through the secondary index,
            // with per-key counts intact.
            for s in 0..KEYS {
                let expected = ROWS / KEYS + u64::from(s < ROWS % KEYS);
                assert_eq!(
                    table.select_by_score(s).execute().unwrap().len() as u64,
                    expected,
                    "secondary index lost entries for key {s} across persist+reload"
                );
            }
        }
    })
}

/// Companion stress case: a single key carrying every row. Every index node
/// holds the same key, so every node boundary is a straddle and same-key node
/// maxima must still resolve against the table of contents — the worst case
/// for the discriminator and node-ordering logic in `from_persisted`.
#[test]
fn test_single_key_all_duplicates_survives_reload() {
    const ROWS: u64 = 10_000;

    let config = DiskConfig::new_with_table_name(
        "tests/data/duplicate_key_index_reload/single_key",
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
        remove_dir_if_exists("tests/data/duplicate_key_index_reload/single_key".to_string()).await;

        {
            let engine = DuplicateKeyReloadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = DuplicateKeyReloadWorkTable::load(engine).await.unwrap();

            for i in 0..ROWS {
                table
                    .insert(DuplicateKeyReloadRow {
                        id: i,
                        score: 42,
                        label: format!("row-{i}"),
                    })
                    .unwrap();
            }
            assert_eq!(table.select_by_score(42).execute().unwrap().len() as u64, ROWS);

            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence stalled on bulk insert");
        }
        {
            let engine = DuplicateKeyReloadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = DuplicateKeyReloadWorkTable::load(engine).await.unwrap();

            assert_eq!(table.select_all().execute().unwrap().len() as u64, ROWS);
            assert_eq!(
                table.select_by_score(42).execute().unwrap().len() as u64,
                ROWS,
                "single-key secondary index lost entries across persist+reload"
            );

            // The reloaded table must also stay writable: post-reload CDC
            // events address nodes by their maximum, which this workload makes
            // ambiguous per key on purpose.
            table
                .insert(DuplicateKeyReloadRow {
                    id: ROWS,
                    score: 42,
                    label: "post-reload".to_string(),
                })
                .unwrap();
            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence stalled on post-reload insert");
            assert_eq!(table.select_by_score(42).execute().unwrap().len() as u64, ROWS + 1);
        }
    })
}
