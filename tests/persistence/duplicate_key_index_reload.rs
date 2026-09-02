use std::collections::{BTreeMap, BTreeSet};
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
        bucket: String,
        label: String,
    },
    indexes: {
        score_idx: score,
        bucket_idx: bucket,
    },
    queries: {
        update: {
            ScoreById(score) by id,
        }
    }
);

const ROWS: u64 = 10_000;
const KEYS: u64 = 97;
const BUCKETS: u64 = 23;

fn bucket_name(i: u64) -> String {
    format!("bucket-{:02}", i % BUCKETS)
}

/// Expected state: score -> id set and bucket -> id set.
#[derive(Default)]
struct Model {
    by_score: BTreeMap<u64, BTreeSet<u64>>,
    by_bucket: BTreeMap<String, BTreeSet<u64>>,
}

impl Model {
    fn insert(&mut self, id: u64, score: u64, bucket: String) {
        self.by_score.entry(score).or_default().insert(id).await;
        self.by_bucket.entry(bucket).or_default().insert(id).await;
    }

    fn remove(&mut self, id: u64) {
        self.by_score.retain(|_, ids| {
            ids.remove(&id);
            !ids.is_empty()
        });
        self.by_bucket.retain(|_, ids| {
            ids.remove(&id);
            !ids.is_empty()
        });
    }

    fn move_score(&mut self, id: u64, new_score: u64) {
        self.by_score.retain(|_, ids| {
            ids.remove(&id);
            !ids.is_empty()
        });
        self.by_score.entry(new_score).or_default().insert(id).await;
    }

    /// Compares EXACT id sets, not counts: a count can pass while one link is
    /// missing and another is duplicated.
    fn assert_matches(&self, table: &DuplicateKeyReloadWorkTable, stage: &str) {
        for (score, expected_ids) in &self.by_score {
            let got: BTreeSet<u64> = table
                .select_by_score(*score)
                .execute()
                .unwrap()
                .into_iter()
                .map(|r| r.id)
                .collect();
            assert_eq!(&got, expected_ids, "score {score} id set mismatch at stage: {stage}");
        }
        // No phantom rows either.
        let live: u64 = self.by_score.values().map(|s| s.len() as u64).sum();
        assert_eq!(
            table.select_all().execute().unwrap().len() as u64,
            live,
            "row count at {stage}"
        );

        for (bucket, expected_ids) in &self.by_bucket {
            let got: BTreeSet<u64> = table
                .select_by_bucket(bucket.clone())
                .execute()
                .unwrap()
                .into_iter()
                .map(|r| r.id)
                .collect();
            assert_eq!(&got, expected_ids, "bucket {bucket} id set mismatch at stage: {stage}");
        }
    }
}

/// Asserts the persisted score index actually has the topology the
/// regression depends on: multiple pages, with at least one key's duplicates
/// crossing a page boundary. Without this, a page-capacity or serialization
/// change could shrink the workload into one node and the test would go
/// green without exercising the bug.
async fn assert_straddling_topology(dir: &str) {
    use data_bucket::INNER_PAGE_SIZE;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;
    use tokio::fs::OpenOptions;

    let path = format!("{dir}/duplicate_key_reload/score_idx.wt.idx");
    let mut file = OpenOptions::new().read(true).write(true).open(&path).await.unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(1));
    let toc = IndexTableOfContents::<(u64, Link), { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    let mappings: Vec<_> = toc.iter().map(|(_, page_id)| *page_id).collect();
    assert!(
        mappings.len() >= 2,
        "score index fits in a single page ({} pages); the workload no longer exercises node straddling",
        mappings.len()
    );

    let mut pages_per_key: BTreeMap<u64, u64> = BTreeMap::new();
    for page_id in mappings {
        let page = parse_page::<IndexPage<u64>, { DUPLICATE_KEY_RELOAD_PAGE_SIZE as u32 }>(&mut file, page_id.into())
            .await
            .unwrap();
        let keys: BTreeSet<u64> = page.inner.index_values[..page.inner.current_length as usize]
            .iter()
            .map(|v| v.key)
            .collect();
        for key in keys {
            *pages_per_key.entry(key).or_default() += 1;
        }
    }
    assert!(
        pages_per_key.values().any(|&pages| pages >= 2),
        "no key's duplicates cross a page boundary; the workload no longer exercises the regression"
    );
}

/// Regression test for lossy reconstruction of duplicate-key secondary
/// indexes (sized u64 and unsized String), including post-reload mutations.
///
/// Index pages store entries in event-arrival order, but `from_persisted`
/// used to treat the last entry of every page as the node's maximum and
/// re-append it with discriminator `u64::MAX - 1`. For pages that were
/// incrementally updated through CDC events (any bulk load), that "maximum"
/// was an arbitrary entry, so the reconstructed in-memory node index
/// registered wrong node maxima and every entry sorting above one became
/// unreachable through `select_by_*` — while `select_all` still returned
/// every row. Reconstruction (see `reconstruct_multi_index_nodes`) now sorts
/// each page, pins the persisted node id as each node's maximum, orders
/// same-max-key nodes by their minimum entry key, and assigns discriminators
/// that keep growing across node boundaries within one key.
///
/// The second phase mutates the reloaded table (inserts into existing
/// duplicate runs, deletes spread across every node including whole-key
/// removal, updates that move rows between keys) and reloads again: those
/// operations are routed through the reconstructed node maxima and the
/// on-disk table of contents, so they prove the reload left persistence in
/// an addressable state, not just a readable one.
#[test]
fn test_duplicate_key_secondary_index_survives_reload() {
    let dir = "tests/data/duplicate_key_index_reload/persisted";
    let config = DiskConfig::new_with_table_name(
        dir,
        DuplicateKeyReloadWorkTable::name_snake_case(),
        DuplicateKeyReloadWorkTable::version(),
    );

    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists(dir.to_string()).await;

        let mut model = Model::default();
        {
            let engine = DuplicateKeyReloadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = DuplicateKeyReloadWorkTable::load(engine).await.unwrap();

            for i in 0..ROWS {
                let bucket = bucket_name(i);
                table
                    .insert(DuplicateKeyReloadRow {
                        id: i,
                        score: i % KEYS,
                        bucket: bucket.clone(),
                        label: format!("row-{i}-{}", "x".repeat((i % 50) as usize)),
                    })
                    .unwrap();
                model.insert(i, i % KEYS, bucket).await;
            }

            model.assert_matches(&table, "in-memory before first persist");

            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence stalled on bulk insert")
                .expect("persistence engine failed");
        }

        assert_straddling_topology(dir).await;

        {
            let engine = DuplicateKeyReloadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = DuplicateKeyReloadWorkTable::load(engine).await.unwrap();

            model.assert_matches(&table, "after first reload");

            // Insert new rows into existing duplicate runs.
            for j in 0..500u64 {
                let id = 20_000 + j;
                let bucket = bucket_name(j);
                table
                    .insert(DuplicateKeyReloadRow {
                        id,
                        score: j % KEYS,
                        bucket: bucket.clone(),
                        label: format!("late-{j}"),
                    })
                    .unwrap();
                model.insert(id, j % KEYS, bucket).await;
            }
            // Deletes spread over every region of the key space (every 13th
            // row hits assorted in-node and boundary positions), plus one
            // whole key removed end to end so entire nodes drain.
            for id in (0..ROWS).step_by(13) {
                table.delete(id).await.unwrap();
                model.remove(id);
            }
            let score_42_ids: Vec<u64> = model
                .by_score
                .get(&42)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect();
            for id in score_42_ids {
                if id < ROWS {
                    table.delete(id).await.unwrap();
                    model.remove(id);
                }
            }
            // Updates that move rows between secondary keys.
            for id in (0..ROWS).step_by(17) {
                if model.by_score.values().any(|ids| ids.contains(&id)) {
                    let new_score = (id % KEYS) + 1_000;
                    table
                        .update_score_by_id(ScoreByIdQuery { score: new_score }, id)
                        .await
                        .unwrap();
                    model.move_score(id, new_score);
                }
            }

            model.assert_matches(&table, "in-memory after post-reload mutations");

            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence stalled on post-reload mutations")
                .expect("persistence engine failed");
        }
        {
            let engine = DuplicateKeyReloadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = DuplicateKeyReloadWorkTable::load(engine).await.unwrap();

            model.assert_matches(&table, "after second reload");
        }
    })
}

/// Companion stress case: a single key carrying every row. Every index node
/// holds the same key, so every node boundary is a straddle and same-key node
/// maxima must still resolve against the table of contents — the worst case
/// for the discriminator and node-ordering logic in `from_persisted`.
#[test]
fn test_single_key_all_duplicates_survives_reload() {
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
                        bucket: "the-bucket".to_string(),
                        label: format!("row-{i}"),
                    })
                    .unwrap();
            }
            assert_eq!(table.select_by_score(42).execute().unwrap().len() as u64, ROWS);

            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence stalled on bulk insert")
                .expect("persistence engine failed");
        }
        {
            let engine = DuplicateKeyReloadPersistenceEngine::new(config.clone()).await.unwrap();
            let table = DuplicateKeyReloadWorkTable::load(engine).await.unwrap();

            let ids: BTreeSet<u64> = table
                .select_by_score(42)
                .execute()
                .unwrap()
                .into_iter()
                .map(|r| r.id)
                .collect();
            assert_eq!(
                ids,
                (0..ROWS).collect::<BTreeSet<u64>>(),
                "single-key secondary index lost or duplicated ids across persist+reload"
            );

            // The reloaded table must also stay writable: post-reload CDC
            // events address nodes by their maximum, which this workload makes
            // ambiguous per key on purpose.
            table
                .insert(DuplicateKeyReloadRow {
                    id: ROWS,
                    score: 42,
                    bucket: "the-bucket".to_string(),
                    label: "post-reload".to_string(),
                })
                .unwrap();
            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence stalled on post-reload insert")
                .expect("persistence engine failed");
            assert_eq!(table.select_by_score(42).execute().unwrap().len() as u64, ROWS + 1);
        }
    })
}

/// Control for the reload test: the identical mutation battery with NO
/// reload in between. If this stalls too, the failure is in the live CDC
/// space-index path for duplicate keys, not in reconstruction.
#[test]
fn test_duplicate_key_mutations_without_reload() {
    let dir = "tests/data/duplicate_key_index_reload/no_reload";
    let config = DiskConfig::new_with_table_name(
        dir,
        DuplicateKeyReloadWorkTable::name_snake_case(),
        DuplicateKeyReloadWorkTable::version(),
    );

    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists(dir.to_string()).await;

        let mut model = Model::default();
        let engine = DuplicateKeyReloadPersistenceEngine::new(config.clone()).await.unwrap();
        let table = DuplicateKeyReloadWorkTable::load(engine).await.unwrap();

        for i in 0..ROWS {
            let bucket = bucket_name(i);
            table
                .insert(DuplicateKeyReloadRow {
                    id: i,
                    score: i % KEYS,
                    bucket: bucket.clone(),
                    label: format!("row-{i}-{}", "x".repeat((i % 50) as usize)),
                })
                .unwrap();
            model.insert(i, i % KEYS, bucket).await;
        }
        timeout(Duration::from_secs(30), table.wait_for_ops())
            .await
            .expect("persistence stalled on bulk insert")
            .expect("persistence engine failed");

        for j in 0..500u64 {
            let id = 20_000 + j;
            let bucket = bucket_name(j);
            table
                .insert(DuplicateKeyReloadRow {
                    id,
                    score: j % KEYS,
                    bucket: bucket.clone(),
                    label: format!("late-{j}"),
                })
                .unwrap();
            model.insert(id, j % KEYS, bucket).await;
        }
        for id in (0..ROWS).step_by(13) {
            table.delete(id).await.unwrap();
            model.remove(id);
        }
        let score_42_ids: Vec<u64> = model
            .by_score
            .get(&42)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        for id in score_42_ids {
            if id < ROWS {
                table.delete(id).await.unwrap();
                model.remove(id);
            }
        }
        for id in (0..ROWS).step_by(17) {
            if model.by_score.values().any(|ids| ids.contains(&id)) {
                let new_score = (id % KEYS) + 1_000;
                table
                    .update_score_by_id(ScoreByIdQuery { score: new_score }, id)
                    .await
                    .unwrap();
                model.move_score(id, new_score);
            }
        }

        model.assert_matches(&table, "in-memory after mutations (no reload)");

        timeout(Duration::from_secs(30), table.wait_for_ops())
            .await
            .expect("persistence stalled on mutations without any reload")
            .expect("persistence engine failed");
    })
}
