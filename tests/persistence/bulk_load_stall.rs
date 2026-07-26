use std::collections::HashMap;
use std::time::Duration;

use tokio::time::timeout;

use crate::remove_dir_if_exists;
use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: BulkLoadStall,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        test: i64,
        another: u64,
        exchange: String,
    },
    indexes: {
        test_idx: test unique,
        another_idx: another,
        exchange_idx: exchange,
    },
);

/// Reproduction for a pre-existing index-space batching bug: an unthrottled
/// bulk insert+delete panics the persistence engine task with "page should be
/// available in table of contents" (src/persistence/space/index/mod.rs /
/// unsized_.rs, process_change_event_batch), after which wait_for_ops hangs
/// forever; the timeout below turns that hang into a failure.
///
/// The failure is timing/batch-boundary dependent and hits roughly 4 out of 5
/// runs, so it is ignored in the normal suite. Run it with:
/// `cargo test --test mod persistence::bulk_load_stall -- --ignored`.
/// No vacuum is involved; tests/persistence/vacuum.rs throttles its bulk
/// phases specifically to stay clear of this bug. Once fixed, un-ignore this
/// test and drop that throttling.
#[test]
#[ignore = "exposes a known index-space TOC bug; fails ~4/5 runs until it is fixed"]
fn test_bulk_insert_delete_persistence() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/bulk_load_stall/persisted",
        BulkLoadStallWorkTable::name_snake_case(),
        BulkLoadStallWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/bulk_load_stall/persisted".to_string()).await;

        let mut rows = HashMap::new();
        {
            let engine = BulkLoadStallPersistenceEngine::new(config.clone()).await.unwrap();
            let table = BulkLoadStallWorkTable::load(engine).await.unwrap();

            for i in 0..1000i64 {
                let row = BulkLoadStallRow {
                    id: table.get_next_pk().into(),
                    test: i,
                    another: i as u64,
                    exchange: format!("test{i}"),
                };
                let id = row.id;
                table.insert(row.clone()).unwrap();
                rows.insert(id, row);
            }

            let mut ids: Vec<_> = rows.keys().cloned().collect();
            ids.sort_unstable();
            let deleted: Vec<u64> = ids.into_iter().take(50).collect();
            for id in &deleted {
                table.delete(*id).await.unwrap();
            }

            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence stalled on bulk insert+delete");

            for id in &deleted {
                rows.remove(id);
            }
        }
        {
            let engine = BulkLoadStallPersistenceEngine::new(config.clone()).await.unwrap();
            let table = BulkLoadStallWorkTable::load(engine).await.unwrap();

            assert_eq!(table.select_all().execute().unwrap().len(), rows.len());
            for (id, expected) in &rows {
                assert_eq!(table.select(*id).as_ref(), Some(expected));
            }
        }
    })
}
