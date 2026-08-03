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

/// Regression test for an index-space batching bug: an unthrottled bulk
/// insert+delete used to panic the persistence engine task with "page should
/// be available in table of contents" (process_change_event_batch), after
/// which wait_for_ops hung forever; the timeout below turns that hang into a
/// failure.
///
/// Page-grouped batch collection puts the delete events (ids far ahead of the
/// batched inserts) into the same batch as the first data page's inserts, so
/// the prepared event stream has an interior id gap. `validate_events` used to
/// scan only 30 events back from the end; the 50 contiguous delete events hid
/// the gap, the batch was applied with the hole, and the on-disk index lost
/// track of node max transitions carried by the missing events. The scan is
/// now unbounded, so the tail after the gap is deferred until the missing
/// events arrive.
///
/// The same unthrottled load also used to hit a second bug: a lagging batch
/// creating several data pages at once picked `last_page_id` from unordered
/// HashMap keys in `save_batch_data`, so a later batch could re-create an
/// existing page zero-filled and this test's reload phase read back zeroed
/// rows. Both fixes are needed for this test to be stable.
#[test]
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
                .expect("persistence stalled on bulk insert+delete")
                .expect("persistence engine failed");

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
