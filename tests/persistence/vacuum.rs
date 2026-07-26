use std::collections::HashMap;
use std::time::Duration;

use tokio::time::timeout;

use crate::remove_dir_if_exists;
use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: VacuumPersist,
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

/// Vacuum on a persisted table must push the row moves through the CDC
/// persistence stream: the moved links have to reach the on-disk indexes, and
/// the event-id sequence must stay gapless so persistence does not stall.
#[test]
fn test_vacuum_on_persisted_table_survives_reload() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/vacuum/persisted",
        VacuumPersistWorkTable::name_snake_case(),
        VacuumPersistWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/vacuum/persisted".to_string()).await;

        let mut rows = HashMap::new();
        let deleted: Vec<u64>;
        {
            let engine = VacuumPersistPersistenceEngine::new(config.clone()).await.unwrap();
            let table = VacuumPersistWorkTable::load(engine).await.unwrap();

            // row is ~40 bytes so ~409 rows per page; use multiple pages so
            // defragment has non-current pages to move rows from. Drain the
            // persistence queue every 100 rows: unthrottled bulk loads hit a
            // pre-existing index-space batching bug ("page should be available
            // in table of contents") that is unrelated to vacuum.
            for i in 0..1000i64 {
                let row = VacuumPersistRow {
                    id: table.get_next_pk().into(),
                    test: i,
                    another: i as u64,
                    exchange: format!("test{i}"),
                };
                let id = row.id;
                table.insert(row.clone()).unwrap();
                rows.insert(id, row);
                if i % 100 == 99 {
                    timeout(Duration::from_secs(30), table.wait_for_ops())
                        .await
                        .expect("persistence should keep up with throttled inserts");
                }
            }

            let mut ids: Vec<_> = rows.keys().cloned().collect();
            ids.sort_unstable();
            deleted = ids.into_iter().take(50).collect();
            for id in &deleted {
                table.delete(*id).await.unwrap();
            }

            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence should catch up before vacuum");

            let vacuum = table.vacuum();
            let stats = vacuum.vacuum().await.unwrap();
            assert!(stats.pages_freed > 0, "vacuum should have moved rows off a page");

            // Insert after vacuum: these operations carry event ids issued
            // after the moves, so if vacuum consumed ids without queueing the
            // events, the batch validator defers on the gap forever.
            for i in 1000..1100i64 {
                let row = VacuumPersistRow {
                    id: table.get_next_pk().into(),
                    test: i,
                    another: i as u64,
                    exchange: format!("test{i}"),
                };
                let id = row.id;
                table.insert(row.clone()).unwrap();
                rows.insert(id, row);
                if i % 50 == 49 {
                    timeout(Duration::from_secs(30), table.wait_for_ops())
                        .await
                        .expect("persistence stalled after vacuum on persisted table");
                }
            }

            // Without CDC-aware vacuum this stalls forever: the moved links
            // never reach the persistence stream while their event ids are
            // consumed, leaving a permanent gap the batch validator defers on.
            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence stalled after vacuum on persisted table");

            for id in &deleted {
                rows.remove(id);
            }
            for (id, expected) in &rows {
                assert_eq!(table.select(*id).as_ref(), Some(expected));
            }
        }
        {
            let engine = VacuumPersistPersistenceEngine::new(config.clone()).await.unwrap();
            let table = VacuumPersistWorkTable::load(engine).await.unwrap();

            assert_eq!(table.select_all().execute().unwrap().len(), rows.len());
            for (id, expected) in &rows {
                assert_eq!(table.select(*id).as_ref(), Some(expected));
                // Secondary indexes must follow the moved links too.
                assert_eq!(table.select_by_test(expected.test).as_ref(), Some(expected));
                assert_eq!(
                    table.select_by_exchange(expected.exchange.clone()).execute().unwrap(),
                    vec![expected.clone()]
                );
            }
            for id in &deleted {
                assert_eq!(table.select(*id), None);
            }
        }
    })
}
