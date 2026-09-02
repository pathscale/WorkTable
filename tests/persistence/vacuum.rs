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
        let reused_after_reload_id: u64;
        {
            let engine = VacuumPersistPersistenceEngine::new(config.clone()).await.unwrap();
            let table = VacuumPersistWorkTable::load(engine).await.unwrap();

            // row is ~40 bytes so ~409 rows per page; use multiple pages so
            // defragment has non-current pages to move rows from.
            for i in 0..1000i64 {
                let row = VacuumPersistRow {
                    id: table.get_next_pk().into(),
                    test: i,
                    another: i as u64,
                    exchange: format!("test{i}"),
                };
                let id = row.id;
                table.insert(row.clone()).await.unwrap();
                rows.insert(id, row).await;
            }

            let mut ids: Vec<_> = rows.keys().cloned().collect();
            ids.sort_unstable();
            deleted = ids.into_iter().take(50).collect();
            for id in &deleted {
                table.delete(*id).await.unwrap();
            }

            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence should catch up before vacuum")
                .expect("persistence engine failed");
            let physical_bytes_before = table.persisted_data_file_size_bytes().await.unwrap();

            let vacuum = table.vacuum();
            let stats = vacuum.vacuum().await.unwrap();
            assert!(stats.pages_freed > 0, "vacuum should have moved rows off a page");
            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence should catch up after vacuum")
                .expect("persistence engine failed");
            let physical_bytes_after = table.persisted_data_file_size_bytes().await.unwrap();
            assert!(
                physical_bytes_after >= physical_bytes_before,
                "online vacuum may append a relocation page; durable reclamation is reuse, not truncation"
            );

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
                table.insert(row.clone()).await.unwrap();
                rows.insert(id, row).await;
                if i % 50 == 49 {
                    timeout(Duration::from_secs(30), table.wait_for_ops())
                        .await
                        .expect("persistence stalled after vacuum on persisted table")
                        .expect("persistence engine failed");
                }
            }

            // Without CDC-aware vacuum this stalls forever: the moved links
            // never reach the persistence stream while their event ids are
            // consumed, leaving a permanent gap the batch validator defers on.
            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence stalled after vacuum on persisted table")
                .expect("persistence engine failed");

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

            let physical_bytes_before_reuse = table.persisted_data_file_size_bytes().await.unwrap();
            let durable_free_bytes: u64 = table
                .0
                .data
                .get_empty_links()
                .iter()
                .map(|link| u64::from(link.length))
                .sum();
            assert!(
                durable_free_bytes > 0,
                "vacuum-freed ranges must survive reload so later inserts can reuse them"
            );

            // Exercise reuse after reload. Without durable free-page metadata,
            // this insert allocates a new page and grows `.wt.data` again.
            let reused_row = VacuumPersistRow {
                id: table.get_next_pk().into(),
                test: 1_100,
                another: 1_100,
                exchange: "reused-after-reload".to_string(),
            };
            let reused_id = reused_row.id;
            reused_after_reload_id = reused_id;
            table.insert(reused_row.clone()).await.unwrap();
            rows.insert(reused_id, reused_row).await;
            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence should catch up after durable page reuse")
                .expect("persistence engine failed");
            assert_eq!(
                table.persisted_data_file_size_bytes().await.unwrap(),
                physical_bytes_before_reuse,
                "an insert after reload must consume vacuum-freed space instead of extending the file"
            );

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
        {
            // Reload once more and allocate from the remaining durable range.
            // The first reused slot must have been removed from the free
            // metadata before its bytes were written, or this insert could
            // overwrite it after reopening the table.
            let engine = VacuumPersistPersistenceEngine::new(config.clone()).await.unwrap();
            let table = VacuumPersistWorkTable::load(engine).await.unwrap();
            let second_reused_row = VacuumPersistRow {
                id: table.get_next_pk().into(),
                test: 1_101,
                another: 1_101,
                exchange: "second-reuse-after-reload".to_string(),
            };
            table.insert(second_reused_row).await.unwrap();
            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence should catch up after a second durable page reuse")
                .expect("persistence engine failed");

            assert_eq!(
                table.select(reused_after_reload_id).as_ref(),
                rows.get(&reused_after_reload_id),
                "consumed durable free ranges must not be offered again after another reload"
            );
        }
    })
}
