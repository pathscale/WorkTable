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
                rows.insert(id, row);
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
                rows.insert(id, row);
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
            rows.insert(reused_id, reused_row);
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

/// Batching the sweep lets a concurrent insert reuse freed space *while*
/// vacuum runs. On a persisted table that means an insert can claim a link on
/// a page the sweep is still working through, and both the insert and the
/// sweep's row moves go through the same CDC event stream.
///
/// Before the sweep was batched this could not happen at all: vacuum held the
/// registry for its whole duration, so no insert could reuse anything. Nothing
/// on the persistence path had ever seen this interleaving.
///
/// The deletes here are one contiguous block, which is also what makes the
/// freed links coalesce, so this exercises the ranged path rather than a
/// scattered one.
///
/// What this does not pin down: whether a given run hits the window where an
/// insert reuses a link on a page the sweep is mid-way through. That is
/// scheduling-dependent. The test creates the opportunity and asserts the
/// durable outcome; it is not a proof that the window was entered.
///
/// # This is the test that found the CDC event-id gap
///
/// It failed about half of runs with:
///
/// ```text
/// persistence stalled on primary index event gap:
/// last applied Id(2938), next available Id(2940) (attempt 9)
/// ```
///
/// Nothing was lost. Event ids are allocated during the index mutations while
/// an operation id is minted later, at the push site, so two concurrent
/// writers can invert the two orders; vacuum did it systematically because its
/// update lands on the destination page while inserts append to the current
/// one. Page-grouped batch collection then never visited the page holding the
/// missing id, rebuilt the same gapped batch every retry, and failed the
/// engine for that table permanently.
///
/// Fixed by the whole-queue fallback in `collect_batch_from_op_id`. The
/// deterministic version lives in `persistence::task`; this one is the
/// integration case that surfaced it, and it is kept because the race it
/// creates is the one that mattered.
#[test]
fn test_persisted_vacuum_survives_inserts_reusing_space_mid_sweep() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/vacuum/persisted_reuse",
        VacuumPersistWorkTable::name_snake_case(),
        VacuumPersistWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/vacuum/persisted_reuse".to_string()).await;

        let mut rows = HashMap::new();
        let deleted: Vec<u64>;
        {
            let engine = VacuumPersistPersistenceEngine::new(config.clone()).await.unwrap();
            let table = std::sync::Arc::new(VacuumPersistWorkTable::load(engine).await.unwrap());

            for i in 0..2_000i64 {
                let row = VacuumPersistRow {
                    id: table.get_next_pk().into(),
                    test: i,
                    another: i as u64,
                    exchange: format!("test{i}"),
                };
                rows.insert(row.id, row.clone());
                table.insert(row).await.unwrap();
            }

            // One contiguous block, well past the reclamation backlog so the
            // freed links actually reach the registry before the sweep.
            let mut ids: Vec<_> = rows.keys().copied().collect();
            ids.sort_unstable();
            deleted = ids.into_iter().skip(400).take(800).collect();
            for id in &deleted {
                table.delete(*id).await.unwrap();
            }
            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence should catch up before vacuum")
                .expect("persistence engine failed");

            // Inserts run against the table for the whole sweep, so they land
            // in the windows between batches where reuse is possible again.
            let inserting = tokio::spawn({
                let table = std::sync::Arc::clone(&table);
                async move {
                    let mut inserted = Vec::new();
                    for i in 2_000..2_600i64 {
                        let row = VacuumPersistRow {
                            id: table.get_next_pk().into(),
                            test: i,
                            another: i as u64,
                            exchange: format!("test{i}"),
                        };
                        inserted.push(row.clone());
                        let pk = row.id;
                        if let Err(error) = table.insert(row).await {
                            panic!(
                                "insert of fresh autoincrement pk {pk:?} failed: {error:?}; \
                                 already present in table: {}",
                                table.select(pk).is_some()
                            );
                        }
                        tokio::task::yield_now().await;
                    }
                    inserted
                }
            });

            let stats = table.vacuum().vacuum().await.unwrap();
            assert!(
                stats.pages_freed > 0,
                "the sweep must actually reclaim pages, or this test proves nothing about \
                 interleaving with it"
            );
            for row in inserting.await.unwrap() {
                rows.insert(row.id, row);
            }

            // Longer than the engine's own give-up budget, so a stall surfaces
            // as its diagnostic naming the missing event id rather than as a
            // bare timeout here, which says nothing.
            timeout(Duration::from_secs(90), table.wait_for_ops())
                .await
                .expect("persistence stalled after a sweep interleaved with inserts")
                .expect("persistence engine failed");

            for id in &deleted {
                rows.remove(id);
            }
            for (id, expected) in &rows {
                assert_eq!(
                    table.select(*id).as_ref(),
                    Some(expected),
                    "row {id} lost or corrupted by a sweep running alongside inserts"
                );
            }
            let table = std::sync::Arc::into_inner(table).expect("the inserting task has finished");
            table.close().await.unwrap();
        }

        // The reload is the part that a mid-sweep reuse would break: an insert
        // writing through a link the sweep also relocated leaves the on-disk
        // state describing two different rows at one address.
        let engine = VacuumPersistPersistenceEngine::new(config.clone()).await.unwrap();
        let table = VacuumPersistWorkTable::load(engine).await.unwrap();
        for (id, expected) in &rows {
            assert_eq!(
                table.select(*id).as_ref(),
                Some(expected),
                "row {id} did not survive the reload after a sweep interleaved with inserts"
            );
        }
        for id in &deleted {
            assert!(table.select(*id).is_none(), "deleted row {id} came back after reload");
        }
    });
}
