//! Batch insert on a persisted table: durability, CDC replay through the
//! persistence engine, and all-or-nothing rejection.
//!
//! The table mirrors the consumer's shape: u64 autoincrement arctic primary
//! key, u128 unique arctic index, String payload column.

use worktable::prelude::*;
use worktable::worktable;

use crate::remove_dir_if_exists;

worktable! (
    name: BatchPersist,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement using arctic,
        wallet: u128,
        payload: String,
    },
    indexes: {
        wallet_idx: wallet unique using arctic,
    },
);

fn row(id: u64, wallet: u128) -> BatchPersistRow {
    BatchPersistRow {
        id,
        wallet,
        payload: format!("payload-{id}"),
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap()
}

#[test]
fn batch_insert_survives_reload() {
    let dir = "tests/data/insert_many/reload";
    let config = DiskConfig::new_with_table_name(
        dir,
        BatchPersistWorkTable::name_snake_case(),
        BatchPersistWorkTable::version(),
    );

    runtime().block_on(async {
        remove_dir_if_exists(dir.to_string()).await;

        {
            let engine = BatchPersistPersistenceEngine::new(config.clone()).await.unwrap();
            let table = BatchPersistWorkTable::load(engine).await.unwrap();

            // Singles before the batch, so replay interleaves Single and
            // Multi operations.
            for _ in 0..3 {
                let id: u64 = table.get_next_pk().into();
                table.insert(row(id, 10_000 + id as u128)).unwrap();
            }

            let range = table.reserve_pks(200);
            let rows: Vec<_> = range.clone().map(|id| row(id, 20_000 + id as u128)).collect();
            let pks = table.insert_many(rows).unwrap();
            assert_eq!(pks.len(), 200);

            // Read-your-writes before any persistence wait.
            for id in range.clone() {
                assert_eq!(table.select(id).unwrap().wallet, 20_000 + id as u128);
            }

            // Singles after the batch too.
            let id: u64 = table.get_next_pk().into();
            table.insert(row(id, 30_000 + id as u128)).unwrap();

            table.wait_for_ops().await.unwrap();
        }

        {
            // Strict load re-validates primary/secondary state, so a reload
            // is also a full audit of what CDC replay produced.
            let engine = BatchPersistPersistenceEngine::new(config.clone()).await.unwrap();
            let table = BatchPersistWorkTable::load(engine).await.unwrap();

            assert_eq!(table.count(), 204);
            for id in 0..3u64 {
                assert_eq!(table.select(id).unwrap().wallet, 10_000 + id as u128);
            }
            for id in 3..203u64 {
                let selected = table.select(id).expect("batch row lost across reload");
                assert_eq!(selected.wallet, 20_000 + id as u128);
                assert_eq!(selected.payload, format!("payload-{id}"));
                let by_wallet = table
                    .select_by_wallet(20_000 + id as u128)
                    .expect("unique index entry lost across reload");
                assert_eq!(by_wallet.id, id);
            }
            assert_eq!(table.select(203).unwrap().wallet, 30_000 + 203);

            // The generator state survived: new keys continue after the batch.
            let next: u64 = table.get_next_pk().into();
            assert!(next >= 204, "pk generator state regressed to {next}");
            table.wait_for_ops().await.unwrap();
        }
    });
}

#[test]
fn rejected_batch_leaves_no_trace_after_reload() {
    let dir = "tests/data/insert_many/rejected";
    let config = DiskConfig::new_with_table_name(
        dir,
        BatchPersistWorkTable::name_snake_case(),
        BatchPersistWorkTable::version(),
    );

    runtime().block_on(async {
        remove_dir_if_exists(dir.to_string()).await;

        {
            let engine = BatchPersistPersistenceEngine::new(config.clone()).await.unwrap();
            let table = BatchPersistWorkTable::load(engine).await.unwrap();

            table.insert(row(0, 999)).unwrap();

            // Last row collides on the unique wallet index.
            let mut rows: Vec<_> = (1..20u64).map(|id| row(id, 40_000 + id as u128)).collect();
            rows.last_mut().unwrap().wallet = 999;

            let error = table.insert_many(rows).unwrap_err();
            match error {
                BatchInsertError::Row { row_index, source } => {
                    assert_eq!(row_index, 18);
                    assert!(matches!(source, WorkTableError::AlreadyExists(_)));
                }
                other => panic!("expected a row-level rejection, got {other:?}"),
            }
            assert_eq!(table.count(), 1);

            // The rejection's Acknowledge operation must flow through the
            // engine without stalling the event stream.
            table.wait_for_ops().await.unwrap();

            // The pipeline stays usable for later batches.
            let pks = table.insert_many((100..110u64).map(|id| row(id, 50_000 + id as u128)).collect());
            assert_eq!(pks.unwrap().len(), 10);
            table.wait_for_ops().await.unwrap();
        }

        {
            let engine = BatchPersistPersistenceEngine::new(config.clone()).await.unwrap();
            let table = BatchPersistWorkTable::load(engine).await.unwrap();

            assert_eq!(table.count(), 11, "rejected batch rows leaked into persistence");
            for id in 1..20u64 {
                assert!(table.select(id).is_none());
                assert!(table.select_by_wallet(40_000 + id as u128).is_none());
            }
            assert_eq!(table.select_by_wallet(999).unwrap().id, 0);
            for id in 100..110u64 {
                assert_eq!(table.select(id).unwrap().wallet, 50_000 + id as u128);
            }
            table.wait_for_ops().await.unwrap();
        }
    });
}

#[test]
fn batches_and_singles_interleave_through_the_engine() {
    let dir = "tests/data/insert_many/interleaved";
    let config = DiskConfig::new_with_table_name(
        dir,
        BatchPersistWorkTable::name_snake_case(),
        BatchPersistWorkTable::version(),
    );

    runtime().block_on(async {
        remove_dir_if_exists(dir.to_string()).await;

        {
            let engine = BatchPersistPersistenceEngine::new(config.clone()).await.unwrap();
            let table = BatchPersistWorkTable::load(engine).await.unwrap();

            let mut expected = 0u64;
            for round in 0..10u64 {
                let single: u64 = table.get_next_pk().into();
                table.insert(row(single, 60_000 + single as u128)).unwrap();
                expected += 1;

                let range = table.reserve_pks(25);
                let rows: Vec<_> = range.map(|id| row(id, 60_000 + id as u128)).collect();
                table.insert_many(rows).unwrap();
                expected += 25;

                if round % 3 == 0 {
                    table.wait_for_ops().await.unwrap();
                }
            }
            assert_eq!(table.count(), expected as usize);
            table.wait_for_ops().await.unwrap();
        }

        {
            let engine = BatchPersistPersistenceEngine::new(config.clone()).await.unwrap();
            let table = BatchPersistWorkTable::load(engine).await.unwrap();
            assert_eq!(table.count(), 260);
            for id in 0..260u64 {
                let selected = table.select(id).expect("row lost across reload");
                assert_eq!(selected.wallet, 60_000 + id as u128);
            }
            table.wait_for_ops().await.unwrap();
        }
    });
}
