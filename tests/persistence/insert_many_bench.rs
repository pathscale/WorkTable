//! Measures the consumer workload shape: 5,000 rows into a persisted table
//! with a u64 autoincrement arctic primary key, a u128 unique arctic index
//! and a String column, once through the per-row insert loop and once through
//! `insert_many`.
//!
//! Run in release mode for meaningful numbers:
//! `cargo test --release --test mod insert_many_bench -- --nocapture`
//!
//! Two figures are reported per path: the caller-side cost (until the insert
//! call returns and rows are visible) and the durable cost (including
//! `wait_for_ops`). No timing assertion is made here; timings on shared CI
//! hardware are unstable, and the printed numbers are the deliverable.

use std::time::Instant;

use worktable::prelude::*;

use super::insert_many::{BatchPersistPersistenceEngine, BatchPersistRow, BatchPersistWorkTable, row};
use crate::remove_dir_if_exists;

const ROWS: u64 = 5_000;

async fn table_at(dir: &str) -> BatchPersistWorkTable {
    remove_dir_if_exists(dir.to_string()).await;
    let config = DiskConfig::new_with_table_name(
        dir,
        BatchPersistWorkTable::name_snake_case(),
        BatchPersistWorkTable::version(),
    );
    let engine = BatchPersistPersistenceEngine::new(config).await.unwrap();
    BatchPersistWorkTable::load(engine).await.unwrap()
}

fn rows_for(range: std::ops::Range<u64>) -> Vec<BatchPersistRow> {
    range.map(|id| row(id, 70_000 + id as u128)).collect()
}

#[test]
fn insert_many_vs_loop_insert_at_5000_rows() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        // Per-row insert loop.
        let loop_table = table_at("tests/data/insert_many/bench_loop").await;
        let loop_rows = rows_for(loop_table.reserve_pks(ROWS as usize));
        let started = Instant::now();
        for row in loop_rows {
            loop_table.insert(row).unwrap();
        }
        let loop_visible = started.elapsed();
        loop_table.wait_for_ops().await.unwrap();
        let loop_durable = started.elapsed();
        assert_eq!(loop_table.count(), ROWS as usize);

        // One insert_many batch.
        let batch_table = table_at("tests/data/insert_many/bench_batch").await;
        let batch_rows = rows_for(batch_table.reserve_pks(ROWS as usize));
        let started = Instant::now();
        batch_table.insert_many(batch_rows).unwrap();
        let batch_visible = started.elapsed();
        batch_table.wait_for_ops().await.unwrap();
        let batch_durable = started.elapsed();
        assert_eq!(batch_table.count(), ROWS as usize);

        let per_row = |d: std::time::Duration| d.as_nanos() as f64 / ROWS as f64;
        println!("persisted insert of {ROWS} rows (per-row figures):");
        println!(
            "  loop insert:  visible {:>8.0} ns/row, durable {:>8.0} ns/row",
            per_row(loop_visible),
            per_row(loop_durable),
        );
        println!(
            "  insert_many:  visible {:>8.0} ns/row, durable {:>8.0} ns/row",
            per_row(batch_visible),
            per_row(batch_durable),
        );
        println!(
            "  speedup:      visible {:>8.2}x, durable {:>8.2}x",
            per_row(loop_visible) / per_row(batch_visible),
            per_row(loop_durable) / per_row(batch_durable),
        );
    });
}
