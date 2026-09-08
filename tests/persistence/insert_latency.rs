//! Per-insert latency, split by whether the insert had to allocate a page.
//!
//! These are two populations, not one distribution. An insert that fits in the
//! page already open is cheap; one that has to add a page pays for the page
//! list as well. Blending them hides the second behind the first, and how much
//! it hides depends on row size: at 4 KiB a page holds three rows, so a third
//! of all inserts allocate and the expensive population is not a tail at all.

use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable_codegen::worktable;

use crate::remove_dir_if_exists;

worktable!(
    name: InsertLatency,
    persist: true,
    columns: {
        id: u64 primary_key,
        payload: String,
    }
);

worktable!(
    name: InsertLatencyMemory,
    columns: {
        id: u64 primary_key,
        payload: String,
    }
);

fn report(label: &str, mut us: Vec<f64>, of: usize) {
    if us.is_empty() {
        println!("  {label:<34} (none)");
        return;
    }
    us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let at = |q: f64| us[((us.len() as f64 * q) as usize).min(us.len() - 1)];
    println!(
        "  {label:<34} {:>5.1}% of inserts   p50 {:>7.2}   p99 {:>8.2}   max {:>9.2}  (us)",
        100.0 * us.len() as f64 / of as f64,
        at(0.50),
        at(0.99),
        us[us.len() - 1],
    );
}

#[test]
#[ignore = "a measurement, not an assertion"]
fn insert_latency_split_by_page_allocation() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        const ROWS: u64 = 20_000;
        for size in [256usize, 4096] {
            let payload = "x".repeat(size);
            println!("payload {size} B, {ROWS} rows");

            // ---- no persistence
            let table = InsertLatencyMemoryWorkTable::default();
            let (mut same, mut fresh) = (Vec::new(), Vec::new());
            let mut pages = table.0.data.get_page_count();
            for id in 0..ROWS {
                let at = std::time::Instant::now();
                table
                    .insert(InsertLatencyMemoryRow {
                        id,
                        payload: payload.clone(),
                    })
                    .await
                    .unwrap();
                let took = at.elapsed().as_secs_f64() * 1e6;
                let now = table.0.data.get_page_count();
                if now == pages {
                    same.push(took)
                } else {
                    fresh.push(took)
                }
                pages = now;
            }
            report("in memory, existing page", same, ROWS as usize);
            report("in memory, allocated a page", fresh, ROWS as usize);

            // ---- persisted
            let dir = "tests/data/insert_latency";
            remove_dir_if_exists(dir.to_string()).await;
            let config = DiskConfig::new_with_table_name(
                dir,
                InsertLatencyWorkTable::name_snake_case(),
                InsertLatencyWorkTable::version(),
            );
            let engine = InsertLatencyPersistenceEngine::new(config).await.unwrap();
            let persisted = InsertLatencyWorkTable::load(engine).await.unwrap();
            let (mut same, mut fresh) = (Vec::new(), Vec::new());
            let mut pages = persisted.0.data.get_page_count();
            for id in 0..ROWS {
                let at = std::time::Instant::now();
                persisted
                    .insert(InsertLatencyRow {
                        id,
                        payload: payload.clone(),
                    })
                    .await
                    .unwrap();
                let took = at.elapsed().as_secs_f64() * 1e6;
                let now = persisted.0.data.get_page_count();
                if now == pages {
                    same.push(took)
                } else {
                    fresh.push(took)
                }
                pages = now;
            }
            report("persisted, existing page", same, ROWS as usize);
            report("persisted, allocated a page", fresh, ROWS as usize);
            persisted.wait_for_ops().await.expect("the queue drains");
            remove_dir_if_exists(dir.to_string()).await;
        }
    });
}
