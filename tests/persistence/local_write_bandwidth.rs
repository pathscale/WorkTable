//! What WorkTable's local persistence path sustains, in bytes per second.
//!
//! Measured through `insert` and `wait_for_ops` rather than against
//! `persist_page` directly, so it counts everything the engine does to make a
//! write durable and not just the call at the bottom of it.

use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable_codegen::worktable;

use crate::remove_dir_if_exists;

worktable!(
    name: WriteBandwidth,
    persist: true,
    columns: {
        id: u64 primary_key,
        payload: String,
    }
);

// The same table without persistence, so the cost of being a WorkTable can be
// told apart from the cost of writing to disk. Anything this arm spends is
// spent by the persisted one too, before any page is written.
worktable!(
    name: WriteBandwidthMemory,
    columns: {
        id: u64 primary_key,
        payload: String,
    }
);

/// A page of the on-disk format, which is the unit a write actually lands in.
const PAGE: usize = 4096 * 4;

/// Per-page checksums of every file, so two snapshots say how many pages a
/// stretch of work really wrote.
fn page_checksums(dir: &str) -> Vec<(String, Vec<u32>)> {
    fn walk(dir: &std::path::Path, into: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else { return };
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, into);
            } else if path.is_file() {
                into.push(path);
            }
        }
    }
    let mut paths = Vec::new();
    walk(std::path::Path::new(dir), &mut paths);
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            let bytes = std::fs::read(&path).expect("a table file");
            (
                path.to_string_lossy().into_owned(),
                bytes.chunks(PAGE).map(crc32fast::hash).collect(),
            )
        })
        .collect()
}

/// Bytes that differ between two snapshots, counted a page at a time.
fn written_bytes(before: &[(String, Vec<u32>)], after: &[(String, Vec<u32>)]) -> u64 {
    let mut pages = 0u64;
    for (name, now) in after {
        let then = before
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, c)| c.as_slice())
            .unwrap_or(&[]);
        pages += now
            .iter()
            .enumerate()
            .filter(|(index, checksum)| then.get(*index) != Some(*checksum))
            .count() as u64;
    }
    pages * PAGE as u64
}

fn table_bytes(dir: &str) -> u64 {
    fn walk(dir: &std::path::Path, total: &mut u64) {
        let Ok(entries) = std::fs::read_dir(dir) else { return };
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, total);
            } else if let Ok(meta) = entry.metadata() {
                *total += meta.len();
            }
        }
    }
    let mut total = 0;
    walk(std::path::Path::new(dir), &mut total);
    total
}

#[test]
#[ignore = "a measurement, not an assertion"]
fn local_write_bandwidth() {
    let dir = "tests/data/local_write_bandwidth";
    let config = DiskConfig::new_with_table_name(
        dir,
        WriteBandwidthWorkTable::name_snake_case(),
        WriteBandwidthWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists(dir.to_string()).await;
        let engine = WriteBandwidthPersistenceEngine::new(config.clone()).await.unwrap();
        let table = WriteBandwidthWorkTable::load(engine).await.unwrap();
        let payload = "x".repeat(4096);

        // ---- bulk load: consecutive pages, which is the batch path's shape
        const ROWS: u64 = 25_000;
        let at = std::time::Instant::now();
        for id in 0..ROWS {
            table
                .insert(WriteBandwidthRow {
                    id,
                    payload: payload.clone(),
                })
                .await
                .unwrap();
        }
        table.wait_for_ops().await.expect("the queue drains");
        let bulk = at.elapsed().as_secs_f64();
        let bytes = table_bytes(dir);

        // ---- scattered updates into what already exists
        const UPDATES: u64 = 2_000;
        let replacement = "y".repeat(4096);
        let pages_before = page_checksums(dir);
        let at = std::time::Instant::now();
        for n in 0..UPDATES {
            // Spread across the whole table rather than a contiguous run.
            let id = (n * (ROWS / UPDATES)) % ROWS;
            table
                .update(WriteBandwidthRow {
                    id,
                    payload: replacement.clone(),
                })
                .await
                .unwrap();
        }
        table.wait_for_ops().await.expect("the queue drains");
        let scattered = at.elapsed().as_secs_f64();
        let scattered_bytes = written_bytes(&pages_before, &page_checksums(dir));

        println!("table {:.1} MB on disk", bytes as f64 / 1e6);
        println!(
            "  bulk insert, {ROWS} rows       : {:>8.1} ms   {:>7.0} MB/s   {:>8.0} rows/s",
            bulk * 1e3,
            bytes as f64 / 1e6 / bulk,
            ROWS as f64 / bulk,
        );
        println!(
            "  scattered update, {UPDATES} rows : {:>8.1} ms   {:>7.0} MB/s   {:>8.0} rows/s   {:.1} MB written",
            scattered * 1e3,
            scattered_bytes as f64 / 1e6 / scattered,
            UPDATES as f64 / scattered,
            scattered_bytes as f64 / 1e6,
        );

        // ---- the same inserts with nothing underneath them
        let memory = WriteBandwidthMemoryWorkTable::default();
        let at = std::time::Instant::now();
        for id in 0..ROWS {
            memory
                .insert(WriteBandwidthMemoryRow {
                    id,
                    payload: payload.clone(),
                })
                .await
                .unwrap();
        }
        let in_memory = at.elapsed().as_secs_f64();
        println!(
            "  in memory, no persistence    : {:>8.1} ms   {:>7.0} MB/s   {:>8.0} rows/s",
            in_memory * 1e3,
            bytes as f64 / 1e6 / in_memory,
            ROWS as f64 / in_memory,
        );
        println!(
            "    ^ persistence adds {:.1} ms on top of {:.1} ms of table work",
            (bulk - in_memory) * 1e3,
            in_memory * 1e3,
        );

        remove_dir_if_exists(dir.to_string()).await;
    });
}
