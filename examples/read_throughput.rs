//! Concurrent point-read throughput probe.
//!
//! Measures generated `select` throughput at 1, 2, 4, and 8 reader threads
//! over a resident table, plus delete/insert churn throughput while readers
//! run. Built to compare the read-side grace-period mechanisms (global
//! counter vs epoch pin): run it on both revisions with
//!
//! ```text
//! cargo run --release --example read_throughput
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: Bench,
    columns: {
        id: u64 primary_key,
        value: u64,
    }
);

const ROWS: u64 = 10_000;
const READS_PER_THREAD: u64 = 1_000_000;

fn read_pass(table: &Arc<BenchWorkTable>, threads: u64) -> f64 {
    let start = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let table = table.clone();
            std::thread::spawn(move || {
                let mut hits = 0u64;
                for i in 0..READS_PER_THREAD {
                    let pk = (i.wrapping_mul(2_654_435_761).wrapping_add(t)) % ROWS;
                    if table.select(pk).is_some() {
                        hits += 1;
                    }
                }
                hits
            })
        })
        .collect();
    let hits: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    let elapsed = start.elapsed();
    assert_eq!(hits, threads * READS_PER_THREAD);
    (threads * READS_PER_THREAD) as f64 / elapsed.as_secs_f64()
}

/// In-place row mutation throughput on thread-disjoint rows spread across
/// many pages. Under a table-global page write lock these serialize whatever
/// the thread count; per-page barriers let disjoint pages proceed in
/// parallel.
fn write_pass(table: &Arc<BenchWorkTable>, links: &Arc<Vec<Link>>, threads: u64) -> f64 {
    const WRITES_PER_THREAD: u64 = 200_000;
    let start = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let table = table.clone();
            let links = links.clone();
            std::thread::spawn(move || {
                // Thread-disjoint rows: t, t + threads, t + 2*threads, ...
                let mine: Vec<Link> = links
                    .iter()
                    .copied()
                    .skip(t as usize)
                    .step_by(threads as usize)
                    .collect();
                for i in 0..WRITES_PER_THREAD {
                    let link = mine[(i as usize).wrapping_mul(97) % mine.len()];
                    unsafe {
                        table
                            .0
                            .data
                            .with_mut_ref(link, |archived| {
                                archived.inner.value = i.into();
                            })
                            .unwrap();
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();
    (threads * WRITES_PER_THREAD) as f64 / elapsed.as_secs_f64()
}

fn main() {
    let table = Arc::new(BenchWorkTable::default());
    for id in 0..ROWS {
        table.insert(BenchRow { id, value: id * 3 }).unwrap();
    }

    let links: Arc<Vec<Link>> = Arc::new(
        (0..ROWS)
            .map(|id| {
                table
                    .0
                    .primary_index
                    .pk_map
                    .get_value(&BenchPrimaryKey::from(id))
                    .unwrap()
                    .0
            })
            .collect(),
    );

    for threads in [1u64, 2, 4, 8] {
        let _ = write_pass(&table, &links, threads);
        let ops = write_pass(&table, &links, threads);
        println!("write threads={threads} {:>12.0} ops/s", ops);
    }

    for threads in [1u64, 2, 4, 8] {
        // Warm-up pass keeps publication hydration and thread setup out of
        // the measured pass.
        let _ = read_pass(&table, threads);
        let ops = read_pass(&table, threads);
        println!("read  threads={threads} {:>12.0} ops/s", ops);
    }

    // Delete/insert churn while 4 readers scan without pause: measures how
    // the reclamation scheme behaves under sustained reader overlap.
    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));
    let readers: Vec<_> = (0..4u64)
        .map(|t| {
            let table = table.clone();
            let stop = stop.clone();
            let reads = reads.clone();
            std::thread::spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let pk = (i.wrapping_mul(2_654_435_761).wrapping_add(t)) % ROWS;
                    if table.select(pk).is_some() {
                        reads.fetch_add(1, Ordering::Relaxed);
                    }
                    i += 1;
                }
            })
        })
        .collect();

    const CHURN: u64 = 200_000;
    let start = Instant::now();
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        for i in 0..CHURN {
            let id = ROWS + (i % 64);
            table.insert(BenchRow { id, value: i }).unwrap();
            table.delete(id).await.unwrap();
        }
    });
    let elapsed = start.elapsed();
    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }
    println!(
        "churn under 4 readers: {:>12.0} pairs/s ({} reads observed, {} pages)",
        CHURN as f64 / elapsed.as_secs_f64(),
        reads.load(Ordering::Relaxed),
        table.0.data.get_page_count(),
    );
}
