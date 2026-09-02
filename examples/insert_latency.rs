//! Per-insert latency, serialized and concurrent.
//!
//! The question this settles: does making `insert` async cost anything on the
//! single-row path? An `async fn` builds a state machine even when it never
//! awaits, and a caller in a sync context pays a `block_on` per call.
use std::sync::Arc;
use std::time::Instant;
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: Bench, persist: false,
    columns: { id: u64 primary_key, payload: u64, bucket: u32 },
    indexes: { payload_idx: payload unique using worktables_index, bucket_idx: bucket using worktables_index },
);

const N: u64 = 200_000;
const SWEEP: [u64; 5] = [1, 2, 4, 8, 16];

fn row(id: u64) -> BenchRow { BenchRow { id, payload: 1_000_000 + id, bucket: (id % 16) as u32 } }

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    println!("{:>8} {:>14} {:>16} {:>12}", "threads", "latency/op", "throughput", "vs 1T");
    println!("{}", "-".repeat(54));
    let mut single = 0.0f64;
    for &threads in &SWEEP {
        let mut best = f64::MAX;
        for _ in 0..5 {
            let t = Arc::new(BenchWorkTable::default());
            let per = N / threads;
            let start = Instant::now();
            let mut handles = Vec::new();
            for w in 0..threads {
                let t = Arc::clone(&t);
                handles.push(tokio::spawn(async move {
                    for i in (w * per)..((w + 1) * per) {
                        t.insert(row(i)).await.unwrap();
                    }
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
            // Wall clock over the whole run, so this is aggregate throughput
            // and the per-op figure is the amortised cost, not a single call's
            // latency under no contention.
            let ns = start.elapsed().as_nanos() as f64 / N as f64;
            if ns < best {
                best = ns;
            }
        }
        let thr = 1e9 / best;
        if threads == 1 {
            single = thr;
        }
        println!("{threads:>8} {best:>11.0} ns {:>13.0} /s {:>11.2}x", thr, thr / single);
    }
}
