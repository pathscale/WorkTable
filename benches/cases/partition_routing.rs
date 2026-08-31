//! Routing cost for `partition_by`.
//!
//! The design was justified with a bare `Vec` index measured at 0.73 ns. That
//! is not what the public API does: `partition` revives an `Arc`, so it pays a
//! fetch-add and a fetch-sub, and on a key several threads share those contend.
//! These cases measure what a caller actually executes, so a future change in
//! ordering, refcounting or allocation shows up here rather than in production.
//!
//! Read the same-key group before quoting any single-thread number: aggregate
//! throughput there falls as readers are added, which is the whole point.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group};
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: Route,
    partition_by: symbol_id: u16,
    columns: {
        exchange_id: u8 primary_key,
        bid: f64,
        ask: f64
    }
);

const PARTITIONS: u16 = 500;

fn populated() -> RoutePartitions {
    let routes = RoutePartitions::new();
    for k in 0..PARTITIONS {
        let table = routes.partition_or_create(k).unwrap();
        table
            .insert(RouteRow {
                exchange_id: 1,
                bid: k as f64,
                ask: k as f64 + 1.0,
            })
            .unwrap();
    }
    routes
}

/// The four ways to reach a partition, single threaded, one hot key.
fn lookup(c: &mut Criterion) {
    let routes = populated();
    let cached = routes.partition(7).unwrap();

    let mut group = c.benchmark_group("partition_lookup");

    // The floor: the handle is already in hand. Routing cost is measured
    // against this, and the cases below deliberately do no table work, because
    // a `select` costs an order of magnitude more and hides the difference.
    group.bench_function("cached_handle", |b| b.iter(|| black_box(&*cached)));

    // Routing loads only, no refcount.
    group.bench_function("contains", |b| b.iter(|| black_box(routes.contains(7))));

    // Borrowed lookup: three dependent loads, no atomic RMW.
    group.bench_function("partition_ref", |b| b.iter(|| black_box(routes.partition_ref(7))));

    // Reference counted lookup: two atomic RMWs, one on the way in and one
    // when the returned handle drops.
    group.bench_function("partition_arc", |b| b.iter(|| black_box(routes.partition(7))));

    // What a tick actually costs: routing plus the smallest real table read.
    group.bench_function("partition_ref_then_select", |b| {
        b.iter(|| black_box(routes.partition_ref(7)).map(|t| t.select(1)))
    });

    group.finish();
}

fn contended(c: &mut Criterion, name: &str, same_key: bool) {
    let mut group = c.benchmark_group(name);
    for threads in [1usize, 2, 4, 8] {
        group.throughput(Throughput::Elements(threads as u64));
        for api in ["partition_ref", "partition_arc"] {
            group.bench_with_input(BenchmarkId::new(api, threads), &threads, |b, &threads| {
                b.iter_custom(|iters| {
                    let routes = Arc::new(populated());
                    let go = Arc::new(AtomicBool::new(false));
                    let arc_api = api == "partition_arc";

                    let workers: Vec<_> = (0..threads)
                        .map(|t| {
                            let routes = routes.clone();
                            let go = go.clone();
                            // Same key means every thread hammers one
                            // strong count; distinct keys spread them.
                            let key = if same_key { 7u16 } else { (t as u16) % PARTITIONS };
                            std::thread::spawn(move || {
                                while !go.load(Ordering::Relaxed) {
                                    std::hint::spin_loop();
                                }
                                let start = std::time::Instant::now();
                                for _ in 0..iters {
                                    if arc_api {
                                        black_box(routes.partition(key));
                                    } else {
                                        black_box(routes.partition_ref(key));
                                    }
                                }
                                start.elapsed()
                            })
                        })
                        .collect();

                    go.store(true, Ordering::Relaxed);
                    // Slowest worker: the wall clock the caller sees.
                    workers.into_iter().map(|w| w.join().unwrap()).max().unwrap_or_default()
                })
            });
        }
    }
    group.finish();
}

/// Every thread routes to one key, so the strong count is a single shared line.
fn same_key_readers(c: &mut Criterion) {
    contended(c, "partition_same_key", true);
}

/// Threads route to different keys, which is the case partitioning is for.
fn distinct_key_readers(c: &mut Criterion) {
    contended(c, "partition_distinct_keys", false);
}

/// Accounting over 500 partitions. These were routed through `system_info`,
/// which copied every data page, and through `keys` then `partition` per key.
fn metrics(c: &mut Criterion) {
    let routes = populated();
    let mut group = c.benchmark_group("partition_metrics");
    group.bench_function("memory_total", |b| b.iter(|| black_box(routes.memory_total())));
    group.bench_function("rows_by_key", |b| b.iter(|| black_box(routes.rows_by_key())));
    group.bench_function("keys", |b| b.iter(|| black_box(routes.keys())));
    group.finish();
}

criterion_group!(
    partition_routing_benchmarks,
    lookup,
    same_key_readers,
    distinct_key_readers,
    metrics
);
