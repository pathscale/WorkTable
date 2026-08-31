//! Non-unique arctic (u128 keys) against its WorkTablesIndex String
//! equivalent — the dependency-graph adjacency shape this backend replaces:
//! a hash key resolved to every row that carries it.
//!
//! Fan-outs mirror realistic adjacency degrees (1, 10, 1000 rows per key).
//! Fixture sizes shrink as fan-out grows so every point stays inside a small,
//! fixed time budget.

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group};
use std::time::Duration;
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: WtiStringAdjacency,
    columns: {
        id: u64 primary_key autoincrement,
        source: String,
        payload: u64,
    },
    indexes: {
        source_idx: source,
    }
);

worktable!(
    name: ArcticHashAdjacency,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement,
        source: u128,
        payload: u64,
    },
    indexes: {
        source_idx: source using arctic,
    }
);

/// (fan-out, distinct keys): totals stay in the same few-thousand-row band.
const SHAPES: [(u64, u64); 3] = [(1, 4096), (10, 512), (1000, 8)];

fn hash_of(key: u64) -> u128 {
    // Cheap splitmix-style spread so keys are not sequential in the index.
    let spread = (key as u128).wrapping_mul(0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C834);
    spread ^ (spread >> 64)
}

fn string_key(key: u64) -> String {
    format!("{:032x}", hash_of(key))
}

fn populated_wti(fan_out: u64, keys: u64) -> WtiStringAdjacencyWorkTable {
    let table = WtiStringAdjacencyWorkTable::default();
    for key in 0..keys {
        for copy in 0..fan_out {
            let row = WtiStringAdjacencyRow {
                id: table.get_next_pk().into(),
                source: string_key(key),
                payload: copy,
            };
            table.insert(row).unwrap();
        }
    }
    table
}

fn populated_arctic(fan_out: u64, keys: u64) -> ArcticHashAdjacencyWorkTable {
    let table = ArcticHashAdjacencyWorkTable::default();
    for key in 0..keys {
        for copy in 0..fan_out {
            let row = ArcticHashAdjacencyRow {
                id: table.get_next_pk().into(),
                source: hash_of(key),
                payload: copy,
            };
            table.insert(row).unwrap();
        }
    }
    table
}

fn select_by_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("nonunique_select_by_key");
    for (fan_out, keys) in SHAPES {
        group.throughput(Throughput::Elements(fan_out));

        let table = populated_wti(fan_out, keys);
        group.bench_with_input(BenchmarkId::new("wti_string", fan_out), &fan_out, |b, _| {
            b.iter(|| {
                let key = string_key(fastrand::u64(0..keys));
                black_box(table.select_by_source(key).execute().unwrap())
            })
        });

        let table = populated_arctic(fan_out, keys);
        group.bench_with_input(BenchmarkId::new("arctic_u128", fan_out), &fan_out, |b, _| {
            b.iter(|| {
                let key = hash_of(fastrand::u64(0..keys));
                black_box(table.select_by_source(key).execute().unwrap())
            })
        });
    }
    group.finish();
}

fn insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("nonunique_insert");
    for (fan_out, keys) in SHAPES {
        // Steady state: the table already holds `fan_out` rows per key and
        // each measured insert lands on an existing key.
        let table = populated_wti(fan_out, keys);
        group.bench_with_input(BenchmarkId::new("wti_string", fan_out), &fan_out, |b, _| {
            b.iter_batched(
                || WtiStringAdjacencyRow {
                    id: table.get_next_pk().into(),
                    source: string_key(fastrand::u64(0..keys)),
                    payload: u64::MAX,
                },
                |row| table.insert(black_box(row)).unwrap(),
                BatchSize::SmallInput,
            )
        });

        let table = populated_arctic(fan_out, keys);
        group.bench_with_input(BenchmarkId::new("arctic_u128", fan_out), &fan_out, |b, _| {
            b.iter_batched(
                || ArcticHashAdjacencyRow {
                    id: table.get_next_pk().into(),
                    source: hash_of(fastrand::u64(0..keys)),
                    payload: u64::MAX,
                },
                |row| table.insert(black_box(row)).unwrap(),
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

/// Twelve points share this group; a trimmed budget keeps the whole
/// comparison bounded while the per-op times (tens of ns to a few us) still
/// collect thousands of samples.
fn configure() -> Criterion {
    Criterion::default()
        .sample_size(60)
        .measurement_time(Duration::from_secs(3))
        .warm_up_time(Duration::from_secs(1))
}

criterion_group! {
    name = nonunique_arctic_vs_wti_benchmarks;
    config = configure();
    targets = select_by_key, insert,
}
