use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

use crate::common::*;

fn single_row_update_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_row_update_contention");

    let rt = Runtime::new().unwrap();
    let table = Arc::new(FullFeaturedWorkTable::default());

    let pk: u64 = rt.block_on(async {
        let row = FullFeaturedRow {
            id: table.get_next_pk().into(),
            val: 0,
            val1: 0,
            another: "test".to_string(),
            something: 0,
        };
        table.insert(row).unwrap().into()
    });

    for contention_level in [2, 4, 8, 16, 32] {
        group.throughput(Throughput::Elements(contention_level as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(contention_level),
            &contention_level,
            |b, &level| {
                b.to_async(&rt).iter(|| async {
                    let mut join_set = JoinSet::new();
                    for _ in 0..level {
                        let table_clone = table.clone();
                        join_set.spawn(async move {
                            let row = FullFeaturedRow {
                                id: pk,
                                val: fastrand::i64(..),
                                val1: fastrand::u64(..),
                                another: format!("upd_{}", fastrand::u64(..)),
                                something: fastrand::u64(..),
                            };
                            black_box(table_clone.update(row).await)
                        });
                    }
                    while join_set.join_next().await.is_some() {}
                })
            },
        );
    }
    group.finish();
}

fn single_row_in_place_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_row_in_place_contention");

    let rt = Runtime::new().unwrap();
    let table = Arc::new(FullFeaturedWorkTable::default());

    let pk: u64 = rt.block_on(async {
        let row = FullFeaturedRow {
            id: table.get_next_pk().into(),
            val: 0,
            val1: 0,
            another: "test".to_string(),
            something: 0,
        };
        table.insert(row).unwrap().into()
    });

    for contention_level in [2, 4, 8, 16, 32] {
        group.throughput(Throughput::Elements(contention_level as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(contention_level),
            &contention_level,
            |b, &level| {
                b.to_async(&rt).iter(|| async {
                    let mut join_set = JoinSet::new();
                    for _ in 0..level {
                        let table_clone = table.clone();
                        join_set.spawn(async move {
                            black_box(table_clone.update_val_by_id_in_place(|val| *val += 1, pk).await)
                        });
                    }
                    while join_set.join_next().await.is_some() {}
                })
            },
        );
    }
    group.finish();
}

criterion_group! {
    name = update_contention_benchmarks;
    config = crate::common::config::configure_criterion();
    targets = single_row_update_contention, single_row_in_place_contention,
}
