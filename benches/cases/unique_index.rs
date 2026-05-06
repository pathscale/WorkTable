use criterion::{black_box, criterion_group, Criterion, BatchSize, BenchmarkId, Throughput};
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::common::*;

fn insert(c: &mut Criterion) {
    let table = UniqueIndexWorkTable::default();

    c.bench_function("unique_index_insert", |b| {
        b.iter_batched(
            || (),
            |_| {
                let row = UniqueIndexRow {
                    id: table.get_next_pk().into(),
                    test: fastrand::i64(..),
                    another: fastrand::u64(..),
                };
                table.insert(black_box(row))
            },
            BatchSize::SmallInput,
        )
    });
}

fn select_by_pk(c: &mut Criterion) {
    let table = UniqueIndexWorkTable::default();
    let pks: Vec<_> = (0..1000)
        .map(|_| {
            let row = UniqueIndexRow {
                id: table.get_next_pk().into(),
                test: fastrand::i64(..),
                another: fastrand::u64(..),
            };
            table.insert(row).unwrap()
        })
        .collect();

    c.bench_function("unique_index_select_by_pk", |b| {
        b.iter(|| {
            let pk = pks[fastrand::usize(0..pks.len())];
            black_box(table.select(pk))
        })
    });
}

fn select_by_unique_index(c: &mut Criterion) {
    let table = UniqueIndexWorkTable::default();

    for i in 1..=1000i64 {
        let row = UniqueIndexRow {
            id: table.get_next_pk().into(),
            test: i,
            another: i as u64,
        };
        table.insert(row).unwrap();
    }

    c.bench_function("unique_index_select_by_test", |b| {
        b.iter(|| {
            let test = fastrand::i64(1..=1000);
            black_box(table.select_by_test(test))
        })
    });
}

fn update(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let table = Arc::new(UniqueIndexWorkTable::default());

    let pks: Vec<_> = rt.block_on(async {
        let mut pks = Vec::new();
        for i in 0..100u64 {
            let row = UniqueIndexRow {
                id: table.get_next_pk().into(),
                test: i as i64,
                another: i,
            };
            pks.push(table.insert(row).unwrap());
        }
        pks
    });

    c.bench_function("unique_index_update", |b| {
        b.to_async(&rt).iter(|| async {
            let idx = fastrand::usize(0..pks.len());
            let pk = pks[idx].clone();
            let row = UniqueIndexRow {
                id: pk.into(),
                test: fastrand::i64(..),
                another: fastrand::u64(..),
            };
            black_box(table.update(row).await)
        })
    });
}

fn delete(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let table = Arc::new(UniqueIndexWorkTable::default());

    c.bench_function("unique_index_delete", |b| {
        b.iter_batched(
            || {
                let row = UniqueIndexRow {
                    id: table.get_next_pk().into(),
                    test: fastrand::i64(..),
                    another: fastrand::u64(..),
                };
                table.insert(row).unwrap()
            },
            |pk: UniqueIndexPrimaryKey| {
                rt.block_on(async {
                    table.delete(black_box(pk)).await.unwrap()
                })
            },
            BatchSize::SmallInput,
        )
    });
}

fn upsert_insert(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let table = Arc::new(UniqueIndexWorkTable::default());

    c.bench_function("unique_index_upsert_insert", |b| {
        b.to_async(&rt).iter(|| async {
            let row = UniqueIndexRow {
                id: table.get_next_pk().into(),
                test: fastrand::i64(..),
                another: fastrand::u64(..),
            };
            black_box(table.upsert(row).await)
        })
    });
}

fn upsert_update(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let table = Arc::new(UniqueIndexWorkTable::default());

    rt.block_on(async {
        for i in 0..50u64 {
            let row = UniqueIndexRow {
                id: table.get_next_pk().into(),
                test: i as i64,
                another: i,
            };
            table.upsert(row).await.unwrap();
        }
    });

    c.bench_function("unique_index_upsert_update", |b| {
        b.to_async(&rt).iter(|| async {
            let id = fastrand::u64(0..50);
            let row = UniqueIndexRow {
                id,
                test: fastrand::i64(..),
                another: fastrand::u64(..),
            };
            black_box(table.upsert(row).await)
        })
    });
}

fn batch_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("unique_index_batch_insert");

    for size in [100usize, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter_batched(
                || UniqueIndexWorkTable::default(),
                |table: UniqueIndexWorkTable| {
                    for i in 0..size {
                        let row = UniqueIndexRow {
                            id: table.get_next_pk().into(),
                            test: i as i64,
                            another: i as u64,
                        };
                        table.insert(black_box(row)).unwrap();
                    }
                    black_box(table)
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn batch_select_pk(c: &mut Criterion) {
    let mut group = c.benchmark_group("unique_index_batch_select_pk");

    for size in [100usize, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));

        let table = UniqueIndexWorkTable::default();
        let pks: Vec<_> = (0..size)
            .map(|_| {
                let row = UniqueIndexRow {
                    id: table.get_next_pk().into(),
                    test: fastrand::i64(..),
                    another: fastrand::u64(..),
                };
                table.insert(row).unwrap()
            })
            .collect();

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                for pk in &pks {
                    black_box(table.select(*pk));
                }
            })
        });
    }

    group.finish();
}

criterion_group! {
    name = unique_index_benchmarks;
    config = crate::common::config::configure_criterion();
    targets =
        insert,
        select_by_pk,
        select_by_unique_index,
        update,
        delete,
        upsert_insert,
        upsert_update,
        batch_insert,
        batch_select_pk,
}