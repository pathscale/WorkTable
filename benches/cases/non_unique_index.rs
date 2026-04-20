use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group};
use std::sync::Arc;
use tokio::runtime::Runtime;
use worktable::prelude::SelectQueryExecutor;

use crate::common::*;

fn insert(c: &mut Criterion) {
    let table = NonUniqueIndexWorkTable::default();

    c.bench_function("non_unique_index_insert", |b| {
        b.iter_batched(
            || (),
            |_| {
                let row = NonUniqueIndexRow {
                    id: table.get_next_pk().into(),
                    value: fastrand::u64(..),
                    category: fastrand::u64(0..10),
                };
                table.insert(black_box(row))
            },
            BatchSize::SmallInput,
        )
    });
}

fn select_by_pk(c: &mut Criterion) {
    let table = NonUniqueIndexWorkTable::default();
    let pks: Vec<_> = (0..1000)
        .map(|_| {
            let row = NonUniqueIndexRow {
                id: table.get_next_pk().into(),
                value: fastrand::u64(..),
                category: fastrand::u64(0..10),
            };
            table.insert(row).unwrap()
        })
        .collect();

    c.bench_function("non_unique_index_select_by_pk", |b| {
        b.iter(|| {
            let pk = pks[fastrand::usize(0..pks.len())].clone();
            black_box(table.select(pk))
        })
    });
}

fn select_by_non_unique_index(c: &mut Criterion) {
    let table = NonUniqueIndexWorkTable::default();

    for i in 0..1000u64 {
        let row = NonUniqueIndexRow {
            id: table.get_next_pk().into(),
            value: fastrand::u64(..),
            category: i % 10,
        };
        table.insert(row).unwrap();
    }

    c.bench_function("non_unique_index_select_by_category", |b| {
        b.iter(|| {
            let cat = fastrand::u64(0..10);
            black_box(table.select_by_category(cat).execute())
        })
    });
}

fn update(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let table = Arc::new(NonUniqueIndexWorkTable::default());

    let pks: Vec<_> = rt.block_on(async {
        let mut pks = Vec::new();
        for i in 0..100u64 {
            let row = NonUniqueIndexRow {
                id: table.get_next_pk().into(),
                value: i,
                category: i % 10,
            };
            pks.push(table.insert(row).unwrap());
        }
        pks
    });

    c.bench_function("non_unique_index_update", |b| {
        b.to_async(&rt).iter(|| async {
            let idx = fastrand::usize(0..pks.len());
            let pk = pks[idx].clone();
            let row = NonUniqueIndexRow {
                id: pk.into(),
                value: fastrand::u64(..),
                category: fastrand::u64(0..10),
            };
            black_box(table.update(row).await)
        })
    });
}

fn delete(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let table = Arc::new(NonUniqueIndexWorkTable::default());

    c.bench_function("non_unique_index_delete", |b| {
        b.iter_batched(
            || {
                let row = NonUniqueIndexRow {
                    id: table.get_next_pk().into(),
                    value: fastrand::u64(..),
                    category: fastrand::u64(0..10),
                };
                table.insert(row).unwrap()
            },
            |pk: NonUniqueIndexPrimaryKey| {
                rt.block_on(async { table.delete(black_box(pk)).await.unwrap() })
            },
            BatchSize::SmallInput,
        )
    });
}

fn upsert_insert(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let table = Arc::new(NonUniqueIndexWorkTable::default());

    c.bench_function("non_unique_index_upsert_insert", |b| {
        b.to_async(&rt).iter(|| async {
            let row = NonUniqueIndexRow {
                id: table.get_next_pk().into(),
                value: fastrand::u64(..),
                category: fastrand::u64(0..10),
            };
            black_box(table.upsert(row).await)
        })
    });
}

fn upsert_update(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let table = Arc::new(NonUniqueIndexWorkTable::default());

    rt.block_on(async {
        for i in 0..50u64 {
            let row = NonUniqueIndexRow {
                id: table.get_next_pk().into(),
                value: i,
                category: i % 10,
            };
            table.upsert(row).await.unwrap();
        }
    });

    c.bench_function("non_unique_index_upsert_update", |b| {
        b.to_async(&rt).iter(|| async {
            let id = fastrand::u64(0..50);
            let row = NonUniqueIndexRow {
                id,
                value: fastrand::u64(..),
                category: fastrand::u64(0..10),
            };
            black_box(table.upsert(row).await)
        })
    });
}

fn batch_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("non_unique_index_batch_insert");

    for size in [100usize, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter_batched(
                || NonUniqueIndexWorkTable::default(),
                |table: NonUniqueIndexWorkTable| {
                    for i in 0..size {
                        let row = NonUniqueIndexRow {
                            id: table.get_next_pk().into(),
                            value: i as u64,
                            category: (i % 10) as u64,
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
    let mut group = c.benchmark_group("non_unique_index_batch_select_pk");

    for size in [100usize, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));

        let table = NonUniqueIndexWorkTable::default();
        let pks: Vec<_> = (0..size)
            .map(|_| {
                let row = NonUniqueIndexRow {
                    id: table.get_next_pk().into(),
                    value: fastrand::u64(..),
                    category: fastrand::u64(0..10),
                };
                table.insert(row).unwrap()
            })
            .collect();

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                for pk in &pks {
                    black_box(table.select(pk.clone()));
                }
            })
        });
    }

    group.finish();
}

criterion_group! {
    name = non_unique_index_benchmarks;
    config = config::configure_criterion();
    targets =
        insert,
        select_by_pk,
        select_by_non_unique_index,
        update,
        delete,
        upsert_insert,
        upsert_update,
        batch_insert,
        batch_select_pk,
}
