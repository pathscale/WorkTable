use criterion::{black_box, criterion_group, Criterion, BatchSize, BenchmarkId, Throughput};
use std::sync::Arc;
use tokio::runtime::Runtime;
use worktable::prelude::SelectQueryExecutor;

use crate::common::*;

fn insert(c: &mut Criterion) {
    let table = FullFeaturedWorkTable::default();

    c.bench_function("full_featured_insert", |b| {
        b.iter_batched(
            || (),
            |_| {
                let row = FullFeaturedRow {
                    id: table.get_next_pk().into(),
                    val: fastrand::i64(..),
                    val1: fastrand::u64(..),
                    another: format!("another_{}", fastrand::u64(..)),
                    something: fastrand::u64(..),
                };
                table.insert(black_box(row))
            },
            BatchSize::SmallInput,
        )
    });
}

fn select_by_pk(c: &mut Criterion) {
    let table = FullFeaturedWorkTable::default();
    let pks: Vec<_> = (0..1000)
        .map(|_| {
            let row = FullFeaturedRow {
                id: table.get_next_pk().into(),
                val: fastrand::i64(..),
                val1: fastrand::u64(..),
                another: format!("another_{}", fastrand::u64(..)),
                something: fastrand::u64(..),
            };
            table.insert(row).unwrap()
        })
        .collect();

    c.bench_function("full_featured_select_by_pk", |b| {
        b.iter(|| {
            let pk = pks[fastrand::usize(0..pks.len())];
            black_box(table.select(pk))
        })
    });
}

fn select_by_unique_index(c: &mut Criterion) {
    let table = FullFeaturedWorkTable::default();

    for i in 0..1000u64 {
        let row = FullFeaturedRow {
            id: table.get_next_pk().into(),
            val: i as i64,
            val1: i,
            another: format!("another_{}", i),
            something: i,
        };
        table.insert(row).unwrap();
    }

    c.bench_function("full_featured_select_by_val1", |b| {
        b.iter(|| {
            let val1 = fastrand::u64(0..1000);
            black_box(table.select_by_val1(val1))
        })
    });
}

fn select_by_non_unique_index(c: &mut Criterion) {
    let table = FullFeaturedWorkTable::default();

    for i in 0..1000u64 {
        let row = FullFeaturedRow {
            id: table.get_next_pk().into(),
            val: i as i64,
            val1: i,
            another: format!("cat_{}", i % 10),
            something: i,
        };
        table.insert(row).unwrap();
    }

    c.bench_function("full_featured_select_by_another", |b| {
        b.iter(|| {
            let cat = format!("cat_{}", fastrand::u64(0..10));
            black_box(table.select_by_another(cat).execute())
        })
    });
}

fn update(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let table = Arc::new(FullFeaturedWorkTable::default());

    let pks: Vec<_> = rt.block_on(async {
        let mut pks = Vec::new();
        for i in 0..100u64 {
            let row = FullFeaturedRow {
                id: table.get_next_pk().into(),
                val: i as i64,
                val1: i,
                another: format!("another_{}", i),
                something: i,
            };
            pks.push(table.insert(row).unwrap());
        }
        pks
    });

    c.bench_function("full_featured_update", |b| {
        b.to_async(&rt).iter(|| async {
            let idx = fastrand::usize(0..pks.len());
            let pk = pks[idx].clone();
            let row = FullFeaturedRow {
                id: pk.into(),
                val: fastrand::i64(..),
                val1: fastrand::u64(..),
                another: format!("updated_{}", fastrand::u64(..)),
                something: fastrand::u64(..),
            };
            black_box(table.update(row).await)
        })
    });
}

fn update_by_pk_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let table = Arc::new(FullFeaturedWorkTable::default());

    rt.block_on(async {
        for i in 0..100u64 {
            let row = FullFeaturedRow {
                id: table.get_next_pk().into(),
                val: i as i64,
                val1: i,
                another: format!("another_{}", i),
                something: i,
            };
            table.insert(row).unwrap();
        }
    });

    c.bench_function("full_featured_update_another_by_id", |b| {
        b.to_async(&rt).iter(|| async {
            let id = fastrand::u64(0..100);
            let query = AnotherByIdQuery {
                another: format!("upd_{}", fastrand::u64(..)),
            };
            black_box(table.update_another_by_id(query, id).await)
        })
    });
}

fn update_by_unique_index_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let table = Arc::new(FullFeaturedWorkTable::default());

    rt.block_on(async {
        for i in 0..100u64 {
            let row = FullFeaturedRow {
                id: table.get_next_pk().into(),
                val: i as i64,
                val1: i,
                another: format!("another_{}", i),
                something: i,
            };
            table.insert(row).unwrap();
        }
    });

    c.bench_function("full_featured_update_another_by_val1", |b| {
        b.to_async(&rt).iter(|| async {
            let val1 = fastrand::u64(0..100);
            let query = AnotherByVal1Query {
                another: format!("upd_{}", fastrand::u64(..)),
            };
            black_box(table.update_another_by_val_1(query, val1).await)
        })
    });
}

fn in_place_update(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let table = Arc::new(FullFeaturedWorkTable::default());

    let pk: u64 = {
        let row = FullFeaturedRow {
            id: table.get_next_pk().into(),
            val: 0,
            val1: 0,
            another: "test".to_string(),
            something: 0,
        };
        table.insert(row).unwrap().into()
    };

    c.bench_function("full_featured_in_place_update_val", |b| {
        b.to_async(&rt).iter(|| async {
            table
                .update_val_by_id_in_place(|val| *val += 1, black_box(pk))
                .await
        })
    });
}

fn delete(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let table = Arc::new(FullFeaturedWorkTable::default());

    c.bench_function("full_featured_delete", |b| {
        b.iter_batched(
            || {
                let row = FullFeaturedRow {
                    id: table.get_next_pk().into(),
                    val: fastrand::i64(..),
                    val1: fastrand::u64(..),
                    another: format!("temp_{}", fastrand::u64(..)),
                    something: fastrand::u64(..),
                };
                table.insert(row).unwrap()
            },
            |pk: FullFeaturedPrimaryKey| {
                rt.block_on(async {
                    table.delete(black_box(pk)).await.unwrap()
                })
            },
            BatchSize::SmallInput,
        )
    });
}

fn delete_by_index_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let table = Arc::new(FullFeaturedWorkTable::default());

    c.bench_function("full_featured_delete_by_another", |b| {
        b.iter_batched(
            || {
                let another = format!("del_{}", fastrand::u64(..));
                let row = FullFeaturedRow {
                    id: table.get_next_pk().into(),
                    val: fastrand::i64(..),
                    val1: fastrand::u64(..),
                    another: another.clone(),
                    something: fastrand::u64(..),
                };
                table.insert(row).unwrap();
                another
            },
            |another: String| {
                rt.block_on(async {
                    table.delete_by_another(another).await.unwrap()
                })
            },
            BatchSize::SmallInput,
        )
    });
}

fn upsert_insert(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let table = Arc::new(FullFeaturedWorkTable::default());

    c.bench_function("full_featured_upsert_insert", |b| {
        b.to_async(&rt).iter(|| async {
            let row = FullFeaturedRow {
                id: table.get_next_pk().into(),
                val: fastrand::i64(..),
                val1: fastrand::u64(..),
                another: format!("another_{}", fastrand::u64(..)),
                something: fastrand::u64(..),
            };
            black_box(table.upsert(row).await)
        })
    });
}

fn upsert_update(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let table = Arc::new(FullFeaturedWorkTable::default());

    rt.block_on(async {
        for i in 0..50u64 {
            let row = FullFeaturedRow {
                id: table.get_next_pk().into(),
                val: i as i64,
                val1: i,
                another: format!("another_{}", i),
                something: i,
            };
            table.upsert(row).await.unwrap();
        }
    });

    c.bench_function("full_featured_upsert_update", |b| {
        b.to_async(&rt).iter(|| async {
            let id = fastrand::u64(0..50);
            let row = FullFeaturedRow {
                id,
                val: fastrand::i64(..),
                val1: id,
                another: format!("upserted_{}", fastrand::u64(..)),
                something: fastrand::u64(..),
            };
            black_box(table.upsert(row).await)
        })
    });
}

fn batch_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_featured_batch_insert");

    for size in [100usize, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter_batched(
                || FullFeaturedWorkTable::default(),
                |table: FullFeaturedWorkTable| {
                    for i in 0..size {
                        let row = FullFeaturedRow {
                            id: table.get_next_pk().into(),
                            val: i as i64,
                            val1: i as u64,
                            another: format!("another_{}", i),
                            something: i as u64,
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
    let mut group = c.benchmark_group("full_featured_batch_select_pk");

    for size in [100usize, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));

        let table = FullFeaturedWorkTable::default();
        let pks: Vec<_> = (0..size)
            .map(|_| {
                let row = FullFeaturedRow {
                    id: table.get_next_pk().into(),
                    val: fastrand::i64(..),
                    val1: fastrand::u64(..),
                    another: format!("another_{}", fastrand::u64(..)),
                    something: fastrand::u64(..),
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
    name = full_featured_benchmarks;
    config = crate::common::config::configure_criterion();
    targets =
        insert,
        select_by_pk,
        select_by_unique_index,
        select_by_non_unique_index,
        update,
        update_by_pk_query,
        update_by_unique_index_query,
        in_place_update,
        delete,
        delete_by_index_query,
        upsert_insert,
        upsert_update,
        batch_insert,
        batch_select_pk,
}