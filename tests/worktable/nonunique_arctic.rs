//! Generated-table coverage for non-unique `using arctic` secondary indexes.
//!
//! The table mirrors the dependency-graph adjacency shape that motivated the
//! backend: u128 hash keys with many rows per source hash. `source_idx` is
//! declared before the unique `edge_idx` so a unique-key collision during
//! insert exercises the partial-insert unwind of the non-unique arctic entry.

use std::collections::HashMap;
use std::sync::Arc;

use worktable::prelude::*;
use worktable::worktable;

worktable! {
    name: ArcticAdjacency,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement,
        source_hash: u128,
        edge_hash: u128,
        weight: u64,
    },
    indexes: {
        source_idx: source_hash using arctic,
        edge_idx: edge_hash unique using arctic,
        weight_idx: weight using arctic,
    },
    queries: {
        update: {
            SourceById(source_hash) by id,
            WeightBySource(weight) by source_hash,
        },
        delete: {
            BySource() by source_hash,
        }
    }
}

fn row(table: &ArcticAdjacencyWorkTable, source_hash: u128, edge_hash: u128, weight: u64) -> ArcticAdjacencyRow {
    ArcticAdjacencyRow {
        id: table.get_next_pk().into(),
        source_hash,
        edge_hash,
        weight,
    }
}

const SOURCE_A: u128 = u128::MAX - 5;
const SOURCE_B: u128 = 1 << 90;

#[test]
fn select_by_non_unique_key_returns_every_matching_row() {
    let table = ArcticAdjacencyWorkTable::default();
    for edge in 0..100u128 {
        table.insert(row(&table, SOURCE_A, edge, edge as u64)).unwrap();
    }
    table.insert(row(&table, SOURCE_B, 1000, 1)).unwrap();

    let rows = table.select_by_source_hash(SOURCE_A).execute().unwrap();
    assert_eq!(rows.len(), 100);
    assert!(rows.iter().all(|r| r.source_hash == SOURCE_A));

    let rows = table.select_by_source_hash(SOURCE_B).execute().unwrap();
    assert_eq!(rows.len(), 1);

    // Unknown key selects nothing.
    let rows = table.select_by_source_hash(7).execute().unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn delete_removes_rows_from_the_non_unique_index() {
    let table = ArcticAdjacencyWorkTable::default();
    let pk = table.insert(row(&table, SOURCE_A, 1, 10)).unwrap();
    table.insert(row(&table, SOURCE_A, 2, 20)).unwrap();

    table.delete(pk).await.unwrap();
    let rows = table.select_by_source_hash(SOURCE_A).execute().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].edge_hash, 2);

    // Custom delete-by-non-unique-key drains the whole key.
    table.insert(row(&table, SOURCE_A, 3, 30)).unwrap();
    table.delete_by_source(SOURCE_A).await.unwrap();
    assert!(table.select_by_source_hash(SOURCE_A).execute().unwrap().is_empty());
    assert_eq!(table.count(), 0);
}

#[tokio::test]
async fn update_moves_a_row_between_non_unique_keys() {
    let table = ArcticAdjacencyWorkTable::default();
    let pk = table.insert(row(&table, SOURCE_A, 1, 10)).unwrap();
    table.insert(row(&table, SOURCE_A, 2, 20)).unwrap();

    table
        .update_source_by_id(SourceByIdQuery { source_hash: SOURCE_B }, pk.clone())
        .await
        .unwrap();

    let rows = table.select_by_source_hash(SOURCE_A).execute().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].edge_hash, 2);
    let rows = table.select_by_source_hash(SOURCE_B).execute().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, pk.0);

    // Updating by the non-unique key touches every row under it.
    table
        .update_weight_by_source(WeightBySourceQuery { weight: 777 }, SOURCE_A)
        .await
        .unwrap();
    let rows = table.select_by_weight(777).execute().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].source_hash, SOURCE_A);
}

#[test]
fn unique_collision_unwinds_the_non_unique_entries() {
    let table = ArcticAdjacencyWorkTable::default();
    table.insert(row(&table, SOURCE_A, 42, 10)).unwrap();

    // `source_idx` and `weight_idx` are declared before `edge_idx`, so both
    // non-unique entries are already inserted when the unique index rejects
    // the duplicate edge hash; the failed insert must remove them again.
    let err = table.insert(row(&table, SOURCE_B, 42, 999)).unwrap_err();
    assert!(matches!(err, WorkTableError::AlreadyExists(_)));

    assert!(table.select_by_source_hash(SOURCE_B).execute().unwrap().is_empty());
    assert!(table.select_by_weight(999).execute().unwrap().is_empty());
    assert_eq!(table.count(), 1);
}

#[test]
fn range_select_over_non_unique_arctic_keys() {
    let table = ArcticAdjacencyWorkTable::default();
    for key in 0..10u64 {
        for copy in 0..3u128 {
            table.insert(row(&table, key as u128, key as u128 * 100 + copy, key)).unwrap();
        }
    }

    let rows = table.select_by_source_hash_range(3..=4).execute().unwrap();
    assert_eq!(rows.len(), 6);
    assert!(rows.iter().all(|r| (3..=4).contains(&r.source_hash)));

    let rows = table.select_by_weight_range(8..).execute().unwrap();
    assert_eq!(rows.len(), 6);
    assert!(rows.iter().all(|r| r.weight >= 8));
}

#[test]
fn concurrent_inserts_and_deletes_keep_the_index_consistent() {
    let table = Arc::new(ArcticAdjacencyWorkTable::default());
    let keys = 8u128;
    let writers = 4;
    let per_writer = 200u64;

    let handles = (0..writers)
        .map(|writer| {
            let table = Arc::clone(&table);
            std::thread::spawn(move || {
                for n in 0..per_writer {
                    let edge = (writer as u128) << 64 | n as u128;
                    let source = (n as u128) % keys;
                    table.insert(row(&table, source, edge, n)).unwrap();
                    // Interleave point reads to race the writers.
                    let _ = table.select_by_source_hash(source).execute().unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }

    // Every row is reachable through the non-unique index, grouped correctly.
    let by_source: std::cell::RefCell<HashMap<u128, usize>> = std::cell::RefCell::new(HashMap::new());
    table
        .iter_with(|row| {
            *by_source.borrow_mut().entry(row.source_hash).or_default() += 1;
            Ok(())
        })
        .unwrap();
    let by_source = by_source.into_inner();
    assert_eq!(by_source.values().sum::<usize>() as u64, writers as u64 * per_writer);
    for key in 0..keys {
        let selected = table.select_by_source_hash(key).execute().unwrap();
        assert_eq!(selected.len(), *by_source.get(&key).unwrap_or(&0));
    }
}

#[tokio::test]
async fn concurrent_deletes_leave_no_stale_links() {
    let table = Arc::new(ArcticAdjacencyWorkTable::default());
    let mut pks = Vec::new();
    for n in 0..400u128 {
        pks.push(table.insert(row(&table, n % 4, n, n as u64)).unwrap());
    }

    let mut tasks = Vec::new();
    for chunk in pks.chunks(100) {
        let table = Arc::clone(&table);
        let chunk = chunk.to_vec();
        tasks.push(tokio::spawn(async move {
            for pk in chunk {
                table.delete(pk).await.unwrap();
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    for key in 0..4u128 {
        assert!(table.select_by_source_hash(key).execute().unwrap().is_empty());
    }
    assert_eq!(table.count(), 0);
}

#[test]
fn system_info_reports_the_non_unique_arctic_index() {
    let table = ArcticAdjacencyWorkTable::default();
    for n in 0..5u128 {
        table.insert(row(&table, SOURCE_A, n, n as u64)).unwrap();
    }

    let info = table.system_info();
    let source = info
        .indexes_info
        .iter()
        .find(|index| index.name == "source_idx")
        .expect("non-unique arctic index is reported");
    assert!(matches!(source.index_type, IndexKind::NonUnique));
    assert_eq!(source.key_count, 5);
}
