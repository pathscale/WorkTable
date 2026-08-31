use std::sync::Arc;
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: ColumnarMetrics,
    persist: false,
    columns: {
        id: u64 primary_key,
        host_id: u64 columnar(chunk_rows(2), compression(none)),
        timestamp: i64 columnar,
        temperature: i64 columnar(chunk_rows(2)),
        label: String,
    },
    config: {
        columnar_slot_id: ColumnSlotId16,
        columnar_chunk_rows: 4,
    },
    columnar_indexes: {
        host_time: {
            cluster_by: [host_id, timestamp],
        },
    },
    queries: {
        update: {
            TemperatureById(temperature) by id,
        },
        in_place: {
            TimestampById(timestamp) by id,
        }
    },
);

worktable!(
    name: TinyColumnarIds,
    persist: false,
    columns: {
        id: u16 primary_key,
        value: u16 columnar(chunk_rows(32), compression(none)),
    },
    config: {
        columnar_slot_id: ColumnSlotId8,
        columnar_chunk_rows: 32,
    },
);

// Compile coverage for the persisted derive path. The columnar replica is
// intentionally skipped by the existing index file format and rebuilt from
// authoritative rows after load.
worktable!(
    name: PersistedColumnarMetrics,
    persist: true,
    columns: {
        id: u64 primary_key,
        host_id: u64 columnar(chunk_rows(4), compression(none)),
        timestamp: i64 columnar(chunk_rows(4), compression(none)),
    },
    columnar_indexes: {
        host_time: {
            cluster_by: [host_id, timestamp],
        },
    },
);

worktable!(
    name: CongeeColumnarSideIndex,
    persist: false,
    columns: {
        id: u64 primary_key using congee,
        value: u64 columnar,
    },
    columnar_indexes: {
        value_order: {
            cluster_by: [value],
        },
    },
);

worktable!(
    name: ArcticColumnarSideIndex,
    persist: false,
    columns: {
        id: u64 primary_key using arctic,
        value: u64 columnar,
    },
    columnar_indexes: {
        value_order: {
            cluster_by: [value],
        },
    },
);

#[tokio::test]
async fn columnar_fields_and_clustered_index_follow_mutations() {
    let table = ColumnarMetricsWorkTable::default();
    table
        .insert(ColumnarMetricsRow {
            id: 1,
            host_id: 2,
            timestamp: 20,
            temperature: 72,
            label: "second".to_string(),
        })
        .unwrap();
    table
        .insert(ColumnarMetricsRow {
            id: 2,
            host_id: 1,
            timestamp: 10,
            temperature: 68,
            label: "first".to_string(),
        })
        .unwrap();

    let host_two = table.columnar_select_host_time(2, 20).unwrap();
    assert_eq!(host_two.len(), 1);
    assert_eq!(host_two[0].primary_key().0, 1);
    assert_eq!(table.columnar_project_temperature(&host_two).unwrap()[0].1, 72);

    let ordered = table.columnar_scan_host_time().unwrap();
    let projected = table.columnar_project_host_id(&ordered).unwrap();
    assert_eq!(projected.iter().map(|(_, value)| *value).collect::<Vec<_>>(), [1, 2]);

    table
        .update(ColumnarMetricsRow {
            id: 1,
            host_id: 3,
            timestamp: 30,
            temperature: 75,
            label: "updated".to_string(),
        })
        .await
        .unwrap();

    assert!(table.columnar_select_host_time(2, 20).unwrap().is_empty());
    let updated = table.columnar_select_host_time(3, 30).unwrap();
    assert_eq!(updated, host_two, "row identity survives an update");
    assert_eq!(table.columnar_project_temperature(&updated).unwrap()[0].1, 75);

    table
        .update_temperature_by_id(TemperatureByIdQuery { temperature: 76 }, 1)
        .await
        .unwrap();
    assert_eq!(table.columnar_project_temperature(&updated).unwrap()[0].1, 76);

    table
        .update_timestamp_by_id_in_place(|value| *value = 40.into(), 1)
        .await
        .unwrap();
    assert!(table.columnar_is_dirty());
    table.rebuild_columnar().unwrap();
    assert!(!table.columnar_is_dirty());
    assert!(table.columnar_select_host_time(3, 30).unwrap().is_empty());
    assert_eq!(table.columnar_select_host_time(3, 40).unwrap(), updated);

    table.delete(2).await.unwrap();
    assert_eq!(table.columnar_scan_host_id().unwrap().len(), 1);
    assert_eq!(table.columnar_scan_host_time().unwrap(), updated);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_reinsert_and_columnar_refresh_preserve_row_identity() {
    let table = Arc::new(ColumnarMetricsWorkTable::default());
    table
        .insert(ColumnarMetricsRow {
            id: 7,
            host_id: 1,
            timestamp: 0,
            temperature: 1,
            label: "short".to_string(),
        })
        .unwrap();
    let stable_id = table.columnar_select_host_time(1, 0).unwrap()[0].clone();

    let updater = {
        let table = Arc::clone(&table);
        tokio::spawn(async move {
            for value in 1..=200 {
                table
                    .update(ColumnarMetricsRow {
                        id: 7,
                        host_id: 1,
                        timestamp: value,
                        temperature: value,
                        label: if value % 2 == 0 {
                            "a much longer row value".to_string()
                        } else {
                            "tiny".to_string()
                        },
                    })
                    .await
                    .unwrap();
            }
        })
    };

    for _ in 0..200 {
        for (row_id, _) in table.columnar_scan_timestamp().unwrap() {
            assert_eq!(row_id, stable_id);
        }
    }
    updater.await.unwrap();

    assert_eq!(table.columnar_select_host_time(1, 200).unwrap(), [stable_id]);
}

#[tokio::test]
async fn configured_slot_id_capacity_is_checked_and_deleted_slots_are_safe_to_reuse() {
    let table = TinyColumnarIdsWorkTable::default();
    for id in 0..=u8::MAX as u16 {
        table.insert(TinyColumnarIdsRow { id, value: id }).unwrap();
    }

    let stale = table
        .columnar_scan_value()
        .unwrap()
        .into_iter()
        .find(|(row_ref, _)| row_ref.primary_key().0 == 7)
        .unwrap()
        .0;
    let error = table.insert(TinyColumnarIdsRow { id: 256, value: 256 }).unwrap_err();
    assert!(matches!(error, WorkTableError::ColumnSlotIdExhausted(8)));
    assert!(
        table.select(256).is_none(),
        "capacity failure rolls back the authoritative row"
    );

    table.delete(7).await.unwrap();
    table.insert(TinyColumnarIdsRow { id: 256, value: 256 }).unwrap();

    let replacement = table
        .columnar_scan_value()
        .unwrap()
        .into_iter()
        .find(|(row_ref, _)| row_ref.primary_key().0 == 256)
        .unwrap()
        .0;
    assert!(
        table.columnar_project_value(&[stale]).unwrap().is_empty(),
        "a recycled slot cannot alias a different primary key"
    );

    table.delete(256).await.unwrap();
    table.insert(TinyColumnarIdsRow { id: 256, value: 999 }).unwrap();
    assert!(
        table.columnar_project_value(&[replacement]).unwrap().is_empty(),
        "delete and reinsert of the same primary key cannot revive a stale row reference"
    );
    assert_eq!(table.columnar_slots_in_use(), 256);
    assert_eq!(table.columnar_slots_high_water(), 256);
}

#[test]
fn row_refs_are_scoped_to_one_table_incarnation() {
    let first = TinyColumnarIdsWorkTable::default();
    first.insert(TinyColumnarIdsRow { id: 1, value: 11 }).unwrap();
    let retained = first.columnar_scan_value().unwrap()[0].0.clone();

    let second = TinyColumnarIdsWorkTable::default();
    second.insert(TinyColumnarIdsRow { id: 1, value: 22 }).unwrap();

    assert!(
        second.columnar_project_value(&[retained]).unwrap().is_empty(),
        "a ref from another table instance must not alias the same primary key and slot"
    );
}

#[tokio::test]
async fn columnar_side_indexes_compose_with_congee_and_arctic_using_backends() {
    macro_rules! exercise {
        ($table:ident, $row:ident) => {{
            let table = $table::default();
            table.insert($row { id: 1, value: 20 }).unwrap();
            table.insert($row { id: 2, value: 10 }).unwrap();

            let ordered = table.columnar_scan_value_order().unwrap();
            assert_eq!(
                table
                    .columnar_project_value(&ordered)
                    .unwrap()
                    .into_iter()
                    .map(|(_, value)| value)
                    .collect::<Vec<_>>(),
                [10, 20]
            );

            table.update($row { id: 1, value: 5 }).await.unwrap();
            assert_eq!(table.columnar_select_value_order(20).unwrap(), []);
            assert_eq!(table.columnar_select_value_order(5).unwrap().len(), 1);

            table.delete(2).await.unwrap();
            assert_eq!(table.columnar_scan_value().unwrap().len(), 1);
        }};
    }

    exercise!(CongeeColumnarSideIndexWorkTable, CongeeColumnarSideIndexRow);
    exercise!(ArcticColumnarSideIndexWorkTable, ArcticColumnarSideIndexRow);
}
