use std::sync::Arc;
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: ColumnarMetrics,
    persist: false,
    columns: {
        id: u64 primary_key,
        host_id: u64 columnar(chunk_rows(2), compression(auto)),
        timestamp: i64 columnar(chunk_rows(3), compression(none)),
        temperature: i64 columnar(chunk_rows(2), compression(auto)),
        label: String,
    },
    columnar_indexes: {
        host_time: {
            columns: [host_id, timestamp, temperature],
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

// Compile coverage for the persisted derive path. The columnar replica is
// intentionally skipped by the existing index file format and rebuilt from
// authoritative rows after load.
worktable!(
    name: PersistedColumnarMetrics,
    persist: true,
    columns: {
        id: u64 primary_key,
        host_id: u64 columnar(chunk_rows(4), compression(auto)),
        timestamp: i64 columnar(chunk_rows(4), compression(none)),
    },
    columnar_indexes: {
        host_time: {
            columns: [host_id, timestamp],
            cluster_by: [host_id, timestamp],
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

    let host_two = table.columnar_select_host_time(2, 20);
    assert_eq!(host_two.len(), 1);
    assert_eq!(table.columnar_resolve_primary_keys(&host_two)[0].1.0, 1);
    assert_eq!(table.columnar_project_temperature(&host_two)[0].1, 72);

    let ordered = table.columnar_scan_host_time();
    let projected = table.columnar_project_host_id(&ordered);
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

    assert!(table.columnar_select_host_time(2, 20).is_empty());
    let updated = table.columnar_select_host_time(3, 30);
    assert_eq!(updated, host_two, "row identity survives an update");
    assert_eq!(table.columnar_project_temperature(&updated)[0].1, 75);

    table
        .update_temperature_by_id(TemperatureByIdQuery { temperature: 76 }, 1)
        .await
        .unwrap();
    assert_eq!(table.columnar_project_temperature(&updated)[0].1, 76);

    table
        .update_timestamp_by_id_in_place(|value| *value = 40.into(), 1)
        .await
        .unwrap();
    assert!(table.columnar_select_host_time(3, 30).is_empty());
    assert_eq!(table.columnar_select_host_time(3, 40), updated);

    table.delete(2).await.unwrap();
    assert_eq!(table.columnar_scan_host_id().len(), 1);
    assert_eq!(table.columnar_scan_host_time(), updated);
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
    let stable_id = table.columnar_select_host_time(1, 0)[0];

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
        for (row_id, _) in table.columnar_scan_timestamp() {
            assert_eq!(row_id, stable_id);
        }
    }
    updater.await.unwrap();

    assert_eq!(table.columnar_select_host_time(1, 200), [stable_id]);
}
