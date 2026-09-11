//! Executable examples for the callsites in `docs/wt-user-guide.typ`.
//! If the guide drifts from the API, this stops compiling.
use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: Order,
    columns: {
        id: u64 primary_key autoincrement,
        symbol: String,
        quantity: u64,
    },
    indexes: {
        symbol_idx: symbol,
    },
    queries: {
        update: { QuantityById(quantity) by id, }
    }
);

worktable!(
    name: Reading,
    columns: {
        id: u64 primary_key,
        host_id: u64 columnar,
        timestamp: u64 columnar,
    },
    columnar_indexes: { host_time: { cluster_by: [host_id, timestamp], } }
);

worktable!(
    name: Snapshot,
    vec: true,
    columns: { id: u64 primary_key using fxhash, quantity: u64, }
);

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let table = OrderWorkTable::default();
    table
        .insert(OrderRow {
            id: table.get_next_pk().into(),
            symbol: "ETH".into(),
            quantity: 3,
        })
        .await?;
    let found = table.select_by_symbol("ETH".into()).execute()?;
    assert_eq!(found.len(), 1);
    table
        .insert_many(vec![OrderRow {
            id: 100,
            symbol: "BTC".into(),
            quantity: 5,
        }])
        .await?;
    table
        .update_quantity_by_id(QuantityByIdQuery { quantity: 7 }, 100)
        .await?;
    assert_eq!(table.select(100).unwrap().quantity, 7);
    assert_eq!(table.select_all().limit(1).execute()?.len(), 1);
    assert_eq!(table.row_count(), 2);
    assert!(table.used_bytes() > 0);
    let _ = table.system_info();
    table.delete_many(vec![100u64]).await?;
    let vacuum = table.vacuum_with_pacing(VacuumPacing {
        batch_pages: 1,
        ..Default::default()
    });
    vacuum.vacuum().await?;
    assert_eq!(vacuum.diagnostics().completions, 1);

    let readings = ReadingWorkTable::default();
    readings
        .insert(ReadingRow {
            id: 1,
            host_id: 7,
            timestamp: 1000,
        })
        .await?;
    let refs = readings.columnar_select_host_time(7, 1000)?;
    assert_eq!(readings.columnar_project_timestamp(&refs)?[0].1, 1000);
    assert_eq!(readings.columnar_scan_host_id()?.len(), 1);
    assert_eq!(readings.columnar_scan_host_time()?.len(), 1);
    assert_eq!(readings.columnar_slots_in_use(), 1);
    readings.rebuild_columnar()?;
    assert_eq!(readings.columnar_project_timestamp(&refs)?.len(), 1);
    readings.delete(1).await?;
    assert!(readings.columnar_project_timestamp(&refs)?.is_empty());

    let mut snapshot = SnapshotWorkTable::with_capacity(4);
    snapshot.insert(SnapshotRow { id: 1, quantity: 3 }).unwrap();
    assert_eq!(snapshot.select(&1).unwrap().quantity, 3);
    snapshot.delete(&1).expect("existing row");
    assert_eq!(snapshot.ghost_count(), 1);
    snapshot.compact();
    assert!(snapshot.is_empty());
    Ok(())
}
