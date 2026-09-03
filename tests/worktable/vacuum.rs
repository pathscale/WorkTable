macro_rules! vacuum_backend_suite {
    ($module:ident, $using:ident) => {
        mod $module {
use chrono::TimeDelta;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use worktable::prelude::*;
use worktable::vacuum::{VacuumManager, VacuumManagerConfig};
use worktable_codegen::worktable;

worktable!(
    name: VacuumTest,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement using $using,
        value: i64,
        data: String
    },
    indexes: {
        value_idx: value unique,
        data_idx: data,
    }
);

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn vacuum_parallel_with_selects() {
    let config = VacuumManagerConfig {
        // Wake on any freed byte. The reactive equivalent of "run vacuum
        // eagerly": there is no interval to turn down any more, because a
        // short one made the fallback timer win every wake and neither the
        // threshold nor the settle did anything.
        wake_threshold_bytes: 1,
        ..Default::default()
    };
    let vacuum_manager = Arc::new(VacuumManager::with_config(config));
    let table = Arc::new(VacuumTestWorkTable::default());

    // Insert 2000 rows
    let mut rows = Vec::new();
    for i in 0..2000 {
        let row = VacuumTestRow {
            id: table.get_next_pk().into(),
            value: i,
            data: format!("test_data_{}", i),
        };
        let id = row.id;
        table.insert(row.clone()).await.unwrap();
        rows.push((id, row));
    }
    let rows = Arc::new(rows);

    let vacuum = table.vacuum();
    vacuum_manager.register(vacuum);
    let _h = vacuum_manager.run_vacuum_task();

    let delete_table = table.clone();
    let ids_to_delete: Arc<Vec<_>> = Arc::new(rows.iter().step_by(2).map(|p| p.0).collect());
    let task_ids = ids_to_delete.clone();
    let delete_task = tokio::spawn(async move {
        for id in task_ids.iter() {
            delete_table.delete(*id).await.unwrap();
        }
    });

    for _ in 0..10 {
        // Verify all remaining rows are still accessible multiple times while vacuuming
        for (id, expected) in rows.iter().filter(|(i, _)| !ids_to_delete.contains(i)) {
            let row = table.select(*id);
            assert_eq!(row, Some(expected.clone()));
            let row = row.unwrap();
            let by_value = table.select_by_value(row.value);
            assert_eq!(by_value, Some(expected.clone()));
        }
    }

    delete_task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn vacuum_parallel_with_inserts() {
    let config = VacuumManagerConfig {
        // Wake on any freed byte. The reactive equivalent of "run vacuum
        // eagerly": there is no interval to turn down any more, because a
        // short one made the fallback timer win every wake and neither the
        // threshold nor the settle did anything.
        wake_threshold_bytes: 1,
        ..Default::default()
    };
    let vacuum_manager = Arc::new(VacuumManager::with_config(config));
    let table = Arc::new(VacuumTestWorkTable::default());

    // Insert 2000 rows
    let mut rows = Vec::new();
    for i in 0..2000 {
        let row = VacuumTestRow {
            id: table.get_next_pk().into(),
            value: i,
            data: format!("test_data_{}", i),
        };
        let id = row.id;
        table.insert(row.clone()).await.unwrap();
        rows.push((id, row));
    }
    let rows = Arc::new(rows);

    let vacuum = table.vacuum();
    vacuum_manager.register(vacuum);
    let _h = vacuum_manager.run_vacuum_task();

    let delete_table = table.clone();
    let ids_to_delete: Arc<Vec<_>> = Arc::new(rows.iter().step_by(2).map(|p| p.0).collect());
    let task_ids = ids_to_delete.clone();
    let delete_task = tokio::spawn(async move {
        for id in task_ids.iter() {
            delete_table.delete(*id).await.unwrap();
        }
    });

    let mut inserted_rows = Vec::new();
    for i in 2001..3000 {
        let row = VacuumTestRow {
            id: table.get_next_pk().into(),
            value: i,
            data: format!("test_data_{}", i),
        };
        let id = row.id;
        table.insert(row.clone()).await.unwrap();
        inserted_rows.push((id, row));
    }

    // Verify all remaining rows are still accessible
    for (id, expected) in rows.iter().filter(|(i, _)| !ids_to_delete.contains(i)) {
        let row = table.select(*id);
        assert_eq!(row, Some(expected.clone()));
        let row = row.unwrap();
        let by_value = table.select_by_value(row.value);
        assert_eq!(by_value, Some(expected.clone()));
    }
    // Verify all inserted rows are accessible
    for (id, expected) in inserted_rows.iter() {
        let row = table.select(*id);
        assert_eq!(row, Some(expected.clone()));
        let row = row.unwrap();
        let by_value = table.select_by_value(row.value);
        assert_eq!(by_value, Some(expected.clone()));
    }

    delete_task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn vacuum_parallel_with_upserts() {
    let config = VacuumManagerConfig {
        // Wake on any freed byte. The reactive equivalent of "run vacuum
        // eagerly": there is no interval to turn down any more, because a
        // short one made the fallback timer win every wake and neither the
        // threshold nor the settle did anything.
        wake_threshold_bytes: 1,
        ..Default::default()
    };
    let vacuum_manager = Arc::new(VacuumManager::with_config(config));
    let table = Arc::new(VacuumTestWorkTable::default());

    // Insert 3000 rows
    let mut rows = Vec::new();
    for i in 0..3000 {
        let row = VacuumTestRow {
            id: table.get_next_pk().into(),
            value: i,
            data: format!("test_data_{}", i),
        };
        let id = row.id;
        table.insert(row.clone()).await.unwrap();
        rows.push((id, row));
    }
    let rows = Arc::new(rows);

    let vacuum = table.vacuum();
    vacuum_manager.register(vacuum);
    let _h = vacuum_manager.run_vacuum_task();

    let delete_table = table.clone();
    let ids_to_delete: Arc<Vec<_>> = Arc::new(rows.iter().step_by(2).map(|p| p.0).collect());
    let row_state = Arc::new(Mutex::new(rows.iter().cloned().collect::<HashMap<_, _>>()));
    let row_locks = Arc::new((0..3000).map(|_| tokio::sync::Mutex::new(())).collect::<Vec<_>>());
    let task_ids = ids_to_delete.clone();
    let task_row_state = Arc::clone(&row_state);
    let task_row_locks = Arc::clone(&row_locks);
    let delete_task = tokio::spawn(async move {
        for id in task_ids.iter() {
            // Keep the table mutation and oracle transition ordered for this
            // key. Operations on different keys and vacuum remain concurrent.
            let _row_guard = task_row_locks[*id as usize].lock().await;
            delete_table.delete(*id).await.unwrap();
            {
                let mut g = task_row_state.lock();
                g.remove(id);
            }
        }
    });

    for _ in 0..3000 {
        let id = fastrand::u64(0..3000);
        let i = fastrand::i64(0..3000);
        let row = VacuumTestRow {
            id,
            value: id as i64,
            data: format!("test_data_{}", i),
        };
        let id = row.id;
        let _row_guard = row_locks[id as usize].lock().await;
        table.upsert(row.clone()).await.unwrap();
        {
            let mut g = row_state.lock();
            g.entry(id).and_modify(|r| *r = row.clone()).or_insert(row);
        }
    }

    delete_task.await.unwrap();

    // Every secondary entry must resolve to a row carrying its key.
    //
    // Checked before the per-row comparison below, because it fails closer to
    // the cause. The per-row check reports "expected id 1501, got id 63",
    // which is the damage; this reports which index entry points at storage
    // holding something else, which is the defect.
    {
        let stale: Vec<_> = table
            .0
            .indexes
            .value_idx
            .iter()
            .filter_map(|(key, link)| {
                let at = table.0.data.select_non_ghosted(link.into());
                match at {
                    Ok(row) if row.value == key => None,
                    Ok(row) => Some(format!("value_idx[{key}] -> link holding value {} (id {})", row.value, row.id)),
                    Err(e) => Some(format!("value_idx[{key}] -> unreadable link: {e:?}")),
                }
            })
            .take(5)
            .collect();
        assert!(stale.is_empty(), "secondary index entries do not match their rows:\n{}", stale.join("\n"));
    }

    let g = row_state.lock();

    // Verify all inserted rows are accessible
    for (id, expected) in g.iter() {
        let row = table.select(*id);
        assert_eq!(row, Some(expected.clone()));
        let row = row.unwrap();
        let by_value = table.select_by_value(row.value);
        assert_eq!(by_value, Some(expected.clone()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "bounded 10-second vacuum soak test"]
async fn vacuum_loop_test() {
    const SOAK_DURATION: Duration = Duration::from_secs(10);

    let config = VacuumManagerConfig {
        // No interval any more; wake on any freed byte instead.
        wake_threshold_bytes: 1,
        ..Default::default()
    };
    let vacuum_manager = Arc::new(VacuumManager::with_config(config));
    let table = Arc::new(VacuumTestWorkTable::default());

    // Insert 3000 rows
    for i in 0..3000 {
        let row = VacuumTestRow {
            id: table.get_next_pk().into(),
            value: chrono::Utc::now().timestamp_nanos_opt().unwrap(),
            data: format!("test_data_{}", i),
        };
        table.insert(row.clone()).await.unwrap();
    }

    let vacuum = table.vacuum();
    vacuum_manager.register(vacuum);
    let vacuum_task = vacuum_manager.run_vacuum_task();

    let insert_table = table.clone();
    let stop_at = tokio::time::Instant::now() + SOAK_DURATION;
    let task = tokio::spawn(async move {
        let mut i = 3001;
        while tokio::time::Instant::now() < stop_at {
            let row = VacuumTestRow {
                id: insert_table.get_next_pk().into(),
                value: chrono::Utc::now().timestamp_nanos_opt().unwrap(),
                data: format!("test_data_{}", i),
            };
            insert_table.insert(row.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_micros(500)).await;
            i += 1;
        }
    });

    tokio::time::sleep(Duration::from_millis(1_000)).await;

    while tokio::time::Instant::now() < stop_at {
        tokio::time::sleep(Duration::from_millis(1_000)).await;

        let outdated_ts = chrono::Utc::now()
            .checked_sub_signed(TimeDelta::new(1, 0).unwrap())
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap();
        let ids_to_remove = table
            .0
            .indexes
            .value_idx
            .range(..outdated_ts)
            .map(|(v, l)| (table.0.data.select_non_ghosted(*l).unwrap(), l, v))
            .collect::<Vec<_>>();
        for (row, _, _) in ids_to_remove {
            table.delete(row.id).await.unwrap();
        }
    }

    task.await.unwrap();
    vacuum_task.abort();
}

        }
    };
}

vacuum_backend_suite!(wti, worktables_index);
vacuum_backend_suite!(congee, congee);
vacuum_backend_suite!(arctic, arctic);
