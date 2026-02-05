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
    columns: {
        id: u64 primary_key autoincrement,
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
        check_interval: Duration::from_millis(5),
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
        table.insert(row.clone()).unwrap();
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
            delete_table.delete((*id).into()).await.unwrap();
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
        check_interval: Duration::from_millis(5),
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
        table.insert(row.clone()).unwrap();
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
            delete_table.delete((*id).into()).await.unwrap();
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
        table.insert(row.clone()).unwrap();
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
        check_interval: Duration::from_millis(5),
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
        table.insert(row.clone()).unwrap();
        rows.push((id, row));
    }
    let rows = Arc::new(rows);

    let vacuum = table.vacuum();
    vacuum_manager.register(vacuum);
    let _h = vacuum_manager.run_vacuum_task();

    let delete_table = table.clone();
    let ids_to_delete: Arc<Vec<_>> = Arc::new(rows.iter().step_by(2).map(|p| p.0).collect());
    let row_state = Arc::new(Mutex::new(rows.iter().cloned().collect::<HashMap<_, _>>()));
    let task_ids = ids_to_delete.clone();
    let task_row_state = Arc::clone(&row_state);
    let delete_task = tokio::spawn(async move {
        for id in task_ids.iter() {
            delete_table.delete((*id).into()).await.unwrap();
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
        table.upsert(row.clone()).await.unwrap();
        {
            let mut g = row_state.lock();
            g.entry(id).and_modify(|r| *r = row.clone()).or_insert(row);
        }
    }

    delete_task.await.unwrap();

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
//#[ignore]
async fn vacuum_loop_test() {
    let config = VacuumManagerConfig {
        check_interval: Duration::from_millis(1_000),
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
        table.insert(row.clone()).unwrap();
    }

    let vacuum = table.vacuum();
    vacuum_manager.register(vacuum);
    let _h = vacuum_manager.run_vacuum_task();

    let insert_table = table.clone();
    let _task = tokio::spawn(async move {
        let mut i = 3001;
        loop {
            let row = VacuumTestRow {
                id: insert_table.get_next_pk().into(),
                value: chrono::Utc::now().timestamp_nanos_opt().unwrap(),
                data: format!("test_data_{}", i),
            };
            insert_table.insert(row.clone()).unwrap();
            tokio::time::sleep(Duration::from_micros(500)).await;
            i += 1;
        }
    });

    tokio::time::sleep(Duration::from_millis(1_000)).await;

    loop {
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
            .map(|(_, l)| table.0.data.select(**l).unwrap())
            .collect::<Vec<_>>();
        for row in ids_to_remove {
            table.delete(row.id.into()).await.unwrap()
        }
    }
}
