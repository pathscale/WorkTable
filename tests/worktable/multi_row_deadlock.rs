use std::sync::Arc;
use std::time::Duration;

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: MultiRowDeadlock,
    columns: {
        id: u64 primary_key,
        group_a: u64,
        group_b: u64,
        name: String,
    },
    indexes: {
        group_a_idx: group_a,
        group_b_idx: group_b,
    },
    queries: {
        update: {
            NameByGroupA(name) by group_a,
            NameByGroupB(name) by group_b,
        }
    }
);

/// Two overlapping multi-row updates must not deadlock. The old protocol
/// pre-locked the sorted key set with custom locks, then, per row whose
/// unsized field changed size, dropped only that key's guard and re-acquired
/// a full-row lock while still holding later keys' guards - a lock-order
/// inversion between two overlapping updates. Full-row locks are now taken
/// for the whole set up front.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_multi_row_updates_do_not_deadlock() {
    const ROWS: u64 = 16;
    const ITERATIONS: usize = 100;
    let table = Arc::new(MultiRowDeadlockWorkTable::default());
    for id in 0..ROWS {
        table
            .insert(MultiRowDeadlockRow {
                id,
                group_a: 1,
                group_b: 1,
                name: "seed".to_string(),
            })
            .await
            .unwrap();
    }

    let by_a = {
        let table = table.clone();
        tokio::spawn(async move {
            for i in 0..ITERATIONS {
                // Alternating serialized sizes force the reinsert branch.
                let name = if i % 2 == 0 {
                    "a".to_string()
                } else {
                    "a-much-longer-name-value".to_string()
                };
                table
                    .update_name_by_group_a(NameByGroupAQuery { name }, 1)
                    .await
                    .unwrap();
            }
        })
    };
    let by_b = {
        let table = table.clone();
        tokio::spawn(async move {
            for i in 0..ITERATIONS {
                let name = if i % 2 == 0 {
                    "b".to_string()
                } else {
                    "b-much-longer-name-value".to_string()
                };
                table
                    .update_name_by_group_b(NameByGroupBQuery { name }, 1)
                    .await
                    .unwrap();
            }
        })
    };

    tokio::time::timeout(Duration::from_secs(60), async {
        by_a.await.expect("group_a updater must not panic");
        by_b.await.expect("group_b updater must not panic");
    })
    .await
    .expect("overlapping multi-row updates deadlocked");

    for id in 0..ROWS {
        let row = table.select(id).unwrap();
        assert!(row.name.starts_with('a') || row.name.starts_with('b'));
    }
}
