use std::sync::Arc;
use std::time::Duration;

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: LockOrder,
    columns: {
        id: u64 primary_key autoincrement,
        group_a: u64,
        group_b: u64,
        value: u64,
    },
    indexes: {
        group_a_idx: group_a,
        group_b_idx: group_b,
    },
    queries: {
        update: {
            ValueByGroupA(value) by group_a,
            ValueByGroupB(value) by group_b,
        }
    }
);

fn first_primary_key_inversion(order: &[u64]) -> Option<(u64, u64)> {
    order.iter().copied().enumerate().find_map(|(position, blocker)| {
        order[position + 1..]
            .iter()
            .copied()
            .filter(|candidate| *candidate < blocker)
            .min()
            .map(|witness| (blocker, witness))
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_row_update_locks_in_primary_key_order_not_index_order() {
    const ROWS: u64 = 128;
    let table = Arc::new(LockOrderWorkTable::default());

    // Reverse insertion makes insertion- or link-ordered indexes disagree
    // with primary-key order. Hash- or tree-backed indexes are covered by
    // finding an actual inversion in the order they expose.
    for id in (0..ROWS).rev() {
        table
            .insert(LockOrderRow {
                id,
                group_a: 1,
                group_b: 1,
                value: 0,
            })
            .unwrap();
    }

    let order_a: Vec<_> = table
        .0
        .indexes
        .group_a_idx
        .get(&1)
        .map(|(_, link)| table.0.data.select_non_ghosted(link.0).unwrap().id)
        .collect();
    let order_b: Vec<_> = table
        .0
        .indexes
        .group_b_idx
        .get(&1)
        .map(|(_, link)| table.0.data.select_non_ghosted(link.0).unwrap().id)
        .collect();

    let (use_group_a, blocker_id, witness_id) = first_primary_key_inversion(&order_a)
        .map(|(blocker, witness)| (true, blocker, witness))
        .or_else(|| {
            first_primary_key_inversion(&order_b)
                .map(|(blocker, witness)| (false, blocker, witness))
        })
        .expect(
            "secondary indexes exposed only primary-key order; the test needs an inversion to distinguish lock ordering",
        );

    // Hold a row that the chosen secondary index exposes before a smaller
    // primary key. An index-ordered update blocks here before reaching the
    // witness. A primary-key-ordered update registers the witness lock first.
    let blocker_pk = LockOrderPrimaryKey(blocker_id);
    let blocker = Arc::new(Lock::new(u16::MAX));
    let mut blocker_state = LockOrderLock::new();
    blocker_state.value_lock = Some(blocker.clone());
    table
        .0
        .lock_manager
        .insert(blocker_pk.clone(), Arc::new(tokio::sync::RwLock::new(blocker_state)));

    let update_table = table.clone();
    let update = tokio::spawn(async move {
        if use_group_a {
            update_table
                .update_value_by_group_a(ValueByGroupAQuery { value: 1 }, 1)
                .await
        } else {
            update_table
                .update_value_by_group_b(ValueByGroupBQuery { value: 1 }, 1)
                .await
        }
    });

    let blocker_state = table.0.lock_manager.get(&blocker_pk).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let installed_id = blocker_state
                .read()
                .await
                .value_lock
                .as_ref()
                .expect("the update locks value")
                .id();
            if installed_id != blocker.id() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the update did not reach the deliberately blocked row");

    assert!(
        table.0.lock_manager.get(&LockOrderPrimaryKey(witness_id)).is_some(),
        "the update followed secondary-index order instead of locking the smaller primary key first"
    );

    blocker.unlock();
    tokio::time::timeout(Duration::from_secs(2), update)
        .await
        .expect("the update did not resume after the predecessor lock was released")
        .unwrap()
        .unwrap();

    for id in 0..ROWS {
        assert_eq!(table.select(id).unwrap().value, 1);
    }
}
