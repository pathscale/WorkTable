use std::sync::Arc;
use std::time::Duration;

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: CancelSafety,
    columns: {
        id: u64 primary_key,
        value: u64,
        other: u64,
    },
    queries: {
        update: {
            ValueById(value) by id,
        }
    }
);

/// Installs a conflicting held lock for `pk`, exactly as another in-flight
/// operation would leave it while working on the row.
fn install_blocker(table: &CancelSafetyWorkTable, pk: &CancelSafetyPrimaryKey) -> Arc<Lock> {
    let blocker = Arc::new(Lock::new(u16::MAX));
    let mut blocker_state = CancelSafetyLock::new();
    blocker_state.value_lock = Some(blocker.clone());
    blocker_state.other_lock = Some(blocker.clone());
    blocker_state.id_lock = Some(blocker.clone());
    table
        .0
        .lock_manager
        .insert(pk.clone(), Arc::new(tokio::sync::RwLock::new(blocker_state))).await;
    blocker
}

/// A future dropped while awaiting its lock predecessors (tokio timeout,
/// task abort) must not leave its already-registered operation lock held:
/// that would hang every later operation on the same primary key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_full_row_update_releases_registered_lock() {
    let table = Arc::new(CancelSafetyWorkTable::default());
    table
        .insert(CancelSafetyRow {
            id: 1,
            value: 0,
            other: 0,
        })
        .unwrap();
    let pk = CancelSafetyPrimaryKey(1);

    let blocker = install_blocker(&table, &pk);

    // The update registers its operation lock, then parks awaiting the
    // blocker; the timeout cancels it at exactly that await.
    let cancelled = tokio::time::timeout(
        Duration::from_millis(200),
        table.update(CancelSafetyRow {
            id: 1,
            value: 7,
            other: 7,
        }),
    )
    .await;
    assert!(
        cancelled.is_err(),
        "the update should still be blocked when the timeout fires"
    );

    blocker.unlock();

    // Bounded outer timeouts turn a leaked registered lock into a test
    // failure instead of a hang.
    tokio::time::timeout(
        Duration::from_secs(5),
        table.update(CancelSafetyRow {
            id: 1,
            value: 9,
            other: 9,
        }),
    )
    .await
    .expect("update after a cancelled predecessor must not hang")
    .unwrap();
    assert_eq!(table.select(1).unwrap().value, 9);

    tokio::time::timeout(Duration::from_secs(5), table.delete(1))
        .await
        .expect("delete after a cancelled predecessor must not hang")
        .unwrap();
    assert!(table.select(1).is_none());
}

/// Same protocol-level guarantee for a generated custom (per-column) update.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_custom_update_releases_registered_lock() {
    let table = Arc::new(CancelSafetyWorkTable::default());
    table
        .insert(CancelSafetyRow {
            id: 3,
            value: 0,
            other: 0,
        })
        .unwrap();
    let pk = CancelSafetyPrimaryKey(3);

    let blocker = install_blocker(&table, &pk);

    let cancelled = tokio::time::timeout(
        Duration::from_millis(200),
        table.update_value_by_id(ValueByIdQuery { value: 5 }, 3),
    )
    .await;
    assert!(
        cancelled.is_err(),
        "the update should still be blocked when the timeout fires"
    );

    blocker.unlock();

    tokio::time::timeout(
        Duration::from_secs(5),
        table.update_value_by_id(ValueByIdQuery { value: 11 }, 3),
    )
    .await
    .expect("update after a cancelled predecessor must not hang")
    .unwrap();
    assert_eq!(table.select(3).unwrap().value, 11);

    tokio::time::timeout(Duration::from_secs(5), table.delete(3))
        .await
        .expect("delete after a cancelled predecessor must not hang")
        .unwrap();
    assert!(table.select(3).is_none());
}
