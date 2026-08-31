use std::sync::Arc;
use std::time::Duration;

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: WrongRow,
    columns: {
        id: u64 primary_key,
        code: u64,
        value: u64,
    },
    indexes: {
        code_idx: code unique,
    },
    queries: {
        update: {
            ValueByCode(value) by code,
        }
    }
);

/// A unique-keyed update reads the target's primary key from an unlocked
/// link, locks that key, and then must resolve the row BY THE LOCKED KEY.
/// The old protocol re-resolved the index by value after locking, so if the
/// queried value had moved to a different row in between, it mutated that
/// other row without holding its lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unique_update_does_not_mutate_a_row_that_stole_the_value() {
    let table = Arc::new(WrongRowWorkTable::default());
    table
        .insert(WrongRowRow {
            id: 1,
            code: 10,
            value: 0,
        })
        .unwrap();
    let pk = WrongRowPrimaryKey(1);

    // Park the update between "pk read from the unlocked link" and "row
    // locked" by pre-holding the row lock of the original owner of code 10.
    let blocker = Arc::new(Lock::new(u16::MAX));
    let mut blocker_state = WrongRowLock::new();
    blocker_state.id_lock = Some(blocker.clone());
    blocker_state.code_lock = Some(blocker.clone());
    blocker_state.value_lock = Some(blocker.clone());
    table
        .0
        .lock_manager
        .insert(pk.clone(), Arc::new(tokio::sync::RwLock::new(blocker_state)));

    let update = {
        let table = table.clone();
        tokio::spawn(async move { table.update_value_by_code(ValueByCodeQuery { value: 99 }, 10).await })
    };

    // Wait until the update registered its operation lock (it replaces the
    // blocker in the row state) and is parked awaiting the blocker.
    let blocker_state = table.0.lock_manager.get(&pk).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
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
    .expect("the update did not reach the blocked row");

    // While the update is parked: code 10 moves to a different row.
    table.delete_without_lock(1).await.unwrap();
    table
        .insert(WrongRowRow {
            id: 3,
            code: 10,
            value: 0,
        })
        .unwrap();

    blocker.unlock();

    let result = tokio::time::timeout(Duration::from_secs(5), update)
        .await
        .expect("the update must finish")
        .expect("the update task must not panic");
    assert!(
        matches!(result, Err(WorkTableError::NotFound)),
        "the locked row no longer carries the queried value; got {result:?}"
    );

    // The row that now owns code 10 was never locked by the update and must
    // not have been mutated.
    let bystander = table.select(3).unwrap();
    assert_eq!(bystander.value, 0, "update mutated a row whose lock it never held");
}
