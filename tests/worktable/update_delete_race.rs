use std::sync::Arc;
use std::time::Duration;

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: UpdateDeleteRace,
    columns: {
        id: u64 primary_key,
        name: String,
        value: u64,
    },
    queries: {
        update: {
            NameById(name) by id,
        }
    }
);

/// A row deleted in the unlock window of an unsized update (between the
/// query-lock release and the full-row re-lock) is a legal race: the update
/// must surface `WorkTableError::NotFound`, never panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_update_and_delete_never_panics() {
    const ITERATIONS: u64 = 300;
    let table = Arc::new(UpdateDeleteRaceWorkTable::default());
    table
        .insert(UpdateDeleteRaceRow {
            id: 1,
            name: "seed".to_string(),
            value: 0,
        })
        .await
        .unwrap();

    let updater = {
        let table = table.clone();
        tokio::spawn(async move {
            for i in 0..ITERATIONS {
                // Alternate serialized sizes so both the same-size in-place
                // branch and the reinsert branch are exercised.
                let name = if i % 2 == 0 {
                    "tick".to_string()
                } else {
                    "a-longer-replacement-name".to_string()
                };
                match table.update_name_by_id(NameByIdQuery { name }, 1).await {
                    Ok(()) => {}
                    // Deleted concurrently: legal, retried next iteration.
                    Err(WorkTableError::NotFound) => {}
                    Err(e) => panic!("unexpected update error: {e:?}"),
                }
                tokio::task::yield_now().await;
            }
        })
    };

    let deleter = {
        let table = table.clone();
        tokio::spawn(async move {
            for _ in 0..ITERATIONS {
                match table.delete(1).await {
                    Ok(()) => {}
                    Err(WorkTableError::NotFound) => {}
                    Err(e) => panic!("unexpected delete error: {e:?}"),
                }
                let _ = table
                    .insert(UpdateDeleteRaceRow {
                        id: 1,
                        name: "restored".to_string(),
                        value: 0,
                    })
                    .await;
                tokio::task::yield_now().await;
            }
        })
    };

    tokio::time::timeout(Duration::from_secs(60), async {
        updater.await.expect("updater must not panic");
        deleter.await.expect("deleter must not panic");
    })
    .await
    .expect("the update/delete loop must finish");
}
