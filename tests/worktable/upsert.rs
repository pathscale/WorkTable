use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: UpsertChurn,
    columns: {
        id: u64 primary_key,
        val: u64,
    },
);

/// After its optimistic absent-key fast path, upsert holds the generated
/// full-row lock across its repeated existence check and mutation. This test
/// drives sustained adversarial churn on one key while several tasks upsert
/// it, asserting that every operation completes without a spurious conflict
/// error. The timeout turns a lock-order regression into a failure instead of
/// a hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_completes_under_same_key_churn() {
    churn_run(100, 200).await;
}

/// Intense variant for the same-key upsert/delete linearization protocol.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_completes_under_extreme_same_key_churn() {
    // Keep this materially heavier than the normal case without making the
    // assertion depend on runner speed: 2,000 churn flips perform both an
    // upsert and a delete, alongside 4,000 competing upserts.
    churn_run(2_000, 1_000).await;
}

/// A synchronous insert does not participate in the generated async row lock.
/// Its collision and ghost-publication windows must still return typed errors,
/// never panic a concurrent locked delete or strand an upsert waiter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_insert_delete_churn_never_panics_or_stalls() {
    let table = Arc::new(UpsertChurnWorkTable::default());
    const KEY: u64 = 7;

    let churn = {
        let table = table.clone();
        tokio::spawn(async move {
            let mut insert_successes = 0;
            let mut insert_conflicts = 0;
            let mut delete_successes = 0;
            let mut delete_misses = 0;
            for i in 0..5_000u64 {
                match table.insert(UpsertChurnRow { id: KEY, val: i }) {
                    Ok(_) => insert_successes += 1,
                    Err(WorkTableError::PrimaryAlreadyExists) => insert_conflicts += 1,
                    Err(error) => panic!("raw insert returned an unexpected error: {error:?}"),
                }
                match table.delete(KEY).await {
                    Ok(()) => delete_successes += 1,
                    Err(WorkTableError::NotFound) => delete_misses += 1,
                    Err(error) => panic!("delete returned an unexpected error: {error:?}"),
                }
            }

            (insert_successes, insert_conflicts, delete_successes, delete_misses)
        })
    };

    let mut upserters = Vec::new();
    for worker in 0..4u64 {
        let table = table.clone();
        upserters.push(tokio::spawn(async move {
            for i in 0..2_000u64 {
                table
                    .upsert(UpsertChurnRow {
                        id: KEY,
                        val: worker * 10_000 + i,
                    })
                    .await
                    .expect("upsert must never surface a primary-key conflict");
            }
        }));
    }

    let (insert_successes, insert_conflicts, delete_successes, delete_misses) = timeout(Duration::from_secs(60), churn)
        .await
        .expect("raw insert/delete churn starved")
        .unwrap();
    assert_eq!(insert_successes + insert_conflicts, 5_000);
    assert_eq!(delete_successes + delete_misses, 5_000);
    for handle in upserters {
        timeout(Duration::from_secs(60), handle)
            .await
            .expect("upserter starved during raw insert/delete churn")
            .unwrap();
    }

    // Force a deterministic final state, then audit every layer used to reach
    // the row. A liveness-only test would miss a stale reverse entry, a ghost
    // publication, or a primary link that points at unrelated data.
    let expected = UpsertChurnRow { id: KEY, val: 424_242 };
    table.upsert(expected.clone()).await.unwrap();

    let pk = UpsertChurnPrimaryKey::from(KEY);
    let link = table
        .0
        .primary_index
        .pk_map
        .get_value(&pk)
        .expect("final row must have one primary-index entry");
    assert_eq!(table.0.primary_index.pk_map.len(), 1);
    assert_eq!(table.0.primary_index.reverse_pk_map.len(), 1);
    assert_eq!(
        table
            .0
            .primary_index
            .reverse_pk_map
            .get(&link)
            .map(|entry| entry.get().value.clone()),
        Some(pk),
        "reverse index must point back to the final primary key"
    );
    assert_eq!(
        table.0.data.select_non_ghosted(link.0),
        Ok(expected.clone()),
        "primary-index link must resolve to the final non-ghosted row"
    );
    assert_eq!(table.select(KEY), Some(expected));
}

/// Pins the exact publication schedule that used to let delete unwrap a
/// ghosted row: data and primary-index reachability exist, but insert has not
/// yet cleared the lifecycle bit. Delete must linearize before publication and
/// leave the staged insert intact.
#[tokio::test]
async fn delete_during_insert_publication_window_returns_not_found() {
    let table = UpsertChurnWorkTable::default();
    const KEY: u64 = 7;
    let row = UpsertChurnRow { id: KEY, val: 11 };

    let link = table.0.data.insert(row.clone()).unwrap();
    assert!(
        table
            .0
            .primary_index
            .insert_checked(UpsertChurnPrimaryKey::from(KEY), link)
            .is_some()
    );

    assert!(matches!(table.delete(KEY).await, Err(WorkTableError::NotFound)));

    unsafe {
        table.0.data.with_mut_ref(link, |staged| staged.unghost()).unwrap();
    }
    assert_eq!(table.select(KEY), Some(row));
}

async fn churn_run(churn_flips: u64, upserts_per_task: u64) {
    #[allow(non_snake_case)]
    let CHURN_FLIPS = churn_flips;
    #[allow(non_snake_case)]
    let UPSERTS_PER_TASK = upserts_per_task;
    let table = Arc::new(UpsertChurnWorkTable::default());
    const KEY: u64 = 7;

    let churn = {
        let table = table.clone();
        tokio::spawn(async move {
            for i in 0..CHURN_FLIPS {
                // Flip the key's existence as fast as possible through the
                // locked operations. (Raw `insert` is deliberately not used
                // here: it takes no row lock and publishes the pk entry
                // before unghosting the data, which trips unrelated
                // pre-existing races tracked separately in the issue on
                // lock-free insert vs locked mutations.)
                table
                    .upsert(UpsertChurnRow { id: KEY, val: i })
                    .await
                    .expect("churn upsert must not fail");
                let _ = table.delete(KEY).await;
            }
        })
    };

    let mut upserters = Vec::new();
    for w in 0..4u64 {
        let table = table.clone();
        upserters.push(tokio::spawn(async move {
            for i in 0..UPSERTS_PER_TASK {
                table
                    .upsert(UpsertChurnRow {
                        id: KEY,
                        val: w * 10_000 + i,
                    })
                    .await
                    .expect("upsert must never surface a primary-key conflict");
            }
        }));
    }

    timeout(Duration::from_secs(60), churn)
        .await
        .expect("churn task starved")
        .unwrap();
    for handle in upserters {
        timeout(Duration::from_secs(60), handle)
            .await
            .expect("upserter starved")
            .unwrap();
    }

    // Quiesced: a final upsert must land and be visible.
    table.upsert(UpsertChurnRow { id: KEY, val: 424_242 }).await.unwrap();
    assert_eq!(table.select(KEY).map(|r| r.val), Some(424_242));
}
