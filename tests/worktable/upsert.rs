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
            for i in 0..5_000u64 {
                let _ = table.insert(UpsertChurnRow { id: KEY, val: i });
                let _ = table.delete(KEY).await;
            }
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

    timeout(Duration::from_secs(60), churn)
        .await
        .expect("raw insert/delete churn starved")
        .unwrap();
    for handle in upserters {
        timeout(Duration::from_secs(60), handle)
            .await
            .expect("upserter starved during raw insert/delete churn")
            .unwrap();
    }
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
