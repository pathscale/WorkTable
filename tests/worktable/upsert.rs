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

/// Upsert is system-wide lock-free but not wait-free per call: a retry is
/// taken exactly when a concurrent delete/insert flips the key's existence
/// between the check and the operation. This test drives sustained
/// adversarial churn on ONE key while several tasks upsert it and asserts
/// that every upsert completes without surfacing a spurious conflict error,
/// under a timeout that turns pathological starvation into a failure
/// instead of a hang.
/// Moderate tier: 40/40 stable when run solo, but under full-suite parallel
/// load the pre-existing #169 stall still fires (~1/30 suite runs), so both
/// tiers stay ignored until that lands. Run with `-- --ignored` to validate
/// the upsert retry behavior.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "exposes the pre-existing #169 lock-free-insert stall under suite load"]
async fn upsert_completes_under_same_key_churn() {
    churn_run(100, 200).await;
}

/// Intense variant: reliably exposes pre-existing engine races under extreme
/// same-key churn that are unrelated to the upsert retry loop (raw `insert`
/// takes no row lock and publishes the pk entry before unghosting the data;
/// under saturation a churn round can stall past the timeout). Tracked in the
/// lock-free-insert issue; un-ignore when that lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "exposes pre-existing lock-free-insert races under extreme same-key churn"]
async fn upsert_completes_under_extreme_same_key_churn() {
    churn_run(5_000, 2_000).await;
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
