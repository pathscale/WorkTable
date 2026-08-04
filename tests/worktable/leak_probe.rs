//! Leak probe (review follow-up): a long update-churn loop must not grow
//! storage without bound. Reinsert-per-update leaves dead slots / retired
//! publications; if reclamation never catches up, an update-heavy workload
//! balloons memory (the suspected cause of the hung, ballooning test process).
//!
//! Asserts on the table's own storage accounting (row/page counts) rather than
//! process RSS, so it is deterministic and cannot itself hang the harness.

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: LeakProbe,
    columns: {
        id: u64 primary_key,
        payload: String,
    },
    queries: {
        update: {
            Payload(payload) by id,
        }
    }
);

#[tokio::test]
async fn update_churn_does_not_grow_storage_unbounded() {
    let table = LeakProbeWorkTable::default();
    table
        .insert(LeakProbeRow {
            id: 1,
            payload: "0000".to_string(),
        })
        .unwrap();

    // One row, many same-length updates. Logical cardinality stays 1 the whole
    // time; only physical slot churn happens.
    let pages_after_warmup = {
        for i in 0..100u64 {
            table
                .update_payload(
                    PayloadQuery {
                        payload: format!("{:04}", i % 10000),
                    },
                    1,
                )
                .await
                .unwrap();
        }
        table.0.data.get_bytes().len()
    };

    for i in 0..5_000u64 {
        table
            .update_payload(
                PayloadQuery {
                    payload: format!("{:04}", i % 10000),
                },
                1,
            )
            .await
            .unwrap();
    }

    let pages_after_churn = table.0.data.get_bytes().len();

    // Logical row count is unchanged.
    assert_eq!(table.count(), 1, "cardinality drifted under update churn");

    // Physical page count must not grow ~linearly with the number of updates.
    // Allow generous slack, but 5000 updates growing pages by thousands is a
    // reclamation leak, not normal slack.
    assert!(
        pages_after_churn <= pages_after_warmup + 8,
        "update churn leaked pages: {pages_after_warmup} -> {pages_after_churn} over 5000 same-key updates"
    );
}
