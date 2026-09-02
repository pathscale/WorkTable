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

/// Same probe under continuously overlapping readers. The retired scheme
/// required a global zero-reader instant to reclaim anything, so a scan-heavy
/// workload starved reclamation entirely and update churn grew storage without
/// bound. Epoch grace periods only require each individual read to finish, so
/// storage stays bounded even though readers never stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_churn_reclaims_under_concurrent_readers() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let table = Arc::new(LeakProbeWorkTable::default());
    table
        .insert(LeakProbeRow {
            id: 1,
            payload: "0000".to_string(),
        })
        .await
        .unwrap();
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
    let pages_after_warmup = table.0.data.get_page_count();

    let stop = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..3)
        .map(|_| {
            let table = table.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut hits = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    if table.select(1).is_some() {
                        hits += 1;
                    }
                }
                hits
            })
        })
        .collect();

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
    let pages_after_churn = table.0.data.get_page_count();

    stop.store(true, Ordering::Relaxed);
    let hits: u64 = readers.into_iter().map(|r| r.join().unwrap()).sum();

    assert_eq!(table.count(), 1, "cardinality drifted under update churn");
    assert!(
        hits > 1_000,
        "readers were not actually overlapping the churn (only {hits} reads)"
    );
    assert!(
        pages_after_churn <= pages_after_warmup + 8,
        "update churn under concurrent readers leaked pages: \
         {pages_after_warmup} -> {pages_after_churn} over 5000 same-key updates"
    );
}

#[tokio::test]
async fn update_churn_does_not_grow_storage_unbounded() {
    let table = LeakProbeWorkTable::default();
    table
        .insert(LeakProbeRow {
            id: 1,
            payload: "0000".to_string(),
        })
        .await
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
        table.0.data.get_page_count()
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

    let pages_after_churn = table.0.data.get_page_count();

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
