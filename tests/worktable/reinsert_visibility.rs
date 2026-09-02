//! Regression: a reinsert that fails a secondary unique-index check must never
//! expose the new (uncommitted) row values to a concurrent lock-free reader.
//!
//! The broken ordering unghosted the new row and swung the primary index
//! before the secondary unique-index check ran, so a reader could observe the
//! new values of an update that was then rolled back with `AlreadyExists`
//! (a dirty read).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: ReinsertVis,
    columns: {
        id: u64 primary_key autoincrement,
        val: i64,
        payload: String
    },
    indexes: {
        val_idx: val unique,
    }
);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_reinsert_never_exposes_uncommitted_values() {
    let table = Arc::new(ReinsertVisWorkTable::default());

    let row_a = ReinsertVisRow {
        id: table.get_next_pk().into(),
        val: 1,
        payload: "committed".to_string(),
    };
    let row_b = ReinsertVisRow {
        id: table.get_next_pk().into(),
        val: 2,
        payload: "occupant".to_string(),
    };
    table.insert(row_a.clone()).await.unwrap();
    table.insert(row_b.clone()).await.unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let table = Arc::clone(&table);
        let stop = Arc::clone(&stop);
        let expected = row_a.clone();
        std::thread::spawn(move || {
            let mut observations = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let observed = table.select(expected.id);
                assert_eq!(
                    observed.as_ref(),
                    Some(&expected),
                    "reader observed state of a reinsert that was rolled back"
                );
                observations += 1;
            }
            observations
        })
    };

    for i in 0..2_000u64 {
        // Collides with row_b on the unique `val` index, so every reinsert
        // must fail and roll back without a reader ever seeing its values.
        let colliding = ReinsertVisRow {
            id: row_a.id,
            val: 2,
            payload: format!("uncommitted-{i}"),
        };
        let result = table.0.reinsert(row_a.clone(), colliding).await;
        assert!(matches!(result, Err(WorkTableError::AlreadyExists(_))));
    }

    stop.store(true, Ordering::Relaxed);
    let observations = reader.join().expect("reader must not observe dirty state");
    assert!(observations > 0, "reader must have run concurrently");

    // Final state: both rows keep their committed values.
    assert_eq!(table.select(row_a.id), Some(row_a));
    assert_eq!(table.select(row_b.id), Some(row_b));
}
