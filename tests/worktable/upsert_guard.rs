//! `upsert` must not release the row it is deciding about.
//!
//! When `upsert` finds the key absent it wants to insert without letting go of
//! the full row lock it just took. It could not: `insert` re-acquires the same
//! per-key mutation gate that lock's guard already holds, so holding on would
//! deadlock against itself. So it dropped the guard, inserted, and retried
//! with backoff if another writer won the gap.
//!
//! `insert_locked` is that same insert without the acquisition, which lets the
//! decision and the write be one critical section.
//!
//! This is also the guard for making `insert` async. Async does not remove the
//! self-deadlock, it only makes the window worse: the drop-to-insert gap would
//! then contain an await, so a task can be descheduled inside it and a gap of
//! a few hundred nanoseconds of straight-line code becomes a scheduling
//! quantum. If a future change routes this path back through a gate-acquiring
//! insert, this test hangs and then fails on the timeout.

use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: UpsertGuard,
    columns: { id: u64 primary_key, v: u64 },
);

/// Inserting under a gate the caller already holds must complete.
///
/// Red before `insert_locked` existed: the only way to insert was `insert`,
/// which takes the gate again and blocks forever against the guard held here.
#[test]
fn inserting_under_a_held_mutation_gate_completes() {
    let table = Arc::new(UpsertGuardWorkTable::default());
    let pk: UpsertGuardPrimaryKey = 1u64.into();

    // Exactly what `upsert` holds when it discovers the key is absent.
    let _gate = table.0.lock_manager.mutation_guard(&pk);

    // Detached, not scoped. A scope joins its threads, so when the insert
    // deadlocks the test would hang at the end of the scope instead of
    // failing on the timeout, and a hanging test reports nothing.
    let (tx, rx) = mpsc::channel();
    let worker = Arc::clone(&table);
    thread::spawn(move || {
        let outcome = worker.0.insert_locked(UpsertGuardRow { id: 1, v: 7 });
        let _ = tx.send(outcome.is_ok());
    });

    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(inserted) => assert!(inserted, "the insert itself should succeed"),
        Err(_) => panic!(
            "inserting under a held mutation gate did not finish: the insert path re-acquired the \
             gate its caller already holds and deadlocked. `upsert` cannot hold its row lock \
             across the insert while that is true, which is the window it used to drop the guard \
             for."
        ),
    }

    assert_eq!(table.select(1).expect("row present").v, 7);
}

/// And the ordinary path still takes the gate, so a caller that is not already
/// holding one is still serialised.
#[tokio::test]
async fn upsert_still_serialises_concurrent_writers() {
    use std::sync::Arc;
    let table = Arc::new(UpsertGuardWorkTable::default());

    let mut handles = Vec::new();
    for v in 0..32u64 {
        let table = table.clone();
        handles.push(tokio::spawn(
            async move { table.upsert(UpsertGuardRow { id: 1, v }).await },
        ));
    }
    for h in handles {
        h.await.expect("task").expect("upsert");
    }

    // Exactly one row, whichever writer landed last.
    assert_eq!(table.count(), 1);
    assert!(table.select(1).is_some());
}
