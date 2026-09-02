//! Guard test for the synchronous mutation gate (`LockMap::mutation_guard`).
//!
//! The gate is a blocking spin/yield ticket lock, and the generated
//! `update`/`in_place`/`delete` paths hold the resulting `MutationGuard` inside
//! the `LockGuard` **across `.await`** (e.g. `update_with_guard(...).await`,
//! `reinsert(...).await`). A review flagged this as a possible livelock/deadlock
//! when two keys collide on the same 1-of-64 stripe on a constrained runtime.
//!
//! These tests exercise exactly that scenario (colliding keys, single-worker and
//! 2-worker runtimes) and currently PASS: because tokio schedules async tasks
//! cooperatively and the spinner falls back to `thread::yield_now()`, the parked
//! guard-holder is still polled to completion on the same thread, so forward
//! progress holds. They are kept as a standing guard so a future change to the
//! gate (or to spinning under a blocking holder) that DOES introduce the hazard
//! is caught. Every case is wrapped in `tokio::time::timeout`, so if the hazard
//! ever appears it surfaces as a FAILED assertion — never a harness-hanging,
//! memory-ballooning process.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: GateBench,
    columns: {
        id: u64 primary_key,
        val: u64,
    },
    queries: {
        update: {
            Val(val) by id,
        }
    }
);

/// Find two distinct keys that land on the same mutation stripe. The stripe is
/// `DefaultHasher(key) % 64`; brute-force a colliding pair so the test does not
/// depend on gate internals beyond the documented stripe count.
fn colliding_keys() -> (u64, u64) {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    const STRIPES: u64 = 64;
    let stripe = |k: u64| {
        let mut h = DefaultHasher::new();
        GateBenchPrimaryKey::from(k).hash(&mut h);
        h.finish() % STRIPES
    };
    let first = 1u64;
    let target = stripe(first);
    for k in 2..100_000u64 {
        if stripe(k) == target {
            return (first, k);
        }
    }
    panic!("no colliding key pair found");
}

/// Two same-stripe keys updated concurrently must both make progress. On a
/// single-worker runtime, a gate held across `.await` cannot: the parked holder
/// has no thread to resume on while the other task spins.
#[test]
fn concurrent_same_stripe_updates_do_not_deadlock() {
    let (a, b) = colliding_keys();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async move {
        let table = Arc::new(GateBenchWorkTable::default());
        table.insert(GateBenchRow { id: a, val: 0 }).await.unwrap();
        table.insert(GateBenchRow { id: b, val: 0 }).await.unwrap();

        let ta = {
            let table = table.clone();
            tokio::spawn(async move {
                for i in 0..500u64 {
                    table.update_val(ValQuery { val: i }, a).await.unwrap();
                }
            })
        };
        let tb = {
            let table = table.clone();
            tokio::spawn(async move {
                for i in 0..500u64 {
                    table.update_val(ValQuery { val: i }, b).await.unwrap();
                }
            })
        };

        let joined = async {
            ta.await.unwrap();
            tb.await.unwrap();
        };
        timeout(Duration::from_secs(20), joined)
            .await
            .expect("same-stripe concurrent updates deadlocked (gate held across .await)");

        assert_eq!(table.select(a).unwrap().val, 499);
        assert_eq!(table.select(b).unwrap().val, 499);
    });
}

/// Same hazard on a small multi-worker pool: enough same-stripe tasks await
/// while holding the gate that every worker ends up spinning on `serving`.
#[test]
fn many_same_stripe_updates_do_not_starve_worker_pool() {
    let (a, b) = colliding_keys();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async move {
        let table = Arc::new(GateBenchWorkTable::default());
        for k in [a, b] {
            table.insert(GateBenchRow { id: k, val: 0 }).await.unwrap();
        }

        let mut handles = Vec::new();
        for worker in 0..8u64 {
            let table = table.clone();
            let key = if worker % 2 == 0 { a } else { b };
            handles.push(tokio::spawn(async move {
                for i in 0..300u64 {
                    table.update_val(ValQuery { val: i }, key).await.unwrap();
                }
            }));
        }

        let joined = async {
            for h in handles {
                h.await.unwrap();
            }
        };
        timeout(Duration::from_secs(30), joined)
            .await
            .expect("same-stripe pool starved (gate spin held across .await)");
    });
}
