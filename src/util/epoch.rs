//! Per-table epoch pin domains built on `crossbeam-epoch`.
//!
//! # Why a domain per table and not the crossbeam global collector
//!
//! Reclamation here is a *grace period* decision: an item retired at time `t`
//! may be recycled once every reader pinned at `t` has unpinned. With the
//! process-global collector, a long scan on one table (or a pinned guard
//! anywhere else in the process, including indexset's skiplists) delays epoch
//! advancement for every table at once. A domain per table means a reader of
//! table A never delays reclamation on table B, and a test holding a guard on
//! its own table cannot make another test's reclamation assertion flaky.
//!
//! # Why the thread-local handle cache
//!
//! `Collector::register()` pushes a participant record onto the collector's
//! shared list with a CAS and an allocation. Doing that per read would be a
//! shared read-modify-write on the read hot path, which is exactly the cost
//! this module exists to remove (the old scheme's one global `SeqCst`
//! `fetch_add` per read). So each thread registers once per domain and caches
//! the `LocalHandle`; after the first read of a table on a thread, `pin()` is
//! a thread-local list hit plus crossbeam's pin (a store and a fence on the
//! thread's own participant record — no shared cache line is written).
//!
//! The cache is capped. Evicting a `LocalHandle` while one of its guards is
//! still alive is safe: crossbeam keeps the participant record alive until
//! both the handle count and the guard count reach zero, and a later `pin()`
//! for that domain simply registers a fresh handle.

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_epoch::{Collector, Guard, LocalHandle};

/// Upper bound on cached `(domain, handle)` pairs per thread. A thread that
/// touches more domains than this re-registers on rotation, it does not fail.
const LOCAL_CACHE_CAP: usize = 128;

static NEXT_DOMAIN_ID: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static LOCALS: RefCell<Vec<(u64, LocalHandle)>> = const { RefCell::new(Vec::new()) };
}

/// An epoch grace-period domain owned by one table (or one partition set).
///
/// Readers call [`EpochDomain::pin`] and hold the returned [`Guard`] across
/// the window that must be protected (index lookup through acquisition of a
/// stable row version). Writers retire items and use [`Guard::defer`] through
/// a pin of the same domain; a deferred function runs only after every guard
/// pinned in this domain at defer time has been dropped.
#[derive(Debug)]
pub(crate) struct EpochDomain {
    collector: Collector,
    id: u64,
}

impl Default for EpochDomain {
    fn default() -> Self {
        Self::new()
    }
}

impl EpochDomain {
    pub(crate) fn new() -> Self {
        Self {
            collector: Collector::new(),
            id: NEXT_DOMAIN_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// Pin the current thread into this domain.
    ///
    /// Hot path: a hit in the thread-local handle cache (moved to front, so a
    /// thread working one table pays a first-entry hit) followed by
    /// crossbeam's thread-local pin. No shared read-modify-write.
    pub(crate) fn pin(&self) -> Guard {
        LOCALS.with(|locals| {
            let mut locals = locals.borrow_mut();
            if let Some(pos) = locals.iter().position(|(id, _)| *id == self.id) {
                if pos != 0 {
                    locals.swap(0, pos);
                }
                return locals[0].1.pin();
            }
            let handle = self.collector.register();
            let guard = handle.pin();
            if locals.len() >= LOCAL_CACHE_CAP {
                // Dropping the tail handle is safe even if a guard from it is
                // somehow still alive: the participant record is finalized
                // only when its guard count also reaches zero.
                locals.pop();
            }
            locals.insert(0, (self.id, handle));
            guard
        })
    }

    /// One bounded step of epoch maintenance: pin, seal and push this
    /// thread's deferred bag, and let crossbeam try to advance the epoch and
    /// execute a bounded number of expired bags.
    ///
    /// One call advances the global epoch by at most one step, so callers
    /// that need a retired item's grace to expire deterministically (tests,
    /// or a reclaimer that found nothing safe yet) call this a small fixed
    /// number of times. It never blocks on readers: while any guard from
    /// before the retirement is still pinned, the epoch simply does not
    /// advance and the call is a few thread-local operations.
    pub(crate) fn advance(&self) {
        self.pin().flush();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;

    use super::*;

    /// Retire a marker through `domain` that increments `hits` once its grace
    /// period has expired.
    fn defer_marker(domain: &EpochDomain, hits: &Arc<AtomicUsize>) {
        let hits = Arc::clone(hits);
        let guard = domain.pin();
        guard.defer(move || {
            hits.fetch_add(1, Ordering::Release);
        });
        guard.flush();
    }

    fn drive(domain: &EpochDomain) {
        for _ in 0..8 {
            domain.advance();
        }
    }

    #[test]
    fn deferred_work_runs_after_unpin() {
        let domain = EpochDomain::new();
        let hits = Arc::new(AtomicUsize::new(0));
        defer_marker(&domain, &hits);
        drive(&domain);
        assert_eq!(hits.load(Ordering::Acquire), 1);
    }

    #[test]
    fn a_pinned_guard_blocks_the_marker_and_release_unblocks_it() {
        let domain = EpochDomain::new();
        let hits = Arc::new(AtomicUsize::new(0));

        let reader = domain.pin();
        defer_marker(&domain, &hits);
        drive(&domain);
        assert_eq!(
            hits.load(Ordering::Acquire),
            0,
            "grace must not expire while a guard from before the retirement is pinned"
        );

        drop(reader);
        drive(&domain);
        assert_eq!(hits.load(Ordering::Acquire), 1);
    }

    /// A second thread that pins and unpins its own guard on command, so a
    /// test can interleave its guard intervals with the main thread's. On one
    /// thread this cannot be modelled: nested pins keep the participant at
    /// its first epoch, which is precisely not what two overlapping readers
    /// do.
    struct RemoteReader {
        commands: mpsc::Sender<bool>,
        done: mpsc::Receiver<()>,
        thread: Option<std::thread::JoinHandle<()>>,
    }

    impl RemoteReader {
        fn spawn(domain: Arc<EpochDomain>) -> Self {
            let (commands, command_rx) = mpsc::channel::<bool>();
            let (done_tx, done) = mpsc::channel();
            let thread = std::thread::spawn(move || {
                let mut guard = None;
                while let Ok(pin) = command_rx.recv() {
                    drop(guard.take());
                    if pin {
                        guard = Some(domain.pin());
                    }
                    done_tx.send(()).unwrap();
                }
                drop(guard);
            });
            Self {
                commands,
                done,
                thread: Some(thread),
            }
        }

        fn pin(&self) {
            self.commands.send(true).unwrap();
            self.done.recv().unwrap();
        }

        fn unpin(&self) {
            self.commands.send(false).unwrap();
            self.done.recv().unwrap();
        }
    }

    impl Drop for RemoteReader {
        fn drop(&mut self) {
            let (a, _b) = mpsc::channel();
            let _ = std::mem::replace(&mut self.commands, a);
            if let Some(t) = self.thread.take() {
                t.join().unwrap();
            }
        }
    }

    #[test]
    fn overlapping_readers_do_not_stall_grace_expiry() {
        // Hand-over-hand between two threads: at every instant at least one
        // guard is pinned, so a zero-reader instant never occurs. The old
        // counter scheme could never reclaim in this pattern; epochs must.
        let domain = Arc::new(EpochDomain::new());
        let hits = Arc::new(AtomicUsize::new(0));
        let remote = RemoteReader::spawn(Arc::clone(&domain));

        let mine = domain.pin();
        defer_marker(&domain, &hits);
        remote.pin(); // remote guard overlaps `mine`
        drop(mine);
        drive(&domain);
        let mine = domain.pin(); // overlaps the remote guard
        remote.unpin();
        drive(&domain);
        remote.pin(); // overlaps `mine` again
        drop(mine);
        drive(&domain);

        assert_eq!(
            hits.load(Ordering::Acquire),
            1,
            "grace never expired even though every individual reader left long ago"
        );
        remote.unpin();
    }

    #[test]
    fn domains_are_independent() {
        let blocked = EpochDomain::new();
        let free = EpochDomain::new();
        let hits = Arc::new(AtomicUsize::new(0));

        let _reader = blocked.pin();
        defer_marker(&free, &hits);
        drive(&free);
        assert_eq!(
            hits.load(Ordering::Acquire),
            1,
            "a pinned guard in one domain must not delay another domain"
        );
    }

    #[test]
    fn markers_flushed_on_another_thread_are_collectable_here() {
        let domain = Arc::new(EpochDomain::new());
        let hits = Arc::new(AtomicUsize::new(0));

        let (tx, rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let d = Arc::clone(&domain);
        let h = Arc::clone(&hits);
        let thread = std::thread::spawn(move || {
            defer_marker(&d, &h);
            tx.send(()).unwrap();
            // Thread stays alive, idle and unpinned, while the main thread
            // collects; its flushed bag must still be reachable.
            let _ = release_rx.recv();
        });
        rx.recv().unwrap();

        drive(&domain);
        assert_eq!(
            hits.load(Ordering::Acquire),
            1,
            "a marker flushed on the retiring thread must be executable from any thread"
        );
        release_tx.send(()).unwrap();
        thread.join().unwrap();
    }

    #[test]
    fn cache_rotation_keeps_pinning_sound() {
        let hits = Arc::new(AtomicUsize::new(0));
        let domains: Vec<_> = (0..LOCAL_CACHE_CAP + 8).map(|_| EpochDomain::new()).collect();
        // Register more domains than the cache holds, with a live guard on the
        // first one across the whole rotation.
        let guard = domains[0].pin();
        defer_marker(&domains[0], &hits);
        for d in &domains[1..] {
            let g = d.pin();
            g.flush();
        }
        drive(&domains[0]);
        assert_eq!(
            hits.load(Ordering::Acquire),
            0,
            "evicting the cached handle must not unpin the live guard"
        );
        drop(guard);
        drive(&domains[0]);
        assert_eq!(hits.load(Ordering::Acquire), 1);
    }
}
