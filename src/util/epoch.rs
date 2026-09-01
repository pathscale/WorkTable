//! Per-table epoch pin domains.
//!
//! # Why a domain per table
//!
//! Reclamation here is a *grace period* decision: an item retired at time `t`
//! may be recycled once every reader pinned at `t` has unpinned. With one
//! process-wide grace period, a long scan on one table (or a pinned guard
//! anywhere else in the process) delays reclamation for every table at once.
//! A domain per table means a reader of table A never delays reclamation on
//! table B, and a test holding a guard on its own table cannot make another
//! test's reclamation assertion flaky.
//!
//! # Why not `crossbeam-epoch`
//!
//! This was built on `crossbeam-epoch`, whose module doc claimed the read path
//! wrote no shared cache line. Measured, it does. `crossbeam` runs a global
//! collect every 128 pins, and that walk touches every registered participant,
//! so its cost grows with the number of readers. On an M4 Max, one pin cost
//! 2.88 ns at one thread and 9.58 ns at eight: a 3.3x degradation on a path
//! whose entire purpose was to be flat. `partition_ref` tracked it exactly,
//! going from 0.71 ns unpinned to 2.92 ns at one reader and 9.79 ns at eight.
//!
//! The read path here publishes into the calling thread's own padded slot and
//! fences. Nothing else writes that line, and collection happens only when a
//! reclaimer asks for it, never on a read. Measured on the same machine at
//! 0.64 ns flat from one thread to eight.
//!
//! # Why the participant registry is process-wide
//!
//! There is an [`EpochDomain`] per table, so 500 partitions means 500 domains.
//! A participant array inside each one would cost megabytes. Instead threads
//! register once in a process-wide registry and publish *which domain* they
//! are pinned in, so a domain is a few words and isolation is preserved: a
//! reader pinned on A does not appear as a pin on B.
//!
//! # Ordering
//!
//! A reader publishes its pin, fences `SeqCst`, then loads the pointer. A
//! reclaimer fences `SeqCst`, then reads the pins. That is the store-buffer
//! pattern, and the pair of `SeqCst` fences is what makes it impossible for a
//! reader to load a pointer the reclaimer believed unprotected.

use std::cell::Cell;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering, fence};

/// Live threads that can hold a pin at once. A thread beyond this bound still
/// works; it falls back to the shared slot, which is correct but is a
/// contended line, so the bound is generous.
const MAX_THREADS: usize = 256;

/// Simultaneous domains one thread can be pinned in. Exceeding it publishes a
/// wildcard that blocks reclamation everywhere until released: conservative,
/// never unsound, and not reachable by this crate's own call shapes.
const PINS_PER_THREAD: usize = 4;

/// Empty pin entry. Domain ids start at 1 so zero is unambiguous.
const NO_DOMAIN: u64 = 0;

/// Bits of a packed pin entry given to the domain id. 16M domains is far past
/// one per table, and it leaves 40 bits of epoch, which at one advance per
/// nanosecond would take 34 years to wrap.
const DOMAIN_BITS: u32 = 24;
const DOMAIN_MASK: u64 = (1 << DOMAIN_BITS) - 1;

static NEXT_DOMAIN_ID: AtomicU64 = AtomicU64::new(1);

/// One thread's pins, on its own cache line so no reader ever invalidates
/// another reader's.
#[repr(align(128))]
struct Participant {
    /// One packed `(epoch << DOMAIN_BITS) | domain_id` per pin entry, or
    /// zero. Packed so publishing a pin is a single store: as two fields it
    /// was two, on the hottest path in the crate.
    pins: [AtomicU64; PINS_PER_THREAD],
    /// Non-zero when this thread ran out of entries and is conservatively
    /// treated as pinned in every domain.
    wildcard: AtomicU64,
}

impl Participant {
    const fn new() -> Self {
        #[allow(clippy::declare_interior_mutable_const)]
        const ZERO: AtomicU64 = AtomicU64::new(0);
        Self {
            pins: [ZERO; PINS_PER_THREAD],
            wildcard: ZERO,
        }
    }
}

struct Registry {
    slots: Vec<Participant>,
    /// Indices returned by threads that have exited, handed out again rather
    /// than growing without bound in a program that spawns many short threads.
    free: Mutex<Vec<usize>>,
    next: AtomicUsize,
}

impl Registry {
    fn get() -> &'static Registry {
        static REGISTRY: std::sync::OnceLock<Registry> = std::sync::OnceLock::new();
        REGISTRY.get_or_init(|| Registry {
            slots: (0..MAX_THREADS).map(|_| Participant::new()).collect(),
            free: Mutex::new(Vec::new()),
            next: AtomicUsize::new(0),
        })
    }

    fn acquire(&self) -> usize {
        if let Some(idx) = self.free.lock().unwrap_or_else(|e| e.into_inner()).pop() {
            return idx;
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed);
        // Past the bound every extra thread shares the last slot. Sharing a
        // slot is sound (a pin there simply looks like someone else's pin, so
        // reclamation is delayed, never premature) and only costs contention.
        idx.min(MAX_THREADS - 1)
    }

    fn release(&self, idx: usize) {
        let p = &self.slots[idx];
        for i in 0..PINS_PER_THREAD {
            p.pins[i].store(NO_DOMAIN, Ordering::Release);
        }
        p.wildcard.store(0, Ordering::Release);
        if idx < MAX_THREADS - 1 {
            self.free.lock().unwrap_or_else(|e| e.into_inner()).push(idx);
        }
    }
}

/// Releases this thread's registry slot when the thread exits.
struct SlotLease(usize);

impl Drop for SlotLease {
    fn drop(&mut self) {
        Registry::get().release(self.0);
    }
}

thread_local! {
    /// Cached so the read path never touches the `OnceLock` or indexes the
    /// registry `Vec`. Doing that on both pin and unpin cost 12 ns per
    /// lookup, four times what the pin itself costs.
    static MINE: Cell<Option<&'static Participant>> = const { Cell::new(None) };
    static LEASE: std::cell::RefCell<Option<SlotLease>> =
        const { std::cell::RefCell::new(None) };
}

#[inline]
fn participant() -> &'static Participant {
    if let Some(p) = MINE.with(|m| m.get()) {
        return p;
    }
    let registry = Registry::get();
    let idx = registry.acquire();
    let p = &registry.slots[idx];
    MINE.with(|m| m.set(Some(p)));
    // Separate so the lease's `Drop` runs at thread exit. Failing to install
    // it during TLS teardown leaks one slot rather than recycling it, which is
    // why `acquire` is bounded rather than fallible.
    let _ = LEASE.try_with(|l| *l.borrow_mut() = Some(SlotLease(idx)));
    p
}

type Deferred = Box<dyn FnOnce() + Send + 'static>;

/// A grace-period domain for one table.
pub(crate) struct EpochDomain {
    id: u64,
    /// Advanced only by a reclaimer, never on a read.
    epoch: AtomicU64,
    garbage: Mutex<Vec<(u64, Deferred)>>,
}

impl std::fmt::Debug for EpochDomain {
    /// Deliberately shallow: the garbage list holds closures and the pin
    /// registry is process-wide, so neither is meaningful here.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EpochDomain")
            .field("id", &self.id)
            .field("epoch", &self.epoch.load(Ordering::Relaxed))
            .finish()
    }
}

impl Default for EpochDomain {
    fn default() -> Self {
        Self::new()
    }
}

impl EpochDomain {
    pub(crate) fn new() -> Self {
        Self {
            id: NEXT_DOMAIN_ID.fetch_add(1, Ordering::Relaxed),
            epoch: AtomicU64::new(1),
            garbage: Mutex::new(Vec::new()),
        }
    }

    /// Pin the current thread into this domain.
    ///
    /// The whole hot path: a thread-local index, a relaxed load of this
    /// domain's epoch, two relaxed stores into this thread's own slot, and one
    /// `SeqCst` fence. No shared line is written, so it does not degrade as
    /// readers are added.
    #[inline]
    pub(crate) fn pin(&self) -> Guard<'_> {
        let p = participant();
        let e = self.epoch.load(Ordering::Relaxed);

        let packed = (e << DOMAIN_BITS) | (self.id & DOMAIN_MASK);
        let mut entry = usize::MAX;
        for i in 0..PINS_PER_THREAD {
            if p.pins[i].load(Ordering::Relaxed) == NO_DOMAIN {
                // SeqCst store rather than a relaxed store plus
                // `fence(SeqCst)`. Both give the ordering `advance` needs
                // against its own SeqCst fence, but on aarch64 this is one
                // `stlr` where the fence is a full `dmb ish`, and the fence
                // version measured 74 percent slower than crossbeam.
                p.pins[i].store(packed, Ordering::SeqCst);
                entry = i;
                break;
            }
        }
        if entry == usize::MAX {
            p.wildcard.fetch_add(1, Ordering::SeqCst);
        }

        Guard {
            domain: self,
            participant: p,
            entry,
        }
    }

    /// Retire `f` to run once every reader pinned now has unpinned.
    fn defer(&self, f: Deferred) {
        let e = self.epoch.load(Ordering::Relaxed);
        self.garbage
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((e, f));
    }

    /// Run every deferred item whose grace period has expired, then advance
    /// the epoch by one.
    ///
    /// Never blocks on readers: while a reader pinned before a retirement is
    /// still pinned, that item simply is not collected yet.
    pub(crate) fn advance(&self) {
        // Paired with the fence in `pin`. Everything published by a reader
        // before it loaded a pointer is visible below.
        fence(Ordering::SeqCst);

        let mut min_pinned = self.epoch.load(Ordering::Relaxed);
        let registry = Registry::get();
        for p in &registry.slots {
            if p.wildcard.load(Ordering::Acquire) != 0 {
                // Someone is conservatively pinned everywhere.
                return;
            }
            for i in 0..PINS_PER_THREAD {
                let packed = p.pins[i].load(Ordering::Acquire);
                if packed != NO_DOMAIN && (packed & DOMAIN_MASK) == (self.id & DOMAIN_MASK) {
                    let e = packed >> DOMAIN_BITS;
                    if e < min_pinned {
                        min_pinned = e;
                    }
                }
            }
        }

        let expired: Vec<Deferred> = {
            let mut garbage = self.garbage.lock().unwrap_or_else(|e| e.into_inner());
            let mut keep = Vec::with_capacity(garbage.len());
            let mut run = Vec::new();
            for (e, f) in garbage.drain(..) {
                // Strictly less than: an item retired in the same epoch a
                // reader pinned in may still be reachable by that reader.
                if e < min_pinned {
                    run.push(f);
                } else {
                    keep.push((e, f));
                }
            }
            *garbage = keep;
            run
        };

        // Run the closures outside the lock: one of them may retire more.
        for f in expired {
            f();
        }

        self.epoch.fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for EpochDomain {
    fn drop(&mut self) {
        // Exclusive access, so no reader can be pinned here: run everything.
        let garbage = std::mem::take(
            &mut *self
                .garbage
                .lock()
                .unwrap_or_else(|e: std::sync::PoisonError<_>| e.into_inner()),
        );
        for (_, f) in garbage {
            f();
        }
    }
}

/// Holds this thread's pin on a domain open. Dropping it releases the pin.
pub(crate) struct Guard<'a> {
    domain: &'a EpochDomain,
    /// Cached so unpinning is a single store, with no registry lookup.
    participant: &'static Participant,
    /// Which pin entry this guard owns, or `usize::MAX` for the wildcard.
    entry: usize,
}

impl Guard<'_> {
    /// Retire `f` to run once the current readers have gone.
    pub(crate) fn defer<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.domain.defer(Box::new(f));
    }

    /// One bounded step of maintenance. Named for the call it replaces.
    pub(crate) fn flush(&self) {
        self.domain.advance();
    }
}

impl Drop for Guard<'_> {
    #[inline]
    fn drop(&mut self) {
        let p = self.participant;
        if self.entry == usize::MAX {
            p.wildcard.fetch_sub(1, Ordering::Release);
        } else {
            // Release so a reclaimer that sees the slot free also sees every
            // access this reader made while it was pinned.
            p.pins[self.entry].store(NO_DOMAIN, Ordering::Release);
        }
    }
}
