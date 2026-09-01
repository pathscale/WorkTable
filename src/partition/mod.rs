//! Partitioned tables: one table type, many routed instances.
//!
//! A partitioned table is N instances of one generated table type, addressed
//! by an unsigned integer key. The type is generated once, so nothing about
//! generated code size or compile time scales with N.
//!
//! The routing key is never a column. It is not stored in a row and no query
//! can reference it, so it does not need to be a rich type; it needs to be an
//! array index. Names belong in a separate registry table consulted once when
//! a key is first resolved, not on every access.
//!
//! # Storage
//!
//! Partitions live in a segmented vector: a fixed spine of chunk pointers,
//! each chunk holding [`CHUNK`] slots. Chunks are allocated on demand and
//! never move, so a reader indexes straight into one with no lock and no
//! reference counting on the spine. Creating a partition takes a mutex, which
//! is the right trade: routing is hot and creation is rare.
//!
//! Measured on an M4 Max, 500 partitions, single thread, cache-warm: a
//! segmented lookup is within 0.2 ns of a flat `Vec` index, and both are
//! roughly 7x faster than any hash of a string key.
//!
//! # Why a slot is an `AtomicPtr` and not an `Option<Arc<T>>`
//!
//! Readers run without the growth mutex, so a slot is written by one thread
//! while another reads it. That has to be an atomic access or it is a data
//! race, and `remove` makes it more than a formality: a reader that has read
//! the pointer but not yet incremented the strong count would have the
//! allocation freed under it by the removing thread.
//!
//! So a slot holds one owned strong reference as a raw pointer, and `remove`
//! does not drop the reference it takes out of the slot. It moves it to a
//! retire queue and defers an epoch marker in the set's own [`EpochDomain`];
//! every read-path access to a slot pointer happens under an epoch pin, so
//! the marker executes only once every reader that could have loaded the
//! pointer has finished. The count of executed markers releases a
//! front-of-queue prefix that [`PartitionSet::collect`] frees through
//! `&self` — reclamation works through the production `Arc`-shared router,
//! and `remove` and `get_or_create` collect opportunistically so a router
//! that keeps mutating never accumulates removed partitions.
//! [`PartitionSet::gc`] remains as the exhaustive variant for callers that do
//! hold `&mut self`.
//!
//! Under `--cfg wt_loom` the epoch machinery is compiled out (crossbeam-epoch
//! cannot run under loom) and the retire queue is only drained by `gc`, which
//! is the pre-epoch behaviour: the loom models check the slot publication
//! protocol, which is identical in both builds.
//!
//! # What this does not do yet
//!
//! No eviction, no lazy load, no per-partition persistence. Every partition
//! held is resident. That is adequate for a few thousand in-memory partitions
//! and inadequate for a fine-grained persisted axis, because a persisted
//! instance measures 110 KB and 6.1 ms to construct, of which 95 percent is
//! inside `PersistenceEngine::new`.

// Under `--cfg wt_loom` the atomics and the mutex come from loom, which explores
// every interleaving of them rather than whichever one this machine happened
// to produce. `Arc` stays `std`: loom's has no `into_raw` or
// `increment_strong_count`, and std's own atomics are already model-checked
// upstream. What loom is being asked about here is the slot protocol and the
// double-checked lock in `get_or_create`, not reference counting.
#[cfg(wt_loom)]
use loom::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
#[cfg(wt_loom)]
use loom::sync::{Mutex, MutexGuard};
#[cfg(not(wt_loom))]
use parking_lot::{Mutex, MutexGuard};
use std::collections::VecDeque;
use std::sync::Arc;
#[cfg(not(wt_loom))]
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use crate::mem_stat::MemStat;
#[cfg(not(wt_loom))]
use crate::util::epoch::EpochDomain;

/// Most retired partitions one opportunistic collect frees inline.
#[cfg(not(wt_loom))]
const COLLECT_BATCH_LIMIT: usize = 64;

/// Slots per chunk. A chunk of 1024 pointers is 8 KB, so an empty partition
/// set costs one spine and nothing else until a partition is created.
#[cfg(not(wt_loom))]
pub const CHUNK: usize = 1024;
/// Loom explores every interleaving, so the spine is shrunk to keep the state
/// space tractable. The protocol under test is identical.
#[cfg(wt_loom)]
pub const CHUNK: usize = 2;

/// Chunks in the spine. 65,536 partitions is well beyond what a resident
/// partition set can afford: at the measured 15.7 KB floor for a small
/// in-memory table that is already 1 GB.
#[cfg(not(wt_loom))]
pub const MAX_CHUNKS: usize = 64;
#[cfg(wt_loom)]
pub const MAX_CHUNKS: usize = 2;

/// Largest routable key.
pub const MAX_PARTITIONS: usize = CHUNK * MAX_CHUNKS;

/// A chunk of slots. A null slot is empty; a non-null slot holds exactly one
/// owned strong reference, as produced by `Arc::into_raw`.
struct Chunk<T> {
    slots: [AtomicPtr<T>; CHUNK],
}

impl<T> Chunk<T> {
    fn empty() -> Box<Self> {
        Box::new(Chunk {
            slots: std::array::from_fn(|_| AtomicPtr::new(std::ptr::null_mut())),
        })
    }
}

/// Reconstruct an owned handle from a slot's pointer without ever letting the
/// strong count reach zero.
///
/// # Safety
///
/// `p` must be non-null and must point at a live `Arc` allocation. The caller
/// must hold an epoch pin taken before the slot was loaded (or otherwise
/// exclude reclamation): the retire discipline in [`PartitionSet::remove`]
/// keeps any pointer published into a slot alive for as long as a pin from
/// before its retirement exists.
#[inline]
unsafe fn revive<T>(p: *mut T) -> Arc<T> {
    unsafe {
        Arc::increment_strong_count(p as *const T);
        Arc::from_raw(p as *const T)
    }
}

/// A borrowed view of one partition, valid while it is held.
///
/// Holds an epoch pin alongside the borrow, so a concurrent `remove` of this
/// partition cannot have its grace period expire (and therefore cannot free
/// the table) while the reference is alive. Costs no atomic read-modify-write
/// on the table's strong count; see [`PartitionSet::partition_ref`].
pub struct PartRef<'a, T> {
    #[cfg(not(wt_loom))]
    _pin: crate::util::epoch::Guard<'a>,
    value: &'a T,
}

impl<T> std::ops::Deref for PartRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.value
    }
}

/// A set of table instances routed by an unsigned integer key.
pub struct PartitionSet<T> {
    spine: Vec<AtomicPtr<Chunk<T>>>,
    live: AtomicUsize,
    /// Serialises chunk allocation, creation and removal, and owns the retire
    /// queue of removed partitions (in removal order). Never taken on a read
    /// path.
    grow: Mutex<VecDeque<Arc<T>>>,
    /// Grace periods for slot readers. Owned by this set: pins here never
    /// interact with any table's own read guards.
    #[cfg(not(wt_loom))]
    epoch: EpochDomain,
    /// How many queued removals' grace periods have expired; consumed
    /// front-of-queue by [`PartitionSet::collect`]. Shared with the deferred
    /// markers through an `Arc` so a marker outliving the set stays sound.
    #[cfg(not(wt_loom))]
    reclaimable: Arc<AtomicUsize>,
}

impl<T> Default for PartitionSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> PartitionSet<T> {
    pub fn new() -> Self {
        Self {
            spine: (0..MAX_CHUNKS).map(|_| AtomicPtr::new(std::ptr::null_mut())).collect(),
            live: AtomicUsize::new(0),
            grow: Mutex::new(VecDeque::new()),
            #[cfg(not(wt_loom))]
            epoch: EpochDomain::new(),
            #[cfg(not(wt_loom))]
            reclaimable: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Pin this set's epoch domain for the duration of one raw slot access.
    #[cfg(not(wt_loom))]
    #[inline]
    fn read_pin(&self) -> crate::util::epoch::Guard<'_> {
        self.epoch.pin()
    }

    /// Loom builds compile the epoch machinery out; retired partitions are
    /// then freed only by [`PartitionSet::gc`], whose `&mut self` is the
    /// grace proof, exactly as before the epoch scheme.
    #[cfg(wt_loom)]
    #[inline]
    fn read_pin(&self) {}

    /// Number of partitions that currently hold a table.
    pub fn len(&self) -> usize {
        self.live.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// A key that is in range, or `None`. Every entry point funnels through
    /// this so the bound is stated once.
    #[inline]
    fn index(key: u64) -> Option<usize> {
        let idx = usize::try_from(key).ok()?;
        (idx < MAX_PARTITIONS).then_some(idx)
    }

    #[inline]
    fn chunk(&self, idx: usize) -> Option<&Chunk<T>> {
        let p = self.spine.get(idx / CHUNK)?.load(Ordering::Acquire);
        // Safety: a chunk pointer is published with Release after the chunk is
        // fully initialised, and is never freed or replaced while the set
        // lives, so an Acquire load yields a pointer that stays valid for as
        // long as `&self`.
        (!p.is_null()).then(|| unsafe { &*p })
    }

    /// The table routed to by `key`, if that partition exists.
    ///
    /// This is the hot path: a bounds check, an epoch pin (thread-local), two
    /// atomic loads and a strong count increment.
    #[inline]
    pub fn partition(&self, key: u64) -> Option<Arc<T>> {
        let idx = Self::index(key)?;
        let _pin = self.read_pin();
        let p = self.chunk(idx)?.slots[idx % CHUNK].load(Ordering::Acquire);
        // Safety: the pin was taken before the slot load, so a concurrent
        // removal's grace period cannot expire (and the retire queue cannot
        // drop its strong reference) until after `_pin` is released; the
        // strong count cannot reach zero between the load and the increment.
        (!p.is_null()).then(|| unsafe { revive(p) })
    }

    /// The table routed to by `key`, borrowed rather than reference counted.
    ///
    /// [`Self::partition`] costs two atomic read-modify-writes per call: one to
    /// revive the `Arc` and one to drop it. On a shared key those contend, so a
    /// per-tick lookup pays coherence traffic on the table's strong count. This
    /// returns a pinned borrow instead: no atomics beyond the acquire on the
    /// slot are shared with other readers of the same partition.
    ///
    /// Prefer this on a hot path. Prefer [`Self::partition`] when the handle
    /// has to outlive the borrow, be sent to another thread, or be stored.
    /// Holding a [`PartRef`] delays reclamation of everything retired after
    /// it was taken (in this set's domain), so bound its lifetime to the
    /// access, not to a tick loop.
    #[inline]
    pub fn partition_ref(&self, key: u64) -> Option<PartRef<'_, T>> {
        let idx = Self::index(key)?;
        #[cfg(not(wt_loom))]
        let pin = self.read_pin();
        let p = self.chunk(idx)?.slots[idx % CHUNK].load(Ordering::Acquire);
        // Safety: the pin taken above (held inside the returned `PartRef`)
        // blocks grace expiry for any removal of this partition that the
        // slot load could have raced with, and `Drop` takes `&mut self`, so
        // the allocation outlives the borrow.
        (!p.is_null()).then(|| PartRef {
            #[cfg(not(wt_loom))]
            _pin: pin,
            value: unsafe { &*p },
        })
    }

    /// Pin once, then look up many times.
    ///
    /// [`Self::partition_ref`] pins per call, and a pin ends in a `SeqCst`
    /// fence that the slot loads immediately after it must wait on. Measured
    /// on an M4 Max, that is the whole difference between a 0.71 ns lookup and
    /// a 3.4 ns one, and it is the same cost for every reclamation scheme:
    /// `crossbeam-epoch` and `ps-reclaim` are within noise of each other here,
    /// because neither can avoid the fence.
    ///
    /// So a tick loop should not pin per lookup. Pin once, read many:
    ///
    /// ```ignore
    /// let pinned = prices.pinned();
    /// for tick in batch {
    ///     if let Some(book) = pinned.get(tick.symbol_id) {
    ///         book.insert(tick.into())?;
    ///     }
    /// }
    /// ```
    ///
    /// The pin is held for the whole scope, so nothing retired during it is
    /// reclaimed until it drops. That is the trade: hold it for a batch, not
    /// for a session.
    #[inline]
    pub fn pinned(&self) -> Pinned<'_, T> {
        Pinned {
            set: self,
            #[cfg(not(wt_loom))]
            _pin: self.read_pin(),
        }
    }

    /// Whether `key` currently holds a partition.
    pub fn contains(&self, key: u64) -> bool {
        let Some(idx) = Self::index(key) else {
            return false;
        };
        self.chunk(idx)
            .is_some_and(|c| !c.slots[idx % CHUNK].load(Ordering::Acquire).is_null())
    }

    /// Keys that currently hold a partition, ascending.
    pub fn keys(&self) -> Vec<u64> {
        let mut out = Vec::with_capacity(self.len());
        for c in 0..MAX_CHUNKS {
            let Some(chunk) = self.chunk(c * CHUNK) else {
                continue;
            };
            for (i, slot) in chunk.slots.iter().enumerate() {
                if !slot.load(Ordering::Acquire).is_null() {
                    out.push((c * CHUNK + i) as u64);
                }
            }
        }
        out
    }

    /// Visit every live partition once, ascending by key.
    ///
    /// [`Self::iter`] costs a spine scan for `keys`, a `Vec`, a second lookup
    /// per key, a refcount round trip per partition, and a second `Vec`. This
    /// scans each slot once, allocates nothing, and hands out a borrow. Use it
    /// for accounting and telemetry; use `iter` when the handles must outlive
    /// the call.
    ///
    /// Not a snapshot: a partition created or removed during the walk may or
    /// may not be visited. Accounting reads are approximate under concurrent
    /// mutation either way.
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(u64, &T),
    {
        // One pin for the whole walk: deferred work in this domain is only
        // grace markers, so nothing heavy runs inside the pin, and no
        // partition observed below can be freed until the walk ends.
        let _pin = self.read_pin();
        for c in 0..MAX_CHUNKS {
            let Some(chunk) = self.chunk(c * CHUNK) else {
                continue;
            };
            for (i, slot) in chunk.slots.iter().enumerate() {
                let p = slot.load(Ordering::Acquire);
                if !p.is_null() {
                    // Safety: as `partition_ref`: `_pin` predates the load.
                    f((c * CHUNK + i) as u64, unsafe { &*p });
                }
            }
        }
    }

    /// Visit every partition that has been removed but not yet reclaimed.
    ///
    /// These are still resident and still cost memory. Reporting only live
    /// partitions makes a total *fall* after a removal that freed nothing.
    pub fn for_each_retired<F>(&self, mut f: F)
    where
        F: FnMut(&T),
    {
        for table in self.lock().iter() {
            f(table);
        }
    }

    /// Every live partition, paired with its key.
    ///
    /// A snapshot, not a view: a key removed after its slot was scanned is
    /// dropped from the result rather than reported as present.
    pub fn iter(&self) -> Vec<(u64, Arc<T>)> {
        self.keys()
            .into_iter()
            .filter_map(|k| self.partition(k).map(|t| (k, t)))
            .collect()
    }

    /// The growth lock. Infallible: `parking_lot` does not poison, so a panic
    /// in a caller-supplied `make` unwinds and releases the lock instead of
    /// disabling every later creation and removal for the life of the process.
    #[cfg(not(wt_loom))]
    fn lock(&self) -> MutexGuard<'_, VecDeque<Arc<T>>> {
        self.grow.lock()
    }

    #[cfg(wt_loom)]
    fn lock(&self) -> MutexGuard<'_, VecDeque<Arc<T>>> {
        self.grow.lock().expect("loom mutexes do not poison in these models")
    }

    /// Get the partition at `key`, creating it with `make` if absent.
    ///
    /// `make` runs under the growth mutex and only when the slot is empty, so
    /// two threads racing on the same new key create one table, not two.
    pub fn get_or_create<F>(&self, key: u64, make: F) -> Result<Arc<T>, PartitionError>
    where
        F: FnOnce() -> T,
    {
        if let Some(t) = self.partition(key) {
            return Ok(t);
        }
        // A mutation is the natural place to catch up on reclamation.
        #[cfg(not(wt_loom))]
        self.collect();
        let idx = Self::index(key).ok_or(PartitionError::OutOfRange { key })?;

        let _guard = self.lock();
        // Re-check: another thread may have created it while we waited.
        if let Some(t) = self.partition(key) {
            return Ok(t);
        }

        let cell = &self.spine[idx / CHUNK];
        if cell.load(Ordering::Acquire).is_null() {
            // Published only after the chunk is fully initialised.
            cell.store(Box::into_raw(Chunk::<T>::empty()), Ordering::Release);
        }
        // Safety: published above or on an earlier call, and never freed while
        // the set lives.
        let chunk = unsafe { &*cell.load(Ordering::Acquire) };

        let table = Arc::new(make());
        // The slot takes ownership of one strong reference.
        let raw = Arc::into_raw(Arc::clone(&table)) as *mut T;
        chunk.slots[idx % CHUNK].store(raw, Ordering::Release);
        self.live.fetch_add(1, Ordering::AcqRel);
        Ok(table)
    }

    /// Remove the partition at `key`, returning it if it was present.
    ///
    /// The reference the slot owned is moved to the retire queue rather than
    /// dropped, so a reader that loaded the pointer a moment before cannot
    /// find the allocation freed under it. A deferred epoch marker records
    /// when every such reader has finished; [`Self::collect`] (called here
    /// and from [`Self::get_or_create`]) then frees the expired prefix, so
    /// removal through the shared production router reclaims memory instead
    /// of leaking every removed table.
    pub fn remove(&self, key: u64) -> Option<Arc<T>> {
        let idx = Self::index(key)?;
        let table = {
            let mut retired = self.lock();
            let chunk = self.chunk(idx)?;
            let p = chunk.slots[idx % CHUNK].swap(std::ptr::null_mut(), Ordering::AcqRel);
            if p.is_null() {
                return None;
            }
            self.live.fetch_sub(1, Ordering::AcqRel);
            // Safety: the slot owned exactly one strong reference, and we
            // hold the mutex so no other writer can have taken it.
            let table = unsafe { Arc::from_raw(p as *const T) };
            retired.push_back(Arc::clone(&table));
            // Defer the grace marker while still holding the lock, so marker
            // order matches queue order and the executed-marker count always
            // releases a correct prefix.
            #[cfg(not(wt_loom))]
            {
                let reclaimable = Arc::clone(&self.reclaimable);
                let guard = self.epoch.pin();
                guard.retire(move || {
                    reclaimable.fetch_add(1, Ordering::Release);
                });
                drop(guard);
                self.epoch.advance();
            }
            table
        };
        #[cfg(not(wt_loom))]
        self.collect();
        Some(table)
    }

    /// Free retired partitions whose grace period has expired, through
    /// `&self`.
    ///
    /// Returns how many were freed. Never waits for readers: a partition
    /// whose removal a still-pinned reader could have raced simply stays
    /// queued for a later call. Bounded per call, so no mutating caller
    /// absorbs an unbounded backlog inline. Called opportunistically by
    /// [`Self::remove`] and [`Self::get_or_create`]; long-lived routers that
    /// only remove may call it directly.
    #[cfg(not(wt_loom))]
    pub fn collect(&self) -> usize {
        for _ in 0..4 {
            if self.reclaimable.load(Ordering::Acquire) != 0 {
                break;
            }
            self.epoch.advance();
        }
        let claimed = self.reclaimable.swap(0, Ordering::AcqRel);
        if claimed == 0 {
            return 0;
        }
        let freed: Vec<Arc<T>> = {
            let mut retired = self.lock();
            let take = claimed.min(COLLECT_BATCH_LIMIT).min(retired.len());
            if claimed > take {
                self.reclaimable.fetch_add(claimed - take, Ordering::Release);
            }
            retired.drain(..take).collect()
        };
        // Dropping whole tables can be arbitrarily heavy; do it outside the
        // growth lock.
        let n = freed.len();
        drop(freed);
        n
    }

    /// Exhaustively free the retire queue, driving the epoch as needed.
    ///
    /// Returns how many were reclaimed. `&mut self` guarantees no reader of
    /// this set is pinned (every pin lives inside a `&self` call or a
    /// [`PartRef`] borrow), so every queued removal's grace can expire; this
    /// call drives the epoch until the queue is empty. Kept for callers that
    /// do hold exclusive access and want deterministic, complete
    /// reclamation; the shared-router path reclaims through
    /// [`Self::collect`] and does not need this.
    #[cfg(not(wt_loom))]
    pub fn gc(&mut self) -> usize {
        let mut total = 0;
        // Each round advances the epoch at least one step when nothing is
        // claimable yet; a marker needs three steps from defer to execution,
        // and the batch limit needs queue_len / COLLECT_BATCH_LIMIT rounds.
        for _ in 0..64 {
            total += self.collect();
            if self.lock().is_empty() {
                break;
            }
            self.epoch.advance();
        }
        total
    }

    /// Loom builds have no epoch machinery: exclusive access alone is the
    /// grace proof, exactly as before the epoch scheme.
    #[cfg(wt_loom)]
    pub fn gc(&mut self) -> usize {
        let mut retired = self.lock();
        let n = retired.len();
        retired.clear();
        n
    }

    /// How many removed partitions are still held by the retire list.
    pub fn retired_len(&self) -> usize {
        self.lock().len()
    }
}

impl<T> std::fmt::Debug for PartitionSet<T> {
    /// Deliberately shallow: a partition set can hold thousands of tables and
    /// printing them would be useless as well as slow.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionSet")
            .field("live", &self.len())
            .field("table", &std::any::type_name::<T>())
            .finish()
    }
}

impl<T> Drop for PartitionSet<T> {
    fn drop(&mut self) {
        for cell in &self.spine {
            let p = cell.swap(std::ptr::null_mut(), Ordering::AcqRel);
            if p.is_null() {
                continue;
            }
            // Safety: each chunk was allocated by Box::into_raw here, is
            // published exactly once, and nothing else frees it.
            let chunk = unsafe { Box::from_raw(p) };
            for slot in chunk.slots.iter() {
                let sp = slot.swap(std::ptr::null_mut(), Ordering::AcqRel);
                if !sp.is_null() {
                    // Safety: a live slot owns one strong reference.
                    drop(unsafe { Arc::from_raw(sp as *const T) });
                }
            }
        }
    }
}

// Safety: every slot access is atomic, chunk contents are only mutated under
// `grow`, and a published chunk is never freed or replaced while the set is
// alive. The bound is stated explicitly because `AtomicPtr<T>` is `Send` and
// `Sync` for any `T`, which would otherwise be too permissive.
unsafe impl<T: Send + Sync> Send for PartitionSet<T> {}
unsafe impl<T: Send + Sync> Sync for PartitionSet<T> {}

impl<T: MemStat> PartitionSet<T> {
    /// Memory held across every live partition.
    pub fn mem_stat_total(&self) -> usize {
        self.iter().into_iter().map(|(_, t)| t.used_size()).sum()
    }

    /// Memory held per partition, so a caller can answer which key is costing
    /// them rather than only how much is held in total.
    pub fn mem_stat_by_key(&self) -> Vec<(u64, usize)> {
        self.iter().into_iter().map(|(k, t)| (k, t.used_size())).collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionError {
    /// The key exceeds [`MAX_PARTITIONS`].
    OutOfRange { key: u64 },
}

impl std::fmt::Display for PartitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartitionError::OutOfRange { key } => {
                write!(f, "partition key {key} exceeds the maximum of {MAX_PARTITIONS}")
            }
        }
    }
}

impl std::error::Error for PartitionError {}

#[cfg(all(test, not(wt_loom)))]
mod tests;

#[cfg(all(test, wt_loom))]
mod loom_tests;

/// A held pin plus the set it pins, so lookups inside it cost no fence.
///
/// See [`PartitionSet::pinned`]. Not `Send`: a pin belongs to the thread that
/// took it.
pub struct Pinned<'a, T> {
    set: &'a PartitionSet<T>,
    #[cfg(not(wt_loom))]
    _pin: crate::util::epoch::Guard<'a>,
}

impl<T> Pinned<'_, T> {
    /// The table routed to by `key`, if that partition exists.
    ///
    /// Three dependent loads and nothing else: the pin was paid once when this
    /// was created.
    #[inline]
    pub fn get(&self, key: u64) -> Option<&T> {
        let idx = PartitionSet::<T>::index(key)?;
        let p = self.set.chunk(idx)?.slots[idx % CHUNK].load(Ordering::Acquire);
        // Safety: this borrow cannot outlive the pin held by `self`, which
        // blocks grace expiry for anything retired after it was taken.
        (!p.is_null()).then(|| unsafe { &*p })
    }

    /// Whether `key` currently holds a partition.
    #[inline]
    pub fn contains(&self, key: u64) -> bool {
        self.get(key).is_some()
    }
}
