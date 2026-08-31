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
//! retire list that keeps the allocation alive for the whole life of the set,
//! which closes the window without putting anything on the read path.
//! [`PartitionSet::gc`] reclaims the list, and takes `&mut self` because that
//! is the proof that no reader is in flight.
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
use std::sync::Arc;
#[cfg(not(wt_loom))]
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use crate::mem_stat::MemStat;

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
/// `p` must be non-null and must point at a live `Arc` allocation, which the
/// retire-list discipline in [`PartitionSet::remove`] guarantees for any
/// pointer ever published into a slot.
#[inline]
unsafe fn revive<T>(p: *mut T) -> Arc<T> {
    unsafe {
        Arc::increment_strong_count(p as *const T);
        Arc::from_raw(p as *const T)
    }
}

/// A set of table instances routed by an unsigned integer key.
pub struct PartitionSet<T> {
    spine: Vec<AtomicPtr<Chunk<T>>>,
    live: AtomicUsize,
    /// Serialises chunk allocation, creation and removal, and owns the retire
    /// list of removed partitions. Never taken on a read path.
    grow: Mutex<Vec<Arc<T>>>,
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
            grow: Mutex::new(Vec::new()),
        }
    }

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
    /// This is the hot path: a bounds check, two atomic loads and a strong
    /// count increment.
    #[inline]
    pub fn partition(&self, key: u64) -> Option<Arc<T>> {
        let idx = Self::index(key)?;
        let p = self.chunk(idx)?.slots[idx % CHUNK].load(Ordering::Acquire);
        // Safety: a published slot pointer refers to an allocation that the
        // retire list keeps alive for as long as the set lives, so the strong
        // count cannot have reached zero between the load and the increment.
        (!p.is_null()).then(|| unsafe { revive(p) })
    }

    /// The table routed to by `key`, borrowed rather than reference counted.
    ///
    /// [`Self::partition`] costs two atomic read-modify-writes per call: one to
    /// revive the `Arc` and one to drop it. On a shared key those contend, so a
    /// per-tick lookup pays coherence traffic on the table's strong count. This
    /// returns a borrow instead and costs three dependent loads and no atomics
    /// beyond the acquire on the slot.
    ///
    /// Prefer this on a hot path. Prefer [`Self::partition`] when the handle
    /// has to outlive the borrow, be sent to another thread, or be stored.
    #[inline]
    pub fn partition_ref(&self, key: u64) -> Option<&T> {
        let idx = Self::index(key)?;
        let p = self.chunk(idx)?.slots[idx % CHUNK].load(Ordering::Acquire);
        // Safety: a published slot pointer refers to an allocation that is
        // freed only by `gc` or `Drop`, both of which take `&mut self`. The
        // returned borrow is tied to `&self`, so neither can run while it is
        // alive and the allocation cannot be reclaimed under it.
        (!p.is_null()).then(|| unsafe { &*p })
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
        for c in 0..MAX_CHUNKS {
            let Some(chunk) = self.chunk(c * CHUNK) else {
                continue;
            };
            for (i, slot) in chunk.slots.iter().enumerate() {
                let p = slot.load(Ordering::Acquire);
                if !p.is_null() {
                    // Safety: as `partition_ref`. The borrow cannot outlive
                    // `&self`, and reclamation needs `&mut self`.
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
    fn lock(&self) -> MutexGuard<'_, Vec<Arc<T>>> {
        self.grow.lock()
    }

    #[cfg(wt_loom)]
    fn lock(&self) -> MutexGuard<'_, Vec<Arc<T>>> {
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
    /// The reference the slot owned is moved to the retire list rather than
    /// dropped, so a reader that loaded the pointer a moment before cannot
    /// find the allocation freed under it. [`Self::gc`] reclaims it, and `gc`
    /// needs `&mut self`, so through a shared `Arc` router this never frees
    /// anything. See [`Self::gc`] before removing on a long-lived set.
    pub fn remove(&self, key: u64) -> Option<Arc<T>> {
        let idx = Self::index(key)?;
        let mut retired = self.lock();
        let chunk = self.chunk(idx)?;
        let p = chunk.slots[idx % CHUNK].swap(std::ptr::null_mut(), Ordering::AcqRel);
        if p.is_null() {
            return None;
        }
        self.live.fetch_sub(1, Ordering::AcqRel);
        // Safety: the slot owned exactly one strong reference, and we hold the
        // mutex so no other writer can have taken it.
        let table = unsafe { Arc::from_raw(p as *const T) };
        retired.push(Arc::clone(&table));
        Some(table)
    }

    /// Drop the retire list, freeing partitions removed earlier.
    ///
    /// Returns how many were reclaimed. Takes `&mut self`: exclusive access is
    /// the proof that no reader holds a pointer this could invalidate.
    ///
    /// # This is unreachable through a shared router
    ///
    /// The production shape is `Arc<PartitionSet<T>>` shared across threads,
    /// and an `Arc` never yields `&mut`. So in that shape **nothing removed is
    /// ever freed**: every `remove` retires a whole table, measured at 15 to
    /// 110 KB, and the set grows for the life of the process. A symbol that is
    /// delisted and relisted repeatedly, or any evict-and-recreate loop, leaks
    /// at that rate.
    ///
    /// Until reclamation works through a shared handle, treat a long-lived
    /// shared router as append-only and watch [`Self::retired_len`]. Eviction
    /// is blocked on the same gap: a policy that cannot free through the shared
    /// handle does not evict.
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
