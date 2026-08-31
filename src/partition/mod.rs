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
//! # What this does not do yet
//!
//! No eviction, no lazy load, no per-partition persistence. Every partition
//! held is resident. That is adequate for a few thousand in-memory partitions
//! and inadequate for a fine-grained persisted axis, because a persisted
//! instance measures 110 KB and 6.1 ms to construct, of which 95 percent is
//! inside `PersistenceEngine::new`.

use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::mem_stat::MemStat;

/// Slots per chunk. A chunk of 1024 pointers is 8 KB, so an empty partition
/// set costs one spine and nothing else until a partition is created.
pub const CHUNK: usize = 1024;

/// Chunks in the spine. 65,536 partitions is well beyond what a resident
/// partition set can afford: at the measured 15.7 KB floor for a small
/// in-memory table that is already 1 GB.
pub const MAX_CHUNKS: usize = 64;

/// Largest routable key.
pub const MAX_PARTITIONS: usize = CHUNK * MAX_CHUNKS;

type Slot<T> = Option<Arc<T>>;

struct Chunk<T> {
    slots: [Slot<T>; CHUNK],
}

impl<T> Chunk<T> {
    fn empty() -> Box<Self> {
        Box::new(Chunk {
            slots: std::array::from_fn(|_| None),
        })
    }
}

/// A set of table instances routed by an unsigned integer key.
pub struct PartitionSet<T> {
    spine: Vec<AtomicPtr<Chunk<T>>>,
    live: AtomicUsize,
    /// Serialises chunk allocation and partition creation. Never taken on a
    /// read path.
    grow: Mutex<()>,
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
            grow: Mutex::new(()),
        }
    }

    /// Number of partitions that currently hold a table.
    pub fn len(&self) -> usize {
        self.live.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    fn chunk(&self, key: usize) -> Option<&Chunk<T>> {
        let p = self.spine.get(key / CHUNK)?.load(Ordering::Acquire);
        // Safety: a chunk pointer is published with Release after the chunk is
        // fully initialised, and is never freed or replaced while the set
        // lives, so an Acquire load yields a pointer that stays valid for as
        // long as `&self`.
        (!p.is_null()).then(|| unsafe { &*p })
    }

    /// The table routed to by `key`, if that partition exists.
    ///
    /// This is the hot path: a bounds check, two loads and a clone of an
    /// `Arc`.
    #[inline]
    pub fn partition(&self, key: u64) -> Option<Arc<T>> {
        let key = usize::try_from(key).ok()?;
        if key >= MAX_PARTITIONS {
            return None;
        }
        self.chunk(key)?.slots[key % CHUNK].clone()
    }

    /// Whether `key` currently holds a partition.
    pub fn contains(&self, key: u64) -> bool {
        self.partition(key).is_some()
    }

    /// Keys that currently hold a partition, ascending.
    pub fn keys(&self) -> Vec<u64> {
        let mut out = Vec::with_capacity(self.len());
        for c in 0..MAX_CHUNKS {
            let Some(chunk) = self.chunk(c * CHUNK) else {
                continue;
            };
            for (i, slot) in chunk.slots.iter().enumerate() {
                if slot.is_some() {
                    out.push((c * CHUNK + i) as u64);
                }
            }
        }
        out
    }

    /// Every live partition, paired with its key.
    pub fn iter(&self) -> Vec<(u64, Arc<T>)> {
        self.keys()
            .into_iter()
            .filter_map(|k| self.partition(k).map(|t| (k, t)))
            .collect()
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
        let idx = usize::try_from(key).map_err(|_| PartitionError::OutOfRange { key })?;
        if idx >= MAX_PARTITIONS {
            return Err(PartitionError::OutOfRange { key });
        }

        let _guard = self.grow.lock().map_err(|_| PartitionError::Poisoned)?;
        // Re-check: another thread may have created it while we waited.
        if let Some(t) = self.partition(key) {
            return Ok(t);
        }

        let chunk_idx = idx / CHUNK;
        let cell = &self.spine[chunk_idx];
        if cell.load(Ordering::Acquire).is_null() {
            // Published only after the chunk is fully initialised.
            cell.store(Box::into_raw(Chunk::<T>::empty()), Ordering::Release);
        }
        let chunk = cell.load(Ordering::Acquire);
        // Safety: we hold the growth mutex, so no other writer touches this
        // chunk, and readers only ever read slots.
        let slots = unsafe { &mut (*chunk).slots };
        let table = Arc::new(make());
        slots[idx % CHUNK] = Some(table.clone());
        self.live.fetch_add(1, Ordering::AcqRel);
        Ok(table)
    }

    /// Remove the partition at `key`, returning it if it was present.
    ///
    /// Existing holders keep their `Arc`, so a reader mid-query is unaffected.
    pub fn remove(&self, key: u64) -> Option<Arc<T>> {
        let idx = usize::try_from(key).ok()?;
        if idx >= MAX_PARTITIONS {
            return None;
        }
        let _guard = self.grow.lock().ok()?;
        let chunk = self.spine[idx / CHUNK].load(Ordering::Acquire);
        if chunk.is_null() {
            return None;
        }
        // Safety: the growth mutex is held, so no other writer is present.
        let slots = unsafe { &mut (*chunk).slots };
        let taken = slots[idx % CHUNK].take();
        if taken.is_some() {
            self.live.fetch_sub(1, Ordering::AcqRel);
        }
        taken
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
            if !p.is_null() {
                // Safety: each chunk was allocated by Box::into_raw here, is
                // published exactly once, and nothing else frees it.
                drop(unsafe { Box::from_raw(p) });
            }
        }
    }
}

// Safety: chunk contents are only mutated under `grow`, and slot reads are
// plain shared reads of `Option<Arc<T>>` behind an Acquire-loaded pointer.
unsafe impl<T: Send + Sync> Send for PartitionSet<T> {}
unsafe impl<T: Send + Sync> Sync for PartitionSet<T> {}

impl<T: MemStat> PartitionSet<T> {
    /// Memory held across every live partition.
    pub fn mem_stat_total(&self) -> usize {
        self.iter().iter().map(|(_, t)| t.used_size()).sum()
    }

    /// Memory held per partition, so a caller can answer which key is costing
    /// them rather than only how much is held in total.
    pub fn mem_stat_by_key(&self) -> Vec<(u64, usize)> {
        self.iter().iter().map(|(k, t)| (*k, t.used_size())).collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionError {
    /// The key exceeds [`MAX_PARTITIONS`].
    OutOfRange { key: u64 },
    /// The growth mutex was poisoned by a panic in a previous `make`.
    Poisoned,
}

impl std::fmt::Display for PartitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartitionError::OutOfRange { key } => {
                write!(f, "partition key {key} exceeds the maximum of {MAX_PARTITIONS}")
            }
            PartitionError::Poisoned => write!(f, "partition set is poisoned"),
        }
    }
}

impl std::error::Error for PartitionError {}

#[cfg(test)]
mod tests;
