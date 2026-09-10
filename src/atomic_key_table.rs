//! A fixed-capacity table whose rows are claimed without blocking and updated
//! in place.
//!
//! # What this is for, and why the other two storages cannot do it
//!
//! A `storage: vec` table takes `&mut self` to insert, because it pushes onto a
//! `Vec` and a push can reallocate, which moves every row. That is the right
//! shape for a single writer and unusable from several threads at once. A
//! `storage: paged` table is usable from several threads and buys that with an
//! archived row, links into pages, a row-level lock map and change-data-capture.
//!
//! The case that needs neither is a **counter table**: many writers, a small set
//! of keys that stabilises almost immediately, and an update that is a
//! read-modify-write on the row rather than a replacement of it. Performance
//! measurement is the archetype, sixteen workers recording timings against a
//! handful of named sites.
//!
//! # How it avoids both a lock and a reallocation
//!
//! Capacity is fixed at construction and every value is built then, so **no row
//! ever moves** and a `&V` handed out stays valid for the life of the table. A
//! key is claimed with one `compare_exchange`; after that, finding it is a plain
//! load. The value is updated through `&V`, so `V` supplies its own interior
//! mutability, and this module takes no position on what a row contains.
//!
//! The load-before-claim order is the point. Claiming with a `compare_exchange`
//! on every lookup takes the cache line exclusively even when nothing changes,
//! so writers contending for a key they all already own would serialise on it.
//! A relaxed load is a shared read.
//!
//! # What it does not do
//!
//! No removal, no resize, and no iteration order beyond slot order. A full table
//! refuses rather than growing, and [`AtomicKeyTable::len`] says how many slots
//! are taken so a caller can see it coming.
//!
//! Ported from `worktable-vec`, where it was written and where its own tests
//! still live.

use alloc::vec::Vec;
use core::sync::atomic::{AtomicUsize, Ordering};

// A 64-bit `usize`, and it refuses rather than assuming one. `GOLDEN` below is
// a 64-bit constant, and truncating it to 32 bits leaves an even number, which
// is not invertible and quietly collapses keys onto the same slot. Nothing here
// is built or tested for a narrower target, so the honest answer is to say so
// at compile time instead of carrying a second constant nobody exercises.
#[cfg(not(target_pointer_width = "64"))]
compile_error!("`storage: atomic` requires a 64-bit target");

/// Scatter a key across the table.
///
/// # Why not the low bits, and why not a modulo
///
/// The first version of this in `worktable-vec` shifted the key right by four
/// and took it modulo the capacity, which is wrong twice. The shift assumed a
/// pointer key, whose low bits are alignment zeros; handed small integers it
/// maps every key under sixteen to slot zero, and a measured lookup over
/// sixty-four sequential keys walked a probe chain 9.3x slower than a linear
/// scan of the same rows. The modulo is an integer division on the hottest path.
///
/// Fibonacci hashing fixes the first: multiplying by the golden ratio spreads
/// any input across the whole word, and taking the **high** bits reads that
/// spread. A power-of-two capacity fixes the second: the index is then a mask.
#[inline(always)]
const fn scatter(key: usize, shift: u32, mask: usize) -> usize {
    // 2^64 / phi, odd so the multiply is invertible and no input is lost.
    const GOLDEN: usize = 0x9E37_79B9_7F4A_7C15u64 as usize;
    (key.wrapping_mul(GOLDEN) >> shift) & mask
}

/// Open-addressed slots with linear probing, sized once and never resized.
#[derive(Debug)]
pub struct AtomicKeyTable<V> {
    keys: Vec<AtomicUsize>,
    values: Vec<V>,
    /// Capacity is a power of two, so the index is a mask rather than a division.
    mask: usize,
    shift: u32,
}

impl<V: Default> AtomicKeyTable<V> {
    /// A table with at least `capacity` slots, every value built now.
    ///
    /// Rounded up to a power of two so the slot index is a mask rather than a
    /// division. Size it generously: this is open addressed with linear
    /// probing, so a table much past half full costs a long probe on every miss.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let slots = capacity.max(1).next_power_of_two();
        let mut keys = Vec::with_capacity(slots);
        let mut values = Vec::with_capacity(slots);
        for _ in 0..slots {
            keys.push(AtomicUsize::new(0));
            values.push(V::default());
        }
        Self {
            keys,
            values,
            mask: slots - 1,
            shift: usize::BITS - slots.trailing_zeros(),
        }
    }
}

impl<V> AtomicKeyTable<V> {
    /// The row for this key, claiming a slot if it has none yet.
    ///
    /// It returns the existing row or creates one, and never replaces what is
    /// there. The row is then updated through `&V`, which is where this differs
    /// from a paged `upsert`: the value carries its own interior mutability
    /// rather than being written back whole.
    ///
    /// `None` means the table is full. Zero is the empty sentinel and is
    /// rejected rather than silently colliding with an unclaimed slot.
    pub fn upsert(&self, key: usize) -> Option<&V> {
        if key == 0 || self.keys.is_empty() {
            return None;
        }
        let capacity = self.keys.len();
        let mut at = scatter(key, self.shift, self.mask);
        for _ in 0..capacity {
            match self.keys[at].load(Ordering::Acquire) {
                existing if existing == key => return Some(&self.values[at]),
                0 => match self.keys[at].compare_exchange(0, key, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => return Some(&self.values[at]),
                    Err(taken) if taken == key => return Some(&self.values[at]),
                    Err(_) => at = (at + 1) & self.mask,
                },
                _ => at = (at + 1) & self.mask,
            }
        }
        None
    }

    /// The row for this key, or `None` if no row has been created for it.
    /// Never creates one.
    pub fn select(&self, key: usize) -> Option<&V> {
        if key == 0 || self.keys.is_empty() {
            return None;
        }
        let capacity = self.keys.len();
        let mut at = scatter(key, self.shift, self.mask);
        for _ in 0..capacity {
            match self.keys[at].load(Ordering::Acquire) {
                existing if existing == key => return Some(&self.values[at]),
                0 => return None,
                _ => at = (at + 1) & self.mask,
            }
        }
        None
    }

    /// Every claimed row, in slot order.
    pub fn iter(&self) -> impl Iterator<Item = (usize, &V)> {
        self.keys
            .iter()
            .zip(self.values.iter())
            .filter_map(|(k, v)| match k.load(Ordering::Acquire) {
                0 => None,
                key => Some((key, v)),
            })
    }

    /// How many rows the table holds.
    ///
    /// A walk of every slot, not a counter. Claiming is a `compare_exchange` on
    /// one slot and nothing else, and a shared counter beside it would put back
    /// the contended line the design exists to avoid.
    #[must_use]
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Whether any row has been created.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many rows the table can hold. Fixed at construction.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.keys.len()
    }
}
