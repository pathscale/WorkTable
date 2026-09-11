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
//! # There is no row snapshot, deliberately
//!
//! A row is `&V` and `V` supplies its own interior mutability, so a reader that
//! wants two fields reads two atomics and there is no instant at which it held
//! both. A count of 10 beside a total of 900 can be observed even though no
//! writer ever left the row in that state.
//!
//! That is accepted rather than fixed. The alternatives are a sequence lock or
//! a lock per row, and both put back the contended cache line this type exists
//! to avoid: the whole point of claiming a slot once and then never touching
//! the key again is that a hot row is a shared read.
//!
//! **If you need two values to agree, pack them into one atomic.** Two `u32`
//! counters in an `AtomicU64` are updated with one `fetch_add` of
//! `1 << 32 | delta` and read with one load, and they are then exactly as
//! consistent as each other. That is the supported answer, and it is enough for
//! the case this exists for: a count and a total.
//!
//! # What it does not do
//!
//! No removal, no resize, and no iteration order beyond slot order. A full table
//! refuses rather than growing, and [`AtomicKeyTable::len`] says how many slots
//! are taken so a caller can see it coming.
//!
//! Ported from `worktable-vec`, which this supersedes. That crate is deprecated
//! and this was the last thing in it that lived nowhere else.

use alloc::vec::Vec;
use core::sync::atomic::{AtomicUsize, Ordering};

// A 64-bit `usize`, and it refuses rather than assuming one. `GOLDEN` below is
// a 64-bit constant, and truncating it to 32 bits leaves an even number, which
// is not invertible and quietly collapses keys onto the same slot. Nothing here
// is built or tested for a narrower target, so the honest answer is to say so
// at compile time instead of carrying a second constant nobody exercises.
#[cfg(not(target_pointer_width = "64"))]
compile_error!("`AtomicKeyTable` requires a 64-bit target");

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

#[cfg(test)]
mod tests {
    use core::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    #[derive(Default)]
    struct Counter(AtomicU64);

    #[test]
    fn a_claimed_row_is_found_by_a_plain_load_and_never_reclaimed() {
        let table: AtomicKeyTable<Counter> = AtomicKeyTable::with_capacity(64);
        let first = table.upsert(7).expect("capacity");
        first.0.fetch_add(1, Ordering::Relaxed);
        let again = table.upsert(7).expect("already claimed");
        again.0.fetch_add(1, Ordering::Relaxed);
        assert_eq!(again.0.load(Ordering::Relaxed), 2, "the second call found the same row");
        assert_eq!(table.len(), 1, "one key claimed one slot");
    }

    #[test]
    fn zero_is_the_empty_sentinel_and_is_refused_rather_than_colliding() {
        let table: AtomicKeyTable<Counter> = AtomicKeyTable::with_capacity(8);
        assert!(table.upsert(0).is_none(), "zero would be indistinguishable from empty");
        assert!(table.select(0).is_none());
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn a_full_table_refuses_rather_than_growing() {
        let table: AtomicKeyTable<Counter> = AtomicKeyTable::with_capacity(4);
        for key in 1..=4 {
            assert!(table.upsert(key).is_some(), "slot {key} fits");
        }
        assert_eq!(table.len(), 4);
        assert!(table.upsert(5).is_none(), "the fifth has nowhere to go");
        assert!(table.upsert(3).is_some(), "a claimed key is still reachable when full");
    }

    #[test]
    fn select_never_creates_a_row() {
        let table: AtomicKeyTable<Counter> = AtomicKeyTable::with_capacity(8);
        assert!(table.select(9).is_none());
        assert_eq!(table.len(), 0, "select must not take a slot");
        table.upsert(9).expect("capacity");
        assert!(table.select(9).is_some());
    }

    #[test]
    fn every_claimed_row_is_iterated_and_no_empty_one_is() {
        let table: AtomicKeyTable<Counter> = AtomicKeyTable::with_capacity(32);
        for key in [11usize, 22, 33] {
            table
                .upsert(key)
                .expect("capacity")
                .0
                .store(key as u64, Ordering::Relaxed);
        }
        let mut seen: Vec<(usize, u64)> = table.iter().map(|(k, v)| (k, v.0.load(Ordering::Relaxed))).collect();
        seen.sort_unstable();
        assert_eq!(seen, alloc::vec![(11usize, 11u64), (22, 22), (33, 33)]);
    }

    /// Small sequential keys must not all land in one slot.
    ///
    /// The regression `scatter` exists for. The first version of this, in
    /// `worktable-vec`, shifted the key right by four and took it modulo the
    /// capacity. The shift assumes a pointer key whose low bits are alignment
    /// zeros; handed small integers it maps **every key under sixteen onto slot
    /// zero**, and a measured lookup over sixty-four sequential keys ran 9.3x
    /// slower than a linear scan of the same rows.
    ///
    /// Asserted on the slot distribution rather than through the public API,
    /// because the public API cannot tell the difference: every key is findable
    /// either way, and what breaks is only how far each lookup walks. This
    /// module's own test can see the private index, so it checks the thing that
    /// actually went wrong.
    #[test]
    fn small_sequential_keys_land_on_distinct_slots() {
        let table: AtomicKeyTable<Counter> = AtomicKeyTable::with_capacity(256);

        let mut slots: Vec<usize> = (1..=64usize).map(|key| scatter(key, table.shift, table.mask)).collect();
        slots.sort_unstable();
        slots.dedup();

        // 64 keys into 256 slots: by the birthday bound a good scatter leaves
        // roughly 57 distinct, and the broken one leaves exactly 1. Anything
        // above half is unambiguously the former.
        assert!(
            slots.len() > 32,
            "64 sequential keys landed on only {} distinct slots of 256; this is the \
             low-bits regression",
            slots.len()
        );

        // And the keys the caller would actually use still all resolve.
        for key in 1..=64usize {
            table.upsert(key).expect("capacity");
        }
        assert_eq!(table.len(), 64);
        for key in 1..=64usize {
            assert!(table.select(key).is_some(), "key {key} went missing");
        }
        for key in 65..=128usize {
            assert!(table.select(key).is_none(), "key {key} was never claimed");
        }
    }

    #[test]
    fn concurrent_writers_agree_on_one_row_per_key() {
        extern crate std;
        let table: AtomicKeyTable<Counter> = AtomicKeyTable::with_capacity(512);
        let shared = &table;
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(move || {
                    for round in 0..1_000usize {
                        let key = (round % 16) + 1;
                        shared.upsert(key).expect("capacity").0.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        assert_eq!(
            table.len(),
            16,
            "sixteen keys, sixteen slots, whatever the interleaving"
        );
        let total: u64 = table.iter().map(|(_, v)| v.0.load(Ordering::Relaxed)).sum();
        assert_eq!(total, 8 * 1_000, "no update was lost and none was double counted");
    }

    /// The documented way to make two values agree: pack them into one atomic.
    ///
    /// There is no row snapshot and there will not be one, so this is the
    /// supported answer and it is worth having a worked example of it in the
    /// tests rather than only in prose.
    #[test]
    fn two_values_packed_into_one_atomic_stay_consistent() {
        extern crate std;

        /// Count in the high 32 bits, total in the low 32.
        #[derive(Default)]
        struct CountAndTotal(AtomicU64);

        impl CountAndTotal {
            fn record(&self, value: u32) {
                self.0.fetch_add((1u64 << 32) | u64::from(value), Ordering::Relaxed);
            }

            /// One load, so the pair is exactly as consistent as each other.
            fn read(&self) -> (u32, u32) {
                let packed = self.0.load(Ordering::Relaxed);
                ((packed >> 32) as u32, packed as u32)
            }
        }

        let table: AtomicKeyTable<CountAndTotal> = AtomicKeyTable::with_capacity(64);
        let shared = &table;
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(move || {
                    for _ in 0..500 {
                        shared.upsert(1).expect("capacity").record(3);
                    }
                });
            }
        });

        let (count, total) = table.select(1).expect("claimed").read();
        assert_eq!(count, 4_000);
        assert_eq!(total, 12_000);
        assert_eq!(total, count * 3, "the pair was never observed disagreeing");
    }
}
