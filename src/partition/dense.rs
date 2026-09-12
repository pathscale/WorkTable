//! The payload behind a narrow `partition_max_size`.
//!
//! A partition declared `partition_max_size: u8` holds at most 256 rows. At
//! that size the apparatus a full generated table carries is the entire cost:
//! an empty partition of the 832-byte-row shape web3.trading runs measures
//! 28,395 bytes, and the same partition holding three rows measures 28,459.
//! The rows are free. Everything else is fixed overhead allocated at partition
//! creation, and it is paid once per partition, so two thousand symbols pay it
//! two thousand times.
//!
//! # What a dense partition drops, and why each is safe to drop here
//!
//! - **The primary index.** Position *is* the key. A dense unsigned key in
//!   `0..cap` indexes the row vector directly, so the lookup is a bounds check
//!   and a load rather than a tree descent. This is also the largest saving:
//!   arctic holds about 600 bytes per 24-byte row at 64 rows and does not
//!   settle until a thousand, so at 23 rows the index is most of the table.
//! - **Pages, links, the free list and the epoch domain.** Rows do not move,
//!   because a row's position is its key and never changes.
//! - **The lock map.** A dense key means a lock map would be an array, and an
//!   array of locks over 23 rows is not worth the indirection. See the
//!   granularity note below.
//! - **CDC.** Nothing is persisted, so there is nothing to replay.
//!
//! # Granularity, stated rather than implied
//!
//! Writes serialise **per partition**, not per cell. The full table gives
//! cell-level serialisation through `LockMap` because its writes are async and
//! a query can hold a column across an await; nothing here is async and no
//! write spans a suspension point, so the lock is held for the duration of one
//! `insert`, `update` or `delete` and released.
//!
//! That is a coarser lock over a much smaller thing. A partition is the unit
//! of contention, and there are thousands of them: at 2,000 symbols and 10,000
//! writes a second, two writers collide only when they touch the same symbol.
//! Readers never block each other and never block on a writer they do not
//! share a partition with.
//!
//! # Memory
//!
//! The row vector grows to the highest key inserted, not to the declared cap.
//! An empty partition is one lock, one counter and an empty `Vec`: no
//! allocation at all until the first insert. A partition holding keys 0..23 of
//! an 832-byte row holds 23 slots, which is what a hand-written
//! `HashMap<Symbol, Arc<Vec<Row>>>` holds and 2.5x less than the full table.
//!
//! The declared cap is therefore a bound and not a reservation. It exists to
//! reject a key that does not belong in this partition, and to pick this shape
//! over the full table in the first place.

use alloc::vec::Vec;
use core::fmt;

#[cfg(not(wt_loom))]
use core::sync::atomic::{AtomicUsize, Ordering};
#[cfg(wt_loom)]
use loom::sync::RwLock;
#[cfg(wt_loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};
#[cfg(not(wt_loom))]
use parking_lot::RwLock;

use crate::mem_stat::MemStat;

/// Why a write to a dense partition was refused.
///
/// Both variants are programming errors rather than conditions to retry, and
/// both name the key, because a caller that hits one is looking at a key it
/// computed wrongly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DenseError {
    /// The key is at or past the declared `partition_max_size`.
    ///
    /// A partition declared `u8` holds keys `0..256`. This is the check that
    /// makes the declared width mean something at run time rather than only
    /// selecting a shape at compile time.
    OutOfRange {
        /// The key that was offered.
        ///
        /// `u64` rather than `usize` so a key that does not fit a `usize` at
        /// all, which is a 32-bit target holding a `u64` key, can still be
        /// reported as the number the caller wrote.
        key: u64,
        /// The declared cap, exclusive.
        cap: usize,
    },
    /// A row already occupies that key.
    ///
    /// `insert` refuses rather than overwriting, the same way the full table's
    /// does. `upsert` is the one that replaces.
    Duplicate {
        /// The occupied key.
        key: usize,
    },
}

impl DenseError {
    /// The refusal a key that does not fit a `usize` earns.
    ///
    /// Reachable only on a 32-bit target with a key above `u32::MAX`, and a
    /// cap is at most 65,536, so such a key is out of range by construction.
    #[must_use]
    pub fn out_of_range(key: u64, cap: usize) -> Self {
        Self::OutOfRange { key, cap }
    }
}

impl fmt::Display for DenseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfRange { key, cap } => write!(
                f,
                "key {key} is outside this partition: `partition_max_size` declares {cap} rows, so keys run 0..{cap}"
            ),
            Self::Duplicate { key } => write!(f, "key {key} already holds a row in this partition"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for DenseError {}

/// Rows of one dense partition, addressed by position.
///
/// See the module documentation for what this drops relative to a full
/// generated table and why. Generated code wraps this in a typed facade; the
/// storage lives here so the expansion per partitioned table stays small, for
/// the same reason [`super::PartitionSet`] does.
#[derive(Debug)]
pub struct DenseRows<T> {
    /// Indexed by key. `None` is a key in range that holds no row, which is
    /// every key below the highest one inserted that nobody has used.
    ///
    /// One lock rather than one per slot: a per-slot lock over 23 rows costs
    /// more in indirection than it saves in contention, and the vector has to
    /// be guarded anyway because growing it moves the rows.
    rows: RwLock<Vec<Option<T>>>,
    /// Rows actually present, so `row_count` and `is_empty` do not take the
    /// lock. Kept in step with `rows` under the write lock.
    live: AtomicUsize,
    /// The declared `partition_max_size`, exclusive. Not a capacity: nothing
    /// is allocated against it.
    cap: usize,
}

impl<T> DenseRows<T> {
    /// The rows, for reading.
    ///
    /// A shim, because the two `RwLock`s this compiles against do not agree on
    /// the signature: `parking_lot`'s `read` hands back the guard, and loom's
    /// hands back a `Result` because it models poisoning. Normalising here
    /// keeps the eight call sites below free of `cfg`.
    #[cfg(not(wt_loom))]
    fn read(&self) -> impl core::ops::Deref<Target = Vec<Option<T>>> + '_ {
        self.rows.read()
    }

    #[cfg(wt_loom)]
    fn read(&self) -> impl core::ops::Deref<Target = Vec<Option<T>>> + '_ {
        self.rows.read().expect("nothing panics while holding this lock")
    }

    /// The rows, for writing. See [`Self::read`].
    #[cfg(not(wt_loom))]
    fn write(&self) -> impl core::ops::DerefMut<Target = Vec<Option<T>>> + '_ {
        self.rows.write()
    }

    #[cfg(wt_loom)]
    fn write(&self) -> impl core::ops::DerefMut<Target = Vec<Option<T>>> + '_ {
        self.rows.write().expect("nothing panics while holding this lock")
    }

    /// A partition holding at most `cap` rows, allocating nothing yet.
    #[must_use]
    pub fn new(cap: usize) -> Self {
        Self {
            rows: RwLock::new(Vec::new()),
            live: AtomicUsize::new(0),
            cap,
        }
    }

    /// The declared cap, exclusive. Keys run `0..cap`.
    #[must_use]
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Rows present. Does not take the lock.
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.live.load(Ordering::Acquire)
    }

    /// Whether any row is present. Does not take the lock.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.row_count() == 0
    }

    fn in_range(&self, key: usize) -> Result<(), DenseError> {
        if key < self.cap {
            Ok(())
        } else {
            Err(DenseError::OutOfRange {
                key: key as u64,
                cap: self.cap,
            })
        }
    }

    /// Grow to hold `key`, filling the gap with absent slots.
    ///
    /// Called under the write lock. The vector reaches the highest key used
    /// and no further, which is why a `u16` partition holding three rows costs
    /// three slots rather than 65,536.
    fn make_room(rows: &mut Vec<Option<T>>, key: usize) {
        if key >= rows.len() {
            rows.resize_with(key + 1, || None);
        }
    }
}

impl<T: Clone> DenseRows<T> {
    /// The row at `key`, cloned out.
    ///
    /// Cloned rather than borrowed because the rows sit behind a lock that
    /// cannot outlive this call, which is the same reason the paged table's
    /// `select` clones. A key past the end is absent rather than an error: it
    /// is a key nobody has written, which is what `None` means.
    #[must_use]
    pub fn get(&self, key: usize) -> Option<T> {
        self.read().get(key)?.clone()
    }

    /// Whether `key` holds a row.
    #[must_use]
    pub fn contains(&self, key: usize) -> bool {
        self.read().get(key).is_some_and(Option::is_some)
    }

    /// Every row present, ascending by key, with its key.
    #[must_use]
    pub fn iter(&self) -> Vec<(usize, T)> {
        let rows = self.read();
        rows.iter()
            .enumerate()
            .filter_map(|(key, slot)| slot.clone().map(|row| (key, row)))
            .collect()
    }
}

impl<T> DenseRows<T> {
    /// Place `row` at `key`, refusing a key that is occupied or out of range.
    ///
    /// `Err` carries the reason and not the row. The row is recoverable from
    /// the caller's own value in the generated facade, which is where the row
    /// type is known.
    pub fn insert(&self, key: usize, row: T) -> Result<(), DenseError> {
        self.in_range(key)?;
        let mut rows = self.write();
        Self::make_room(&mut rows, key);
        if rows[key].is_some() {
            return Err(DenseError::Duplicate { key });
        }
        rows[key] = Some(row);
        self.live.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Place `row` at `key`, returning whatever it replaced.
    pub fn upsert(&self, key: usize, row: T) -> Result<Option<T>, DenseError> {
        self.in_range(key)?;
        let mut rows = self.write();
        Self::make_room(&mut rows, key);
        let previous = rows[key].replace(row);
        if previous.is_none() {
            self.live.fetch_add(1, Ordering::Release);
        }
        Ok(previous)
    }

    /// Take the row at `key` out.
    ///
    /// The slot stays, holding nothing. Nothing shifts, because a position is
    /// a key: compacting would renumber every row above it.
    pub fn remove(&self, key: usize) -> Option<T> {
        let mut rows = self.write();
        let taken = rows.get_mut(key)?.take();
        if taken.is_some() {
            self.live.fetch_sub(1, Ordering::Release);
        }
        taken
    }

    /// Run `edit` against the row at `key`, in place.
    ///
    /// The lock is held across the call, so `edit` must not reach back into
    /// this partition. It is the only way to change part of a row without
    /// cloning it out and back, which at an 832-byte row is the difference
    /// between touching one field and copying the row twice.
    pub fn update<R>(&self, key: usize, edit: impl FnOnce(&mut T) -> R) -> Option<R> {
        let mut rows = self.write();
        rows.get_mut(key)?.as_mut().map(edit)
    }

    /// Slots allocated, present or not.
    ///
    /// One past the highest key ever inserted, not the declared cap. Exposed
    /// because it is the figure that explains this shape's memory, and a test
    /// that asserts the cap is not allocated needs to be able to see it.
    #[must_use]
    pub fn slots(&self) -> usize {
        self.read().len()
    }
}

impl<T> Default for DenseRows<T> {
    /// A partition with no cap, for a caller that has not declared one.
    ///
    /// Generated code never reaches this: it always knows the declared width
    /// and calls [`DenseRows::new`]. It exists because the router's
    /// `partition_or_create` names `Default`, and a facade that wraps this has
    /// to be able to derive it.
    fn default() -> Self {
        Self::new(usize::MAX)
    }
}

impl<T: MemStat> MemStat for DenseRows<T> {
    fn heap_size(&self) -> usize {
        let rows = self.read();
        rows.capacity() * core::mem::size_of::<Option<T>>() + rows.iter().map(|slot| slot.heap_size()).sum::<usize>()
    }

    fn used_size(&self) -> usize {
        let rows = self.read();
        rows.len() * core::mem::size_of::<Option<T>>() + rows.iter().map(|slot| slot.used_size()).sum::<usize>()
    }
}

#[cfg(all(test, not(wt_loom)))]
mod tests {
    use super::*;

    #[test]
    fn position_is_the_key() {
        let rows = DenseRows::new(256);
        rows.insert(7, "seven").expect("fresh");
        rows.insert(0, "zero").expect("fresh");

        assert_eq!(rows.get(7), Some("seven"));
        assert_eq!(rows.get(0), Some("zero"));
        // In range, allocated, and holding nothing: not the same as absent.
        assert_eq!(rows.get(3), None);
        assert_eq!(rows.row_count(), 2);
    }

    #[test]
    fn the_cap_is_a_bound_and_not_a_reservation() {
        // The point of the shape. A `u16` partition declares 65,536 rows and a
        // partition holding one row must not allocate 65,536 slots, or the
        // whole saving is spent before any row arrives.
        let rows: DenseRows<u64> = DenseRows::new(65_536);
        assert_eq!(rows.slots(), 0, "an empty partition allocates nothing");

        rows.insert(2, 20).expect("fresh");
        assert_eq!(rows.slots(), 3, "grown to the key used, not to the cap");
        assert_eq!(rows.cap(), 65_536);
    }

    #[test]
    fn a_key_past_the_cap_is_refused_by_name() {
        let rows: DenseRows<u64> = DenseRows::new(4);
        let error = rows.insert(4, 1).expect_err("4 is not in 0..4");
        assert_eq!(error, DenseError::OutOfRange { key: 4, cap: 4 });

        rows.insert(3, 1).expect("3 is the last key in range");
    }

    #[test]
    fn insert_refuses_a_duplicate_and_upsert_replaces_it() {
        let rows = DenseRows::new(8);
        rows.insert(1, 10).expect("fresh");
        assert_eq!(rows.insert(1, 99), Err(DenseError::Duplicate { key: 1 }));
        assert_eq!(rows.get(1), Some(10), "the refused insert changed nothing");

        assert_eq!(rows.upsert(1, 99), Ok(Some(10)));
        assert_eq!(rows.get(1), Some(99));
        assert_eq!(rows.row_count(), 1, "replacing is not a second row");
    }

    #[test]
    fn upsert_into_an_empty_key_counts_a_new_row() {
        // The other half of `upsert`. The test above covers replacing, which
        // must *not* count; this covers arriving, which must. Dropping the
        // increment here survived every other test in this module, which is how
        // it was found.
        let rows = DenseRows::new(8);
        assert_eq!(rows.upsert(4, 40), Ok(None), "nothing was there");
        assert_eq!(rows.row_count(), 1);

        // And again into a key that was emptied rather than never used, which
        // is a different path through the slot vector.
        rows.remove(4).expect("just upserted");
        assert_eq!(rows.row_count(), 0);
        assert_eq!(rows.upsert(4, 41), Ok(None));
        assert_eq!(rows.row_count(), 1);
        assert_eq!(rows.get(4), Some(41));
    }

    #[test]
    fn removing_leaves_the_positions_of_everything_else_alone() {
        // The reason nothing is compacted: a position is a key, so shifting
        // rows down would silently renumber them.
        let rows = DenseRows::new(8);
        for key in 0..4 {
            rows.insert(key, key * 10).expect("fresh");
        }
        assert_eq!(rows.remove(1), Some(10));

        assert_eq!(rows.get(0), Some(0));
        assert_eq!(rows.get(1), None);
        assert_eq!(rows.get(2), Some(20), "key 2 did not become key 1");
        assert_eq!(rows.get(3), Some(30));
        assert_eq!(rows.row_count(), 3);
        assert_eq!(rows.slots(), 4, "the slot stays, holding nothing");

        // And the freed key takes a new row without complaint.
        rows.insert(1, 111).expect("free again");
        assert_eq!(rows.get(1), Some(111));
    }

    #[test]
    fn removing_what_was_never_there_is_not_a_row_lost() {
        let rows: DenseRows<u64> = DenseRows::new(8);
        assert_eq!(rows.remove(3), None);
        assert_eq!(rows.row_count(), 0, "the counter must not go negative");
        rows.insert(3, 1).expect("fresh");
        assert_eq!(rows.remove(3), Some(1));
        assert_eq!(rows.remove(3), None);
        assert_eq!(rows.row_count(), 0);
    }

    #[test]
    fn update_edits_in_place_and_says_whether_it_found_anything() {
        let rows = DenseRows::new(8);
        rows.insert(2, 5u64).expect("fresh");

        assert_eq!(rows.update(2, |row| core::mem::replace(row, 6)), Some(5));
        assert_eq!(rows.get(2), Some(6));

        assert_eq!(rows.update(1, |row| *row), None, "in range, holding nothing");
        assert_eq!(rows.update(99, |row| *row), None, "past the end");
    }

    #[test]
    fn iter_skips_the_holes_and_carries_the_keys() {
        let rows = DenseRows::new(16);
        rows.insert(5, "five").expect("fresh");
        rows.insert(1, "one").expect("fresh");
        assert_eq!(rows.iter(), alloc::vec![(1, "one"), (5, "five")]);
    }

    #[test]
    fn writes_through_a_shared_reference_do_not_lose_rows() {
        // The property the whole shape rests on: `partition_or_create` hands
        // out `Arc<T>`, so every mutation goes through `&self`.
        use alloc::sync::Arc;
        use std::thread;

        let rows: Arc<DenseRows<usize>> = Arc::new(DenseRows::new(256));
        let threads: Vec<_> = (0..8)
            .map(|worker| {
                let rows = Arc::clone(&rows);
                thread::spawn(move || {
                    for step in 0..32 {
                        rows.insert(worker * 32 + step, worker)
                            .expect("each key is written once");
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("worker");
        }

        assert_eq!(rows.row_count(), 256);
        assert_eq!(rows.slots(), 256);
        for key in 0..256 {
            assert_eq!(rows.get(key), Some(key / 32), "key {key}");
        }
    }

    #[test]
    fn concurrent_inserts_of_one_key_produce_exactly_one_winner() {
        use alloc::sync::Arc;
        use std::sync::atomic::AtomicUsize as StdAtomicUsize;
        use std::thread;

        let rows: Arc<DenseRows<usize>> = Arc::new(DenseRows::new(4));
        let won = Arc::new(StdAtomicUsize::new(0));
        let threads: Vec<_> = (0..8)
            .map(|worker| {
                let rows = Arc::clone(&rows);
                let won = Arc::clone(&won);
                thread::spawn(move || {
                    if rows.insert(2, worker).is_ok() {
                        won.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("worker");
        }

        assert_eq!(won.load(Ordering::Relaxed), 1, "exactly one insert may succeed");
        assert_eq!(rows.row_count(), 1);
    }
}

#[cfg(all(test, wt_loom))]
mod loom_tests {
    //! Loom models of the dense partition's counter and lock protocol.
    //!
    //! Run with:
    //!
    //! ```text
    //! RUSTFLAGS="--cfg wt_loom" cargo test --release --lib partition::dense::loom_tests
    //! ```
    //!
    //! **Manual only. Never wired into CI**, by instruction, the same as the
    //! models in [`super::super::loom_tests`] and the Miri runs.
    //!
    //! # What is under test, and what is not
    //!
    //! The rows sit behind one `RwLock`, and loom models that lock, so mutual
    //! exclusion over the vector is not the interesting question: loom would be
    //! checking its own primitive.
    //!
    //! What is interesting is `live`, the row counter, because it is written
    //! under the lock and read **without** it. Three things could go wrong and
    //! each has a model here: it could underflow when a remove races an insert,
    //! it could settle on the wrong value when several writers finish at once,
    //! and it could be read as a number that no interleaving ever produced.
    //!
    //! # These models are narrow, and that is on purpose
    //!
    //! The rows sit behind one lock, so loom serialises nearly everything and
    //! the state space is tiny: all five run in about ten milliseconds. That is
    //! a fair reflection of how little unsynchronised state this type has, not
    //! a sign the models are cheap to the point of being useless. Each was
    //! checked by breaking the thing it claims to check and confirming it
    //! fails: an unguarded `fetch_sub` in `remove` fails two of them, and an
    //! increment moved above the duplicate check in `insert` fails a third.
    //!
    //! That check found a real defect in the first version of these models,
    //! which is recorded on
    //! [`racing_removes_on_an_empty_slot_never_underflow_the_count`].
    //!
    //! The rows themselves are `u64` here rather than a `loom::cell::UnsafeCell`
    //! payload. That is deliberate and it is a limitation: loom cannot see
    //! inside a plain value, so these models say nothing about publication of
    //! the row's contents. They do not need to. Every read of a row goes through
    //! the same `RwLock` as every write, so publication is the lock's guarantee
    //! and not an `Ordering` this module chose. The partition set's models need
    //! `Guarded` because its readers deliberately run outside its mutex; this
    //! one has no such path.

    use super::*;
    use loom::sync::Arc;
    use loom::thread;

    /// Two threads inserting the same key: one wins, and the count agrees.
    ///
    /// The count is the point. `insert` increments only on the branch that
    /// actually stored a row, so a version that incremented before checking for
    /// an occupant would leave `row_count` at 2 with one row present.
    #[test]
    fn one_of_two_racing_inserts_wins_and_the_count_agrees() {
        loom::model(|| {
            let rows: Arc<DenseRows<u64>> = Arc::new(DenseRows::new(4));

            let a = {
                let rows = Arc::clone(&rows);
                thread::spawn(move || rows.insert(1, 10).is_ok())
            };
            let b = {
                let rows = Arc::clone(&rows);
                thread::spawn(move || rows.insert(1, 20).is_ok())
            };

            let won = usize::from(a.join().unwrap()) + usize::from(b.join().unwrap());
            assert_eq!(won, 1, "exactly one insert may store a row");
            assert_eq!(rows.row_count(), 1);
            assert!(matches!(rows.get(1), Some(10) | Some(20)));
        });
    }

    /// Two removes racing over one empty-but-allocated slot must not take the
    /// counter below zero.
    ///
    /// `fetch_sub` on a `usize` wraps, so an unguarded decrement does not panic
    /// in release: it reports a partition holding eighteen quintillion rows.
    /// The guard is that `remove` decrements only when it actually took
    /// something out.
    ///
    /// The setup matters and the first version of this model got it wrong. It
    /// raced a remove against an insert on a *fresh* partition, where the
    /// remove finds the row vector still empty, `get_mut` returns `None`, and
    /// the method returns before reaching the decrement at all. That model
    /// passed against a deliberately unguarded `remove`, which is the only
    /// thing a concurrency model must never do. The slot has to exist and hold
    /// nothing for the guard to be the thing under test, so it is allocated and
    /// emptied first.
    #[test]
    fn racing_removes_on_an_empty_slot_never_underflow_the_count() {
        loom::model(|| {
            let rows: Arc<DenseRows<u64>> = Arc::new(DenseRows::new(4));
            // Allocate slot 2, then empty it: present in the vector, holding
            // nothing, which is the state `remove` has to handle without
            // counting a row it did not take.
            rows.insert(2, 7).expect("fresh");
            rows.remove(2).expect("just inserted");
            assert_eq!(rows.row_count(), 0);

            let a = {
                let rows = Arc::clone(&rows);
                thread::spawn(move || rows.remove(2))
            };
            let b = {
                let rows = Arc::clone(&rows);
                thread::spawn(move || rows.remove(2))
            };
            assert!(a.join().unwrap().is_none());
            assert!(b.join().unwrap().is_none());

            assert_eq!(
                rows.row_count(),
                0,
                "removing nothing twice must leave the count at zero, not wrap"
            );
        });
    }

    /// And the same guard under a remove racing an insert on an existing slot.
    #[test]
    fn a_remove_racing_an_insert_counts_the_row_at_most_once() {
        loom::model(|| {
            let rows: Arc<DenseRows<u64>> = Arc::new(DenseRows::new(4));
            // Slot 2 allocated and empty, so neither thread returns early.
            rows.insert(2, 7).expect("fresh");
            rows.remove(2).expect("just inserted");

            let inserter = {
                let rows = Arc::clone(&rows);
                thread::spawn(move || rows.insert(2, 9).is_ok())
            };
            let remover = {
                let rows = Arc::clone(&rows);
                thread::spawn(move || rows.remove(2))
            };
            let inserted = inserter.join().unwrap();
            let taken = remover.join().unwrap();

            let count = rows.row_count();
            assert!(count <= 1, "a one-key partition cannot hold {count} rows");
            assert_eq!(
                count,
                usize::from(inserted && taken.is_none()),
                "the row is present exactly when it was inserted and not taken"
            );
        });
    }

    /// Two writers on different keys both land, and the count sees both.
    ///
    /// This is where a `Relaxed` increment would show: the second writer's
    /// `fetch_add` has to be ordered against the first's for the final load to
    /// observe two.
    #[test]
    fn concurrent_writers_on_distinct_keys_both_count() {
        loom::model(|| {
            let rows: Arc<DenseRows<u64>> = Arc::new(DenseRows::new(4));

            let a = {
                let rows = Arc::clone(&rows);
                thread::spawn(move || rows.insert(0, 1).expect("key 0 is written once"))
            };
            let b = {
                let rows = Arc::clone(&rows);
                thread::spawn(move || rows.insert(1, 2).expect("key 1 is written once"))
            };
            a.join().unwrap();
            b.join().unwrap();

            assert_eq!(rows.row_count(), 2);
            assert_eq!(rows.get(0), Some(1));
            assert_eq!(rows.get(1), Some(2));
        });
    }

    /// A reader running beside a writer sees a count that some interleaving
    /// produced, never a torn one.
    ///
    /// `row_count` deliberately does not take the lock, so it may be stale.
    /// Stale is fine and is documented; a value that was never true is not.
    #[test]
    fn an_unlocked_count_is_always_a_value_some_interleaving_produced() {
        loom::model(|| {
            let rows: Arc<DenseRows<u64>> = Arc::new(DenseRows::new(4));
            rows.insert(0, 1).expect("fresh");

            let writer = {
                let rows = Arc::clone(&rows);
                thread::spawn(move || {
                    let _ = rows.insert(1, 2);
                })
            };
            let reader = {
                let rows = Arc::clone(&rows);
                thread::spawn(move || rows.row_count())
            };

            writer.join().unwrap();
            let seen = reader.join().unwrap();
            assert!(seen == 1 || seen == 2, "count {seen} matches no interleaving");
            assert_eq!(rows.row_count(), 2, "and it settles once the writer is done");
        });
    }
}
