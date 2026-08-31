//! Loom models of the slot protocol.
//!
//! The native tests run whichever interleaving this machine produced. Loom runs
//! every one of them, which is the only way to say the `Ordering` choices are
//! right rather than untested. Run with:
//!
//! ```text
//! RUSTFLAGS="--cfg wt_loom" cargo test --release --lib partition::loom_tests
//! ```
//!
//! The cfg is `wt_loom` rather than the conventional `loom` because `RUSTFLAGS`
//! applies to every crate in the graph, and tokio gates `tokio::fs` on
//! `cfg(not(loom))` for its own loom builds.
//!
//! Scope: loom models the slot and spine atomics and the growth mutex, so what
//! is checked is publication ordering, the double-checked lock in
//! `get_or_create`, and the removal handshake. `Arc` is std's and its
//! refcounting is opaque to loom, which is fine: the bug this was written
//! against was never in `Arc`. What is *not* fine is assuming loom sees inside
//! the payload, which it does not unless the payload says so; see [`Guarded`].

use super::*;

/// A payload loom can see inside.
///
/// Loom only tracks accesses it owns. An `Arc<u64>` is opaque to it, so a test
/// that reads one proves nothing about publication: weakening the slot store to
/// `Relaxed` leaves such a test passing, which is exactly what happened before
/// this type existed. Putting the value behind `loom::cell::UnsafeCell` is what
/// lets loom notice a read racing the write that built it.
struct Guarded {
    value: loom::cell::UnsafeCell<u64>,
}

// Safety: every access goes through the cell, and the only synchronisation
// claimed is the one under test, the slot's release store and acquire load.
unsafe impl Send for Guarded {}
unsafe impl Sync for Guarded {}

impl Guarded {
    /// Built inside `make`, so the write happens before the slot is published.
    fn build(v: u64) -> Self {
        let g = Guarded {
            value: loom::cell::UnsafeCell::new(0),
        };
        g.value.with_mut(|p| unsafe { *p = v });
        g
    }

    fn read(&self) -> u64 {
        self.value.with(|p| unsafe { *p })
    }
}

/// A reader must see either no partition or a complete one, never a pointer to
/// something still being built. This is what the `Release` store and `Acquire`
/// load on the slot exist for, and weakening either one fails this model.
#[test]
fn a_reader_never_observes_a_half_published_partition() {
    loom::model(|| {
        let set: Arc<PartitionSet<Guarded>> = Arc::new(PartitionSet::new());

        let writer = {
            let set = set.clone();
            loom::thread::spawn(move || {
                set.get_or_create(1, || Guarded::build(0xABCD)).unwrap();
            })
        };
        let reader = {
            let set = set.clone();
            loom::thread::spawn(move || set.partition(1).map(|t| t.read()))
        };

        writer.join().unwrap();
        if let Some(seen) = reader.join().unwrap() {
            assert_eq!(seen, 0xABCD, "a partition was visible before it was built");
        }
        assert_eq!(set.partition(1).map(|t| t.read()), Some(0xABCD));
    });
}

/// The same question for a partition published into a chunk that the reader may
/// also be watching get allocated.
#[test]
fn a_reader_never_observes_a_half_published_chunk() {
    loom::model(|| {
        let set: Arc<PartitionSet<Guarded>> = Arc::new(PartitionSet::new());

        let writer = {
            let set = set.clone();
            loom::thread::spawn(move || {
                // Key 2 lands in the second chunk, so the chunk itself has to
                // be allocated and published during the race.
                set.get_or_create(2, || Guarded::build(0x1234)).unwrap();
            })
        };
        let reader = {
            let set = set.clone();
            loom::thread::spawn(move || set.partition(2).map(|t| t.read()))
        };

        writer.join().unwrap();
        if let Some(seen) = reader.join().unwrap() {
            assert_eq!(seen, 0x1234, "a chunk was visible before it was built");
        }
    });
}

/// Two threads asking for the same absent key must produce one table.
///
/// This is the double-checked lock: both threads miss on the unlocked read,
/// both take the mutex, and the second has to see the first one's store on the
/// re-check. Without the re-check, some interleaving here builds two tables.
#[test]
fn two_threads_creating_one_key_make_exactly_one_table() {
    loom::model(|| {
        let set: Arc<PartitionSet<u64>> = Arc::new(PartitionSet::new());

        let a = {
            let set = set.clone();
            loom::thread::spawn(move || set.get_or_create(0, || 7u64).unwrap())
        };
        let b = {
            let set = set.clone();
            loom::thread::spawn(move || set.get_or_create(0, || 7u64).unwrap())
        };

        let ra = a.join().unwrap();
        let rb = b.join().unwrap();
        assert!(
            Arc::ptr_eq(&ra, &rb),
            "the two threads built different tables for one key"
        );
        assert_eq!(*ra, 7);
        assert_eq!(set.len(), 1, "one key was counted twice");
    });
}

/// Two threads creating different keys that land in the same chunk race on
/// chunk allocation as well as on their own slots. Only one chunk may be
/// published, and neither thread's write may be lost.
#[test]
fn concurrent_creation_in_one_chunk_publishes_a_single_chunk() {
    loom::model(|| {
        let set: Arc<PartitionSet<u64>> = Arc::new(PartitionSet::new());

        let a = {
            let set = set.clone();
            loom::thread::spawn(move || set.get_or_create(0, || 10u64).unwrap())
        };
        let b = {
            let set = set.clone();
            loom::thread::spawn(move || set.get_or_create(1, || 11u64).unwrap())
        };
        a.join().unwrap();
        b.join().unwrap();

        assert_eq!(set.partition(0).map(|t| *t), Some(10));
        assert_eq!(set.partition(1).map(|t| *t), Some(11));
        assert_eq!(set.len(), 2, "a chunk allocation lost a slot");
    });
}

/// The case the retire list exists for. A reader that has loaded the slot
/// pointer and not yet revived an `Arc` from it must still find a live
/// allocation after the removing thread has finished.
#[test]
fn a_reader_racing_a_removal_finds_a_live_table() {
    loom::model(|| {
        let set: Arc<PartitionSet<Guarded>> = Arc::new(PartitionSet::new());
        set.get_or_create(0, || Guarded::build(99)).unwrap();

        let remover = {
            let set = set.clone();
            loom::thread::spawn(move || set.remove(0).is_some())
        };
        let reader = {
            let set = set.clone();
            loom::thread::spawn(move || set.partition(0).map(|t| t.read()))
        };

        assert!(remover.join().unwrap(), "the removal did not find the table");
        if let Some(seen) = reader.join().unwrap() {
            assert_eq!(seen, 99, "a reader revived a dead allocation");
        }
        assert_eq!(set.len(), 0);
        assert_eq!(set.retired_len(), 1, "the removal did not retire");
    });
}

/// A removal of one key racing a creation of another must leave both the
/// counter and the surviving slot consistent, whichever order they land in.
#[test]
fn a_removal_racing_a_creation_keeps_the_count_honest() {
    loom::model(|| {
        let set: Arc<PartitionSet<u64>> = Arc::new(PartitionSet::new());
        set.get_or_create(0, || 1u64).unwrap();

        let creator = {
            let set = set.clone();
            loom::thread::spawn(move || set.get_or_create(1, || 2u64).unwrap())
        };
        let remover = {
            let set = set.clone();
            loom::thread::spawn(move || set.remove(0))
        };
        creator.join().unwrap();
        assert!(remover.join().unwrap().is_some());

        assert_eq!(set.len(), 1, "the two writers disagreed about the count");
        assert!(set.partition(0).is_none());
        assert_eq!(set.partition(1).map(|t| *t), Some(2));
    });
}
