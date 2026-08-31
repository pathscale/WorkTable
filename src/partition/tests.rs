use super::*;
use std::sync::atomic::AtomicU32;

#[derive(Debug, PartialEq)]
struct Counted(u64);

#[test]
fn absent_keys_return_none_rather_than_creating() {
    let set: PartitionSet<Counted> = PartitionSet::new();
    assert!(set.partition(0).is_none());
    assert!(set.partition(7).is_none());
    assert!(set.is_empty());
    assert_eq!(set.keys(), Vec::<u64>::new());
}

#[test]
fn get_or_create_is_idempotent() {
    let set: PartitionSet<Counted> = PartitionSet::new();
    let calls = AtomicU32::new(0);
    let mut make = || {
        calls.fetch_add(1, Ordering::SeqCst);
        Counted(42)
    };
    let a = set.get_or_create(5, &mut make).unwrap();
    let b = set.get_or_create(5, &mut make).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1, "make ran twice for one key");
    assert!(Arc::ptr_eq(&a, &b));
    assert_eq!(set.len(), 1);
}

#[test]
fn keys_span_chunk_boundaries_in_order() {
    let set: PartitionSet<Counted> = PartitionSet::new();
    // Deliberately across three chunks and out of order.
    for k in [CHUNK as u64 + 1, 0, 2 * CHUNK as u64, 7, CHUNK as u64 - 1] {
        set.get_or_create(k, || Counted(k)).unwrap();
    }
    assert_eq!(
        set.keys(),
        vec![0, 7, CHUNK as u64 - 1, CHUNK as u64 + 1, 2 * CHUNK as u64]
    );
    assert_eq!(set.len(), 5);
    for (k, t) in set.iter() {
        assert_eq!(t.0, k, "key {k} routed to the wrong table");
    }
}

#[test]
fn sparse_keys_do_not_allocate_intervening_chunks() {
    let set: PartitionSet<Counted> = PartitionSet::new();
    set.get_or_create(0, || Counted(0)).unwrap();
    set.get_or_create(60 * CHUNK as u64, || Counted(1)).unwrap();
    assert_eq!(set.len(), 2);
    // Everything between is still absent, and reading it must not allocate.
    assert!(set.partition(30 * CHUNK as u64).is_none());
    assert_eq!(set.keys().len(), 2);
}

#[test]
fn out_of_range_keys_are_refused_not_wrapped() {
    let set: PartitionSet<Counted> = PartitionSet::new();
    let key = MAX_PARTITIONS as u64;
    assert_eq!(
        set.get_or_create(key, || Counted(0)),
        Err(PartitionError::OutOfRange { key })
    );
    assert!(set.partition(key).is_none());
    assert!(set.partition(u64::MAX).is_none());
    // The last valid key still works.
    assert!(set.get_or_create(MAX_PARTITIONS as u64 - 1, || Counted(9)).is_ok());
}

#[test]
fn remove_drops_the_slot_but_not_a_held_handle() {
    let set: PartitionSet<Counted> = PartitionSet::new();
    let held = set.get_or_create(3, || Counted(3)).unwrap();
    assert_eq!(set.len(), 1);
    let taken = set.remove(3).expect("was present");
    assert_eq!(set.len(), 0);
    assert!(set.partition(3).is_none());
    // A reader mid-query keeps its handle.
    assert_eq!(held.0, 3);
    assert!(Arc::ptr_eq(&held, &taken));
    assert!(set.remove(3).is_none(), "removing twice must not underflow len");
}

#[test]
fn concurrent_creation_of_one_key_makes_one_table() {
    let set: Arc<PartitionSet<Counted>> = Arc::new(PartitionSet::new());
    let calls = Arc::new(AtomicU32::new(0));
    let mut handles = Vec::new();
    for _ in 0..16 {
        let set = set.clone();
        let calls = calls.clone();
        handles.push(std::thread::spawn(move || {
            let mut got = Vec::new();
            for k in 0..64u64 {
                let c = calls.clone();
                got.push(set.get_or_create(k, move || {
                    c.fetch_add(1, Ordering::SeqCst);
                    Counted(k)
                }));
            }
            got
        }));
    }
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(calls.load(Ordering::SeqCst), 64, "a key was created more than once");
    assert_eq!(set.len(), 64);
    // Every thread must have observed the same table for a given key.
    for k in 0..64usize {
        let first = results[0][k].as_ref().unwrap();
        for r in &results {
            assert!(Arc::ptr_eq(first, r[k].as_ref().unwrap()));
        }
    }
}

#[test]
fn concurrent_readers_see_a_partition_created_under_them() {
    let set: Arc<PartitionSet<Counted>> = Arc::new(PartitionSet::new());
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader = {
        let set = set.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut seen = 0u64;
            while !stop.load(Ordering::Relaxed) {
                for k in 0..256u64 {
                    if let Some(t) = set.partition(k) {
                        assert_eq!(t.0, k, "torn read: key {k} gave {}", t.0);
                        seen += 1;
                    }
                }
            }
            seen
        })
    };
    for k in 0..256u64 {
        set.get_or_create(k, || Counted(k)).unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    reader.join().unwrap();
    assert_eq!(set.len(), 256);
}

// ---------------------------------------------------------------------------
// Reclamation. A slot hands out `Arc`s to readers that hold no lock, so the
// question these answer is when the allocation behind one is allowed to die.
// ---------------------------------------------------------------------------

/// Counts its own drops, so a test can assert that a removed partition is
/// retired rather than freed.
#[derive(Debug)]
struct Tracked {
    key: u64,
    drops: Arc<AtomicU32>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn a_removed_partition_is_retired_not_freed() {
    let drops = Arc::new(AtomicU32::new(0));
    let mut set: PartitionSet<Tracked> = PartitionSet::new();
    for k in 0..4u64 {
        let drops = drops.clone();
        set.get_or_create(k, || Tracked { key: k, drops }).unwrap();
    }

    // Drop every handle the test holds. The set still owns all four.
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    let taken = set.remove(2).expect("was present");
    assert_eq!(taken.key, 2);
    drop(taken);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "a removed partition must outlive the removal: a reader may have \
         loaded its pointer and not yet incremented the strong count"
    );
    assert_eq!(set.retired_len(), 1);
    assert_eq!(set.len(), 3);

    assert_eq!(set.gc(), 1, "gc must report what it reclaimed");
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(set.retired_len(), 0);
    assert_eq!(set.len(), 3, "gc must not disturb live partitions");
}

#[test]
fn dropping_the_set_frees_live_and_retired_partitions_alike() {
    let drops = Arc::new(AtomicU32::new(0));
    {
        let set: PartitionSet<Tracked> = PartitionSet::new();
        for k in 0..8u64 {
            let drops = drops.clone();
            set.get_or_create(k, || Tracked { key: k, drops }).unwrap();
        }
        // Three retired, five still live, and one key well past the first
        // chunk so more than one chunk has to be walked.
        let drops2 = drops.clone();
        set.get_or_create(5000, || Tracked {
            key: 5000,
            drops: drops2,
        })
        .unwrap();
        set.remove(0);
        set.remove(1);
        set.remove(2);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
    }
    assert_eq!(
        drops.load(Ordering::SeqCst),
        9,
        "dropping the set must free every partition exactly once"
    );
}

#[test]
fn gc_on_an_untouched_set_is_a_no_op() {
    let mut set: PartitionSet<Counted> = PartitionSet::new();
    assert_eq!(set.gc(), 0);
    set.get_or_create(1, || Counted(1)).unwrap();
    assert_eq!(set.gc(), 0, "a live partition is not garbage");
    assert_eq!(set.len(), 1);
}

// ---------------------------------------------------------------------------
// Bounds. Every entry point has to agree on what a routable key is.
// ---------------------------------------------------------------------------

#[test]
fn the_first_and_last_keys_are_both_routable() {
    let set: PartitionSet<Counted> = PartitionSet::new();
    let last = MAX_PARTITIONS as u64 - 1;
    set.get_or_create(0, || Counted(0)).unwrap();
    set.get_or_create(last, || Counted(last)).unwrap();
    assert_eq!(set.partition(0).unwrap().0, 0);
    assert_eq!(set.partition(last).unwrap().0, last);
    assert_eq!(set.keys(), vec![0, last]);
}

#[test]
fn every_entry_point_agrees_on_the_bound() {
    let set: PartitionSet<Counted> = PartitionSet::new();
    for key in [MAX_PARTITIONS as u64, MAX_PARTITIONS as u64 + 1, u64::MAX, u64::MAX / 2] {
        assert!(set.partition(key).is_none(), "partition({key})");
        assert!(!set.contains(key), "contains({key})");
        assert!(set.remove(key).is_none(), "remove({key})");
        assert_eq!(
            set.get_or_create(key, || Counted(key)),
            Err(PartitionError::OutOfRange { key }),
            "get_or_create({key})"
        );
    }
    assert!(set.is_empty(), "a refused key must not have allocated");
}

#[test]
fn contains_agrees_with_partition_across_a_removal() {
    let set: PartitionSet<Counted> = PartitionSet::new();
    assert!(!set.contains(9));
    set.get_or_create(9, || Counted(9)).unwrap();
    assert!(set.contains(9));
    assert!(set.partition(9).is_some());
    set.remove(9);
    assert!(!set.contains(9));
    assert!(set.partition(9).is_none());
}

#[test]
fn recreating_a_removed_key_yields_a_fresh_table() {
    let set: PartitionSet<Counted> = PartitionSet::new();
    let first = set.get_or_create(4, || Counted(1)).unwrap();
    set.remove(4);
    let second = set.get_or_create(4, || Counted(2)).unwrap();
    assert!(
        !Arc::ptr_eq(&first, &second),
        "recreation must not resurrect the old table"
    );
    assert_eq!(second.0, 2);
    assert_eq!(set.len(), 1);
}

#[test]
fn len_tracks_a_create_and_remove_cycle() {
    let set: PartitionSet<Counted> = PartitionSet::new();
    for round in 0..3 {
        for k in 0..10u64 {
            set.get_or_create(k, || Counted(k)).unwrap();
        }
        assert_eq!(set.len(), 10, "round {round}");
        // Creating an existing key must not double-count.
        for k in 0..10u64 {
            set.get_or_create(k, || Counted(k)).unwrap();
        }
        assert_eq!(set.len(), 10, "round {round}: recreate inflated len");
        for k in 0..10u64 {
            set.remove(k);
            // Removing twice must not underflow.
            assert!(set.remove(k).is_none());
        }
        assert_eq!(set.len(), 0, "round {round}");
    }
}

#[test]
fn iter_agrees_with_keys() {
    let set: PartitionSet<Counted> = PartitionSet::new();
    for k in [0u64, 1, 1023, 1024, 1025, 5000, 65535] {
        set.get_or_create(k, || Counted(k)).unwrap();
    }
    let keys = set.keys();
    let pairs = set.iter();
    assert_eq!(pairs.len(), keys.len());
    assert_eq!(pairs.iter().map(|(k, _)| *k).collect::<Vec<_>>(), keys);
    for (k, t) in pairs {
        assert_eq!(t.0, k, "iter paired key {k} with the wrong table");
    }
}

#[test]
fn every_chunk_in_the_spine_can_be_populated() {
    let set: PartitionSet<Counted> = PartitionSet::new();
    // One key in each chunk, at a different offset each time so an off-by-one
    // in the chunk/offset split shows up as a wrong key rather than a hit.
    let expected: Vec<u64> = (0..MAX_CHUNKS).map(|c| (c * CHUNK + c % CHUNK) as u64).collect();
    for &k in &expected {
        set.get_or_create(k, || Counted(k)).unwrap();
    }
    assert_eq!(set.len(), MAX_CHUNKS);
    assert_eq!(set.keys(), expected);
    for &k in &expected {
        assert_eq!(set.partition(k).unwrap().0, k);
    }
}

#[test]
fn debug_is_shallow() {
    let set: PartitionSet<Counted> = PartitionSet::new();
    set.get_or_create(1, || Counted(1)).unwrap();
    let s = format!("{set:?}");
    assert!(s.contains("live: 1"), "{s}");
    assert!(s.contains("Counted"), "{s}");
    assert!(!s.contains("Counted(1)"), "must not print partitions: {s}");
}

// ---------------------------------------------------------------------------
// Concurrency. These are the tests the storage exists for.
// ---------------------------------------------------------------------------

#[test]
fn a_reader_racing_a_remove_never_touches_freed_memory() {
    // The reader loads a slot pointer and then revives an `Arc` from it. If
    // `remove` dropped the reference the slot owned, this is the window in
    // which the allocation could be freed between those two steps. Readers
    // hammer the same keys a churn thread is creating and removing.
    #[cfg(miri)]
    const KEYS: u64 = 4;
    #[cfg(miri)]
    const ROUNDS: u32 = 3;
    #[cfg(not(miri))]
    const KEYS: u64 = 32;
    #[cfg(not(miri))]
    const ROUNDS: u32 = 400;

    let drops = Arc::new(AtomicU32::new(0));
    let set: Arc<PartitionSet<Tracked>> = Arc::new(PartitionSet::new());
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = Arc::new(AtomicU32::new(0));

    let readers: Vec<_> = (0..if cfg!(miri) { 2 } else { 3 })
        .map(|_| {
            let set = set.clone();
            let stop = stop.clone();
            let seen = seen.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    for k in 0..KEYS {
                        if let Some(t) = set.partition(k) {
                            // Reading through the handle is what would fault
                            // on a use-after-free.
                            assert_eq!(t.key, k, "key {k} revived as {}", t.key);
                            seen.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    // Yield rather than spin: a reader pinning a core for the
                    // whole churn slows every other test in this binary.
                    std::thread::yield_now();
                }
            })
        })
        .collect();

    let make = |k: u64, drops: &Arc<AtomicU32>| Tracked {
        key: k,
        drops: drops.clone(),
    };
    for _ in 0..ROUNDS {
        for k in 0..KEYS {
            set.get_or_create(k, || make(k, &drops)).unwrap();
        }
        for k in 0..KEYS {
            set.remove(k);
        }
    }

    // One last round held open until a reader has actually caught a live
    // partition. Asserting that they saw one without waiting for it is a race
    // against the scheduler, and under heavy load the readers lose it.
    for k in 0..KEYS {
        set.get_or_create(k, || make(k, &drops)).unwrap();
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while seen.load(Ordering::Relaxed) == 0 && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    for k in 0..KEYS {
        set.remove(k);
    }
    stop.store(true, Ordering::Relaxed);
    for reader in readers {
        reader.join().unwrap();
    }

    let created = (ROUNDS + 1) as usize * KEYS as usize;
    assert!(
        seen.load(Ordering::Relaxed) > 0,
        "no reader observed a live partition within the deadline, so the race \
         this test exists for was never exercised"
    );
    assert_eq!(set.len(), 0);
    assert_eq!(set.retired_len(), created, "every removal must have been retired");
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "nothing may be freed while readers are running"
    );
    // Readers are joined, so exclusive access is real and reclamation is safe.
    let mut set = Arc::try_unwrap(set).expect("readers have exited");
    assert_eq!(set.gc(), created);
    assert_eq!(drops.load(Ordering::SeqCst) as usize, created);
}

#[test]
fn concurrent_creation_and_removal_keeps_len_honest() {
    const THREADS: u64 = if cfg!(miri) { 3 } else { 8 };
    const PER_THREAD: u64 = if cfg!(miri) { 8 } else { 64 };

    let set: Arc<PartitionSet<Counted>> = Arc::new(PartitionSet::new());
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let set = set.clone();
            std::thread::spawn(move || {
                // Disjoint key ranges, so the final count is exact rather than
                // a race between threads over the same keys.
                let base = t * PER_THREAD;
                for k in base..base + PER_THREAD {
                    set.get_or_create(k, || Counted(k)).unwrap();
                }
                // Remove the odd half back out.
                for k in (base..base + PER_THREAD).step_by(2) {
                    assert!(set.remove(k).is_some(), "key {k} vanished");
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let expected = (THREADS * PER_THREAD / 2) as usize;
    assert_eq!(set.len(), expected);
    assert_eq!(set.keys().len(), expected, "keys disagrees with len");
    assert_eq!(set.iter().len(), expected, "iter disagrees with len");
    for (k, t) in set.iter() {
        assert_eq!(t.0, k);
        assert_eq!(k % 2, 1, "an evicted key came back");
    }
}

#[test]
fn concurrent_creation_across_chunk_boundaries_is_sound() {
    // Every thread walks the same keys, and the keys straddle chunk
    // boundaries, so threads race on chunk allocation as well as on slots.
    let keys: Vec<u64> = (0..if cfg!(miri) { 3u64 } else { 8 })
        .flat_map(|c: u64| {
            let base = c * CHUNK as u64;
            [base.saturating_sub(1), base, base + 1, base + CHUNK as u64 - 1]
        })
        .collect();

    let set: Arc<PartitionSet<Counted>> = Arc::new(PartitionSet::new());
    let calls = Arc::new(AtomicU32::new(0));
    let handles: Vec<_> = (0..if cfg!(miri) { 3 } else { 12 })
        .map(|_| {
            let set = set.clone();
            let calls = calls.clone();
            let keys = keys.clone();
            std::thread::spawn(move || {
                for k in keys {
                    let c = calls.clone();
                    let t = set
                        .get_or_create(k, move || {
                            c.fetch_add(1, Ordering::SeqCst);
                            Counted(k)
                        })
                        .unwrap();
                    assert_eq!(t.0, k);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let mut unique = keys.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(
        calls.load(Ordering::SeqCst) as usize,
        unique.len(),
        "a key was created more than once"
    );
    assert_eq!(set.len(), unique.len());
    assert_eq!(set.keys(), unique);
}

#[test]
fn a_panicking_initialiser_poisons_rather_than_corrupts() {
    let set: PartitionSet<Counted> = PartitionSet::new();
    set.get_or_create(1, || Counted(1)).unwrap();

    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        set.get_or_create(2, || panic!("initialiser blew up"))
    }));
    assert!(poisoned.is_err(), "the panic must propagate to the caller");

    // The set is now poisoned. Reads still work, because they never take the
    // mutex; writes report it rather than proceeding on unknown state.
    assert_eq!(set.partition(1).unwrap().0, 1);
    assert!(set.partition(2).is_none(), "the failed key must not exist");
    assert_eq!(set.len(), 1, "a failed creation must not be counted");
    assert_eq!(set.get_or_create(3, || Counted(3)), Err(PartitionError::Poisoned));
}

// ---------------------------------------------------------------------------
// Gaps found by mutation testing: `cargo mutants --file src/partition/mod.rs`.
// Each of these was a mutant that survived, meaning nothing asserted the
// behaviour at all.
// ---------------------------------------------------------------------------

/// A payload with a footprint, so the `MemStat` reporting has something to
/// report. Sizes are distinct and non-round to make a wrong sum obvious.
#[derive(Debug)]
struct Sized_(usize);

impl MemStat for Sized_ {
    fn heap_size(&self) -> usize {
        self.0 * 2
    }
    fn used_size(&self) -> usize {
        self.0
    }
}

#[test]
fn is_empty_is_false_while_a_partition_is_live() {
    let set: PartitionSet<Counted> = PartitionSet::new();
    assert!(set.is_empty());
    set.get_or_create(0, || Counted(0)).unwrap();
    assert!(!set.is_empty(), "a set holding a partition is not empty");
    set.remove(0);
    assert!(set.is_empty(), "a set is empty again once emptied");
}

#[test]
fn mem_stat_reports_each_partition_and_their_sum() {
    let set: PartitionSet<Sized_> = PartitionSet::new();
    assert_eq!(set.mem_stat_total(), 0);
    assert_eq!(set.mem_stat_by_key(), Vec::new());

    // A partition is held as an `Arc`, and `MemStat for Arc<T>` charges
    // `size_of::<T>()` on top of the payload, because that is what the
    // allocation actually holds. Derived rather than hard coded so the
    // expectation is the rule, not one machine's numbers.
    let overhead = std::mem::size_of::<Sized_>();
    for (k, size) in [(3u64, 17usize), (1, 5), (2048, 300)] {
        set.get_or_create(k, || Sized_(size)).unwrap();
    }

    // Ascending by key, not by insertion order or by size.
    assert_eq!(
        set.mem_stat_by_key(),
        vec![(1, 5 + overhead), (3, 17 + overhead), (2048, 300 + overhead)]
    );
    assert_eq!(set.mem_stat_total(), 322 + 3 * overhead);

    // Removing a partition removes its contribution.
    set.remove(3);
    assert_eq!(set.mem_stat_by_key(), vec![(1, 5 + overhead), (2048, 300 + overhead)]);
    assert_eq!(set.mem_stat_total(), 305 + 2 * overhead);
}

#[test]
fn partition_error_says_which_key_and_which_bound() {
    let out_of_range = PartitionError::OutOfRange { key: 70_000 };
    let text = out_of_range.to_string();
    assert!(text.contains("70000"), "the offending key must appear: {text}");
    assert!(
        text.contains(&MAX_PARTITIONS.to_string()),
        "the bound must appear so the caller knows why: {text}"
    );
    assert_eq!(PartitionError::Poisoned.to_string(), "partition set is poisoned");

    // The `Error` impl is what a caller using `?` and `eyre` will format.
    let as_error: &dyn std::error::Error = &out_of_range;
    assert_eq!(as_error.to_string(), text);
}
