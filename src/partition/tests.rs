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
