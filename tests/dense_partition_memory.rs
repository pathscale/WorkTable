//! What `partition_max_size` is worth, in bytes the allocator was asked for.
//!
//! # Why this is a separate binary
//!
//! The claim behind the key is about the *fixed apparatus* a partition
//! allocates at creation: an empty partition of the 88-byte-row shape
//! web3.trading runs measures about 28 KB before it holds a single row.
//!
//! `memory_by_key` and `memory_total` cannot see that. They report `used_bytes`
//! by definition, which is row bytes plus index bytes and explicitly excludes
//! the fixed floor, reserved-but-unused page capacity, the router spine and
//! `Arc` overhead. Measured through them the two shapes look identical, which
//! is true of what they measure and useless for this question. See
//! `memory_total_reports_rows_and_cannot_see_the_apparatus` in
//! `tests/worktable/partitioned.rs`, which pins that so the mistake is not made
//! twice.
//!
//! So this counts what the process actually asked the allocator for, which
//! needs a `#[global_allocator]`, which is per binary. Hence a file of its own.
//!
//! # What is measured
//!
//! One declaration, two widths, everything else identical: the same columns,
//! the same routing key, the same number of partitions and rows. The only
//! difference between the arms is `partition_max_size`, so the difference in
//! the result is what the key buys.
//!
//! This counts requested allocation bytes plus positive reallocation growth.
//! Freed bytes are not subtracted, and allocator bookkeeping is invisible.
//! It measures allocation demand, not resident or retained memory; it does not
//! establish a lower bound on either shape's resident-memory saving.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use worktable::prelude::*;
use worktable::worktable;

/// Counts bytes handed out while it is switched on.
///
/// Each thread has its own region, so concurrent tests and harness allocations
/// on other threads cannot reset or add to the current test's count.
struct Counting;

thread_local! {
    // Constant initialization does not allocate inside the global allocator.
    static ALLOCATED: Cell<Option<usize>> = const { Cell::new(None) };
}

fn charge(bytes: usize) {
    // Allocation during TLS teardown is outside a measurement region.
    let _ = ALLOCATED.try_with(|count| {
        if let Some(total) = count.get() {
            count.set(Some(total + bytes));
        }
    });
}

struct AllocationRegion;

impl AllocationRegion {
    fn start() -> Self {
        ALLOCATED.with(|count| {
            assert!(count.get().is_none(), "allocation regions must not overlap");
            count.set(Some(0));
        });
        Self
    }

    fn finish(self) -> usize {
        ALLOCATED.with(|count| count.take().expect("active allocation region"))
    }
}

impl Drop for AllocationRegion {
    fn drop(&mut self) {
        // Restore disabled accounting on both normal return and panic.
        ALLOCATED.with(|count| count.set(None));
    }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size > layout.size() {
            charge(new_size - layout.size());
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Bytes the allocator was asked for while `work` ran.
///
/// Single-threaded by construction: every caller below builds its partitions on
/// this thread, so the counter is not picking up a background task's
/// allocations. A `worktable!` with `persist: false` starts no tasks.
fn allocated_by<T>(work: impl FnOnce() -> T) -> (T, usize) {
    let region = AllocationRegion::start();
    let out = work();
    (out, region.finish())
}

// The shape web3.trading runs: an exchange id inside a symbol.
//
// Wide on purpose. The whole finding is that the apparatus dominates a small
// partition, and a narrow row makes the apparatus look even larger relative to
// the data, so a wide row is the conservative choice for the claim.
//
// Written out twice rather than shared through a `macro_rules!`: `worktable!`
// reads tokens and does not expand a nested macro, so a shared block would not
// reach it. The two must stay identical, which is what
// `the_two_arms_declare_the_same_row` checks.
worktable!(
    name: Dense,
    partition_by: symbol_id: u16,
    partition_max_size: u8,
    columns: {
        exchange_id: u8 primary_key,
        bid: f64,
        ask: f64,
        bid_size: f64,
        ask_size: f64,
        last: f64,
        volume: f64,
        open_interest: f64,
        funding: f64,
        updated_at: u64,
        sequence: u64,
    }
);

worktable!(
    name: Full,
    partition_by: symbol_id: u16,
    partition_max_size: u64,
    columns: {
        exchange_id: u8 primary_key,
        bid: f64,
        ask: f64,
        bid_size: f64,
        ask_size: f64,
        last: f64,
        volume: f64,
        open_interest: f64,
        funding: f64,
        updated_at: u64,
        sequence: u64,
    }
);

/// Exchanges per symbol. `Exchange::TOTAL` is 22 and the loop that fills an
/// order book is inclusive, so the real count is 23, not the 3 an earlier
/// measurement assumed.
const ROWS: u8 = 23;
/// Symbols. The low end of the 40-to-2,000 range the real system runs.
const PARTITIONS: u16 = 200;

fn dense_row(exchange_id: u8) -> DenseRow {
    DenseRow {
        exchange_id,
        bid: 1.0,
        ask: 2.0,
        bid_size: 3.0,
        ask_size: 4.0,
        last: 5.0,
        volume: 6.0,
        open_interest: 7.0,
        funding: 8.0,
        updated_at: 9,
        sequence: 10,
    }
}

fn full_row(exchange_id: u8) -> FullRow {
    FullRow {
        exchange_id,
        bid: 1.0,
        ask: 2.0,
        bid_size: 3.0,
        ask_size: 4.0,
        last: 5.0,
        volume: 6.0,
        open_interest: 7.0,
        funding: 8.0,
        updated_at: 9,
        sequence: 10,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_dense_partition_costs_a_fraction_of_a_full_one() {
    // Warm both shapes first. The first partition of either kind pulls in
    // one-off allocations that belong to neither arm, and charging them to
    // whichever ran first is how a benchmark gets an answer it likes.
    {
        let warm = DensePartitions::new();
        let table = warm.partition_or_create(0).expect("fresh");
        table.insert(dense_row(0)).expect("fresh");

        let warm = FullPartitions::new();
        let table = warm.partition_or_create(0).expect("fresh");
        table.insert(full_row(0)).await.expect("fresh");
    }

    let (dense, dense_bytes) = allocated_by(|| {
        let books = DensePartitions::new();
        for symbol in 0..PARTITIONS {
            let book = books.partition_or_create(symbol).expect("fresh");
            for exchange_id in 0..ROWS {
                book.insert(dense_row(exchange_id)).expect("fresh");
            }
        }
        books
    });

    // The full table's `insert` is async, so the counter is started and stopped
    // around the awaits by hand rather than through `allocated_by`. This arm
    // therefore carries whatever the futures cost, which is a real cost of the
    // shape and not a measurement artefact: a caller of the full table pays it.
    //
    // The runtime is `current_thread`, so this future stays on the thread whose
    // region is active. Other test threads have independent counters.
    let region = AllocationRegion::start();
    let books = FullPartitions::new();
    for symbol in 0..PARTITIONS {
        let book = books.partition_or_create(symbol).expect("fresh");
        for exchange_id in 0..ROWS {
            book.insert(full_row(exchange_id)).await.expect("fresh");
        }
    }
    let full_bytes = region.finish();

    let rows = usize::from(PARTITIONS) * usize::from(ROWS);
    let payload = rows * core::mem::size_of::<DenseRow>();

    eprintln!(
        "DENSE-PARTITION-MEMORY partitions={PARTITIONS} rows_each={ROWS} row={}B payload={payload}B\n\
         \x20 dense={dense_bytes}B ({:.0} B/partition)\n\
         \x20 full={full_bytes}B ({:.0} B/partition)\n\
         \x20 saving={:.1}x",
        core::mem::size_of::<DenseRow>(),
        dense_bytes as f64 / f64::from(PARTITIONS),
        full_bytes as f64 / f64::from(PARTITIONS),
        full_bytes as f64 / dense_bytes as f64,
    );

    assert_eq!(dense.len(), usize::from(PARTITIONS));
    assert_eq!(books.len(), usize::from(PARTITIONS));

    // The claim, as a test rather than a printout. A factor of two is well
    // inside what was measured and leaves room for an allocator that rounds
    // differently, so this fails on a regression and not on a machine.
    assert!(
        full_bytes > dense_bytes * 2,
        "a dense partition should cost a fraction of a full one: {dense_bytes} against {full_bytes}"
    );

    // And the dense arm should be close to its rows, because there is nothing
    // else in it. Four times the payload allows for the slot vector doubling as
    // it grows and the router's own spine.
    assert!(
        dense_bytes < payload * 4,
        "a dense partition should be mostly rows: {dense_bytes} against {payload} of payload"
    );
}

#[test]
fn an_empty_dense_partition_allocates_almost_nothing() {
    // The sharpest form of the finding: the full shape's cost is paid at
    // creation, before any row exists, so an empty partition is where the gap
    // is widest.
    {
        let warm = DensePartitions::new();
        warm.partition_or_create(0).expect("fresh");
        let warm = FullPartitions::new();
        warm.partition_or_create(0).expect("fresh");
    }

    let (_, dense_bytes) = allocated_by(|| {
        let books = DensePartitions::new();
        for symbol in 0..PARTITIONS {
            books.partition_or_create(symbol).expect("fresh");
        }
        books
    });

    let (_, full_bytes) = allocated_by(|| {
        let books = FullPartitions::new();
        for symbol in 0..PARTITIONS {
            books.partition_or_create(symbol).expect("fresh");
        }
        books
    });

    eprintln!(
        "EMPTY-PARTITION-MEMORY partitions={PARTITIONS}\n\
         \x20 dense={dense_bytes}B ({:.0} B/partition)\n\
         \x20 full={full_bytes}B ({:.0} B/partition)\n\
         \x20 saving={:.1}x",
        dense_bytes as f64 / f64::from(PARTITIONS),
        full_bytes as f64 / f64::from(PARTITIONS),
        full_bytes as f64 / dense_bytes.max(1) as f64,
    );

    assert!(
        full_bytes > dense_bytes * 4,
        "an empty full partition carries apparatus an empty dense one does not: \
         {dense_bytes} against {full_bytes}"
    );
}

#[test]
fn concurrent_allocation_regions_are_independent() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let ready = AtomicUsize::new(0);
    let done = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let measure = |bytes| {
            let (allocation, counted) = allocated_by(|| {
                ready.fetch_add(1, Ordering::SeqCst);
                while ready.load(Ordering::SeqCst) != 2 {
                    std::hint::spin_loop();
                }
                let allocation = vec![0u8; bytes];
                std::hint::black_box(&allocation);
                done.fetch_add(1, Ordering::SeqCst);
                while done.load(Ordering::SeqCst) != 2 {
                    std::hint::spin_loop();
                }
                allocation
            });
            assert_eq!(counted, bytes);
            assert_eq!(allocation.len(), bytes);
        };
        let first = scope.spawn(move || measure(1024));
        let second = scope.spawn(move || measure(4096));
        first.join().unwrap();
        second.join().unwrap();
    });
}

#[test]
fn unwinding_disables_allocation_accounting() {
    let _ = std::panic::catch_unwind(|| allocated_by(|| panic!("end region")));
    ALLOCATED.with(|count| assert_eq!(count.get(), None));
    let (allocation, counted) = allocated_by(|| vec![0u8; 1024]);
    std::hint::black_box(&allocation);
    assert_eq!(counted, 1024);
}
