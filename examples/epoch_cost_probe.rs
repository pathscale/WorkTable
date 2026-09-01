//! What does a reader-side grace-period pin actually cost, and does it stay
//! flat as readers are added?
//!
//! Decides the replacement design. If a bare `SeqCst` fence is already most of
//! crossbeam's pin cost, then no hand-rolled scheme reaches the 0.71 ns the
//! unpinned borrow used to hit, and the fix has to amortise the pin across a
//! batch rather than make each pin cheaper.

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering, fence};
use std::sync::{Arc, Barrier};
use std::time::Instant;

const ITERS: u64 = 20_000_000;

#[repr(align(128))]
struct Padded(AtomicU64);

fn bench<F: Fn()>(name: &str, threads: usize, f: F)
where
    F: Sync,
{
    let barrier = Arc::new(Barrier::new(threads));
    let f = &f;
    let elapsed = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let barrier = barrier.clone();
                s.spawn(move || {
                    barrier.wait();
                    let start = Instant::now();
                    for _ in 0..ITERS {
                        f();
                    }
                    start.elapsed()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .max()
            .unwrap()
    });
    let ns = elapsed.as_nanos() as f64 / ITERS as f64;
    println!("  {name:<34} {threads} thread(s): {ns:>6.2} ns/op");
}

/// A stable per-thread index into the participant array.
fn slot_index() -> usize {
    thread_local! {
        static IDX: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    }
    static NEXT: AtomicU64 = AtomicU64::new(0);
    IDX.with(|i| match i.get() {
        Some(v) => v,
        None => {
            let v = (NEXT.fetch_add(1, Ordering::Relaxed) % 16) as usize;
            i.set(Some(v));
            v
        }
    })
}

fn main() {
    let global = Box::leak(Box::new(AtomicU64::new(1)));
    let slots: &'static Vec<Padded> = Box::leak(Box::new(
        (0..16).map(|_| Padded(AtomicU64::new(0))).collect::<Vec<_>>(),
    ));

    for threads in [1usize, 2, 4, 8] {
        println!("--- {threads} thread(s) ---");

        // The floor: a relaxed load of a shared, rarely written line.
        bench("relaxed load of global epoch", threads, || {
            black_box(global.load(Ordering::Relaxed));
        });

        // A SeqCst fence alone. Any reader-registration scheme needs this, or
        // the reclaimer can miss a reader that has not yet published its pin.
        bench("SeqCst fence alone", threads, || {
            fence(Ordering::SeqCst);
        });

        // A hand-rolled pin: publish into this thread's own padded line, fence,
        // then unpublish. No shared read-modify-write anywhere.
        // Each thread must own its slot, or this measures false sharing rather
        // than the scheme. The first version of this probe did exactly that and
        // reported 25 ns at eight threads.
        bench("padded store + fence + clear", threads, || {
            let idx = slot_index();
            let e = global.load(Ordering::Relaxed);
            slots[idx].0.store(e | 1, Ordering::Relaxed);
            fence(Ordering::SeqCst);
            black_box(());
            slots[idx].0.store(0, Ordering::Release);
        });

        // What is in the tree today.
        // `LocalHandle` is not `Sync`, so each thread registers its own, which
        // is also how the real code uses it.
        let collector = crossbeam_epoch::Collector::new();
        let collector = &collector;
        bench("crossbeam pin", threads, || {
            thread_local! {
                static H: std::cell::RefCell<Option<crossbeam_epoch::LocalHandle>> =
                    const { std::cell::RefCell::new(None) };
            }
            H.with(|h| {
                let mut h = h.borrow_mut();
                let handle = h.get_or_insert_with(|| collector.register());
                let g = handle.pin();
                black_box(&g);
            });
        });
        println!();
    }
}
