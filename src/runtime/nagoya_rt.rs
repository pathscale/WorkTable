//! The nagoya backend, and the three pool flavors a schema can name.

use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::time::Duration;
use std::sync::{Mutex, OnceLock};

use nagoya::Executor;
use st3::fanout::{Pool, StdHost, Tuning};

use super::{
    Elapsed, FlavorMarker, Runtime, RuntimeJoinHandle, RuntimeNotified, RuntimeNotify, RuntimeRwLock, RuntimeSemaphore,
    RuntimeSemaphorePermit,
};

/// Keep a woken task on the worker that woke it.
///
/// The default, and what `nagoya::runtime::background()` already runs with.
/// For work whose wakes are a chain: an update path handing a row lock to its
/// successor wants the lines the releasing worker just touched.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Locality;

/// Send every wake to the injector, where any worker can take it.
///
/// For work whose wakes are independent, which is what read-mostly and
/// insert-mostly tables look like.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Spread;

/// Fewer, larger trips to the injector.
///
/// For a firehose of short independent operations submitted from outside the
/// pool, where the trip to the shared queue is the cost.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Throughput;

impl FlavorMarker for Locality {
    fn tuning() -> Tuning {
        Tuning::locality()
    }
}

impl FlavorMarker for Spread {
    fn tuning() -> Tuning {
        Tuning::spread()
    }
}

impl FlavorMarker for Throughput {
    fn tuning() -> Tuning {
        Tuning::throughput()
    }
}

/// The nagoya backend, at one of the [`FlavorMarker`] tunings.
///
/// This is a type-level selection and never a value: every [`Runtime`] method
/// is associated, so `NagoyaRt<Spread>` appears in a signature and nowhere
/// else.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NagoyaRt<F: FlavorMarker>(PhantomData<fn() -> F>);

/// Threads for a flavor's pool.
///
/// The machine's parallelism, which is what `nagoya::runtime::background` uses
/// and what `tokio::spawn` gave the callers this replaces. Changing the count
/// and the tuning in one step makes any measurement of the tuning unreadable.
fn workers() -> usize {
    std::thread::available_parallelism().map_or(2, core::num::NonZeroUsize::get)
}

/// One started pool per distinct tuning, plus the shared one for the default.
///
/// # Why the registry, rather than a `OnceLock` per flavor
///
/// A `static` inside a generic function is shared across every instantiation
/// of that function, so `NagoyaRt::<Spread>` and `NagoyaRt::<Throughput>`
/// would race for the same slot and whichever ran first would decide the
/// tuning for both. Keying on the tuning itself is correct for any
/// [`FlavorMarker`], including one this crate did not write.
///
/// Entries are leaked. There is one per distinct tuning a process uses, which
/// is three at most today, and a pool whose threads are detached has nothing
/// useful to do with a `Drop` anyway.
fn executor_for(tuning: Tuning) -> &'static Executor {
    // `Tuning::default()` is `Tuning::locality()`, so the shared pool is
    // already at that tuning. Taking it rather than starting a fourth pool is
    // not only cheaper: `nagoya::runtime::Runtime` marks its threads as pool
    // workers, and `nagoya::task::mark_current` is private, so a pool started
    // from here cannot. That marker is exactly what makes `local_wakes` do
    // anything, and `Tuning::locality` is the only one of the three that turns
    // it on. Spread and throughput both set it to `false`, where a wake takes
    // the injector whether the thread is marked or not, so for those two the
    // pool below behaves identically to one nagoya started itself.
    if tuning == Tuning::locality() {
        return nagoya::runtime::background().executor();
    }

    static POOLS: OnceLock<Mutex<Vec<(Tuning, &'static Executor)>>> = OnceLock::new();
    let pools = POOLS.get_or_init(|| Mutex::new(Vec::new()));
    let mut pools = pools
        .lock()
        .expect("the pool registry holds no state a panic could corrupt");
    if let Some((_, executor)) = pools.iter().find(|(known, _)| *known == tuning) {
        return executor;
    }
    let executor: &'static Executor = Box::leak(Box::new(start_pool(tuning)));
    pools.push((tuning, executor));
    executor
}

/// Start a pool at `tuning` and hand back an executor over it.
fn start_pool(tuning: Tuning) -> Executor {
    let workers = workers();
    let host = Arc::new(StdHost::new(workers));
    let pool = Pool::with_tuning(workers, 1024, host, tuning);
    for id in 0..workers {
        let pool = pool.clone();
        let runner = pool.runner(id);
        std::thread::Builder::new()
            .name(alloc::format!("worktable-rt-{id}"))
            .spawn(move || {
                let _ = pool.run(runner);
            })
            .expect("a runtime thread");
    }
    Executor::new(pool)
}

impl<F: FlavorMarker> Runtime for NagoyaRt<F> {
    type RwLock<T: Send + Sync + 'static> = nagoya::sync::RwLock<T>;
    type Notify = nagoya::sync::Notify;
    type Semaphore = nagoya::sync::Semaphore;
    type JoinHandle<T: Send + 'static> = nagoya::JoinHandle<T>;

    fn spawn<Fut>(future: Fut) -> Self::JoinHandle<Fut::Output>
    where
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        executor_for(F::tuning()).spawn(future)
    }

    fn sleep(duration: Duration) -> impl Future<Output = ()> + Send {
        nagoya::sleep(duration)
    }

    fn timeout<Fut>(duration: Duration, future: Fut) -> impl Future<Output = Result<Fut::Output, Elapsed>> + Send
    where
        Fut: Future + Send,
    {
        nagoya::timeout(duration, future)
    }

    fn yield_now() -> impl Future<Output = ()> + Send {
        nagoya::yield_now()
    }
}

impl<T: Send + Sync + 'static> RuntimeRwLock<T> for nagoya::sync::RwLock<T> {
    type ReadGuard<'a> = nagoya::sync::RwLockReadGuard<'a, T>;
    type WriteGuard<'a> = nagoya::sync::RwLockWriteGuard<'a, T>;
    type OwnedReadGuard = nagoya::sync::OwnedRwLockReadGuard<T>;

    fn new(value: T) -> Self {
        nagoya::sync::RwLock::new(value)
    }

    fn write(&self) -> impl Future<Output = Self::WriteGuard<'_>> + Send {
        nagoya::sync::RwLock::write(self)
    }

    fn try_read(&self) -> Option<Self::ReadGuard<'_>> {
        nagoya::sync::RwLock::try_read(self)
    }

    fn try_read_owned(self: Arc<Self>) -> Option<Self::OwnedReadGuard> {
        nagoya::sync::RwLock::try_read_owned(self)
    }
}

impl RuntimeNotify for nagoya::sync::Notify {
    type Notified<'a> = nagoya::sync::Notified<'a>;

    fn new() -> Self {
        nagoya::sync::Notify::new()
    }

    fn notify_one(&self) {
        nagoya::sync::Notify::notify_one(self);
    }

    fn notify_waiters(&self) {
        nagoya::sync::Notify::notify_waiters(self);
    }

    fn notified(&self) -> Self::Notified<'_> {
        nagoya::sync::Notify::notified(self)
    }
}

impl RuntimeNotified for nagoya::sync::Notified<'_> {
    fn enable(self: Pin<&mut Self>) -> bool {
        nagoya::sync::Notified::enable(self)
    }
}

impl RuntimeSemaphore for nagoya::sync::Semaphore {
    type Permit<'a> = nagoya::sync::SemaphorePermit<'a>;

    fn new(permits: usize) -> Self {
        nagoya::sync::Semaphore::new(permits)
    }

    fn add_permits(&self, permits: usize) {
        nagoya::sync::Semaphore::add_permits(self, permits);
    }

    fn acquire(&self) -> impl Future<Output = Self::Permit<'_>> + Send {
        nagoya::sync::Semaphore::acquire(self)
    }
}

impl RuntimeSemaphorePermit for nagoya::sync::SemaphorePermit<'_> {
    fn forget(self) {
        nagoya::sync::SemaphorePermit::forget(self);
    }
}

impl<T: Send + 'static> RuntimeJoinHandle<T> for nagoya::JoinHandle<T> {
    fn cancel(self) {
        nagoya::JoinHandle::cancel(self);
    }

    fn is_finished(&self) -> bool {
        nagoya::JoinHandle::is_finished(self)
    }
}
