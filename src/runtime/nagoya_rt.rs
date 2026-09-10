//! The nagoya backend, and the three pool flavors a schema can name.

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::time::Duration;
use std::sync::OnceLock;

use nagoya::Executor;
use st3::fanout::{Pool, StdHost};

use super::flavor::{FLAVOR_COUNT, Flavor, env_override};
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

/// Locality's wake routing, with a worker looking again eight times sooner.
///
/// `backoff_spins: 128`. Buys wake latency and spends CPU; see
/// [`Flavor::LowLatency`] for what has to be reported alongside it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LowLatency;

/// Spread's wake routing, with one long trip to the injector.
///
/// `injector_batch: 32`, for work submitted in chunks.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WideInjector;

impl FlavorMarker for Locality {
    const FLAVOR: Flavor = Flavor::Locality;
}

impl FlavorMarker for Spread {
    const FLAVOR: Flavor = Flavor::Spread;
}

impl FlavorMarker for Throughput {
    const FLAVOR: Flavor = Flavor::Throughput;
}

impl FlavorMarker for LowLatency {
    const FLAVOR: Flavor = Flavor::LowLatency;
}

impl FlavorMarker for WideInjector {
    const FLAVOR: Flavor = Flavor::WideInjector;
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

/// The pool for a flavor, started on first use and shared thereafter.
///
/// # Why an array and not a registry
///
/// This is called from `spawn`, so it runs once per spawned task. It used to
/// build a `Tuning` struct, compare it field by field against
/// `Tuning::locality()`, and then, for anything that was not locality, take a
/// **process-wide mutex and linear-scan a `Vec` comparing `Tuning` structs by
/// value**. Locality returned before the lock and paid none of it.
///
/// That is not a small constant, it is a serialization point, and it fell on
/// precisely the flavors that are supposed to win. Any A/B run through it
/// would have measured the incumbent running free against every challenger
/// through a contended lock, and the conclusion would have come out backwards.
///
/// A fixed array indexed by the discriminant has no lock, no allocation and
/// nothing to compare. When the caller is `NagoyaRt<F>` the index is a
/// compile-time constant, so a warm lookup is one acquire load and a branch,
/// and every flavor pays the same, which is the property the A/B depends on.
///
/// Entries are leaked. There is one per flavor a process actually uses, and a
/// pool whose threads are detached has nothing useful to do with a `Drop`.
static EXECUTORS: [OnceLock<&'static Executor>; FLAVOR_COUNT] = [const { OnceLock::new() }; FLAVOR_COUNT];

#[inline]
fn executor_for(flavor: Flavor) -> &'static Executor {
    EXECUTORS[flavor as usize].get_or_init(|| start_or_share(flavor))
}

/// The flavor a spawn actually runs on, given the one its type names.
///
/// `WT_DEFAULT_RUNTIME` outranks the declared flavor, which is what lets one
/// benchmark binary sweep every flavor with no rebuild. One acquire load and
/// a branch; see [`env_override`] for why the variable is read exactly once.
#[inline]
pub(crate) fn resolved(declared: Flavor) -> Flavor {
    env_override().unwrap_or(declared)
}

/// The pool the **engine's own** background work runs on.
///
/// The persistence worker and the vacuum sweep are the whole of the engine's
/// async spawning; everything else runs inline on the caller's executor. They
/// are not generic over a runtime, so they cannot read a table's declared
/// flavor and instead take the process-level selection: `WT_DEFAULT_RUNTIME`,
/// or locality.
///
/// Routing them matters more than their two call sites suggest. A benchmark
/// that moved only its client tasks to a flavor would leave the engine's
/// flush loop and vacuum sweep on the locality pool, so the two halves of the
/// stack would be on different schedulers contending for the same cores, and
/// the arm would describe a configuration nobody would ship.
#[must_use]
pub fn engine_executor() -> &'static Executor {
    executor_for(resolved(Flavor::Locality))
}

/// The flavor the engine's background work resolved to, for a benchmark to
/// print and record next to its numbers.
///
/// An A/B where one arm silently fell back to the default is the easiest way
/// to publish a wrong table, and it has happened on this project already.
#[must_use]
pub fn engine_flavor() -> Flavor {
    resolved(Flavor::Locality)
}

/// The shared pool for locality, or a fresh one at this flavor's tuning.
///
/// `Tuning::default()` is `Tuning::locality()`, so the process-wide pool is
/// already at that tuning and taking it beats starting a second one. It is
/// not only cheaper: `nagoya::runtime::Runtime` marks its threads as pool
/// workers and `nagoya::task::mark_current` is private, so a pool started from
/// here cannot. **That marker is exactly what makes `local_wakes` do
/// anything.**
///
/// Which is a real limitation, not a footnote. Spread, throughput and
/// wide_injector all set `local_wakes: false`, where a wake takes the injector
/// whether the thread is marked or not, so for those three the pool below
/// behaves identically to one nagoya started itself. `low_latency` does not:
/// it asks for local wakes on a pool that is not the process-wide one, so it
/// runs with local wakes inert until nagoya exposes either `mark_current` or a
/// tuned constructor. It is therefore measured as "locality's routing minus
/// the marker, at a shorter backoff", and a result from it means less than it
/// looks like until that is fixed.
fn start_or_share(flavor: Flavor) -> &'static Executor {
    if flavor == Flavor::Locality {
        return nagoya::runtime::background().executor();
    }
    Box::leak(Box::new(start_pool(flavor)))
}

/// Start a pool at `tuning` and hand back an executor over it.
fn start_pool(flavor: Flavor) -> Executor {
    let workers = workers();
    let host = Arc::new(StdHost::new(workers));
    let pool = Pool::with_tuning(workers, 1024, host, flavor.tuning());
    for id in 0..workers {
        let pool = pool.clone();
        let runner = pool.runner(id);
        std::thread::Builder::new()
            .name(alloc::format!("wt-{}-{id}", flavor.name()))
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
        executor_for(resolved(F::FLAVOR)).spawn(future)
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
