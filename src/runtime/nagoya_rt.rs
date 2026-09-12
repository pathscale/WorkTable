//! The Nagoya backend and its six selectable pool flavors.

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::time::Duration;
use std::sync::OnceLock;

use nagoya::Executor;

use super::flavor::{FLAVOR_COUNT, Flavor, env_override};
use super::{
    Elapsed, FlavorMarker, Runtime, RuntimeJoinHandle, RuntimeNotified, RuntimeNotify, RuntimeRwLock, RuntimeSemaphore,
    RuntimeSemaphorePermit,
};

/// Keep a woken task on the worker that woke it.
///
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

/// Locality routing with a longer idle-spin budget before parking.
///
/// Runs 512 empty search rounds of 128 spin hints rather than the default four.
/// Compare CPU use between arrivals alongside latency; see [`Flavor::LowLatency`].
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

/// Locality's routing, sharing displaced work after the first private inbox job.
///
/// See [`Flavor::SharedSlot`] for the two failure modes this sits between.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SharedSlot;

impl FlavorMarker for SharedSlot {
    const FLAVOR: Flavor = Flavor::SharedSlot;
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
    // `WT_RUNTIME_WORKERS` overrides it, read once for the same reason
    // `WT_DEFAULT_RUNTIME` is: this is on the pool-construction path, and a
    // sweep over worker counts should not need a rebuild per arm.
    //
    // The default is the machine's parallelism, which is what
    // `nagoya::runtime::background` uses and what `tokio::spawn` gave the
    // callers this replaces. Changing the count and the tuning in one step
    // makes any measurement of the tuning unreadable, so the count is a knob
    // rather than something a flavor sets.
    //
    // Worth sweeping on a heterogeneous machine: `available_parallelism`
    // counts efficiency cores, so on a 12P + 4E part it starts four workers
    // that drain their queues substantially slower than the other twelve.
    static WORKERS: OnceLock<usize> = OnceLock::new();
    *WORKERS.get_or_init(|| {
        std::env::var("WT_RUNTIME_WORKERS")
            .ok()
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .filter(|count| *count > 0)
            .unwrap_or_else(|| std::thread::available_parallelism().map_or(2, core::num::NonZeroUsize::get))
            // Nagoya's worker membership is represented by one `usize` bitset.
            // A larger count shifts past that bitset while the pool starts.
            .min(usize::BITS as usize)
    })
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
    EXECUTORS[flavor as usize].get_or_init(|| start_pool(flavor))
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
    executor_for(engine_flavor())
}

/// The shared executor for one named flavor, started on first use.
///
/// This Rust callsite lets a caller submit owned work to a selected pool. A
/// cross-pool submission uses the injector and may wake another worker; include
/// dispatch cost when comparing it with inline work. Generated query profiles
/// and runtime-annotated mutations expose their own owned execution contracts.
#[must_use]
pub fn executor_for_flavor(flavor: Flavor) -> &'static Executor {
    executor_for(flavor)
}

/// The flavor the engine's background work resolved to, for a benchmark to
/// print and record next to its numbers.
///
/// An A/B where one arm silently fell back to the default is the easiest way
/// to publish a wrong table, and it has happened on this project already.
#[must_use]
pub fn engine_flavor() -> Flavor {
    resolved(Flavor::default())
}

/// A pool at this flavor's tuning, with its threads marked as pool workers.
///
/// # Why every flavor gets its own pool, including the default
///
/// It would be cheaper for locality to take `nagoya::runtime::background()`,
/// which is already at that tuning, and that is what this did. It is also
/// what made a flavor comparison unreadable: the shared pool is started by
/// nagoya, which sizes it from `available_parallelism`, while every other
/// flavor got a pool started here. Two arms that differ in who started the
/// threads are not two tunings, they are two configurations, and the tuning
/// is only one of the differences between them.
///
/// Building all of them the same way costs one extra pool in a process that
/// also calls `nagoya::spawn` directly, and buys arms that differ in exactly
/// the thing being measured.
fn start_pool(flavor: Flavor) -> &'static Executor {
    // `Runtime::with_tuning` rather than a hand-built `Pool`, and this is the
    // whole reason nagoya grew that constructor. `nagoya::task::mark_current`
    // is private, and that marker is the only thing that makes `local_wakes`
    // do anything: without it a wake takes the injector whatever the tuning
    // says. A pool built here by hand therefore ran every locality-flavored
    // tuning as if it were spread, silently, which is how `low_latency` would
    // have been measured as a backoff change with its routing quietly
    // disabled.
    let runtime = Box::leak(Box::new(nagoya::runtime::Runtime::with_tuning(
        workers(),
        flavor.tuned(),
        "wt",
    )));
    runtime.executor()
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

impl<F: FlavorMarker, G: FlavorMarker> super::RuntimeCompatibleWith<NagoyaRt<G>> for NagoyaRt<F> {}
