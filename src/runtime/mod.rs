//! The runtime a table's async work runs on.
//!
//! # Why a trait, when there is only one runtime in the graph
//!
//! Every async primitive this crate touches came from `nagoya::sync` and
//! `nagoya::time` after the move off tokio, which is a hardcoded choice rather
//! than a made one. [`Runtime`] turns it into a type parameter so a schema can
//! name a backend, and so the tokio comparison arm is something a build can
//! select rather than something a fork has to carry.
//!
//! # The surface is exactly what this crate uses
//!
//! The helper traits below are not a general async abstraction. They were
//! derived by reading every `nagoya::` path in `src/`, and each method has at
//! least one call site today:
//!
//! ```text
//! RwLock::new                 src/lock/map.rs, src/lock/mod.rs
//! RwLock::write().await       src/lock/map.rs, src/table/vacuum/vacuum.rs,
//!                             src/in_memory/empty_link_registry.rs, codegen
//! RwLock::try_read            src/lock/map.rs
//! RwLock::try_read_owned      src/in_memory/empty_link_registry.rs
//! Notify::new / Default       src/persistence/task.rs, empty_link_registry.rs
//! Notify::notify_one          src/persistence/task.rs, empty_link_registry.rs
//! Notify::notify_waiters      src/persistence/task.rs
//! Notified::enable            src/persistence/task.rs
//! Semaphore::new              src/persistence/task.rs
//! Semaphore::add_permits      src/persistence/task.rs
//! Semaphore::acquire          src/persistence/task.rs
//! SemaphorePermit::forget     src/persistence/task.rs
//! JoinHandle await            src/persistence/task.rs
//! JoinHandle::cancel          src/persistence/task.rs, tests/worktable/
//! JoinHandle::is_finished     src/persistence/task.rs, src/table/vacuum/
//! spawn                       src/persistence/task.rs, src/table/vacuum/manager.rs
//! sleep / timeout / yield_now src/persistence/task.rs, src/table/vacuum/
//! ```
//!
//! `RwLock::read().await` is **deliberately absent**: every `.read()` in this
//! crate is on a `parking_lot` lock, not an async one, and the async row lock
//! is only ever taken exclusively. Adding it is a three-line change in each of
//! the two impls if a call site ever appears.
//!
//! # Why the module needs `std`
//!
//! [`Runtime::spawn`] is the reason the trait exists, and spawning needs
//! threads. `nagoya`'s own `runtime` module is `std`-gated for the same
//! reason. A `no_std` build of this crate has neither persistence nor vacuum,
//! which are the only two things here that spawn.

use alloc::sync::Arc;
use core::future::Future;
use core::ops::{Deref, DerefMut};
use core::pin::Pin;
use core::time::Duration;

/// What a [`Runtime::timeout`] returns when the future did not finish in time.
///
/// Normalised on nagoya's, which is a unit struct. See [`TokioRt`] for what
/// that costs the other impl.
pub use nagoya::Elapsed;
/// The idle policy a nagoya pool runs with.
///
/// Through nagoya rather than from `ps-st3` directly. Selecting an idle
/// policy is a nagoya-level decision, and naming the type through the crate
/// that owns that decision is what lets this crate stop depending on the
/// queues underneath it for one struct.
pub use nagoya::Tuning;

/// The backends themselves need `std`, because the only reason a backend
/// exists is to spawn and spawning needs threads. The trait, the flavor
/// markers' contract and the profile machinery do not, so they stay available
/// to a `no_std` build: a table that never spawns still names its runtime in
/// types that have to resolve.
#[cfg(feature = "std")]
mod nagoya_rt;

mod flavor;
mod profile;
#[cfg(all(feature = "std", feature = "tokio-runtime"))]
mod tokio_rt;

pub use flavor::{FLAVOR_COUNT, Flavor, RESERVED};
#[cfg(feature = "std")]
pub use flavor::{describe_tuning, env_override, parse_selection, tuning_overrides};
#[cfg(feature = "std")]
pub use nagoya_rt::{
    Locality, LowLatency, NagoyaRt, SharedSlot, Spread, Throughput, WideInjector, engine_executor, engine_flavor,
    executor_for_flavor,
};

pub use profile::{Profile, RuntimeUnpinned, TableRuntime};
#[cfg(all(feature = "std", feature = "tokio-runtime"))]
pub use tokio_rt::{TokioJoinHandle, TokioRt};

#[cfg(all(test, feature = "std"))]
mod tests;

/// An async runtime, named by a table rather than assumed.
///
/// Every method is associated rather than taken on `&self`, because a backend
/// is a type in a schema and never a value anyone holds. The associated types
/// carry their own helper trait, since an associated type with no bound is a
/// type nothing can be called on.
pub trait Runtime: Send + Sync + 'static {
    /// The async reader-writer lock guarding one row.
    type RwLock<T: Send + Sync + 'static>: RuntimeRwLock<T>;
    /// The wake primitive the persistence worker and the vacuum share.
    type Notify: RuntimeNotify;
    /// The counting gate the persistence tests step the worker with.
    type Semaphore: RuntimeSemaphore;
    /// A handle to a spawned task.
    type JoinHandle<T: Send + 'static>: RuntimeJoinHandle<T>;

    /// Run `future` on this runtime's threads.
    fn spawn<F>(future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;

    /// A future that is ready once `duration` has passed.
    fn sleep(duration: Duration) -> impl Future<Output = ()> + Send;

    /// Run `future`, giving up after `duration`.
    fn timeout<F>(duration: Duration, future: F) -> impl Future<Output = Result<F::Output, Elapsed>> + Send
    where
        F: Future + Send;

    /// Hand the scheduler a chance to run something else.
    fn yield_now() -> impl Future<Output = ()> + Send;
}

/// The async reader-writer lock surface this crate uses.
pub trait RuntimeRwLock<T>: Send + Sync + 'static
where
    T: Send + Sync + 'static,
{
    /// A read guard borrowed from the lock.
    type ReadGuard<'a>: Deref<Target = T> + Send
    where
        Self: 'a;
    /// A write guard borrowed from the lock.
    type WriteGuard<'a>: DerefMut<Target = T> + Send
    where
        Self: 'a;
    /// A read guard that owns its `Arc` instead of borrowing.
    ///
    /// Public API depends on this one: `EmptyLinkRegistry`'s `PoppedLink` is a
    /// pair whose second element is an owned read guard held across awaits.
    type OwnedReadGuard: Deref<Target = T> + Send + 'static;

    /// A lock holding `value`, unlocked.
    fn new(value: T) -> Self
    where
        Self: Sized;

    /// Wait for exclusive access.
    fn write(&self) -> impl Future<Output = Self::WriteGuard<'_>> + Send;

    /// Take shared access if it is free, without waiting.
    ///
    /// The lock-map cleanup path probes with this while holding a synchronous
    /// map guard, which is why it must not be a future.
    fn try_read(&self) -> Option<Self::ReadGuard<'_>>;

    /// Take shared access if it is free, keeping the `Arc` alive.
    fn try_read_owned(self: Arc<Self>) -> Option<Self::OwnedReadGuard>;
}

/// The wake primitive this crate uses.
pub trait RuntimeNotify: Default + Send + Sync + 'static {
    /// The future [`RuntimeNotify::notified`] returns.
    type Notified<'a>: RuntimeNotified
    where
        Self: 'a;

    /// A notify with no stored permit.
    fn new() -> Self
    where
        Self: Sized;

    /// Wake one waiter, storing a permit if there is none.
    fn notify_one(&self);

    /// Wake every current waiter, storing no permit.
    fn notify_waiters(&self);

    /// A future that resolves on the next notification.
    fn notified(&self) -> Self::Notified<'_>;
}

/// The future a [`RuntimeNotify`] hands out.
///
/// `enable` is here because `notify_waiters` stores no permit: a waiter that
/// reads state before registering can lose a transition that lands between the
/// read and the first poll. Both backends spell the fix the same way.
pub trait RuntimeNotified: Future<Output = ()> + Send {
    /// Register this waiter now, and report whether a notification is already
    /// waiting for it.
    fn enable(self: Pin<&mut Self>) -> bool;
}

/// The counting semaphore this crate uses.
pub trait RuntimeSemaphore: Send + Sync + 'static {
    /// A held permit.
    type Permit<'a>: RuntimeSemaphorePermit
    where
        Self: 'a;

    /// A semaphore starting with `permits` available.
    fn new(permits: usize) -> Self
    where
        Self: Sized;

    /// Hand the semaphore `permits` more than it was created with.
    fn add_permits(&self, permits: usize);

    /// Wait for a permit.
    ///
    /// Normalised on nagoya's shape, which has no closed state and so returns
    /// the permit rather than a `Result`. See [`TokioRt`] for the adaptation.
    fn acquire(&self) -> impl Future<Output = Self::Permit<'_>> + Send;
}

/// A permit taken from a [`RuntimeSemaphore`].
pub trait RuntimeSemaphorePermit {
    /// Drop the permit without returning it, shrinking the semaphore by one.
    fn forget(self);
}

/// A handle to a spawned task.
///
/// Two deltas between the backends are normalised here, both on nagoya's
/// shape. Cancellation is `cancel(self)`, not tokio's `abort(&self)`, so a
/// cancelled handle cannot be awaited afterwards. Awaiting yields `Option<T>`,
/// not tokio's `Result<T, JoinError>`, so `None` means cancelled and a panic in
/// the task unwinds through the await rather than arriving as a value.
pub trait RuntimeJoinHandle<T>: Future<Output = Option<T>> + Send + Sized + 'static {
    /// Stop the task at its next suspension point and throw away its output.
    fn cancel(self);

    /// Whether the task has finished, without waiting for it.
    fn is_finished(&self) -> bool;
}

/// A nagoya pool tuning, named as a type so a schema can select one.
///
/// The three markers below are the whole set. See [`Tuning`] for what each one
/// trades, and note that the numbers behind them were measured on one machine
/// against one workload shape.
pub trait FlavorMarker: Send + Sync + 'static {
    /// Which pool this flavor selects, as one byte.
    ///
    /// **This is what the hot path reads.** `spawn` resolves a flavor to a
    /// pool on every call, so the flavor's representation is a per-task cost.
    /// Because `F` is a type parameter the discriminant is a compile-time
    /// constant, the array index folds, and a warm lookup is one acquire load
    /// and a branch.
    const FLAVOR: Flavor;

    /// The idle policy the pool for this flavor runs with.
    ///
    /// Called once, to build the pool, and never on the hot path. Defaulted
    /// through [`Flavor::tuning`] so the registry is the only place a flavor's
    /// numbers are written down.
    fn tuning() -> Tuning {
        Self::FLAVOR.tuning()
    }
}
