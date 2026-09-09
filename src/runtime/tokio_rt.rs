//! The tokio backend, behind the `tokio-runtime` feature.
//!
//! # Why this is optional and off
//!
//! Getting tokio out of the normal dependency graph was the whole of the work
//! this builds on: it used to arrive through six `tokio::` paths that
//! `worktable!` emitted into consumer crates, which made a runtime part of the
//! macro's contract for every consumer whether they ran one or not. Adding it
//! back unconditionally would undo that. `cargo tree -e normal -i tokio` prints
//! nothing in the default feature set, and that is the check.
//!
//! # What selecting it costs
//!
//! [`TokioRt::spawn`] needs an ambient tokio runtime and panics without one,
//! where the nagoya backend starts its own threads on first use. That is
//! tokio's shape, not something this wrapper can paper over.

use alloc::sync::Arc;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;

use super::{
    Elapsed, Runtime, RuntimeJoinHandle, RuntimeNotified, RuntimeNotify, RuntimeRwLock, RuntimeSemaphore,
    RuntimeSemaphorePermit,
};

/// The tokio backend.
///
/// Unflavored: tokio's scheduler exposes no equivalent of `ps-st3`'s
/// [`Tuning`](super::Tuning), which is why the schema grammar accepts
/// `runtime: tokio` and rejects `runtime: tokio(spread)`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokioRt;

impl Runtime for TokioRt {
    type RwLock<T: Send + Sync + 'static> = tokio::sync::RwLock<T>;
    type Notify = tokio::sync::Notify;
    type Semaphore = tokio::sync::Semaphore;
    type JoinHandle<T: Send + 'static> = TokioJoinHandle<T>;

    fn spawn<F>(future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        TokioJoinHandle(tokio::spawn(future))
    }

    fn sleep(duration: Duration) -> impl Future<Output = ()> + Send {
        tokio::time::sleep(duration)
    }

    async fn timeout<F>(duration: Duration, future: F) -> Result<F::Output, Elapsed>
    where
        F: Future + Send,
    {
        tokio::time::timeout(duration, future).await.map_err(|_| Elapsed)
    }

    fn yield_now() -> impl Future<Output = ()> + Send {
        tokio::task::yield_now()
    }
}

/// A `tokio::task::JoinHandle` wearing nagoya's shape.
///
/// Two things change. Cancellation consumes the handle, because nagoya's does
/// and because `abort(&self)` invites awaiting a handle that will never
/// produce a value. And the output is `Option<T>`, so `None` is cancellation;
/// a panic in the task is resumed here rather than handed back as a value,
/// which is what nagoya does by letting it unwind through the await.
#[derive(Debug)]
pub struct TokioJoinHandle<T>(tokio::task::JoinHandle<T>);

impl<T> Future for TokioJoinHandle<T> {
    type Output = Option<T>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(output)) => Poll::Ready(Some(output)),
            Poll::Ready(Err(error)) if error.is_cancelled() => Poll::Ready(None),
            Poll::Ready(Err(error)) => std::panic::resume_unwind(error.into_panic()),
        }
    }
}

impl<T: Send + 'static> RuntimeJoinHandle<T> for TokioJoinHandle<T> {
    fn cancel(self) {
        self.0.abort();
    }

    fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}

impl<T: Send + Sync + 'static> RuntimeRwLock<T> for tokio::sync::RwLock<T> {
    type ReadGuard<'a> = tokio::sync::RwLockReadGuard<'a, T>;
    type WriteGuard<'a> = tokio::sync::RwLockWriteGuard<'a, T>;
    type OwnedReadGuard = tokio::sync::OwnedRwLockReadGuard<T>;

    fn new(value: T) -> Self {
        tokio::sync::RwLock::new(value)
    }

    fn write(&self) -> impl Future<Output = Self::WriteGuard<'_>> + Send {
        tokio::sync::RwLock::write(self)
    }

    fn try_read(&self) -> Option<Self::ReadGuard<'_>> {
        tokio::sync::RwLock::try_read(self).ok()
    }

    fn try_read_owned(self: Arc<Self>) -> Option<Self::OwnedReadGuard> {
        tokio::sync::RwLock::try_read_owned(self).ok()
    }
}

impl RuntimeNotify for tokio::sync::Notify {
    type Notified<'a> = tokio::sync::futures::Notified<'a>;

    fn new() -> Self {
        tokio::sync::Notify::new()
    }

    fn notify_one(&self) {
        tokio::sync::Notify::notify_one(self);
    }

    fn notify_waiters(&self) {
        tokio::sync::Notify::notify_waiters(self);
    }

    fn notified(&self) -> Self::Notified<'_> {
        tokio::sync::Notify::notified(self)
    }
}

impl RuntimeNotified for tokio::sync::futures::Notified<'_> {
    fn enable(self: Pin<&mut Self>) -> bool {
        tokio::sync::futures::Notified::enable(self)
    }
}

impl RuntimeSemaphore for tokio::sync::Semaphore {
    type Permit<'a> = tokio::sync::SemaphorePermit<'a>;

    fn new(permits: usize) -> Self {
        tokio::sync::Semaphore::new(permits)
    }

    fn add_permits(&self, permits: usize) {
        tokio::sync::Semaphore::add_permits(self, permits);
    }

    /// nagoya's semaphore has no closed state, so the normalised signature has
    /// no error to carry. Nothing in this crate closes a semaphore, and `close`
    /// is not on [`RuntimeSemaphore`], so the only way to reach the panic is a
    /// caller going past the trait to the concrete tokio type.
    async fn acquire(&self) -> Self::Permit<'_> {
        tokio::sync::Semaphore::acquire(self)
            .await
            .expect("nothing closes a worktable semaphore")
    }
}

impl RuntimeSemaphorePermit for tokio::sync::SemaphorePermit<'_> {
    fn forget(self) {
        tokio::sync::SemaphorePermit::forget(self);
    }
}
