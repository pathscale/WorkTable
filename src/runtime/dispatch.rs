//! Owned query dispatch without borrowed tasks or blocking a pool worker.

use super::{Profile, Runtime, RuntimeJoinHandle};
use crate::WorkTableError;
use alloc::{boxed::Box, sync::Arc};
use core::{
    future::{Future, poll_fn},
    pin::Pin,
};
use parking_lot::Mutex;

/// A type-erased submission function retained by a select plan.
pub type Dispatch = fn(Box<dyn FnOnce() + Send>) -> Pin<Box<dyn Future<Output = Result<(), WorkTableError>> + Send>>;

struct CancelOnDrop<R: Runtime, T: Send + 'static>(Option<R::JoinHandle<T>>);
impl<R: Runtime, T: Send + 'static> Drop for CancelOnDrop<R, T> {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.cancel();
        }
    }
}

/// Await an owned task. Dropping the wait cancels the task at its next suspension.
/// Synchronous work already running is allowed to finish.
pub async fn run_on<R, F>(future: F) -> Result<F::Output, WorkTableError>
where
    R: Runtime,
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    R::JoinHandle<F::Output>: Unpin,
{
    let mut guard = CancelOnDrop::<R, F::Output>(Some(R::spawn(future)));
    let result = poll_fn(|cx| Pin::new(guard.0.as_mut().expect("task present until completion")).poll(cx)).await;
    guard.0.take();
    result.ok_or(WorkTableError::RuntimeCancelled)
}

/// Dispatch through a named profile, preserving its existing backend identity.
pub async fn run_profile<P, F>(future: F) -> Result<F::Output, WorkTableError>
where
    P: Profile,
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    <P::Backend as Runtime>::JoinHandle<F::Output>: Unpin,
{
    run_on::<P::Backend, F>(future).await
}

/// The standard dispatcher for a concrete backend.
pub fn dispatcher<R>(work: Box<dyn FnOnce() + Send>) -> Pin<Box<dyn Future<Output = Result<(), WorkTableError>> + Send>>
where
    R: Runtime,
    R::JoinHandle<()>: Unpin,
{
    Box::pin(run_on::<R, _>(async move {
        work();
    }))
}

/// Execute owned CPU work through a saved profile dispatcher.
pub async fn run_owned<T: Send + 'static>(
    dispatch: Dispatch,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, WorkTableError> {
    let result = Arc::new(Mutex::new(None));
    let output = result.clone();
    dispatch(Box::new(move || {
        *output.lock() = Some(work());
    }))
    .await?;
    result.lock().take().ok_or(WorkTableError::RuntimeCancelled)
}
