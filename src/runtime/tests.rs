//! One conformance body, run against every [`Runtime`] impl.
//!
//! The macro is the point. Two impls that pass different tests prove nothing
//! about a schema being able to swap them, and the integration lane reuses this
//! same macro rather than writing a third copy.

use alloc::sync::Arc;
/// Only `tokio_block_on` names it, and that is behind the feature.
#[cfg(feature = "tokio-runtime")]
use core::future::Future;
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;

use super::{Runtime, RuntimeJoinHandle, RuntimeNotify, RuntimeRwLock, RuntimeSemaphore, RuntimeSemaphorePermit};

/// Exercises the associated types through the trait only, so a backend that
/// compiles here is one a generic call site can name.
async fn primitives<R: Runtime>() {
    let lock: Arc<R::RwLock<u32>> = Arc::new(RuntimeRwLock::new(7));
    {
        let mut guard = lock.write().await;
        *guard += 1;
    }
    let owned = lock.clone().try_read_owned().expect("nobody holds the lock");
    assert_eq!(*owned, 8);
    // The probe the lock map does while holding its synchronous map guard.
    assert!(lock.try_read().is_some());
    drop(owned);

    let notify = <R::Notify as RuntimeNotify>::new();
    notify.notify_one();
    // A stored permit, so this resolves without a second task.
    notify.notified().await;
    notify.notify_waiters();

    let semaphore = <R::Semaphore as RuntimeSemaphore>::new(0);
    semaphore.add_permits(1);
    semaphore.acquire().await.forget();
}

/// A task that finishes returns its output.
async fn spawn_and_await<R: Runtime>() {
    let handle = R::spawn(async { 41 + 1 });
    assert_eq!(handle.await, Some(42));
}

/// A cancelled task stops, and its handle is consumed rather than left
/// awaitable.
async fn cancel<R: Runtime>() {
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    let handle = R::spawn(async move {
        R::sleep(Duration::from_millis(300)).await;
        flag.store(true, Ordering::Release);
    });
    handle.cancel();
    R::sleep(Duration::from_millis(600)).await;
    assert!(!ran.load(Ordering::Acquire), "the cancelled task ran to completion");
}

/// Sleeping waits at least as long as it was asked to.
async fn sleep<R: Runtime>() {
    let started = std::time::Instant::now();
    R::sleep(Duration::from_millis(50)).await;
    assert!(started.elapsed() >= Duration::from_millis(50));
}

/// A future that never completes times out.
async fn timeout_elapses<R: Runtime>() {
    let result = R::timeout(Duration::from_millis(50), core::future::pending::<()>()).await;
    assert!(result.is_err());
}

/// A future that completes inside its budget is not punished for it.
async fn timeout_returns<R: Runtime>() {
    let result = R::timeout(Duration::from_secs(30), async { 7 }).await;
    assert_eq!(result.ok(), Some(7));
}

/// Yielding resumes.
async fn yields<R: Runtime>() {
    R::yield_now().await;
}

/// Runs every conformance body above against `$runtime`, driving each with
/// `$block_on`.
///
/// `$block_on` is a parameter because entering a runtime is the one thing a
/// runtime cannot abstract over: nagoya has a free `block_on` and tokio needs a
/// `Runtime` value built first.
macro_rules! runtime_conformance_tests {
    ($module:ident, $runtime:ty, $block_on:path) => {
        mod $module {
            #[test]
            fn primitives() {
                $block_on(super::primitives::<$runtime>());
            }

            #[test]
            fn spawn_and_await() {
                $block_on(super::spawn_and_await::<$runtime>());
            }

            #[test]
            fn cancel() {
                $block_on(super::cancel::<$runtime>());
            }

            #[test]
            fn sleep() {
                $block_on(super::sleep::<$runtime>());
            }

            #[test]
            fn timeout_elapses() {
                $block_on(super::timeout_elapses::<$runtime>());
            }

            #[test]
            fn timeout_returns() {
                $block_on(super::timeout_returns::<$runtime>());
            }

            #[test]
            fn yields() {
                $block_on(super::yields::<$runtime>());
            }
        }
    };
}

/// Exported for the integration lane, which runs the same bodies against the
/// backend a generated table selected. Unused inside this module, which is why
/// the allow: the invocations below reach the macro directly.
#[allow(unused_imports)]
pub(crate) use runtime_conformance_tests;

runtime_conformance_tests!(
    nagoya_locality,
    crate::runtime::NagoyaRt<crate::runtime::Locality>,
    nagoya::block_on
);
runtime_conformance_tests!(
    nagoya_spread,
    crate::runtime::NagoyaRt<crate::runtime::Spread>,
    nagoya::block_on
);
runtime_conformance_tests!(
    nagoya_throughput,
    crate::runtime::NagoyaRt<crate::runtime::Throughput>,
    nagoya::block_on
);

#[cfg(feature = "tokio-runtime")]
runtime_conformance_tests!(tokio_backend, crate::runtime::TokioRt, super::tokio_block_on);

/// Tokio has no free `block_on`: a runtime has to exist first, and `spawn`
/// needs it to be the ambient one.
#[cfg(feature = "tokio-runtime")]
fn tokio_block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a tokio runtime")
        .block_on(future)
}
