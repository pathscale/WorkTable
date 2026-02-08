mod map;
mod row_lock;

use std::cell::Cell;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use futures::task::AtomicWaker;
use parking_lot::Mutex;

pub use map::LockMap;
pub use row_lock::{FullRowLock, RowLock};

/// RAII guard that automatically unlocks a [`Lock`] when dropped.
///
/// The [`Lock`] is automatically released when the guard is dropped, or can be
/// explicitly released early using the `unlock()` method.
pub struct LockGuard {
    lock: Option<Arc<Lock>>,
    /// Marker to make this type `!Sync` (but still `Send`)
    _not_sync: PhantomData<Cell<()>>,
}

impl LockGuard {
    /// Explicitly unlocks the lock before the guard is dropped.
    pub fn unlock(mut self) {
        if let Some(lock) = self.lock.take() {
            lock.unlock();
        }
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if let Some(lock) = &self.lock {
            lock.unlock();
        }
    }
}

#[derive(Debug)]
pub struct Lock {
    id: u16,
    locked: Arc<AtomicBool>,
    wakers: Mutex<Vec<Arc<AtomicWaker>>>,
}

impl PartialEq for Lock {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for Lock {}

impl Hash for Lock {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.id, state)
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        self.unlock()
    }
}

impl Lock {
    pub fn new(id: u16) -> Self {
        Self {
            id,
            locked: Arc::new(AtomicBool::from(true)),
            wakers: Mutex::new(vec![]),
        }
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    pub fn unlock(&self) {
        self.locked.store(false, Ordering::Relaxed);
        let guard = self.wakers.lock();
        for w in guard.iter() {
            w.wake()
        }
    }

    pub fn lock(&self) {
        self.locked.store(true, Ordering::Relaxed);
    }

    pub fn is_locked(&self) -> bool {
        self.locked.load(Ordering::Relaxed)
    }

    pub fn wait(&self) -> LockWait {
        let mut guard = self.wakers.lock();
        let waker = Arc::new(AtomicWaker::new());
        guard.push(waker.clone());
        LockWait {
            locked: self.locked.clone(),
            waker,
        }
    }

    /// Creates a [`LockGuard`] that will automatically unlock this lock when
    /// dropped.
    pub fn guard(self: Arc<Self>) -> LockGuard {
        LockGuard {
            lock: Some(self),
            _not_sync: PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct LockWait {
    locked: Arc<AtomicBool>,
    waker: Arc<AtomicWaker>,
}

impl Future for LockWait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.locked.load(Ordering::Acquire) {
            return Poll::Ready(());
        }

        self.waker.register(cx.waker());

        if self.locked.load(Ordering::Acquire) {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::AssertUnwindSafe;

    #[test]
    fn test_unlock_on_drop() {
        let lock = Arc::new(Lock::new(1));
        assert!(lock.is_locked());

        {
            let _guard = lock.clone().guard();
            assert!(lock.is_locked());
        }

        assert!(!lock.is_locked());
    }

    #[test]
    fn test_explicit_unlock() {
        let lock = Arc::new(Lock::new(1));
        assert!(lock.is_locked());

        let guard = lock.clone().guard();
        assert!(lock.is_locked());

        guard.unlock();

        assert!(!lock.is_locked());
    }

    #[test]
    fn test_panic_releases_lock() {
        let lock = Arc::new(Lock::new(1));
        assert!(lock.is_locked());

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = lock.clone().guard();
            panic!("test panic");
        }));

        assert!(result.is_err());

        assert!(!lock.is_locked());
    }

    #[test]
    fn test_multiple_guards_can_be_held() {
        let lock1 = Arc::new(Lock::new(1));
        let lock2 = Arc::new(Lock::new(2));
        let lock3 = Arc::new(Lock::new(3));

        assert!(lock1.is_locked());
        assert!(lock2.is_locked());
        assert!(lock3.is_locked());

        {
            let _guard1 = lock1.clone().guard();
            let _guard2 = lock2.clone().guard();
            let _guard3 = lock3.clone().guard();

            assert!(lock1.is_locked());
            assert!(lock2.is_locked());
            assert!(lock3.is_locked());
        }

        assert!(!lock1.is_locked());
        assert!(!lock2.is_locked());
        assert!(!lock3.is_locked());
    }

    #[test]
    fn test_guard_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<LockGuard>();
    }
}
