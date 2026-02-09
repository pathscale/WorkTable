mod map;
mod row_lock;

use std::cell::Cell;
use std::fmt::Debug;
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
/// The [`Lock`] is automatically released when the [`LockGuard`] is
/// [`Drop`]ped, or can be explicitly released early using the `unlock()`
/// method.
///
/// The guard will also attempt to remove the lock entry from the map on drop
/// (preventing memory leaks).
pub struct LockGuard<LockType: RowLock, PrimaryKey: Hash + Eq + Debug + Clone> {
    lock: Arc<Lock>,
    lock_map: Arc<LockMap<LockType, PrimaryKey>>,
    primary_key: PrimaryKey,
    /// Marker to make this type ![`Sync`] (but still [`Send`])
    _not_sync: PhantomData<Cell<()>>,
}

impl<LockType, PrimaryKey> LockGuard<LockType, PrimaryKey>
where
    LockType: RowLock,
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    /// Creates a new [`LockGuard`] that will clean up the [`Lock`] entry from
    /// the [`LockMap`] on [`Drop`].
    pub fn new(
        lock: Arc<Lock>,
        lock_map: Arc<LockMap<LockType, PrimaryKey>>,
        primary_key: PrimaryKey,
    ) -> Self {
        Self {
            lock,
            lock_map,
            primary_key,
            _not_sync: PhantomData,
        }
    }

    /// Explicitly unlocks the [`Lock`] before the [`LockGuard`] is [`Drop`]ped.
    pub fn unlock(self) {
        self.lock.unlock();
        self.lock_map.remove_with_lock_check(&self.primary_key);
    }
}

impl<LockType, PrimaryKey> Drop for LockGuard<LockType, PrimaryKey>
where
    LockType: RowLock,
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    fn drop(&mut self) {
        self.lock.unlock();
        self.lock_map.remove_with_lock_check(&self.primary_key);
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
        let lock_map: Arc<LockMap<FullRowLock, u64>> = Arc::new(LockMap::default());
        let pk = 1u64;
        assert!(lock.is_locked());

        {
            let _guard = LockGuard::<FullRowLock, u64>::new(lock.clone(), lock_map.clone(), pk);
            assert!(lock.is_locked());
        }

        assert!(!lock.is_locked());
    }

    #[test]
    fn test_explicit_unlock() {
        let lock = Arc::new(Lock::new(1));
        let lock_map: Arc<LockMap<FullRowLock, u64>> = Arc::new(LockMap::default());
        let pk = 1u64;
        assert!(lock.is_locked());

        let guard = LockGuard::<FullRowLock, u64>::new(lock.clone(), lock_map.clone(), pk);
        assert!(lock.is_locked());

        guard.unlock();

        assert!(!lock.is_locked());
    }

    #[test]
    fn test_panic_releases_lock() {
        let lock = Arc::new(Lock::new(1));
        let lock_map: Arc<LockMap<FullRowLock, u64>> = Arc::new(LockMap::default());
        let pk = 1u64;
        assert!(lock.is_locked());

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = LockGuard::<FullRowLock, u64>::new(lock.clone(), lock_map.clone(), pk);
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
        let lock_map: Arc<LockMap<FullRowLock, u64>> = Arc::new(LockMap::default());

        assert!(lock1.is_locked());
        assert!(lock2.is_locked());
        assert!(lock3.is_locked());

        {
            let _guard1 = LockGuard::<FullRowLock, u64>::new(lock1.clone(), lock_map.clone(), 1u64);
            let _guard2 = LockGuard::<FullRowLock, u64>::new(lock2.clone(), lock_map.clone(), 2u64);
            let _guard3 = LockGuard::<FullRowLock, u64>::new(lock3.clone(), lock_map.clone(), 3u64);

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
        // LockGuard is Send if LockType and PrimaryKey are Send
        assert_send::<LockGuard<FullRowLock, u64>>();
    }

    #[tokio::test]
    async fn test_lock_cleanup_on_guard_drop() {
        use crate::lock::FullRowLock;
        use crate::lock::RowLock;

        let lock_map: Arc<LockMap<FullRowLock, u64>> = Arc::new(LockMap::default());
        let pk = 42u64;

        // Create and insert a lock
        let (lock_type, lock) = FullRowLock::with_lock(lock_map.next_id());
        let rw_lock = Arc::new(tokio::sync::RwLock::new(lock_type));
        lock_map.insert(pk, rw_lock);

        // Verify the lock is in the map
        assert!(lock_map.get(&pk).is_some());

        // Create a guard and drop it
        {
            let _guard = LockGuard::new(lock, lock_map.clone(), pk);
        }

        // Verify the lock entry was removed from the map
        assert!(lock_map.get(&pk).is_none());
    }
}
