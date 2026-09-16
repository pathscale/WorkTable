use alloc::vec::Vec;
mod map;
mod row_lock;

use alloc::sync::Arc;
use core::cell::Cell;
use core::fmt::Debug;
use core::future::Future;
use core::hash::{Hash, Hasher};
use core::marker::PhantomData;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll};

use futures::task::AtomicWaker;
use parking_lot::Mutex;

pub use map::{LockAcquirer, LockMap, MutationGuard};
pub use row_lock::{FullRowLock, RowLock};

/// Maximum number of spin iterations before falling back to async waiting.
const MAX_SPINS: u32 = 12;

/// RAII guard that automatically unlocks a [`Lock`] when dropped.
///
/// The [`Lock`] is automatically released when the [`LockGuard`] is
/// [`Drop`]ped, or can be explicitly released early using the `unlock()`
/// method.
///
/// The guard will also attempt to remove the lock entry from the map on drop
/// (preventing memory leaks).
#[doc(hidden)]
pub struct LockGuard<LockType: RowLock, PrimaryKey: Hash + Eq + Debug + Clone> {
    lock: Arc<Lock>,
    /// Borrowed, not an `Arc` clone: see [`LockAcquirer`]'s field of the same
    /// name. Per-operation refcount traffic on the map is the shared-write
    /// bottleneck, and it is independent of the key.
    ///
    /// # Safety
    ///
    /// Every guard is created inside one operation on a table that holds the
    /// map's `Arc` for the duration of that call, so the map outlives it.
    lock_map: *const LockMap<LockType, PrimaryKey>,
    primary_key: PrimaryKey,
    /// Present for single-row operations. Multi-row queries acquire one
    /// mutation stripe only while processing each row, after all row locks are
    /// held, so stripe collisions cannot invert their primary-key lock order.
    _mutation_guard: Option<MutationGuard>,
    /// Marker to make this type ![`Sync`] (but still [`Send`])
    _not_sync: PhantomData<Cell<()>>,
}

// SAFETY: `LockMap` is `Sync` and the borrowed pointer is live for the guard's
// whole lifetime (see the field note), so a guard is as safe to send as the
// `&LockMap` it stands in for. The type stays `!Sync` via `_not_sync`.
unsafe impl<LockType, PrimaryKey> Send for LockGuard<LockType, PrimaryKey>
where
    LockType: RowLock + Send + Sync,
    PrimaryKey: Hash + Eq + Debug + Clone + Send,
{
}

impl<LockType, PrimaryKey> LockGuard<LockType, PrimaryKey>
where
    LockType: RowLock,
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    /// Creates a new [`LockGuard`] that will clean up the [`Lock`] entry from
    /// the [`LockMap`] on [`Drop`].
    ///
    /// # Safety
    ///
    /// The guard borrows the map as a raw pointer and dereferences it on
    /// `Drop`, so `lock_map`'s allocation must outlive the returned guard:
    /// holding the `Arc` only until this call returns is not enough. See the
    /// "Borrowed guards" note on [`LockMap`].
    pub unsafe fn new(lock: Arc<Lock>, lock_map: &Arc<LockMap<LockType, PrimaryKey>>, primary_key: PrimaryKey) -> Self {
        Self::from_raw(lock, Arc::as_ptr(lock_map), primary_key)
    }

    /// As [`Self::new`], for a caller that already holds the borrowed map
    /// pointer (a [`PendingLock`] being converted).
    ///
    /// # Safety
    ///
    /// `lock_map` must outlive the guard; see the field note.
    pub(crate) fn from_raw(
        lock: Arc<Lock>,
        lock_map: *const LockMap<LockType, PrimaryKey>,
        primary_key: PrimaryKey,
    ) -> Self {
        Self {
            lock,
            lock_map,
            primary_key,
            _mutation_guard: None,
            _not_sync: PhantomData,
        }
    }

    /// Creates a row guard that also serializes the mutation phase with the
    /// synchronous insert path for the same primary key.
    ///
    /// # Safety
    ///
    /// `lock_map` must point to a live map that outlives the returned guard.
    /// It is dereferenced here to take the mutation gate, and again when the
    /// guard drops; see the field note on `lock_map`.
    pub unsafe fn new_with_mutation(
        lock: Arc<Lock>,
        lock_map: *const LockMap<LockType, PrimaryKey>,
        primary_key: PrimaryKey,
    ) -> Self {
        // SAFETY: guaranteed by this function's own contract.
        let mutation_guard = unsafe { (*lock_map).mutation_guard(&primary_key) };
        Self {
            lock,
            lock_map,
            primary_key,
            _mutation_guard: Some(mutation_guard),
            _not_sync: PhantomData,
        }
    }

    /// Explicitly unlocks the [`Lock`] before the [`LockGuard`] is [`Drop`]ped.
    pub fn unlock(self) {
        drop(self);
    }
}

impl<LockType, PrimaryKey> Drop for LockGuard<LockType, PrimaryKey>
where
    LockType: RowLock,
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    fn drop(&mut self) {
        self.lock.unlock();
        // SAFETY: see the field note; the map outlives this guard.
        unsafe { (*self.lock_map).remove_with_lock_check(&self.primary_key) };
    }
}

/// Owns an operation lock that is registered into a row's [`RowLock`] but
/// whose predecessor wait has not completed yet.
///
/// The generated lock protocol registers the operation lock into the row
/// state, releases the row-state guard, and only then awaits every
/// predecessor lock. That await is a cancellation point: if the future is
/// dropped there (`tokio::time::timeout`, task abort), the registered lock
/// would stay held forever and every later operation on the same primary key
/// would wait on it. `PendingLock` covers exactly that window. Dropping it
/// before conversion unlocks the registered lock and retries lock-map
/// cleanup; converting it into the final [`LockGuard`] with
/// [`Self::into_guard`] or [`Self::into_guard_with_mutation`] defuses that
/// cleanup and hands ownership over.
#[doc(hidden)]
pub struct PendingLock<LockType: RowLock, PrimaryKey: Hash + Eq + Debug + Clone> {
    lock: Option<Arc<Lock>>,
    /// Borrowed, not an `Arc` clone: see [`LockAcquirer`]'s field of the same
    /// name. Per-operation refcount traffic on the map is the shared-write
    /// bottleneck, and it is independent of the key.
    ///
    /// # Safety
    ///
    /// Every guard is created inside one operation on a table that holds the
    /// map's `Arc` for the duration of that call, so the map outlives it.
    lock_map: *const LockMap<LockType, PrimaryKey>,
    primary_key: PrimaryKey,
}

// SAFETY: `LockMap` is `Sync` and the pointer is live for the guard's whole
// lifetime (see the field note), so this is as safe to move as a `&LockMap`.
unsafe impl<LockType, PrimaryKey> Send for PendingLock<LockType, PrimaryKey>
where
    LockType: RowLock + Send + Sync,
    PrimaryKey: Hash + Eq + Debug + Clone + Send,
{
}

impl<LockType, PrimaryKey> PendingLock<LockType, PrimaryKey>
where
    LockType: RowLock,
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    /// Takes ownership of a freshly registered operation lock. Must be called
    /// synchronously after registration, before the predecessor wait.
    ///
    /// # Safety
    ///
    /// As [`LockGuard::new`]: the map's allocation must outlive this value and
    /// the [`LockGuard`] it is converted into.
    pub unsafe fn new(lock: Arc<Lock>, lock_map: &Arc<LockMap<LockType, PrimaryKey>>, primary_key: PrimaryKey) -> Self {
        Self {
            lock: Some(lock),
            lock_map: Arc::as_ptr(lock_map),
            primary_key,
        }
    }

    /// Defuses the cancellation cleanup and converts into a [`LockGuard`].
    pub fn into_guard(mut self) -> LockGuard<LockType, PrimaryKey> {
        let lock = self
            .lock
            .take()
            .expect("pending lock is intact until conversion or drop");
        LockGuard::from_raw(lock, self.lock_map, self.primary_key.clone())
    }

    /// Defuses the cancellation cleanup and converts into a [`LockGuard`]
    /// that also serializes the mutation phase with the synchronous insert
    /// path for the same primary key.
    ///
    /// The mutation stripe is acquired here, i.e. only after the predecessor
    /// wait completed: the stripe gate is a synchronous spin lock and must
    /// never be held across an `.await` (see [`LockMap::mutation_guard`]).
    pub fn into_guard_with_mutation(mut self) -> LockGuard<LockType, PrimaryKey> {
        let lock = self
            .lock
            .take()
            .expect("pending lock is intact until conversion or drop");
        // SAFETY: a pending lock is a local of the operation that took it, and
        // that operation holds the map's `Arc` for its whole call.
        unsafe { LockGuard::new_with_mutation(lock, self.lock_map, self.primary_key.clone()) }
    }
}

impl<LockType, PrimaryKey> Drop for PendingLock<LockType, PrimaryKey>
where
    LockType: RowLock,
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    fn drop(&mut self) {
        if let Some(lock) = self.lock.take() {
            lock.unlock();
            // SAFETY: see the field note; the map outlives this guard.
            unsafe { (*self.lock_map).remove_with_lock_check(&self.primary_key) };
        }
    }
}

#[derive(Debug)]
pub struct Lock {
    // A wrapping diagnostic label, not dependency identity. The lock's own
    // allocation is what stays unique and stable for its lifetime.
    id: u16,
    /// Inline, not an `Arc<AtomicBool>`.
    ///
    /// It was separately allocated so a [`LockWait`] could outlive the lock it
    /// waits on. A wait can hold an `Arc<Lock>` instead and keep the whole lock
    /// alive, which costs one pointer in a rarely-built future and saves an
    /// allocation and a free on **every** locked operation, built or not.
    /// Freeing was 30% of the profile on the in-place update path once the
    /// map's exclusive acquisitions were out of the way.
    locked: AtomicBool,
    wakers: Mutex<Vec<Arc<AtomicWaker>>>,
}

impl PartialEq for Lock {
    fn eq(&self, other: &Self) -> bool {
        core::ptr::eq(self, other)
    }
}

impl Eq for Lock {}

impl Hash for Lock {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&(self as *const Self), state)
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
            locked: AtomicBool::new(true),
            wakers: Mutex::new(vec![]),
        }
    }

    /// A lock born in the released state (`is_locked() == false`, waiting on
    /// it returns immediately). Used for placeholder state, e.g. a fresh
    /// [`FullRowLock`] that no operation holds yet.
    /// "Released" refers to the acquisition flag, distinguishing it from
    /// [`Lock::new`], which starts held by the creating operation.
    pub fn new_released(id: u16) -> Self {
        Self {
            id,
            locked: AtomicBool::new(false),
            wakers: Mutex::new(vec![]),
        }
    }

    /// Diagnostic label; labels may repeat and do not define lock equality.
    pub fn id(&self) -> u16 {
        self.id
    }

    pub fn unlock(&self) {
        self.locked.store(false, Ordering::Release);
        let guard = self.wakers.lock();
        for w in guard.iter() {
            w.wake()
        }
    }

    pub fn lock(&self) {
        self.locked.store(true, Ordering::Relaxed);
    }

    pub fn is_locked(&self) -> bool {
        self.locked.load(Ordering::Acquire)
    }

    /// Takes `&Arc<Self>` because the returned wait keeps the lock alive: the
    /// flag it polls lives in the lock now rather than in its own allocation.
    pub fn wait(self: &Arc<Self>) -> LockWait {
        let mut guard = self.wakers.lock();
        let waker = Arc::new(AtomicWaker::new());
        guard.push(waker.clone());
        LockWait {
            lock: Arc::clone(self),
            waker,
        }
    }
}

#[derive(Debug)]
pub struct LockWait {
    lock: Arc<Lock>,
    waker: Arc<AtomicWaker>,
}

impl Future for LockWait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Fast path: already unlocked
        if !self.lock.locked.load(Ordering::Acquire) {
            return Poll::Ready(());
        }

        // Spin phase: try up to MAX_SPINS before going async
        for _ in 0..MAX_SPINS {
            core::hint::spin_loop();
            if !self.lock.locked.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
        }

        // Async phase: register waker and wait
        self.waker.register(cx.waker());
        if self.lock.locked.load(Ordering::Acquire) {
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
    #[allow(clippy::mutable_key_type)]
    fn repeated_labels_do_not_remove_distinct_dependencies() {
        let first = Arc::new(Lock::new(7));
        let second = Arc::new(Lock::new(7));
        let dependencies: hashbrown::HashSet<_> =
            hashbrown::HashSet::from_iter([first.clone(), first.clone(), second.clone()]);
        assert_eq!(dependencies.len(), 2);
        first.unlock();
        assert_eq!(dependencies.iter().filter(|lock| lock.is_locked()).count(), 1);
        second.unlock();
        assert!(dependencies.iter().all(|lock| !lock.is_locked()));
    }

    #[test]
    fn test_unlock_on_drop() {
        let lock = Arc::new(Lock::new(1));
        let lock_map: Arc<LockMap<FullRowLock, u64>> = Arc::new(LockMap::default());
        let pk = 1u64;
        assert!(lock.is_locked());

        {
            // SAFETY: `lock_map` is a local `Arc` that outlives this guard.
            let _guard = unsafe { LockGuard::<FullRowLock, u64>::new(lock.clone(), &lock_map, pk) };
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

        // SAFETY: `lock_map` is a local `Arc` that outlives this guard.
        let guard = unsafe { LockGuard::<FullRowLock, u64>::new(lock.clone(), &lock_map, pk) };
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
            // SAFETY: `lock_map` outlives the unwind this closure triggers.
            let _guard = unsafe { LockGuard::<FullRowLock, u64>::new(lock.clone(), &lock_map, pk) };
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
            // SAFETY: `lock_map` is a local `Arc` that outlives all three guards.
            let _guard1 = unsafe { LockGuard::<FullRowLock, u64>::new(lock1.clone(), &lock_map, 1u64) };
            let _guard2 = unsafe { LockGuard::<FullRowLock, u64>::new(lock2.clone(), &lock_map, 2u64) };
            let _guard3 = unsafe { LockGuard::<FullRowLock, u64>::new(lock3.clone(), &lock_map, 3u64) };

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
        let rw_lock = Arc::new(nagoya::sync::RwLock::new(lock_type));
        lock_map.insert(pk, rw_lock);

        // Verify the lock is in the map
        assert!(lock_map.get(&pk).is_some());

        // Create a guard and drop it
        {
            // SAFETY: `lock_map` outlives this scope and so outlives the guard.
            let _guard = unsafe { LockGuard::new(lock, &lock_map, pk) };
        }

        // Verify the lock entry was removed from the map
        assert!(lock_map.get(&pk).is_none());
    }
}
