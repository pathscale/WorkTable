use alloc::sync::Arc;
use core::fmt::Debug;
use core::hash::Hash;

use crate::lock::{Lock, LockGuard, LockMap, LockWait};

pub trait RowLock {
    /// Creates a new [`RowLock`] with no columns locked.
    fn new() -> Self
    where
        Self: Sized;
    /// Checks if any column of this row is locked.
    fn is_locked(&self) -> bool;
    /// Creates new [`RowLock`] with all columns locked.
    fn with_lock(id: u16) -> (Self, Arc<Lock>)
    where
        Self: Sized;
    /// Locks full [`RowLock`], returning the predecessors to wait on.
    ///
    /// A `Vec`, not a `HashSet`. The collection holds one entry per column this
    /// lock type covers, deduplicated by pointer, which in every shipping
    /// schema is a handful. Building a `hashbrown::HashSet` for that seeded a
    /// fresh `foldhash` hasher on every operation, which was about a tenth of
    /// the profile on the in-place update path, to hash at most a few `Arc`
    /// pointers. Linear dedup over a short `Vec` is cheaper and the caller only
    /// iterates the result.
    fn lock(&mut self, id: u16) -> (Vec<Arc<Lock>>, Arc<Lock>);
    /// Merges two [`RowLock`]'s.
    fn merge(&mut self, other: &mut Self) -> Vec<Arc<Lock>>
    where
        Self: Sized;
}

/// Full row lock represented by a single lock.
/// Unlike generated per-column lock types, this uses one lock for the entire
/// row.
#[derive(Debug)]
pub struct FullRowLock {
    l: Arc<Lock>,
}

impl FullRowLock {
    pub fn unlock(&self) {
        self.l.unlock();
    }

    /// Creates a [`LockGuard`] that will automatically unlock this lock when
    /// dropped.
    pub fn guard<PrimaryKey: Clone + Hash + Eq + Debug>(
        self,
        lock_map: &Arc<LockMap<Self, PrimaryKey>>,
        primary_key: PrimaryKey,
    ) -> LockGuard<Self, PrimaryKey> {
        LockGuard::new(self.l, lock_map, primary_key)
    }

    pub fn wait(&self) -> LockWait {
        self.l.wait()
    }
}

#[allow(clippy::mutable_key_type)]
impl RowLock for FullRowLock {
    fn new() -> Self
    where
        Self: Sized,
    {
        // Placeholder: no operation holds this row yet, so the initial lock is
        // born released and any wait on it completes immediately. The id is
        // never observed because `lock()` replaces the placeholder before
        // handing anything out.
        FullRowLock {
            l: Arc::new(Lock::new_released(0)),
        }
    }

    fn is_locked(&self) -> bool {
        self.l.is_locked()
    }

    fn with_lock(id: u16) -> (Self, Arc<Lock>)
    where
        Self: Sized,
    {
        let l = Arc::new(Lock::new(id));
        (FullRowLock { l: l.clone() }, l)
    }

    fn lock(&mut self, id: u16) -> (Vec<Arc<Lock>>, Arc<Lock>) {
        let l = Arc::new(Lock::new(id));
        let set = vec![self.l.clone()];
        self.l = l.clone();

        (set, l)
    }

    fn merge(&mut self, other: &mut Self) -> Vec<Arc<Lock>>
    where
        Self: Sized,
    {
        let set = vec![self.l.clone()];
        self.l = other.l.clone();
        set
    }
}
