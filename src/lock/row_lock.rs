use crate::lock::{Lock, LockWait};
use std::collections::HashSet;
use std::sync::Arc;

pub trait RowLock {
    /// Checks if any column of this row is locked.
    fn is_locked(&self) -> bool;
    /// Creates new [`RowLock`] with all columns locked.
    fn with_lock(id: u16) -> (Self, Arc<Lock>)
    where
        Self: Sized;
    /// Locks full [`RowLock`].
    #[allow(clippy::mutable_key_type)]
    fn lock(&mut self, id: u16) -> (HashSet<Arc<Lock>>, Arc<Lock>);
    /// Merges two [`RowLock`]'s.
    #[allow(clippy::mutable_key_type)]
    fn merge(&mut self, other: &mut Self) -> HashSet<Arc<Lock>>
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

    pub fn wait(&self) -> LockWait {
        self.l.wait()
    }
}

#[allow(clippy::mutable_key_type)]
impl RowLock for FullRowLock {
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

    fn lock(&mut self, id: u16) -> (HashSet<Arc<Lock>>, Arc<Lock>) {
        let mut set = HashSet::new();
        let l = Arc::new(Lock::new(id));
        set.insert(self.l.clone());
        self.l = l.clone();

        (set, l)
    }

    fn merge(&mut self, other: &mut Self) -> HashSet<Arc<Lock>>
    where
        Self: Sized,
    {
        let set = HashSet::from_iter([self.l.clone()]);
        self.l = other.l.clone();
        set
    }
}
