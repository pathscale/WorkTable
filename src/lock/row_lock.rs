use std::collections::HashSet;
use crate::lock::Lock;
use std::sync::Arc;

pub trait RowLock {
    /// Checks if any column of this row is locked.
    fn is_locked(&self) -> bool;
    /// Creates new [`RowLock`] with all columns locked.
    fn with_lock(id: u16) -> (Self, Arc<Lock>)
    where
        Self: Sized;
    /// Locks full [`RowLock`].
    fn lock(&self, id: u16) -> (HashSet<Arc<Lock>>, Arc<Lock>);
    /// Merges two [`RowLock`]'s.
    fn merge(&mut self, other: &Self)
    where
        Self: Sized;
}
