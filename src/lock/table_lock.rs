use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use data_bucket::page::PageId;

use crate::lock::map::LockMap;
use crate::lock::row_lock::{FullRowLock, RowLock};

/// Unified lock manager for WorkTable operations.
///
/// Combines row-level locking with
/// page-level locking (for defragmentation operations).
#[derive(Debug)]
pub struct WorkTableLock<LockType, PrimaryKey> {
    pub row_locks: LockMap<LockType, PrimaryKey>,
    pub vacuum_lock: Arc<LockMap<FullRowLock, PageId>>,
}

impl<LockType, PrimaryKey> Default for WorkTableLock<LockType, PrimaryKey> {
    fn default() -> Self {
        Self {
            row_locks: LockMap::default(),
            vacuum_lock: Arc::new(LockMap::default()),
        }
    }
}

impl<LockType, PrimaryKey> WorkTableLock<LockType, PrimaryKey>
where
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    /// Locks a page for vacuum operations, returning the [`FullRowLock`].
    ///
    /// If a lock already exists for the page, it returns the existing lock.
    /// Otherwise, creates a new lock.
    pub fn lock_page(&self, page_id: PageId) -> Arc<tokio::sync::RwLock<FullRowLock>> {
        if let Some(lock) = self.vacuum_lock.get(&page_id) {
            return lock;
        }

        let (row_lock, _) = FullRowLock::with_lock(self.vacuum_lock.next_id());
        let lock = Arc::new(tokio::sync::RwLock::new(row_lock));
        self.vacuum_lock.insert(page_id, lock.clone());
        lock
    }

    pub fn get_page_lock(&self, page_id: PageId) -> Option<Arc<tokio::sync::RwLock<FullRowLock>>> {
        self.vacuum_lock.get(&page_id)
    }

    pub fn remove_page_lock(&self, page_id: &PageId) {
        self.vacuum_lock.remove_with_lock_check(page_id);
    }

    /// Checks if a page is locked by vacuum operations and awaits the lock if it is.
    ///
    /// This should be called before any operation that accesses data on a specific page.
    ///
    /// Returns `false` if not waited at all, `true` if waited.
    pub async fn await_page_lock(&self, page_id: PageId) -> bool {
        if let Some(lock) = self.get_page_lock(page_id) {
            let guard = lock.read().await;
            let wait = guard.wait();
            drop(guard);
            wait.await;

            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn test_default_creates_empty_locks() {
        let locks: WorkTableLock<(), String> = WorkTableLock::default();

        assert!(locks.get_page_lock(1.into()).is_none());
        assert!(locks.get_page_lock(2.into()).is_none());
    }

    #[tokio::test]
    async fn test_lock_page_returns_lock() {
        let locks: WorkTableLock<(), String> = WorkTableLock::default();

        let lock = locks.lock_page(5.into());

        let guard = lock.read().await;
        assert!(guard.is_locked());
    }

    #[tokio::test]
    async fn test_lock_page_same_id_returns_same_lock() {
        let locks: WorkTableLock<(), String> = WorkTableLock::default();

        let lock1 = locks.lock_page(10.into());
        let lock2 = locks.lock_page(10.into());

        let ptr1 = Arc::as_ptr(&lock1);
        let ptr2 = Arc::as_ptr(&lock2);
        assert_eq!(ptr1, ptr2);
    }

    #[tokio::test]
    async fn test_lock_page_different_ids_returns_different_locks() {
        let locks: WorkTableLock<(), String> = WorkTableLock::default();

        let lock1 = locks.lock_page(1.into());
        let lock2 = locks.lock_page(2.into());

        let ptr1 = Arc::as_ptr(&lock1);
        let ptr2 = Arc::as_ptr(&lock2);
        assert_ne!(ptr1, ptr2);
    }

    #[tokio::test]
    async fn test_get_page_lock_returns_none_when_no_lock() {
        let locks: WorkTableLock<(), String> = WorkTableLock::default();

        assert!(locks.get_page_lock(999.into()).is_none());
    }

    #[tokio::test]
    async fn test_get_page_lock_returns_some_when_locked() {
        let locks: WorkTableLock<(), String> = WorkTableLock::default();

        locks.lock_page(42.into());
        let retrieved = locks.get_page_lock(42.into());

        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_remove_page_lock_removes_unlocked_lock() {
        let locks: WorkTableLock<(), String> = WorkTableLock::default();

        let lock = locks.lock_page(7.into());

        {
            let guard = lock.read().await;
            guard.unlock();
        }

        locks.remove_page_lock(&7.into());

        assert!(locks.get_page_lock(7.into()).is_none());
    }

    #[tokio::test]
    async fn test_remove_page_lock_does_not_remove_locked_lock() {
        let locks: WorkTableLock<(), String> = WorkTableLock::default();

        let lock = locks.lock_page(8.into());

        let _guard = lock.write().await;

        locks.remove_page_lock(&8.into());

        assert!(locks.get_page_lock(8.into()).is_some());
    }

    #[tokio::test]
    async fn test_await_page_lock_returns_immediately_when_no_lock() {
        let locks: WorkTableLock<(), String> = WorkTableLock::default();

        let result = timeout(Duration::from_millis(10), locks.await_page_lock(100.into())).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_await_page_lock_blocks_when_locked() {
        let locks = Arc::new(WorkTableLock::<(), String>::default());

        let lock = locks.lock_page(99.into());

        let guard = lock.write().await;

        let locks_clone = locks.clone();
        let await_task = tokio::spawn(async move {
            locks_clone.await_page_lock(99.into()).await;
        });

        let result = timeout(Duration::from_millis(50), await_task).await;
        assert!(result.is_err());

        guard.unlock();
        drop(guard);

        let result = timeout(Duration::from_millis(100), async move {
            locks.await_page_lock(99.into()).await;
        })
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_multiple_pages_can_be_locked_independently() {
        let locks: WorkTableLock<(), String> = WorkTableLock::default();

        let page1 = locks.lock_page(1.into());
        let page2 = locks.lock_page(2.into());

        let lock1 = page1.write().await;
        let lock2 = page2.write().await;

        assert!(lock1.is_locked());
        assert!(lock2.is_locked());

        drop(lock2);
        drop(lock1);
    }
}
