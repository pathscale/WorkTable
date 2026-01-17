use std::sync::Arc;

use data_bucket::Link;
use data_bucket::page::PageId;

use crate::lock::{FullRowLock, LockMap, RowLock};

/// Lock manager for vacuum operations.
/// Supports locking at both page and link granularity.
#[derive(Debug, Default)]
pub struct VacuumLock {
    per_link_lock: Arc<LockMap<FullRowLock, Link>>,
    per_page_lock: Arc<LockMap<FullRowLock, PageId>>,
}

impl VacuumLock {
    /// Locks a page, returning the [`FullRowLock`].
    pub fn lock_page(&self, page_id: PageId) -> Arc<tokio::sync::RwLock<FullRowLock>> {
        if let Some(lock) = self.per_page_lock.get(&page_id) {
            return lock;
        }

        let (row_lock, _) = FullRowLock::with_lock(self.per_page_lock.next_id());
        let lock = Arc::new(tokio::sync::RwLock::new(row_lock));
        self.per_page_lock.insert(page_id, lock.clone());
        lock
    }

    /// Locks a [`Link`], returning the [`FullRowLock`].
    pub fn lock_link(&self, link: Link) -> Arc<tokio::sync::RwLock<FullRowLock>> {
        if let Some(lock) = self.per_link_lock.get(&link) {
            return lock;
        }

        let (row_lock, _lock) = FullRowLock::with_lock(self.per_link_lock.next_id());
        let lock = Arc::new(tokio::sync::RwLock::new(row_lock));
        self.per_link_lock.insert(link, lock.clone());
        lock
    }

    /// Checks if a [`Link`] is locked.
    /// [`Link`] is locked if it was locked OR its page is locked.
    pub fn is_link_locked(&self, link: &Link) -> bool {
        if let Some(page_lock) = self.per_page_lock.get(&link.page_id) {
            match page_lock.try_read() {
                Ok(guard) => {
                    if guard.is_locked() {
                        return true;
                    }
                }
                Err(_) => return true, // write lock held
            }
        }

        if let Some(link_lock) = self.per_link_lock.get(link) {
            match link_lock.try_read() {
                Ok(guard) => {
                    if guard.is_locked() {
                        return true;
                    }
                }
                Err(_) => return true, // write lock held
            }
        }

        false
    }

    /// Checks if a page is locked.
    pub fn is_page_locked(&self, page_id: &PageId) -> bool {
        if let Some(page_lock) = self.per_page_lock.get(page_id) {
            match page_lock.try_read() {
                Ok(guard) => guard.is_locked(),
                Err(_) => true, // write lock held
            }
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_page_locked_not_locked() {
        let vacuum_lock = VacuumLock::default();
        let page_id = PageId::from(1);

        assert!(!vacuum_lock.is_page_locked(&page_id));
    }

    #[tokio::test]
    async fn test_is_page_locked_after_lock() {
        let vacuum_lock = VacuumLock::default();
        let page_id = PageId::from(1);

        let _lock = vacuum_lock.lock_page(page_id);

        assert!(vacuum_lock.is_page_locked(&page_id));
    }

    #[tokio::test]
    async fn test_is_page_locked_with_write_lock() {
        let vacuum_lock = VacuumLock::default();
        let page_id = PageId::from(1);

        let lock = vacuum_lock.lock_page(page_id);
        let _write_guard = lock.write().await;

        assert!(vacuum_lock.is_page_locked(&page_id));
    }

    #[test]
    fn test_is_link_locked_not_locked() {
        let vacuum_lock = VacuumLock::default();
        let link = Link {
            page_id: PageId::from(1),
            offset: 0,
            length: 100,
        };

        assert!(!vacuum_lock.is_link_locked(&link));
    }

    #[tokio::test]
    async fn test_is_link_locked_by_link() {
        let vacuum_lock = VacuumLock::default();
        let link = Link {
            page_id: PageId::from(1),
            offset: 0,
            length: 100,
        };

        let _lock = vacuum_lock.lock_link(link);

        assert!(vacuum_lock.is_link_locked(&link));
    }

    #[tokio::test]
    async fn test_is_link_locked_by_page() {
        let vacuum_lock = VacuumLock::default();
        let link = Link {
            page_id: PageId::from(1),
            offset: 0,
            length: 100,
        };

        let _lock = vacuum_lock.lock_page(link.page_id);

        assert!(vacuum_lock.is_link_locked(&link));
    }

    #[tokio::test]
    async fn test_is_link_locked_with_link_write_lock() {
        let vacuum_lock = VacuumLock::default();
        let link = Link {
            page_id: PageId::from(1),
            offset: 0,
            length: 100,
        };

        let lock = vacuum_lock.lock_link(link);
        let _write_guard = lock.write().await;

        assert!(vacuum_lock.is_link_locked(&link));
    }

    #[tokio::test]
    async fn test_lock_page_returns_same_lock() {
        let vacuum_lock = VacuumLock::default();
        let page_id = PageId::from(1);

        let lock1 = vacuum_lock.lock_page(page_id);
        let lock2 = vacuum_lock.lock_page(page_id);

        // Same pointer = same lock instance
        assert!(Arc::ptr_eq(&lock1, &lock2));
    }

    #[tokio::test]
    async fn test_lock_link_returns_same_lock() {
        let vacuum_lock = VacuumLock::default();
        let link = Link {
            page_id: PageId::from(1),
            offset: 0,
            length: 100,
        };

        let lock1 = vacuum_lock.lock_link(link);
        let lock2 = vacuum_lock.lock_link(link);

        assert!(Arc::ptr_eq(&lock1, &lock2));
    }
}
