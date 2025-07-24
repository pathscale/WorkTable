use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use crate::lock::RowLock;
use data_bucket::Link;
use parking_lot::RwLock;

#[derive(Debug)]
pub struct LockMap<LockType> {
    set: RwLock<HashMap<Link, Arc<RwLock<LockType>>>>,
    next_id: AtomicU16,
}

impl<LockType> Default for LockMap<LockType> {
    fn default() -> Self {
        Self::new()
    }
}

impl<LockType> LockMap<LockType> {
    pub fn new() -> Self {
        Self {
            set: RwLock::new(HashMap::new()),
            next_id: AtomicU16::default(),
        }
    }

    pub fn insert(&self, key: Link, lock: Arc<RwLock<LockType>>) -> Option<Arc<RwLock<LockType>>> {
        self.set.write().insert(key, lock)
    }

    pub fn get(&self, key: &Link) -> Option<Arc<RwLock<LockType>>> {
        self.set.read().get(key).cloned()
    }

    pub fn remove(&mut self, key: &Link) {
        self.set.write().remove(key);
    }

    pub fn remove_with_lock_check(&self, key: &Link)
    where
        LockType: RowLock,
    {
        let mut set = self.set.write();
        let remove = if let Some(lock) = set.get(key) {
            !lock.read().is_locked()
        } else {
            false
        };

        if remove {
            set.remove(key);
        }
    }

    pub fn next_id(&self) -> u16 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}
