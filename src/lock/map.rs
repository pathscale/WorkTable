use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use indexset::concurrent::set::BTreeSet;
use parking_lot::RwLock;

use crate::lock::RowLock;

#[derive(Debug)]
pub struct LockMap<LockType, PrimaryKey>
where
    PrimaryKey: Ord + Clone + 'static,
{
    map: RwLock<HashMap<PrimaryKey, Arc<RwLock<LockType>>>>,
    reinsert_set: BTreeSet<PrimaryKey>,
    next_id: AtomicU16,
}

impl<LockType, PrimaryKey> Default for LockMap<LockType, PrimaryKey>
where
    PrimaryKey: Debug + Ord + Clone + Send + 'static,
{
    fn default() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            reinsert_set: BTreeSet::new(),
            next_id: AtomicU16::default(),
        }
    }
}

impl<LockType, PrimaryKey> LockMap<LockType, PrimaryKey>
where
    PrimaryKey: Hash + Eq + Debug + Ord + Clone + Send + 'static,
{
    pub fn insert(
        &self,
        key: PrimaryKey,
        lock: Arc<RwLock<LockType>>,
    ) -> Option<Arc<RwLock<LockType>>> {
        self.map.write().insert(key, lock)
    }

    pub fn get(&self, key: &PrimaryKey) -> Option<Arc<RwLock<LockType>>> {
        self.map.read().get(key).cloned()
    }

    pub fn remove(&mut self, key: &PrimaryKey) {
        self.map.write().remove(key);
    }

    pub fn remove_with_lock_check(&self, key: &PrimaryKey)
    where
        LockType: RowLock,
    {
        let mut set = self.map.write();
        let remove = if let Some(lock) = set.get(key) {
            !lock.read().is_locked()
        } else {
            false
        };

        if remove {
            set.remove(key);
        }
    }

    pub fn mark_reinsert(&self, pk: PrimaryKey) {
        self.reinsert_set.insert(pk);
    }

    pub fn is_reinsert(&self, pk: &PrimaryKey) -> bool {
        self.reinsert_set.contains(pk)
    }

    pub fn finish_reinsert(&self, pk: &PrimaryKey) {
        self.reinsert_set.remove(pk);
    }

    pub fn next_id(&self) -> u16 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}
