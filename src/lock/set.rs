use std::sync::Arc;

use lockfree::map::Map;

#[derive(Debug)]
pub struct LockMap<LockType, PkType>
where
    PkType: std::hash::Hash + std::cmp::Ord,
{
    set: Map<PkType, Option<Arc<LockType>>>,
}

impl<LockType, PkType> Default for LockMap<LockType, PkType>
where
    PkType: std::hash::Hash + std::cmp::Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<LockType, PkType> LockMap<LockType, PkType>
where
    PkType: std::hash::Hash + std::cmp::Ord,
{
    pub fn new() -> Self {
        Self { set: Map::new() }
    }

    pub fn insert(&self, id: PkType, lock: Arc<LockType>) {
        self.set.insert(id, Some(lock));
    }

    pub fn get(&self, id: &PkType) -> Option<Arc<LockType>> {
        self.set.get(id).map(|v| v.val().clone())?
    }

    pub fn remove(&self, id: &PkType) {
        self.set.remove(id);
    }
}
