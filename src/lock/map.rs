use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::lock::RowLock;

const MUTATION_STRIPE_COUNT: usize = 64;

#[derive(Debug, Default)]
struct MutationStripe {
    next_ticket: AtomicU64,
    serving: AtomicU64,
}

/// Synchronous, task-safe gate for one primary-key mutation stripe.
///
/// Generated async row locks and synchronous inserts share these gates so a
/// synchronous API entry point cannot interleave its multi-structure
/// publication with an update or delete of the same key.
#[derive(Debug)]
pub struct MutationGuard {
    stripes: Arc<[MutationStripe; MUTATION_STRIPE_COUNT]>,
    stripe: usize,
}

impl Drop for MutationGuard {
    fn drop(&mut self) {
        self.stripes[self.stripe].serving.fetch_add(1, Ordering::Release);
    }
}

#[derive(Debug)]
pub struct LockMap<LockType, PrimaryKey> {
    map: RwLock<HashMap<PrimaryKey, Arc<tokio::sync::RwLock<LockType>>>>,
    next_id: AtomicU16,
    mutation_stripes: Arc<[MutationStripe; MUTATION_STRIPE_COUNT]>,
}

impl<LockType, PrimaryKey> Default for LockMap<LockType, PrimaryKey> {
    fn default() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            next_id: AtomicU16::default(),
            mutation_stripes: Arc::new(std::array::from_fn(|_| MutationStripe::default())),
        }
    }
}

impl<LockType, PrimaryKey> LockMap<LockType, PrimaryKey>
where
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    pub fn insert(
        &self,
        key: PrimaryKey,
        lock: Arc<tokio::sync::RwLock<LockType>>,
    ) -> Option<Arc<tokio::sync::RwLock<LockType>>> {
        self.map.write().insert(key, lock)
    }

    pub fn get(&self, key: &PrimaryKey) -> Option<Arc<tokio::sync::RwLock<LockType>>> {
        self.map.read().get(key).cloned()
    }

    /// Returns the lock for `key`, inserting one built by `f` if absent.
    ///
    /// The check and the insert happen under a single write guard. Doing them
    /// as separate `get` then `insert` calls is a check-then-act race: two
    /// tasks can both observe no entry, both build a lock, and both believe
    /// they hold the row. The loser's `insert` returns the winner's lock and
    /// can merge into it, but the *winner* already registered its operation on
    /// a lock that is no longer in the map, so it never waits for the loser and
    /// both proceed into the row at once.
    pub fn get_or_insert_with<F>(&self, key: PrimaryKey, f: F) -> Arc<tokio::sync::RwLock<LockType>>
    where
        F: FnOnce() -> LockType,
    {
        // Fast path: the row is usually already locked by someone, and a read
        // guard keeps unrelated rows' acquisitions concurrent. The clone happens
        // under the guard, so `remove_with_lock_check` (which needs the write
        // lock) either runs before we looked or sees our extra strong reference
        // and keeps the entry.
        if let Some(lock) = self.map.read().get(&key) {
            return lock.clone();
        }
        let mut map = self.map.write();
        // Re-check: another task can insert between the read and write guards.
        if let Some(lock) = map.get(&key) {
            return lock.clone();
        }
        let lock = Arc::new(tokio::sync::RwLock::new(f()));
        map.insert(key, lock.clone());
        lock
    }

    pub fn remove(&mut self, key: &PrimaryKey) {
        self.map.write().remove(key);
    }

    pub fn remove_with_lock_check(&self, key: &PrimaryKey)
    where
        LockType: RowLock,
    {
        let mut set = self.map.write();
        if let Some(lock) = set.get(key).cloned()
            && let Ok(guard) = lock.try_read()
            && !guard.is_locked()
            // Two strong references means this map entry and our own `lock`
            // clone above, and nothing else. Any higher count is a task that
            // has already taken this Arc out of `get_or_insert_with` and is
            // about to register on it; removing the entry now would let the
            // next caller build a *second* lock for the same row, and the two
            // would not serialise against each other.
            //
            // Known trade-off: if that other task is cancelled between taking
            // the Arc and registering its operation, nothing re-triggers this
            // cleanup and the (unlocked, unused) entry stays in the map until
            // the next operation on the same key drops its guard. That leaks at
            // most one empty lock per abandoned key and never affects mutual
            // exclusion.
            && Arc::strong_count(&lock) == 2
        {
            set.remove(key);
        }
    }

    pub fn next_id(&self) -> u16 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Serializes the synchronous mutation phase for this key.
    ///
    /// The holder must not perform a suspending `.await`. Generated locked
    /// operations acquire this only after their async predecessor wait has
    /// completed, and the synchronous `insert` path never awaits.
    pub fn mutation_guard(&self, key: &PrimaryKey) -> MutationGuard {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let stripe = (hasher.finish() as usize) % MUTATION_STRIPE_COUNT;
        let gate = &self.mutation_stripes[stripe];
        let ticket = gate.next_ticket.fetch_add(1, Ordering::Relaxed);
        let mut spins = 0u32;

        while gate.serving.load(Ordering::Acquire) != ticket {
            if spins < 16 {
                spins += 1;
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }

        MutationGuard {
            stripes: self.mutation_stripes.clone(),
            stripe,
        }
    }
}
