use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU64, AtomicUsize, Ordering};

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

/// Operation-wide activity signal for a chunked bulk mutation.
///
/// It does not hold a row or stripe lock. Its only job is to keep background
/// vacuum out while a bulk operation deliberately releases locks between
/// chunks, so those gaps are not mistaken for the table becoming idle.
#[doc(hidden)]
#[derive(Debug)]
pub struct BulkMutationGuard {
    active: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct LockEntry<LockType> {
    lock: Arc<tokio::sync::RwLock<LockType>>,
    acquirers: Arc<AtomicUsize>,
}

/// A tracked reference to one row-lock entry while an operation registers.
///
/// Dropping this handle, including through async task cancellation, retries
/// map cleanup after releasing its lock reference. Clones remain tracked so an
/// entry cannot be removed while any caller may still register against it.
#[derive(Debug)]
pub struct LockAcquirer<LockType, PrimaryKey>
where
    LockType: RowLock,
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    lock: Option<Arc<tokio::sync::RwLock<LockType>>>,
    acquirers: Arc<AtomicUsize>,
    lock_map: Arc<LockMap<LockType, PrimaryKey>>,
    primary_key: PrimaryKey,
}

impl<LockType, PrimaryKey> Clone for LockAcquirer<LockType, PrimaryKey>
where
    LockType: RowLock,
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    fn clone(&self) -> Self {
        self.acquirers.fetch_add(1, Ordering::AcqRel);
        Self {
            lock: self.lock.clone(),
            acquirers: self.acquirers.clone(),
            lock_map: self.lock_map.clone(),
            primary_key: self.primary_key.clone(),
        }
    }
}

impl<LockType, PrimaryKey> Deref for LockAcquirer<LockType, PrimaryKey>
where
    LockType: RowLock,
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    type Target = tokio::sync::RwLock<LockType>;

    fn deref(&self) -> &Self::Target {
        self.lock.as_deref().expect("the acquisition lock exists until drop")
    }
}

impl<LockType, PrimaryKey> Drop for LockAcquirer<LockType, PrimaryKey>
where
    LockType: RowLock,
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    fn drop(&mut self) {
        self.acquirers.fetch_sub(1, Ordering::AcqRel);
        drop(self.lock.take());
        self.lock_map.remove_with_lock_check(&self.primary_key);
    }
}

impl Drop for MutationGuard {
    fn drop(&mut self) {
        self.stripes[self.stripe].serving.fetch_add(1, Ordering::Release);
    }
}

impl Drop for BulkMutationGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Release);
    }
}

/// Registry for per-row async locks and synchronous mutation stripes.
///
/// # Sync/async lock boundary
///
/// The `parking_lot` map guard is never returned and never crosses an
/// `.await`. Acquisition clones a tracked `Arc<tokio::sync::RwLock<_>>` before
/// releasing the map guard. Cleanup may synchronously take the short-lived map
/// write guard, but only probes the per-row lock with `try_read`; it never waits
/// on a Tokio lock while holding the map. This one-way boundary prevents a
/// map-lock/per-row-lock cycle during cancellation and `Drop`.
#[derive(Debug)]
pub struct LockMap<LockType, PrimaryKey> {
    map: RwLock<HashMap<PrimaryKey, LockEntry<LockType>>>,
    next_id: AtomicU16,
    mutation_stripes: Arc<[MutationStripe; MUTATION_STRIPE_COUNT]>,
    bulk_mutations: Arc<AtomicUsize>,
}

impl<LockType, PrimaryKey> Default for LockMap<LockType, PrimaryKey> {
    fn default() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            next_id: AtomicU16::default(),
            mutation_stripes: Arc::new(std::array::from_fn(|_| MutationStripe::default())),
            bulk_mutations: Arc::default(),
        }
    }
}

impl<LockType, PrimaryKey> LockMap<LockType, PrimaryKey>
where
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    /// Inserts a raw lock entry.
    ///
    /// A returned or externally retained `Arc` pins cleanup through
    /// `Arc::strong_count`. Generated operations should prefer
    /// [`Self::get_or_insert_with`], whose [`LockAcquirer`] makes cancellation
    /// tracking explicit.
    pub fn insert(
        &self,
        key: PrimaryKey,
        lock: Arc<tokio::sync::RwLock<LockType>>,
    ) -> Option<Arc<tokio::sync::RwLock<LockType>>> {
        self.map
            .write()
            .insert(
                key,
                LockEntry {
                    lock,
                    acquirers: Arc::new(AtomicUsize::new(0)),
                },
            )
            .map(|entry| entry.lock)
    }

    /// Returns an untracked raw lock clone, which keeps the map entry alive
    /// until that clone is dropped.
    pub fn get(&self, key: &PrimaryKey) -> Option<Arc<tokio::sync::RwLock<LockType>>> {
        self.map.read().get(key).map(|entry| entry.lock.clone())
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
    pub fn get_or_insert_with<F>(self: &Arc<Self>, key: PrimaryKey, f: F) -> LockAcquirer<LockType, PrimaryKey>
    where
        LockType: RowLock,
        F: FnOnce() -> LockType,
    {
        // Fast path: the row is usually already locked by someone, and a read
        // guard keeps unrelated rows' acquisitions concurrent. The clone happens
        // under the guard, so `remove_with_lock_check` (which needs the write
        // lock) either runs before we looked or sees our extra strong reference
        // and keeps the entry.
        if let Some(entry) = self.map.read().get(&key) {
            entry.acquirers.fetch_add(1, Ordering::AcqRel);
            return LockAcquirer {
                lock: Some(entry.lock.clone()),
                acquirers: entry.acquirers.clone(),
                lock_map: self.clone(),
                primary_key: key,
            };
        }
        let mut map = self.map.write();
        // Re-check: another task can insert between the read and write guards.
        let entry = map.entry(key.clone()).or_insert_with(|| LockEntry {
            lock: Arc::new(tokio::sync::RwLock::new(f())),
            acquirers: Arc::new(AtomicUsize::new(0)),
        });
        entry.acquirers.fetch_add(1, Ordering::AcqRel);
        LockAcquirer {
            lock: Some(entry.lock.clone()),
            acquirers: entry.acquirers.clone(),
            lock_map: self.clone(),
            primary_key: key,
        }
    }

    pub fn remove(&mut self, key: &PrimaryKey) {
        self.map.write().remove(key);
    }

    pub fn remove_with_lock_check(&self, key: &PrimaryKey)
    where
        LockType: RowLock,
    {
        let mut set = self.map.write();
        let should_remove = set.get(key).is_some_and(|entry| {
            let Ok(guard) = entry.lock.try_read() else {
                return false;
            };
            !guard.is_locked()
                // Every acquisition is counted before the map guard is released.
                // A non-zero count means a caller may still register an operation;
                // removing now would let a second lock be created for the same row.
                && entry.acquirers.load(Ordering::Acquire) == 0
                // `insert` is public and accepts an Arc, so retain the old safety
                // check for callers holding a raw clone outside tracked acquisition.
                && Arc::strong_count(&entry.lock) == 1
        });
        if should_remove {
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
        self.mutation_guard_for_stripe(Self::stripe_of(key))
    }

    /// Serializes the synchronous mutation phase for every key in `keys` at
    /// once, for all-or-nothing batch mutations.
    ///
    /// Stripes are deduplicated and acquired in ascending order, so two
    /// concurrent batch acquisitions cannot deadlock against each other, a
    /// batch cannot deadlock against itself when two keys share a stripe, and
    /// single-key holders (which never nest stripe acquisitions) cannot form a
    /// cycle with a batch. The same no-`.await` rule as
    /// [`Self::mutation_guard`] applies for the whole guard set's lifetime.
    pub fn mutation_guards<'a>(&self, keys: impl Iterator<Item = &'a PrimaryKey>) -> Vec<MutationGuard>
    where
        PrimaryKey: 'a,
    {
        let mut stripes: Vec<usize> = keys.map(Self::stripe_of).collect();
        stripes.sort_unstable();
        stripes.dedup();
        stripes
            .into_iter()
            .map(|stripe| self.mutation_guard_for_stripe(stripe))
            .collect()
    }

    fn stripe_of(key: &PrimaryKey) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % MUTATION_STRIPE_COUNT
    }

    /// Mutation stripes currently held or being waited on.
    ///
    /// A live read of "is anything writing to this table right now", which is
    /// what a background job needs before it takes an exclusion. Every insert,
    /// delete and upsert passes through one of these gates, so unlike counting
    /// requests for reclaimable space it cannot miss a workload: deletes never
    /// ask for space at all, and an upsert that fits in place does not either,
    /// so a sweep watching that signal saw an idle table under a load of
    /// exactly those and walked straight in.
    ///
    /// Each stripe is a ticket lock, so a handed-out ticket that is not yet
    /// being served is a writer either inside the gate or queued for it.
    pub fn mutations_in_flight(&self) -> usize {
        let striped = self
            .mutation_stripes
            .iter()
            .filter(|stripe| stripe.next_ticket.load(Ordering::Acquire) != stripe.serving.load(Ordering::Acquire))
            .count();
        striped + usize::from(self.bulk_mutations.load(Ordering::Acquire) > 0)
    }

    /// Monotonic-with-wrap count of completed mutation-stripe entries.
    ///
    /// Vacuum samples this between polls so a continuous mutation stream
    /// cannot look idle merely because both polls landed between operations.
    /// It is derived from the ticket locks' existing counters and adds no
    /// atomic operation to the foreground path.
    #[doc(hidden)]
    pub fn mutation_epoch(&self) -> u64 {
        self.mutation_stripes.iter().fold(0u64, |epoch, stripe| {
            epoch.wrapping_add(stripe.serving.load(Ordering::Acquire))
        })
    }

    /// Keeps vacuum out for the duration of a chunked bulk mutation without
    /// holding any row or mutation-stripe lock.
    ///
    /// One increment and one decrement are paid per whole operation, not per
    /// row or chunk.
    #[doc(hidden)]
    pub fn bulk_mutation_guard(&self) -> BulkMutationGuard {
        self.bulk_mutations.fetch_add(1, Ordering::AcqRel);
        BulkMutationGuard {
            active: Arc::clone(&self.bulk_mutations),
        }
    }

    fn mutation_guard_for_stripe(&self, stripe: usize) -> MutationGuard {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lock::FullRowLock;

    /// A batch over more keys than stripes necessarily maps several keys to
    /// one stripe; acquisition must dedupe instead of deadlocking on the
    /// second ticket for the same stripe, and everything must be released on
    /// drop so later single-key guards proceed.
    #[test]
    fn batch_mutation_guards_dedupe_stripes_and_release() {
        let lock_map: LockMap<FullRowLock, u64> = LockMap::default();
        let keys: Vec<u64> = (0..1000).collect();

        let guards = lock_map.mutation_guards(keys.iter());
        assert!(guards.len() <= MUTATION_STRIPE_COUNT);
        drop(guards);

        for key in 0..1000u64 {
            let _guard = lock_map.mutation_guard(&key);
        }
    }

    #[test]
    fn bulk_mutation_guard_spans_chunk_gaps_without_holding_a_stripe() {
        let lock_map: LockMap<FullRowLock, u64> = LockMap::default();
        assert_eq!(lock_map.mutations_in_flight(), 0);

        let first = lock_map.bulk_mutation_guard();
        assert_eq!(lock_map.mutations_in_flight(), 1);
        {
            let second = lock_map.bulk_mutation_guard();
            assert_eq!(lock_map.mutations_in_flight(), 1);
            drop(second);
        }
        assert_eq!(lock_map.mutations_in_flight(), 1);

        drop(first);
        assert_eq!(lock_map.mutations_in_flight(), 0);
    }

    #[test]
    fn mutation_epoch_detects_work_that_finished_between_checks() {
        let lock_map: LockMap<FullRowLock, u64> = LockMap::default();
        let before = lock_map.mutation_epoch();

        let guard = lock_map.mutation_guard(&17);
        assert_eq!(lock_map.mutations_in_flight(), 1);
        drop(guard);

        assert_eq!(lock_map.mutations_in_flight(), 0);
        assert_ne!(lock_map.mutation_epoch(), before);
    }

    /// Two threads acquiring overlapping key sets in opposite caller order
    /// must not deadlock: stripe ordering, not caller ordering, decides
    /// acquisition order.
    #[test]
    fn concurrent_batch_mutation_guards_do_not_deadlock() {
        let lock_map: Arc<LockMap<FullRowLock, u64>> = Arc::new(LockMap::default());
        let forward: Vec<u64> = (0..256).collect();
        let mut backward = forward.clone();
        backward.reverse();

        let other_map = lock_map.clone();
        let handle = std::thread::spawn(move || {
            for _ in 0..100 {
                let _guards = other_map.mutation_guards(backward.iter());
            }
        });
        for _ in 0..100 {
            let _guards = lock_map.mutation_guards(forward.iter());
        }
        handle.join().unwrap();
    }

    /// Regression for issue #33: cleanup can run while a task owns the value
    /// returned by `get_or_insert_with`, then that task can be cancelled before
    /// registering an operation. Dropping the acquisition must retry cleanup.
    #[test]
    fn cancelled_acquirer_removes_the_abandoned_entry() {
        let lock_map: Arc<LockMap<FullRowLock, u64>> = Arc::new(LockMap::default());
        let acquirer = lock_map.get_or_insert_with(31, FullRowLock::new);

        lock_map.remove_with_lock_check(&31);
        assert!(lock_map.map.read().contains_key(&31));

        drop(acquirer);
        assert!(!lock_map.map.read().contains_key(&31));
    }

    /// Cloning the acquisition handle represents two tasks between lookup and
    /// registration. The first cancellation must retain the shared lock, and
    /// only the last handle may remove it.
    #[test]
    fn cleanup_waits_for_every_acquirer_to_drop() {
        let lock_map: Arc<LockMap<FullRowLock, u64>> = Arc::new(LockMap::default());
        let first = lock_map.get_or_insert_with(33, FullRowLock::new);
        let second = first.clone();

        drop(first);
        assert!(lock_map.map.read().contains_key(&33));

        drop(second);
        assert!(!lock_map.map.read().contains_key(&33));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelling_async_waiter_releases_tracking_without_deadlock() {
        let lock_map: Arc<LockMap<FullRowLock, u64>> = Arc::new(LockMap::default());
        let owner = lock_map.get_or_insert_with(41, FullRowLock::new);
        let owner_guard = owner.write().await;
        let waiter = lock_map.get_or_insert_with(41, FullRowLock::new);
        let waiting_task = tokio::spawn(async move {
            let _guard = waiter.write().await;
        });
        tokio::task::yield_now().await;

        waiting_task.abort();
        assert!(waiting_task.await.unwrap_err().is_cancelled());
        assert!(lock_map.map.read().contains_key(&41));

        drop(owner_guard);
        drop(owner);
        assert!(!lock_map.map.read().contains_key(&41));
    }
}
