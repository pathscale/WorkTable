use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt::Debug;
use core::hash::{Hash, Hasher};
use core::ops::Deref;
use core::sync::atomic::{AtomicU16, AtomicU64, AtomicUsize, Ordering};
use hashbrown::HashMap;
use rustc_hash::FxHasher as DefaultHasher;

use parking_lot::RwLock;

use crate::lock::RowLock;

/// Gates for the synchronous mutation phase.
///
/// Sixty-four is enough: raising it to 1024 on top of the shard count below
/// moves `paged_in_place` at eight workers from 2.81x to 2.92x, which does not
/// pay for sixteen times the gates. The gate is entered and left in tens of
/// nanoseconds, so collisions on it stay rare at this count.
const MUTATION_STRIPE_COUNT: usize = 64;
/// Independent `RwLock<HashMap>` shards of the row-lock map.
///
/// Far more than the stripe count, because a locked operation costs the map
/// two exclusive shard acquisitions where it costs the gate one short critical
/// section: a disjoint-key writer inserts its entry on the way in and removes
/// it on the way out, and a shard collision parks the loser in the kernel.
/// Measured on `paged_in_place`, eight workers over 16384 keys:
///
/// | shards | w8 vs w1 |
/// |---:|---:|
/// | 64 | 1.70x |
/// | 256 | 2.39x |
/// | 512 | 2.63x |
/// | 1024 | **2.81x** |
/// | 2048 | 3.07x |
///
/// 2048 still gains, but it doubles a per-table array for 9%. A shard holds no
/// buckets until a key lands in it, so what this count costs is the array, not
/// the maps.
///
/// One table-wide `RwLock<HashMap>` serialized every acquire and drop before
/// any of this: 8 disjoint writers then burned ~6 cores for 1.27x replace.
const MAP_SHARD_COUNT: usize = 1024;

/// One mutation stripe: a ticket lock and the lock-label counter for the same
/// set of keys.
///
/// Aligned to a whole coherency granule. Unpadded, the three fields are 20
/// bytes, so eight stripes shared one line: a writer taking a ticket for its
/// own stripe invalidated the line seven unrelated stripes were spinning on,
/// and the `serving` spin re-read it every time. Sixty-four stripes behaved
/// like eight. The label counter lives here rather than in its own array so a
/// locked operation touches one line for both, not two.
#[derive(Debug, Default)]
#[repr(align(128))]
struct MutationStripe {
    next_ticket: AtomicU64,
    serving: AtomicU64,
    next_label: AtomicU16,
}

/// Synchronous, task-safe gate for one primary-key mutation stripe.
///
/// Generated async row locks and synchronous inserts share these gates so a
/// synchronous API entry point cannot interleave its multi-structure
/// publication with an update or delete of the same key.
#[derive(Debug)]
pub struct MutationGuard {
    /// Borrowed, not an `Arc` clone.
    ///
    /// Cloning the map's stripe array put an atomic increment and a matching
    /// decrement on one refcount word into every mutation. That word is shared
    /// by every worker on the table and is independent of the key, so striping
    /// cannot dilute it and a larger key space does not either: it is the same
    /// defect as the `Arc<LockMap>` clones removed from `LockAcquirer` and
    /// `PendingLock`, in the one place on the path that still had it.
    ///
    /// # Safety
    ///
    /// A guard is a local of the operation that took it, and that operation
    /// reached this map through the table's own `Arc<LockMap>`, which it holds
    /// for the whole call. The array therefore outlives every guard taken from
    /// it.
    stripes: *const [MutationStripe; MUTATION_STRIPE_COUNT],
    stripe: usize,
}

// SAFETY: the pointed-to array is `Sync` and outlives the guard (see the field
// note), so a guard is no less safe to move or share than a `&[MutationStripe]`.
unsafe impl Send for MutationGuard {}
unsafe impl Sync for MutationGuard {}

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

/// One shard of the row-lock map.
type LockShard<LockType, PrimaryKey> = RwLock<HashMap<PrimaryKey, LockEntry<LockType>>>;

#[derive(Debug)]
struct LockEntry<LockType> {
    lock: Arc<nagoya::sync::RwLock<LockType>>,
    /// Callers that may still register an operation against this entry.
    ///
    /// Inline, not an `Arc`. An acquirer used to clone it so it could
    /// decrement without touching the map, but its drop then took the shard's
    /// write lock anyway, to re-look the entry up for the removal check. The
    /// clone bought nothing and cost an allocation and a free on every
    /// operation; the decrement now happens under that same write guard.
    acquirers: AtomicUsize,
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
    lock: Option<Arc<nagoya::sync::RwLock<LockType>>>,
    /// Borrowed, not an `Arc` clone.
    ///
    /// Cloning the map's `Arc` here put an atomic increment and a matching
    /// decrement on **one** refcount word into every operation, and that word
    /// is shared by every worker on the table. It is independent of the key,
    /// so sharding the map cannot help and a larger key space does not dilute
    /// it: measured, `paged_in_place` scales the same at 1k rows and at 262k.
    /// Three such clones per operation reproduce the whole negative slope in a
    /// twenty-line program with no WorkTable in it (92M ops/s at one worker,
    /// 5.4M at eight).
    ///
    /// # Safety
    ///
    /// The acquirer is a local of the operation that took it, and that
    /// operation reached this map through the table's own
    /// `Arc<LockMap>`, which it holds for the whole call. The map therefore
    /// outlives every acquirer taken from it.
    lock_map: *const LockMap<LockType, PrimaryKey>,
    primary_key: PrimaryKey,
}

// SAFETY: `LockMap` is `Sync`, and the pointer is only ever dereferenced while
// the owning `Arc` is alive (see the field note), so an acquirer is no less
// safe to move or share than a `&LockMap` would be.
unsafe impl<LockType, PrimaryKey> Send for LockAcquirer<LockType, PrimaryKey>
where
    LockType: RowLock + Send + Sync,
    PrimaryKey: Hash + Eq + Debug + Clone + Send,
{
}
unsafe impl<LockType, PrimaryKey> Sync for LockAcquirer<LockType, PrimaryKey>
where
    LockType: RowLock + Send + Sync,
    PrimaryKey: Hash + Eq + Debug + Clone + Sync,
{
}

impl<LockType, PrimaryKey> Clone for LockAcquirer<LockType, PrimaryKey>
where
    LockType: RowLock,
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    fn clone(&self) -> Self {
        // SAFETY: see the field note; the map outlives this acquirer.
        unsafe { (*self.lock_map).retain_acquirer(&self.primary_key) };
        Self {
            lock: self.lock.clone(),
            lock_map: self.lock_map,
            primary_key: self.primary_key.clone(),
        }
    }
}

impl<LockType, PrimaryKey> Deref for LockAcquirer<LockType, PrimaryKey>
where
    LockType: RowLock,
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    type Target = nagoya::sync::RwLock<LockType>;

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
        drop(self.lock.take());
        // SAFETY: see the field note on `lock_map`; the owning map outlives
        // this acquirer.
        unsafe { (*self.lock_map).release_acquirer(&self.primary_key) };
    }
}

impl Drop for MutationGuard {
    fn drop(&mut self) {
        // SAFETY: see the field note; the map outlives this guard.
        unsafe { (*self.stripes)[self.stripe].serving.fetch_add(1, Ordering::Release) };
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
/// The `parking_lot` shard guard is never returned and never crosses an
/// `.await`. Acquisition clones a tracked `Arc<nagoya::sync::RwLock<_>>` before
/// releasing the shard guard. Cleanup may synchronously take the short-lived
/// shard write guard, but only probes the per-row lock with `try_read`; it never
/// waits on a Tokio lock while holding the shard. This one-way boundary prevents
/// a map-lock/per-row-lock cycle during cancellation and `Drop`.
///
/// # Borrowed guards
///
/// [`LockAcquirer`], [`MutationGuard`] and their callers in [`crate::lock`]
/// hold a raw `*const` to this map or to its stripe array rather than an
/// `Arc` clone or a `&`. The `Arc` clone is what they are avoiding: it is a
/// read-modify-write on one key-independent cache line per operation, and
/// removing three of them took `paged_in_place` from losing throughput with
/// every added worker to holding it. A `&` would be sound and equally fast,
/// but these guards are held across `.await` inside generated operations whose
/// futures must be `'static` to spawn, so a lifetime on the guard becomes a
/// lifetime on the future.
///
/// What that costs is a safe public API that can be misused: taking a guard
/// and then dropping the last `Arc<LockMap>` while the guard lives is
/// undefined behaviour. Every in-crate and generated caller takes its guard as
/// a local of an operation that holds the table's own `Arc<LockMap>` for the
/// whole call, which is why this is sound as used.
///
/// # Sharding
///
/// The map is `MAP_SHARD_COUNT` independent `RwLock<HashMap>`s. A single
/// table-wide map lock made every `get_or_insert_with` miss and every
/// `LockAcquirer` drop exclusive against every other row. Shards and mutation
/// stripes share one hash of the key but reduce it separately, because the two
/// counts answer to different costs.
#[derive(Debug)]
pub struct LockMap<LockType, PrimaryKey> {
    map: Box<[LockShard<LockType, PrimaryKey>; MAP_SHARD_COUNT]>,
    /// Table-wide label counter, for the cold callers that have no key in hand
    /// (vacuum, and the raw `FullRowLock` helper). Locked operations must use
    /// [`Self::next_id_for`] instead: see the note on `mutation_stripes`.
    next_id: AtomicU16,
    /// Per-shard label counters, one to a cache line.
    ///
    /// `Lock::id` is a diagnostic label. It is not dependency identity, which
    /// is `Arc` pointer equality on the lock's `locked` flag, and nothing in
    /// the protocol reads it back. Minting it from one table-wide
    /// `AtomicU16::fetch_add` nevertheless put a read-modify-write on a single
    /// shared cache line into every locked operation, and, being independent
    /// of the key, it was a line that eight writers on disjoint rows still
    /// fought over. That is the same shape of defect as the `Arc<LockMap>`
    /// refcount clone removed just before it, and it survived that fix because
    /// the label is minted in generated code rather than in the map.
    ///
    /// Striping by the key's shard makes the line key-dependent, so disjoint
    /// writers stop sharing it. Labels may now repeat across shards sooner
    /// than a single counter would repeat, which the type already allows: a
    /// `u16` wraps every 65536 operations regardless.
    ///
    /// The counters live in `mutation_stripes`, one per stripe, because a
    /// locked operation already touches its stripe's line.
    mutation_stripes: Arc<[MutationStripe; MUTATION_STRIPE_COUNT]>,
    bulk_mutations: Arc<AtomicUsize>,
}

impl<LockType, PrimaryKey> Default for LockMap<LockType, PrimaryKey> {
    fn default() -> Self {
        Self {
            map: Box::new(core::array::from_fn(|_| RwLock::new(HashMap::new()))),
            next_id: AtomicU16::default(),
            mutation_stripes: Arc::new(core::array::from_fn(|_| MutationStripe::default())),
            bulk_mutations: Arc::default(),
        }
    }
}

impl<LockType, PrimaryKey> LockMap<LockType, PrimaryKey>
where
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    fn shard(&self, key: &PrimaryKey) -> &LockShard<LockType, PrimaryKey> {
        &self.map[Self::shard_of(key)]
    }

    #[cfg(test)]
    fn contains_key(&self, key: &PrimaryKey) -> bool {
        self.shard(key).read().contains_key(key)
    }

    /// Inserts a raw lock entry.
    ///
    /// A returned or externally retained `Arc` pins cleanup through
    /// `Arc::strong_count`. Generated operations should prefer
    /// [`Self::get_or_insert_with`], whose [`LockAcquirer`] makes cancellation
    /// tracking explicit.
    pub fn insert(
        &self,
        key: PrimaryKey,
        lock: Arc<nagoya::sync::RwLock<LockType>>,
    ) -> Option<Arc<nagoya::sync::RwLock<LockType>>> {
        self.shard(&key)
            .write()
            .insert(
                key,
                LockEntry {
                    lock,
                    acquirers: AtomicUsize::new(0),
                },
            )
            .map(|entry| entry.lock)
    }

    /// Returns an untracked raw lock clone, which keeps the map entry alive
    /// until that clone is dropped.
    pub fn get(&self, key: &PrimaryKey) -> Option<Arc<nagoya::sync::RwLock<LockType>>> {
        self.shard(key).read().get(key).map(|entry| entry.lock.clone())
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
        if let Some(entry) = self.shard(&key).read().get(&key) {
            entry.acquirers.fetch_add(1, Ordering::AcqRel);
            return LockAcquirer {
                lock: Some(entry.lock.clone()),
                lock_map: Arc::as_ptr(self),
                primary_key: key,
            };
        }
        let mut map = self.shard(&key).write();
        // Re-check: another task can insert between the read and write guards.
        let entry = map.entry(key.clone()).or_insert_with(|| LockEntry {
            lock: Arc::new(nagoya::sync::RwLock::new(f())),
            acquirers: AtomicUsize::new(0),
        });
        entry.acquirers.fetch_add(1, Ordering::AcqRel);
        LockAcquirer {
            lock: Some(entry.lock.clone()),
            lock_map: Arc::as_ptr(self),
            primary_key: key,
        }
    }

    pub fn remove(&mut self, key: &PrimaryKey) {
        self.shard(key).write().remove(key);
    }

    /// Registers one more caller that may still acquire `key`'s entry.
    ///
    /// Only [`LockAcquirer::clone`] needs this; the acquiring paths increment
    /// while they already hold a shard guard.
    fn retain_acquirer(&self, key: &PrimaryKey) {
        if let Some(entry) = self.shard(key).read().get(key) {
            entry.acquirers.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Drops one acquirer of `key` and removes the entry if that was the last
    /// reason to keep it.
    ///
    /// The decrement and the removal check share one shard write guard. Split
    /// across an atomic and a separate call they were two shared-memory
    /// operations where one does, and the check has to take the guard either
    /// way.
    fn release_acquirer(&self, key: &PrimaryKey)
    where
        LockType: RowLock,
    {
        let mut set = self.shard(key).write();
        if let Some(entry) = set.get(key) {
            entry.acquirers.fetch_sub(1, Ordering::AcqRel);
        }
        Self::remove_if_unused(&mut set, key);
    }

    pub fn remove_with_lock_check(&self, key: &PrimaryKey)
    where
        LockType: RowLock,
    {
        let mut set = self.shard(key).write();
        Self::remove_if_unused(&mut set, key);
    }

    fn remove_if_unused(set: &mut HashMap<PrimaryKey, LockEntry<LockType>>, key: &PrimaryKey)
    where
        LockType: RowLock,
    {
        let should_remove = set.get(key).is_some_and(|entry| {
            let Some(guard) = entry.lock.try_read() else {
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

    /// Mints a lock label from `key`'s own shard counter.
    ///
    /// The hot-path form of [`Self::next_id`]. Generated locked operations
    /// call this; see the note on `next_ids` for why the table-wide counter is
    /// not acceptable there.
    pub fn next_id_for(&self, key: &PrimaryKey) -> u16 {
        self.mutation_stripes[Self::stripe_of(key)]
            .next_label
            .fetch_add(1, Ordering::Relaxed)
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

    fn hash_of(key: &PrimaryKey) -> usize {
        let mut hasher = DefaultHasher::default();
        key.hash(&mut hasher);
        hasher.finish() as usize
    }

    fn stripe_of(key: &PrimaryKey) -> usize {
        Self::hash_of(key) % MUTATION_STRIPE_COUNT
    }

    /// The row-lock shard for `key`.
    ///
    /// Derived from the same hash as the mutation stripe but reduced
    /// separately: the two counts are tuned against different costs and are
    /// not required to match. Folding the stripe index into the shard index
    /// instead, as one modulo of the other, silently caps the shard count at
    /// the stripe count.
    fn shard_of(key: &PrimaryKey) -> usize {
        Self::hash_of(key) % MAP_SHARD_COUNT
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
                core::hint::spin_loop();
            } else {
                crate::util::yield_now();
            }
        }

        MutationGuard {
            stripes: Arc::as_ptr(&self.mutation_stripes),
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
        assert!(lock_map.contains_key(&31));

        drop(acquirer);
        assert!(!lock_map.contains_key(&31));
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
        assert!(lock_map.contains_key(&33));

        drop(second);
        assert!(!lock_map.contains_key(&33));
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
        assert!(lock_map.contains_key(&41));

        drop(owner_guard);
        drop(owner);
        assert!(!lock_map.contains_key(&41));
    }

    /// Disjoint keys must not share a map write lock. Eight threads each
    /// acquiring and dropping a private key 10_000 times used to serialize on
    /// one `RwLock<HashMap>`; they must complete without deadlock.
    #[test]
    fn disjoint_keys_do_not_share_a_map_write_lock() {
        let lock_map: Arc<LockMap<FullRowLock, u64>> = Arc::new(LockMap::default());
        let mut handles = Vec::new();
        for worker in 0..8u64 {
            let map = lock_map.clone();
            handles.push(std::thread::spawn(move || {
                for step in 0..10_000u64 {
                    let key = worker << 32 | step;
                    let acquirer = map.get_or_insert_with(key, FullRowLock::new);
                    drop(acquirer);
                    assert!(!map.contains_key(&key));
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
