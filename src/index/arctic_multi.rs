//! Arctic adapter for memory-only non-unique WorkTable indexes.
//!
//! Arctic is a strictly unique-key map whose payload is a single
//! pointer-tagged 64-bit word, so a non-unique index cannot store its links
//! in the tree directly. This adapter publishes one boxed link slot per key
//! (a heap pointer satisfies Arctic's below-2^63 payload invariant) and keeps
//! every link with that key inside the slot.
//!
//! # Design
//!
//! Three shapes were considered:
//!
//! 1. **Boxed link collection per key (chosen).** One Arctic entry per
//!    distinct key; the payload is `Box<RwLock<LinkSlot>>`. Point lookups stay
//!    a single native ART probe plus one short read-lock, insert/remove are a
//!    probe plus a short write-lock, and memory is proportional to keys plus
//!    links.
//! 2. **Composite sub-keying (rejected).** Packing a link discriminator into
//!    unused key bits only works for keys narrower than Arctic's u128
//!    maximum. The primary consumer indexes full-width u128 hashes, so there
//!    are no bits to steal, and a design that misses the primary use case is
//!    not worth its complexity.
//! 3. **Nested ART per key (rejected).** A `Box<ArcticIndex<...>>` payload
//!    keyed by link position gives O(log fan-out) mutations without any lock,
//!    but costs a whole second tree per key. Typical fan-outs are small, so
//!    per-key constant overhead dominates and iteration gets slower; the
//!    read-copy-free slot already keeps mutations O(fan-out) only in the
//!    worst case (`Vec::remove`) and O(1) amortized for insert.
//!
//! Mutating the slot in place (instead of read-copy-update through Arctic's
//! `upsert_with`) avoids cloning the whole link vector on every insert, which
//! would make bulk-loading a hot key quadratic. The `RwLock` is per key, held
//! for a few instructions, and never nested, so it cannot deadlock and only
//! serializes writers of the same key — the same-key serialization WorkTable
//! cannot provide itself, because its mutation guard is per primary key and
//! two rows sharing a secondary key mutate this index concurrently.
//!
//! # Slot lifecycle
//!
//! Removing the last link of a key must drop the Arctic entry, but a
//! concurrent inserter may already hold a reference to the doomed slot. The
//! `dead` flag closes that race: a remover that empties a slot marks it dead
//! under the write lock, and every writer checks the flag under the same
//! lock. Once dead, a slot is never revived — a late inserter helps delete
//! the dead entry and retries with a fresh slot, and the SMR guard it holds
//! keeps the memory valid throughout.

use std::borrow::Borrow;
use std::fmt::{self, Debug};
use std::ops::{ControlFlow, RangeBounds};
use std::sync::atomic::{AtomicUsize, Ordering};

use arctic::{ConcurrentMap, Key as ArcticNativeKey, Order};
use parking_lot::RwLock;

use super::arctic::{ArcticKey, raw_inclusive_bounds};

/// Links of a single key, guarded by the slot's `RwLock`.
struct LinkSlot<V> {
    /// Set (permanently) when the slot empties and its Arctic entry is about
    /// to be removed. Writers observing it must not touch `links`.
    dead: bool,
    links: Vec<V>,
}

impl<V> LinkSlot<V> {
    fn with_link(value: V) -> Self {
        Self {
            dead: false,
            links: vec![value],
        }
    }
}

/// Arctic's lock-free adaptive radix tree with multimap semantics, for
/// WorkTable's non-unique secondary indexes.
///
/// Duplicate `(key, link)` pairs are stored as-is (multiset semantics,
/// mirroring the WorkTablesIndex multimap): `insert` always succeeds and
/// `remove` drops exactly one matching occurrence.
pub struct ArcticMultiIndex<K: ArcticKey, V> {
    inner: ConcurrentMap<K::Raw, Box<RwLock<LinkSlot<V>>>>,
    /// Total number of `(key, link)` pairs, maintained with relaxed atomics:
    /// exact once writers quiesce, monotonic enough for stats meanwhile.
    len: AtomicUsize,
}

impl<K: ArcticKey, V> Debug for ArcticMultiIndex<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArcticMultiIndex")
            .field("len", &self.len.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<K: ArcticKey, V> Default for ArcticMultiIndex<K, V> {
    fn default() -> Self {
        Self {
            inner: ConcurrentMap::default(),
            len: AtomicUsize::new(0),
        }
    }
}

impl<K, V> ArcticMultiIndex<K, V>
where
    K: ArcticKey,
    V: Clone + Debug + PartialEq + Send + Sync + 'static,
{
    /// Adds one `(key, value)` pair.
    pub fn insert_pair(&self, key: K, value: V) {
        let raw = key.to_arctic();
        let mut value = Some(value);
        loop {
            match self.inner.insert_with(raw.as_insert(), || {
                Box::new(RwLock::new(LinkSlot::with_link(
                    value.take().expect("consumed at most once per attempt"),
                )))
            }) {
                Ok(_) => {
                    self.len.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                Err((slot, allocated)) => {
                    if let Some(allocated) = allocated {
                        // The closure ran but lost to a concurrent insert of
                        // the same key; take the link back for this retry.
                        value = Some(
                            (*allocated)
                                .into_inner()
                                .links
                                .pop()
                                .expect("freshly allocated slot holds exactly one link"),
                        );
                    }
                    let mut links = slot.write();
                    if links.dead {
                        drop(links);
                        drop(slot);
                        // A remover emptied this slot; help delete the dead
                        // entry so the retry can publish a fresh one.
                        self.remove_dead_slot(&raw);
                        continue;
                    }
                    links
                        .links
                        .push(value.take().expect("reclaimed above or never consumed"));
                    self.len.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            }
        }
    }

    /// Removes one occurrence of `(key, value)` and returns it, or `None` if
    /// the pair is not present.
    pub fn remove_pair(&self, key: &K, value: &V) -> Option<V> {
        let raw = key.to_arctic();
        let slot = self.inner.get(raw.borrow())?;
        let mut links = slot.write();
        if links.dead {
            return None;
        }
        let position = links.links.iter().position(|existing| existing == value)?;
        let removed = links.links.remove(position);
        let now_empty = links.links.is_empty();
        if now_empty {
            links.dead = true;
        }
        drop(links);
        drop(slot);
        if now_empty {
            self.remove_dead_slot(&raw);
        }
        self.len.fetch_sub(1, Ordering::Relaxed);
        Some(removed)
    }

    /// Deletes a slot marked dead. Break on a live slot: the dead entry was
    /// already removed and the key re-inserted with a fresh slot.
    fn remove_dead_slot(&self, raw: &K::Raw) {
        self.inner.remove_with(raw.borrow(), |slot| {
            if slot.read().dead {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        });
    }

    /// Returns every `(key, value)` pair stored under `key`, in insertion
    /// order, as a stable snapshot. An unknown key yields an empty iterator.
    pub fn get(&self, key: &K) -> std::vec::IntoIter<(K, V)> {
        let raw = key.to_arctic();
        let Some(slot) = self.inner.get(raw.borrow()) else {
            return Vec::new().into_iter();
        };
        let links = slot.read();
        links
            .links
            .iter()
            .map(|value| (key.clone(), value.clone()))
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Total number of `(key, value)` pairs.
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the pairs whose keys fall in `range`, ascending by key and in
    /// insertion order within a key, as a stable snapshot.
    pub fn range<'a, R>(&'a self, range: R) -> impl DoubleEndedIterator<Item = (K, V)> + 'a
    where
        R: RangeBounds<K> + 'a,
    {
        let Some((lower, upper)) = raw_inclusive_bounds(&range) else {
            return Vec::new().into_iter();
        };

        // `EntryIter` only implements `Iterator` for cloneable payloads, so
        // walk the shard with the lending API instead: the slot stays behind
        // its `RwLock` while its links are copied out.
        macro_rules! collect_range {
            ($shard:expr) => {{
                let shard = $shard;
                let mut entries = shard.entries(Order::Ascend);
                let mut pairs = Vec::new();
                while let Some((key, slot)) = entries.lend() {
                    let raw = <K::Raw as ArcticNativeKey>::insert_to_key(key);
                    let links = slot.read();
                    pairs.extend(
                        links
                            .links
                            .iter()
                            .map(|value| (K::from_arctic(raw), value.clone())),
                    );
                }
                pairs
            }};
        }

        let values = match (lower, upper) {
            (Some(lower), Some(upper)) if lower <= upper => {
                collect_range!(self.inner.range(lower.borrow()..=upper.borrow()))
            }
            (Some(_), Some(_)) => Vec::new(),
            (Some(lower), None) => collect_range!(self.inner.range(lower.borrow()..)),
            (None, Some(upper)) => collect_range!(self.inner.range(..=upper.borrow())),
            (None, None) => collect_range!(self.inner.all()),
        };
        values.into_iter()
    }

    /// Returns every pair in the index, ascending by key.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (K, V)> + '_ {
        self.range(..)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::{Arc, Barrier};

    use super::ArcticMultiIndex;

    #[test]
    fn many_links_per_key_round_trip() {
        let index = ArcticMultiIndex::<u64, u64>::default();
        for value in 0..100 {
            index.insert_pair(7, value);
        }
        index.insert_pair(8, 1000);

        let links = index.get(&7).collect::<Vec<_>>();
        assert_eq!(links.len(), 100);
        assert_eq!(links[0], (7, 0));
        assert_eq!(links[99], (7, 99));
        assert_eq!(index.get(&8).collect::<Vec<_>>(), vec![(8, 1000)]);
        assert_eq!(index.len(), 101);
    }

    #[test]
    fn unknown_and_emptied_keys_yield_nothing() {
        let index = ArcticMultiIndex::<u128, u64>::default();
        assert_eq!(index.get(&42).collect::<Vec<_>>(), vec![]);
        assert_eq!(index.remove_pair(&42, &1), None);

        index.insert_pair(42, 1);
        assert_eq!(index.remove_pair(&42, &1), Some(1));
        assert_eq!(index.get(&42).collect::<Vec<_>>(), vec![]);
        assert_eq!(index.remove_pair(&42, &1), None);
        assert!(index.is_empty());

        // The key slot is fully deleted and can be repopulated.
        index.insert_pair(42, 2);
        assert_eq!(index.get(&42).collect::<Vec<_>>(), vec![(42, 2)]);
    }

    #[test]
    fn duplicate_pairs_are_kept_and_removed_one_at_a_time() {
        let index = ArcticMultiIndex::<u64, u64>::default();
        index.insert_pair(5, 9);
        index.insert_pair(5, 9);
        assert_eq!(index.len(), 2);
        assert_eq!(index.get(&5).len(), 2);

        assert_eq!(index.remove_pair(&5, &9), Some(9));
        assert_eq!(index.get(&5).collect::<Vec<_>>(), vec![(5, 9)]);
        assert_eq!(index.remove_pair(&5, &9), Some(9));
        assert_eq!(index.get(&5).collect::<Vec<_>>(), vec![]);
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn removing_one_link_keeps_the_others() {
        let index = ArcticMultiIndex::<u64, u64>::default();
        for value in 0..5 {
            index.insert_pair(3, value);
        }
        assert_eq!(index.remove_pair(&3, &2), Some(2));
        assert_eq!(index.get(&3).map(|(_, v)| v).collect::<Vec<_>>(), vec![0, 1, 3, 4]);
    }

    #[test]
    fn range_flattens_links_in_key_order() {
        let index = ArcticMultiIndex::<u64, u64>::default();
        for key in 0..10u64 {
            for value in 0..3u64 {
                index.insert_pair(key, key * 10 + value);
            }
        }

        let pairs = index.range(3..5).collect::<Vec<_>>();
        assert_eq!(pairs, vec![(3, 30), (3, 31), (3, 32), (4, 40), (4, 41), (4, 42)]);

        // Degenerate excluded bounds are empty, not unbounded.
        assert_eq!(index.range(..0).count(), 0);
        assert_eq!(index.range((Bound::Excluded(u64::MAX), Bound::Unbounded)).count(), 0);
        assert_eq!(index.iter().count(), 30);
    }

    #[test]
    fn concurrent_same_key_inserts_lose_nothing() {
        let index = Arc::new(ArcticMultiIndex::<u64, u64>::default());
        let threads = 8;
        let per_thread = 200;
        let barrier = Arc::new(Barrier::new(threads));
        let handles = (0..threads as u64)
            .map(|thread| {
                let index = Arc::clone(&index);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for value in 0..per_thread as u64 {
                        index.insert_pair(11, thread * 1000 + value);
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(index.len(), threads * per_thread);
        assert_eq!(index.get(&11).len(), threads * per_thread);
    }

    #[test]
    fn concurrent_insert_and_full_remove_agree_on_the_survivors() {
        // Removers drain a key to empty (exercising the dead-slot handoff)
        // while inserters keep publishing to the same key.
        let index = Arc::new(ArcticMultiIndex::<u64, u64>::default());
        let rounds = 500u64;
        let barrier = Arc::new(Barrier::new(2));

        let inserter = {
            let index = Arc::clone(&index);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                for value in 0..rounds {
                    index.insert_pair(1, value);
                }
            })
        };
        let remover = {
            let index = Arc::clone(&index);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                let mut removed = 0;
                for value in 0..rounds {
                    if index.remove_pair(&1, &value).is_some() {
                        removed += 1;
                    }
                }
                removed
            })
        };

        inserter.join().unwrap();
        let removed: usize = remover.join().unwrap();
        let survivors = index.get(&1).len();
        assert_eq!(removed + survivors, rounds as usize);
        assert_eq!(index.len(), survivors);
    }
}
