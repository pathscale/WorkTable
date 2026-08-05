//! Logical change capture for persisted unique WorkTablesIndex instances.
//!
//! The normal persisted path asks the live index to produce structural CDC
//! events. With `logical-index-persistence`, this wrapper instead emits one
//! logical Set/Remove event. The background disk-side shadow index translates
//! it back into structural events, preserving the existing on-disk format
//! while removing structural CDC bookkeeping from foreground mutations.
//! `index = 0` and `max_value == value` are an intentionally synthetic marker,
//! not claims about the live WTI node position or maximum; the shadow validates
//! that marker and derives the real structural metadata itself.

use std::array;
use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use data_bucket::Link;
use indexset::cdc::change::{ChangeEvent, Id};
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;
use parking_lot::{Mutex, MutexGuard};
use rustc_hash::FxHasher;

use crate::index::UniqueIndex;
use crate::util::OffsetEqLink;
use crate::{IndexMap, TableIndexCdc};

// A stripe provides per-key exclusion only: FxHasher has no relationship
// to key order, so callers must not infer range or cross-key ordering from the
// selected mutex. Reads never touch this fixed inline table. Logical batches
// are ordered independently by event id in the persistence worker.
const MUTATION_STRIPES: usize = 64;

#[inline]
fn mutation_stripe_index<Q: Hash + ?Sized>(key: &Q) -> usize {
    // Stripe selection is not a security boundary. FxHasher avoids SipHash's
    // per-mutation cost and distributes the overwhelmingly common sequential
    // integer keys across the power-of-two stripe table.
    let mut hasher = FxHasher::default();
    key.hash(&mut hasher);
    hasher.finish() as usize & (MUTATION_STRIPES - 1)
}

/// A persisted WorkTablesIndex whose foreground mutations emit logical CDC.
///
/// Point reads delegate directly to the native index. There is no runtime
/// feature check, read lock, or consistency branch on the select path.
pub struct PersistentWtiIndex<K, V, Node = Vec<Pair<K, V>>>
where
    K: Send + Ord + Clone + 'static,
    V: Send + Clone + 'static,
    Node: NodeLike<Pair<K, V>>,
{
    inner: IndexMap<K, V, Node>,
    next_event_id: AtomicU64,
    // Fixed inline allocation: 64 parking_lot mutexes per persisted WTI. The
    // enclosing index's allocation size accounts for these; there is no
    // per-mutation or per-key mutex allocation.
    mutation_stripes: [Mutex<()>; MUTATION_STRIPES],
}

impl<K, V, Node> Debug for PersistentWtiIndex<K, V, Node>
where
    K: Send + Ord + Clone + 'static,
    V: Send + Clone + 'static,
    Node: NodeLike<Pair<K, V>>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PersistentWtiIndex")
            .field("next_event_id", &self.next_event_id.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<K, V, Node> Default for PersistentWtiIndex<K, V, Node>
where
    K: Send + Ord + Clone + 'static,
    V: Send + Clone + 'static,
    Node: NodeLike<Pair<K, V>>,
    IndexMap<K, V, Node>: Default,
{
    fn default() -> Self {
        Self::from_inner(IndexMap::default())
    }
}

impl<K, V, Node> PersistentWtiIndex<K, V, Node>
where
    K: Send + Ord + Clone + 'static,
    V: Send + Clone + 'static,
    Node: NodeLike<Pair<K, V>>,
{
    pub fn from_inner(inner: IndexMap<K, V, Node>) -> Self {
        Self {
            inner,
            next_event_id: AtomicU64::new(0),
            mutation_stripes: array::from_fn(|_| Mutex::new(())),
        }
    }

    pub fn into_inner(self) -> IndexMap<K, V, Node> {
        self.inner
    }

    pub fn inner(&self) -> &IndexMap<K, V, Node> {
        &self.inner
    }

    #[inline]
    fn mutation_stripe<Q: Hash + ?Sized>(&self, key: &Q) -> MutexGuard<'_, ()> {
        self.mutation_stripes[mutation_stripe_index(key)].lock()
    }

    fn next_event_id(&self) -> Id {
        self.next_event_id.fetch_add(1, Ordering::AcqRel).into()
    }
}

impl<K, V, Node> PersistentWtiIndex<K, V, Node>
where
    K: Debug + Send + Ord + Clone + 'static,
    V: Debug + Send + Clone + 'static,
    Node: NodeLike<Pair<K, V>> + Send + 'static,
{
    pub fn with_maximum_node_size(node_capacity: usize) -> Self {
        Self::from_inner(IndexMap::with_maximum_node_size(node_capacity))
    }

    pub fn attach_node(&self, node: Node) {
        self.inner.attach_node(node);
    }

    pub fn iter_nodes(&self) -> impl Iterator<Item = Arc<Mutex<Node>>> + '_ {
        self.inner.iter_nodes()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&K, &V)> + '_ {
        self.inner.iter()
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn node_count(&self) -> usize {
        self.inner.node_count()
    }
}

impl<K, V, Node> UniqueIndex<K, V> for PersistentWtiIndex<K, V, Node>
where
    K: Debug + Eq + Hash + Clone + Send + Ord + 'static,
    V: Debug + Clone + Send + Ord + 'static,
    Node: NodeLike<Pair<K, V>> + Send + 'static,
{
    #[inline(always)]
    fn get_value(&self, key: &K) -> Option<V> {
        self.inner.lookup_for_select(key)
    }

    #[inline(always)]
    fn lookup_for_select(&self, key: &K) -> Option<V> {
        self.inner.lookup_for_select(key)
    }

    #[inline(always)]
    fn with_value<R>(&self, key: &K, read: impl FnOnce(&V) -> R) -> Option<R> {
        self.inner.lookup_for_select(key).as_ref().map(read)
    }

    #[inline(always)]
    fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    #[inline]
    fn insert_value(&self, key: K, value: V) -> Option<V> {
        self.inner.insert(key, value)
    }

    #[inline]
    fn insert_value_checked(&self, key: K, value: V) -> Option<()> {
        self.inner.checked_insert(key, value)
    }

    #[inline]
    fn remove_value(&self, key: &K) -> Option<(K, V)> {
        self.inner.remove(key)
    }

    #[inline]
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn iter_values(&self) -> impl DoubleEndedIterator<Item = (K, V)> + '_ {
        self.inner.iter().map(|(key, value)| (key.clone(), value.clone()))
    }

    fn iter_links(&self) -> impl DoubleEndedIterator<Item = V> + '_ {
        self.inner.iter().map(|(_, value)| value.clone())
    }

    fn range_values<'a, R>(&'a self, range: R) -> impl DoubleEndedIterator<Item = (K, V)> + 'a
    where
        R: RangeBounds<K> + 'a,
    {
        self.inner.range(range).map(|(key, value)| (key.clone(), value.clone()))
    }

    fn range_links<'a, R>(&'a self, range: R) -> impl DoubleEndedIterator<Item = V> + 'a
    where
        R: RangeBounds<K> + 'a,
    {
        self.inner.range(range).map(|(_, value)| value.clone())
    }
}

impl<T, Node, const N: usize> TableIndexCdc<T> for PersistentWtiIndex<T, OffsetEqLink<N>, Node>
where
    T: Debug + Eq + Hash + Clone + Send + Ord + 'static,
    Node: NodeLike<Pair<T, OffsetEqLink<N>>> + Send + 'static,
{
    fn insert_cdc(&self, value: T, link: Link) -> (Option<Link>, Vec<ChangeEvent<Pair<T, Link>>>) {
        let _sequence_guard = self.mutation_stripe(&value);
        let old = self
            .inner
            .insert(value.clone(), OffsetEqLink(link))
            .map(|value| value.0);
        let pair = Pair {
            key: value,
            value: link,
        };
        let event = ChangeEvent::InsertAt {
            event_id: self.next_event_id(),
            max_value: pair.clone(),
            value: pair,
            index: 0,
        };
        (old, vec![event])
    }

    fn insert_checked_cdc(&self, value: T, link: Link) -> Option<Vec<ChangeEvent<Pair<T, Link>>>> {
        let _sequence_guard = self.mutation_stripe(&value);
        self.inner.checked_insert(value.clone(), OffsetEqLink(link))?;
        let pair = Pair {
            key: value,
            value: link,
        };
        Some(vec![ChangeEvent::InsertAt {
            event_id: self.next_event_id(),
            max_value: pair.clone(),
            value: pair,
            index: 0,
        }])
    }

    fn remove_cdc(&self, value: T, _: Link) -> (Option<(T, Link)>, Vec<ChangeEvent<Pair<T, Link>>>) {
        let _sequence_guard = self.mutation_stripe(&value);
        let Some((key, old)) = self.inner.remove(&value) else {
            return (None, Vec::new());
        };
        let pair = Pair {
            key: key.clone(),
            value: old.0,
        };
        let event = ChangeEvent::RemoveAt {
            event_id: self.next_event_id(),
            max_value: pair.clone(),
            value: pair,
            index: 0,
        };
        (Some((key, old.0)), vec![event])
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use data_bucket::page::PageId;

    use super::*;

    fn link(offset: u32) -> Link {
        Link {
            page_id: PageId::from(1),
            offset,
            length: 8,
        }
    }

    #[test]
    fn reads_delegate_and_mutations_emit_one_logical_event() {
        let index = PersistentWtiIndex::<u64, OffsetEqLink<4096>>::default();
        let (old, events) = index.insert_cdc(7, link(7));
        assert_eq!(old, None);
        assert_eq!(events.len(), 1);
        assert_eq!(index.get_value(&7), Some(OffsetEqLink(link(7))));

        let (removed, events) = index.remove_cdc(7, link(7));
        assert_eq!(removed, Some((7, link(7))));
        assert_eq!(events.len(), 1);
        assert_eq!(index.get_value(&7), None);
    }

    #[test]
    fn rejected_checked_insert_does_not_consume_an_event_id() {
        let index = PersistentWtiIndex::<u64, OffsetEqLink<4096>>::default();
        assert!(index.insert_checked_cdc(7, link(7)).is_some());
        assert_eq!(index.next_event_id.load(Ordering::Relaxed), 1);

        assert!(index.insert_checked_cdc(7, link(8)).is_none());
        assert_eq!(index.next_event_id.load(Ordering::Relaxed), 1);

        assert!(index.insert_checked_cdc(8, link(8)).is_some());
        assert_eq!(index.next_event_id.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn sequential_integer_keys_use_every_mutation_stripe() {
        let stripes = (0_u64..4_096)
            .map(|key| mutation_stripe_index(&key))
            .collect::<HashSet<_>>();
        assert_eq!(stripes.len(), MUTATION_STRIPES);
    }
}
