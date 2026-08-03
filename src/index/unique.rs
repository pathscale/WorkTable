//! Backend-neutral operations required by a unique WorkTable index.
//!
//! The trait deliberately returns copied/cloned values instead of exposing a
//! backend's guard type. That keeps generated code independent from the
//! concurrency and reclamation strategy used by each index implementation.

use std::fmt::Debug;
use std::hash::Hash;
use std::ops::RangeBounds;

use crate::IndexMap;
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;
use vanilla_indexset::concurrent::map::BTreeMap as VanillaIndexMap;
use vanilla_indexset::core::node::NodeLike as VanillaNodeLike;
use vanilla_indexset::core::pair::Pair as VanillaPair;

/// Point, mutation, and ordered-scan operations used by generated unique
/// indexes. Implementations are statically dispatched; this adds no virtual
/// call to the lookup path.
pub trait UniqueIndex<K, V>: Default
where
    K: Clone + Ord,
    V: Clone,
{
    fn get_value(&self, key: &K) -> Option<V>;
    fn insert_value(&self, key: K, value: V) -> Option<V>;
    fn insert_value_checked(&self, key: K, value: V) -> Option<()>;
    fn remove_value(&self, key: &K) -> Option<(K, V)>;
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn iter_values(&self) -> impl DoubleEndedIterator<Item = (K, V)> + '_;

    fn iter_links(&self) -> impl DoubleEndedIterator<Item = V> + '_;

    fn range_values<'a, R>(&'a self, range: R) -> impl DoubleEndedIterator<Item = (K, V)> + 'a
    where
        R: RangeBounds<K> + 'a;

    fn range_links<'a, R>(&'a self, range: R) -> impl DoubleEndedIterator<Item = V> + 'a
    where
        R: RangeBounds<K> + 'a;
}

impl<K, V, Node> UniqueIndex<K, V> for IndexMap<K, V, Node>
where
    K: Debug + Eq + Hash + Clone + Send + Ord + 'static,
    V: Debug + Clone + Send + Ord + 'static,
    Node: NodeLike<Pair<K, V>> + Send + 'static,
{
    #[inline]
    fn get_value(&self, key: &K) -> Option<V> {
        self.get(key).map(|entry| entry.get().value.clone())
    }

    #[inline]
    fn insert_value(&self, key: K, value: V) -> Option<V> {
        self.insert(key, value)
    }

    #[inline]
    fn insert_value_checked(&self, key: K, value: V) -> Option<()> {
        self.checked_insert(key, value)
    }

    #[inline]
    fn remove_value(&self, key: &K) -> Option<(K, V)> {
        self.remove(key)
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn iter_values(&self) -> impl DoubleEndedIterator<Item = (K, V)> + '_ {
        self.iter().map(|(key, value)| (key.clone(), value.clone()))
    }

    #[inline]
    fn iter_links(&self) -> impl DoubleEndedIterator<Item = V> + '_ {
        self.iter().map(|(_, value)| value.clone())
    }

    #[inline]
    fn range_values<'a, R>(&'a self, range: R) -> impl DoubleEndedIterator<Item = (K, V)> + 'a
    where
        R: RangeBounds<K> + 'a,
    {
        self.range(range).map(|(key, value)| (key.clone(), value.clone()))
    }

    #[inline]
    fn range_links<'a, R>(&'a self, range: R) -> impl DoubleEndedIterator<Item = V> + 'a
    where
        R: RangeBounds<K> + 'a,
    {
        self.range(range).map(|(_, value)| value.clone())
    }
}

impl<K, V, Node> UniqueIndex<K, V> for VanillaIndexMap<K, V, Node>
where
    K: Debug + Eq + Hash + Clone + Send + Ord + 'static,
    V: Debug + Clone + Send + Ord + 'static,
    Node: VanillaNodeLike<VanillaPair<K, V>> + Send + 'static,
{
    #[inline]
    fn get_value(&self, key: &K) -> Option<V> {
        self.get(key).map(|entry| entry.get().value.clone())
    }

    #[inline]
    fn insert_value(&self, key: K, value: V) -> Option<V> {
        self.insert(key, value)
    }

    #[inline]
    fn insert_value_checked(&self, key: K, value: V) -> Option<()> {
        self.checked_insert(key, value)
    }

    #[inline]
    fn remove_value(&self, key: &K) -> Option<(K, V)> {
        self.remove(key)
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn iter_values(&self) -> impl DoubleEndedIterator<Item = (K, V)> + '_ {
        self.iter().map(|(key, value)| (key.clone(), value.clone()))
    }

    #[inline]
    fn iter_links(&self) -> impl DoubleEndedIterator<Item = V> + '_ {
        self.iter().map(|(_, value)| value.clone())
    }

    #[inline]
    fn range_values<'a, R>(&'a self, range: R) -> impl DoubleEndedIterator<Item = (K, V)> + 'a
    where
        R: RangeBounds<K> + 'a,
    {
        self.range(range).map(|(key, value)| (key.clone(), value.clone()))
    }

    #[inline]
    fn range_links<'a, R>(&'a self, range: R) -> impl DoubleEndedIterator<Item = V> + 'a
    where
        R: RangeBounds<K> + 'a,
    {
        self.range(range).map(|(_, value)| value.clone())
    }
}

/// Vanilla upstream IndexSet map, kept distinct from WorkTable's default
/// WorkTablesIndex alias so both implementations may coexist in one binary.
pub type UpstreamIndexMap<K, V, Node = Vec<VanillaPair<K, V>>> = VanillaIndexMap<K, V, Node>;
pub type UpstreamIndexPair<K, V> = VanillaPair<K, V>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{UniqueIndex, UpstreamIndexMap};
    use crate::{ArcticIndex, CongeeIndex, IndexMap};

    fn assert_unique_index_contract<I>()
    where
        I: UniqueIndex<u64, u64>,
    {
        let index = I::default();
        assert!(index.is_empty());
        assert_eq!(index.insert_value_checked(2, 20), Some(()));
        assert_eq!(index.insert_value_checked(1, 10), Some(()));
        assert_eq!(index.insert_value_checked(2, 99), None);
        assert_eq!(index.get_value(&2), Some(20));
        assert_eq!(index.insert_value(2, 22), Some(20));
        assert_eq!(index.iter_values().collect::<Vec<_>>(), vec![(1, 10), (2, 22)]);
        assert_eq!(index.iter_links().collect::<Vec<_>>(), vec![10, 22]);
        assert_eq!(index.range_values(2..=2).collect::<Vec<_>>(), vec![(2, 22)]);
        assert_eq!(index.range_links(2..=2).collect::<Vec<_>>(), vec![22]);
        assert_eq!(index.remove_value(&1), Some((1, 10)));
        assert_eq!(index.len(), 1);
    }

    fn assert_disjoint_concurrent_insert_then_remove<I>()
    where
        I: UniqueIndex<u64, u64> + Send + Sync + 'static,
    {
        let index = Arc::new(I::default());
        let mut threads = Vec::new();
        for worker in 0..8_u64 {
            let index = Arc::clone(&index);
            threads.push(std::thread::spawn(move || {
                for sequence in 0..1_000_u64 {
                    let key = worker * 1_000 + sequence;
                    assert_eq!(index.insert_value_checked(key, key + 1), Some(()));
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }

        assert_eq!(index.len(), 8_000);
        for key in 0..8_000_u64 {
            assert_eq!(index.get_value(&key), Some(key + 1));
        }

        let mut threads = Vec::new();
        for worker in 0..8_u64 {
            let index = Arc::clone(&index);
            threads.push(std::thread::spawn(move || {
                for sequence in 0..1_000_u64 {
                    let key = worker * 1_000 + sequence;
                    assert_eq!(index.remove_value(&key), Some((key, key + 1)));
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(index.is_empty());
    }

    fn assert_immediate_disjoint_crud<I>()
    where
        I: UniqueIndex<u64, u64> + Send + Sync + 'static,
    {
        let index = Arc::new(I::default());
        let mut threads = Vec::new();
        for worker in 0..8_u64 {
            let index = Arc::clone(&index);
            threads.push(std::thread::spawn(move || {
                for sequence in 0..1_000_u64 {
                    let key = worker * 1_000 + sequence;
                    assert_eq!(index.insert_value_checked(key, key + 1), Some(()));
                    assert_eq!(index.get_value(&key), Some(key + 1));
                    assert_eq!(index.remove_value(&key), Some((key, key + 1)));
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(index.is_empty());
    }

    #[test]
    fn worktables_index_implements_contract() {
        assert_unique_index_contract::<IndexMap<u64, u64>>();
    }

    #[test]
    fn upstream_indexset_implements_contract() {
        assert_unique_index_contract::<UpstreamIndexMap<u64, u64>>();
    }

    #[test]
    fn all_backends_preserve_disjoint_concurrent_mutations() {
        assert_disjoint_concurrent_insert_then_remove::<IndexMap<u64, u64>>();
        assert_disjoint_concurrent_insert_then_remove::<UpstreamIndexMap<u64, u64>>();
        assert_disjoint_concurrent_insert_then_remove::<CongeeIndex<u64, u64>>();
        assert_disjoint_concurrent_insert_then_remove::<ArcticIndex<u64, u64>>();
    }

    #[test]
    fn art_backends_make_disjoint_mutations_immediately_visible() {
        assert_immediate_disjoint_crud::<CongeeIndex<u64, u64>>();
        assert_immediate_disjoint_crud::<ArcticIndex<u64, u64>>();
    }
}
