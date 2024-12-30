use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicUsize, Ordering};

use data_bucket::{Link, SizeMeasurable, PAGE_SIZE};

use crate::TableIndex;

/// A wrapper around TreeIndex that provides size measurement capabilities
#[derive(Default, Debug)]
pub struct SpaceTreeIndex<I, K, V> {
    inner: I,
    _phantom: PhantomData<(K, V)>,
}

impl<I, K, V> SpaceTreeIndex<I, K, V>
where
    K: Default + SizeMeasurable,
{
    pub fn record_size() -> usize {
        (K::default(), Link::default()).aligned_size()
    }

    pub fn node_size<const PAGE_SIZE: usize>() -> usize {
        PAGE_SIZE / Self::record_size()
    }
}

impl<I, K, V> SpaceTreeIndex<I, K, V>
where
    I: TableIndex<K, V>,
    K: Default + Clone + Ord + Send + Sync + SizeMeasurable + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
        }
    }
}

impl<I, K, V> TableIndex<K, V> for SpaceTreeIndex<I, K, V>
where
    I: TableIndex<K, V>,
    K: Clone + Ord + Send + Sync + SizeMeasurable + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn insert(&self, key: K, value: V) -> Result<(), (K, V)> {
        self.inner.insert(key, value)
    }

    fn peek(&self, key: &K) -> Option<V> {
        self.inner.peek(key)
    }

    fn remove(&self, key: &K) -> bool {
        self.inner.remove(key)
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a,
    {
        self.inner.iter()
    }

    fn range<'a, R: RangeBounds<K>>(&'a self, range: R) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a,
    {
        self.inner.range(range)
    }
}
