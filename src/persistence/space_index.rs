use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicUsize, Ordering};

use data_bucket::{SizeMeasurable, PAGE_SIZE};

use crate::TableIndex;

/// A wrapper around TreeIndex that provides size measurement capabilities
pub struct MeasuredTreeIndex<I, K, V>
where
    I: TableIndex<K, V>,
{
    inner: I,
    total_size: AtomicUsize,
    _phantom: PhantomData<(K, V)>,
}

impl<I, K, V> MeasuredTreeIndex<I, K, V>
where
    I: TableIndex<K, V>,
    K: Clone + Ord + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            total_size: AtomicUsize::new(0),
            _phantom: PhantomData,
        }
    }

    pub fn estimate_size(&self) -> usize {
        self.total_size.load(Ordering::Relaxed)
    }

    pub fn pages_count(&self) -> usize {
        let size = self.total_size.load(Ordering::Relaxed);
        if size == 0 {
            0
        } else {
            (size + PAGE_SIZE - 1) / PAGE_SIZE
        }
    }
}

impl<I, K, V> TableIndex<K, V> for MeasuredTreeIndex<I, K, V>
where
    I: TableIndex<K, V>,
    K: Clone + Ord + Send + Sync + SizeMeasurable + 'static,
    V: Clone + Send + Sync + SizeMeasurable + 'static ,
{
    fn insert(&self, key: K, value: V) -> Result<(), (K, V)> {
        let key_size = key.approx_size();
        let value_size = value.approx_size();

        match self.inner.insert(key, value) {
            Ok(()) => {
                self.total_size
                    .fetch_add(key_size + value_size, Ordering::Relaxed);
                Ok(())
            }
            Err((k, v)) => Err((k, v)),
        }
    }

    fn peek(&self, key: &K) -> Option<V> {
        self.inner.peek(key)
    }

    fn remove(&self, key: &K) -> bool {
        if let Some(value) = self.peek(key) {
            if self.inner.remove(key) {
                let size_reduction = key.approx_size() + value.approx_size();
                self.total_size.fetch_sub(size_reduction, Ordering::Relaxed);
                return true;
            }
        }
        false
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
