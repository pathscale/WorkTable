use std::intrinsics::transmute;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicUsize, Ordering};

use scc::ebr::Guard;

use crate::util::SizeMeasurable;
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
    K: Clone + Ord + Send + Sync + 'static + SizeMeasurable,
    V: Clone + Send + Sync + 'static + SizeMeasurable,
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

impl<K, V> TableIndex<K, V> for scc::TreeIndex<K, V>
where
    K: Clone + Ord + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn insert(&self, key: K, value: V) -> Result<(), (K, V)> {
        scc::TreeIndex::insert(self, key, value)
    }

    fn peek(&self, key: &K) -> Option<V> {
        let guard = Guard::new();
        scc::TreeIndex::peek(self, key, &guard).cloned()
    }

    fn remove(&self, key: &K) -> bool {
        scc::TreeIndex::remove(self, key)
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a,
    {
        let guard = Guard::new();
        let guard: &'a Guard = unsafe { transmute(&guard) };
        scc::TreeIndex::iter(self, guard)
    }

    fn range<'a, R: RangeBounds<K>>(&'a self, range: R) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a,
    {
        let guard = Guard::new();
        let guard: &'a Guard = unsafe { transmute(&guard) };
        scc::TreeIndex::range(self, range, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sized_tree_index() {
        let index: scc::TreeIndex<u32, String> = scc::TreeIndex::new();
        let measured_index =
            MeasuredTreeIndex::<scc::TreeIndex<u32, String>, u32, String>::new(index);

        assert_eq!(measured_index.estimate_size(), 0);

        measured_index.insert(1, "one".to_string()).unwrap();
        measured_index.insert(2, "two".to_string()).unwrap();
        measured_index.insert(3, "three".to_string()).unwrap();

        assert_eq!(measured_index.peek(&1), Some("one".to_string()));
        assert_eq!(measured_index.peek(&2), Some("two".to_string()));
        assert_eq!(measured_index.peek(&3), Some("three".to_string()));

        let size = measured_index.estimate_size();
        assert!(size > 0, "Size should be greater than 0");

        assert!(measured_index.remove(&2));
        assert_eq!(measured_index.peek(&2), None);

        let new_size = measured_index.estimate_size();
        assert!(new_size < size, "Size should decrease after removal");
    }
}
