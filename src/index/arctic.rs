//! Arctic adapter for memory-only unique WorkTable indexes.

use std::borrow::Borrow;
use std::fmt::{self, Debug};
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicUsize, Ordering};

use arctic::{ConcurrentMap, Key, Order};

use super::UniqueIndex;

/// Lossless conversion between a WorkTable key and a native Arctic key.
///
/// Keeping this trait local lets generated primary-key newtypes delegate to
/// their underlying integer without implementing Arctic's low-level key API.
pub trait ArcticKey: Clone + Debug + Ord + Send + Sync + 'static {
    type Raw: Key + Clone + Debug + Ord + Send + Sync + 'static;

    fn to_arctic(&self) -> Self::Raw;
    fn from_arctic(value: Self::Raw) -> Self;
}

macro_rules! impl_arctic_key {
    ($($ty:ty),* $(,)?) => {
        $(
            impl ArcticKey for $ty {
                type Raw = Self;

                #[inline]
                fn to_arctic(&self) -> Self::Raw { *self }

                #[inline]
                fn from_arctic(value: Self::Raw) -> Self { value }
            }
        )*
    };
}

impl_arctic_key!(u16, u32, u64, u128);

/// Arctic's lock-free adaptive radix tree with WorkTable's unique-index
/// contract.
///
/// WorkTable links are stored as boxed values because Arctic's inline value
/// representation is limited to 64 bits. Point operations remain directly
/// backed by Arctic; ordered scans are collected into a stable snapshot to
/// satisfy WorkTable's double-ended query interface.
pub struct ArcticIndex<K: ArcticKey, V> {
    inner: ConcurrentMap<K::Raw, Box<V>>,
    len: AtomicUsize,
}

impl<K: ArcticKey, V> Debug for ArcticIndex<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArcticIndex")
            .field("len", &self.len.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<K, V> Default for ArcticIndex<K, V>
where
    K: ArcticKey,
{
    fn default() -> Self {
        Self {
            inner: ConcurrentMap::default(),
            len: AtomicUsize::new(0),
        }
    }
}

impl<K, V> UniqueIndex<K, V> for ArcticIndex<K, V>
where
    K: ArcticKey,
    V: Clone + Debug + Send + Sync + 'static,
{
    #[inline]
    fn get_value(&self, key: &K) -> Option<V> {
        let key = key.to_arctic();
        self.inner.get(key.borrow()).map(|value| (*value).clone())
    }

    #[inline]
    fn insert_value(&self, key: K, value: V) -> Option<V> {
        let key = key.to_arctic();
        let updated = self.inner.upsert(key.as_insert(), Box::new(value));
        let old = updated.old().cloned();
        if old.is_none() {
            self.len.fetch_add(1, Ordering::Relaxed);
        }
        old
    }

    #[inline]
    fn insert_value_checked(&self, key: K, value: V) -> Option<()> {
        let key = key.to_arctic();
        match self.inner.insert(key.as_insert(), Box::new(value)) {
            Ok(_) => {
                self.len.fetch_add(1, Ordering::Relaxed);
                Some(())
            }
            Err((_old, new)) => {
                drop(new);
                None
            }
        }
    }

    #[inline]
    fn remove_value(&self, key: &K) -> Option<(K, V)> {
        let raw_key = key.to_arctic();
        let old = self.inner.remove(raw_key.borrow())?;
        self.len.fetch_sub(1, Ordering::Relaxed);
        Some((key.clone(), (*old).clone()))
    }

    #[inline]
    fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    fn iter_values(&self) -> impl DoubleEndedIterator<Item = (K, V)> + '_ {
        let shard = self.inner.all();
        shard
            .entries(Order::Ascend)
            .map(|(key, value)| (K::from_arctic(key), value.clone()))
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn iter_links(&self) -> impl DoubleEndedIterator<Item = V> + '_ {
        self.iter_values().map(|(_, value)| value)
    }

    fn range_values<'a, R>(&'a self, range: R) -> impl DoubleEndedIterator<Item = (K, V)> + 'a
    where
        R: RangeBounds<K> + 'a,
    {
        self.iter_values().filter(move |(key, _)| range.contains(key))
    }

    fn range_links<'a, R>(&'a self, range: R) -> impl DoubleEndedIterator<Item = V> + 'a
    where
        R: RangeBounds<K> + 'a,
    {
        self.range_values(range).map(|(_, value)| value)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::{ArcticIndex, UniqueIndex};

    #[test]
    fn implements_unique_index_contract() {
        let index = ArcticIndex::<u64, u64>::default();
        assert_eq!(index.insert_value_checked(1, 10), Some(()));
        assert_eq!(index.insert_value_checked(1, 11), None);
        assert_eq!(index.get_value(&1), Some(10));
        assert_eq!(index.insert_value(1, 12), Some(10));
        assert_eq!(index.range_values(1..=1).collect::<Vec<_>>(), vec![(1, 12)]);
        assert_eq!(index.remove_value(&1), Some((1, 12)));
        assert!(index.is_empty());
    }

    #[test]
    fn checked_insert_has_one_winner_under_contention() {
        let index = Arc::new(ArcticIndex::<u64, u64>::default());
        let barrier = Arc::new(Barrier::new(9));
        let mut threads = Vec::new();

        for value in 0..8 {
            let index = Arc::clone(&index);
            let barrier = Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                index.insert_value_checked(7, value).is_some()
            }));
        }

        barrier.wait();
        let winners = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .filter(|won| *won)
            .count();
        assert_eq!(winners, 1);
        assert_eq!(index.len(), 1);
    }
}
