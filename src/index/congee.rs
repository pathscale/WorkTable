//! Congee adapter for memory-only unique WorkTable indexes.

use std::fmt::{self, Debug};
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use congee::{Congee, DefaultAllocator};

use super::UniqueIndex;

/// Lossless conversion between a WorkTable key and Congee's machine-word key.
///
/// Congee 0.4 stores keys as one `usize`, so this backend intentionally accepts
/// only integer keys that fit without truncation. NanoID and composite keys
/// should use WorkTablesIndex, IndexSet, or Arctic.
pub trait CongeeKey: Copy + Debug + Ord + Send + Sync + 'static {
    fn into_congee(self) -> usize;
    fn from_congee(value: usize) -> Self;
}

macro_rules! impl_congee_key {
    ($($ty:ty),* $(,)?) => {
        $(
            impl CongeeKey for $ty {
                #[inline]
                fn into_congee(self) -> usize { self as usize }

                #[inline]
                fn from_congee(value: usize) -> Self { value as Self }
            }
        )*
    };
}

impl_congee_key!(u8, u16, u32, usize);

#[cfg(target_pointer_width = "64")]
impl_congee_key!(u64);

/// A Congee adaptive radix tree with WorkTable's unique-index contract.
///
/// Values are held through `Arc` pointers because Congee's payload is one
/// machine word while a WorkTable `Link` is wider. The adapter follows the
/// reclamation pattern used by Congee's own `CongeeArc` implementation.
pub struct CongeeIndex<K, V> {
    inner: Congee<usize, usize>,
    len: AtomicUsize,
    marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Debug for CongeeIndex<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CongeeIndex")
            .field("len", &self.len.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<K, V> Default for CongeeIndex<K, V>
where
    K: CongeeKey,
    V: Clone + Debug + Send + Sync + 'static,
{
    fn default() -> Self {
        let drainer = |_key: usize, pointer: usize| {
            // SAFETY: every payload inserted below originates from
            // `Arc::into_raw` with exactly one tree-owned strong reference.
            drop(unsafe { Arc::from_raw(pointer as *const V) });
        };
        Self {
            inner: Congee::new_with_drainer(DefaultAllocator {}, drainer),
            len: AtomicUsize::new(0),
            marker: std::marker::PhantomData,
        }
    }
}

impl<K, V> CongeeIndex<K, V>
where
    K: CongeeKey,
    V: Clone + Debug + Send + Sync + 'static,
{
    #[inline]
    fn clone_pointer(pointer: usize) -> Arc<V> {
        // SAFETY: the caller holds a Congee epoch guard, so the tree-owned
        // strong reference cannot be reclaimed while it is cloned.
        let owned = unsafe { Arc::from_raw(pointer as *const V) };
        let cloned = Arc::clone(&owned);
        let _ = Arc::into_raw(owned);
        cloned
    }

    #[inline]
    fn retire_old(pointer: usize, guard: &congee::epoch::Guard) -> Arc<V> {
        // SAFETY: a successful replacement/removal transfers the tree-owned
        // strong reference to this call.
        let owned = unsafe { Arc::from_raw(pointer as *const V) };
        let delayed = Arc::clone(&owned);
        guard.defer(move || drop(delayed));
        owned
    }

    #[cold]
    fn allocation_failure() -> ! {
        panic!("Congee failed to allocate an index node")
    }
}

impl<K, V> UniqueIndex<K, V> for CongeeIndex<K, V>
where
    K: CongeeKey,
    V: Clone + Debug + Send + Sync + 'static,
{
    #[inline]
    fn get_value(&self, key: &K) -> Option<V> {
        let guard = self.inner.pin();
        let pointer = self.inner.get(&key.into_congee(), &guard)?;
        Some(Self::clone_pointer(pointer).as_ref().clone())
    }

    #[inline]
    fn insert_value(&self, key: K, value: V) -> Option<V> {
        let guard = self.inner.pin();
        let pointer = Arc::into_raw(Arc::new(value)) as usize;
        match self.inner.insert(key.into_congee(), pointer, &guard) {
            Ok(Some(old)) => Some(Self::retire_old(old, &guard).as_ref().clone()),
            Ok(None) => {
                self.len.fetch_add(1, Ordering::Relaxed);
                None
            }
            Err(_) => {
                // SAFETY: insertion failed, so ownership never transferred.
                drop(unsafe { Arc::from_raw(pointer as *const V) });
                Self::allocation_failure()
            }
        }
    }

    #[inline]
    fn insert_value_checked(&self, key: K, value: V) -> Option<()> {
        let guard = self.inner.pin();
        let pointer = Arc::into_raw(Arc::new(value)) as usize;
        let result = self
            .inner
            .compute_or_insert(key.into_congee(), |old| old.unwrap_or(pointer), &guard);

        match result {
            Ok(Some(_)) => {
                // The closure returned the existing pointer, so the new value
                // was never installed.
                drop(unsafe { Arc::from_raw(pointer as *const V) });
                None
            }
            Ok(None) => {
                self.len.fetch_add(1, Ordering::Relaxed);
                Some(())
            }
            Err(_) => {
                // SAFETY: insertion failed, so ownership never transferred.
                drop(unsafe { Arc::from_raw(pointer as *const V) });
                Self::allocation_failure()
            }
        }
    }

    #[inline]
    fn remove_value(&self, key: &K) -> Option<(K, V)> {
        let guard = self.inner.pin();
        let pointer = self.inner.remove(&key.into_congee(), &guard)?;
        self.len.fetch_sub(1, Ordering::Relaxed);
        Some((*key, Self::retire_old(pointer, &guard).as_ref().clone()))
    }

    #[inline]
    fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    fn iter_values(&self) -> impl DoubleEndedIterator<Item = (K, V)> + '_ {
        let mut values = self
            .inner
            .keys()
            .into_iter()
            .filter_map(|key| {
                let key = K::from_congee(key);
                self.get_value(&key).map(|value| (key, value))
            })
            .collect::<Vec<_>>();
        values.sort_unstable_by_key(|entry| entry.0);
        values.into_iter()
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

    use super::{CongeeIndex, UniqueIndex};

    #[test]
    fn implements_unique_index_contract() {
        let index = CongeeIndex::<u64, u64>::default();
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
        let index = Arc::new(CongeeIndex::<u64, u64>::default());
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
