//! Congee adapter for memory-only unique WorkTable indexes.

use std::fmt::{self, Debug};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use congee::{CongeeRaw, DefaultAllocator};
use parking_lot::Mutex;

use super::UniqueIndex;

const INITIAL_RANGE_CAPACITY: usize = 64;

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
    inner: CongeeRaw<usize, usize>,
    // congee-wt 0.4.1 can lose disjoint insert/remove mutations when their
    // structural updates overlap. Keep point reads native and concurrent, but
    // serialize mutations until the backend offers the required visibility.
    mutation: Mutex<()>,
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
            drop(unsafe { Self::arc_from_pointer(pointer) });
        };
        Self {
            inner: CongeeRaw::new_with_drainer(DefaultAllocator {}, drainer),
            mutation: Mutex::new(()),
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
    unsafe fn arc_from_pointer(pointer: usize) -> Arc<V> {
        // SAFETY: callers guarantee that `pointer` was produced by
        // `Arc::into_raw(...).expose_provenance()` for the same `V` and still
        // owns one strong reference.
        unsafe { Arc::from_raw(std::ptr::with_exposed_provenance(pointer)) }
    }

    #[inline]
    fn retire_old(pointer: usize, guard: &congee::epoch::Guard) -> Arc<V> {
        // SAFETY: a successful replacement/removal transfers the tree-owned
        // strong reference to this call.
        let owned = unsafe { Self::arc_from_pointer(pointer) };
        let delayed = Arc::clone(&owned);
        guard.defer(move || drop(delayed));
        owned
    }

    #[cold]
    fn allocation_failure() -> ! {
        panic!("Congee failed to allocate an index node")
    }

    fn collect_native_range<R>(&self, range: &R) -> Vec<(K, V)>
    where
        R: RangeBounds<K>,
    {
        let start = match range.start_bound() {
            Bound::Included(key) => key.into_congee(),
            Bound::Excluded(key) => match key.into_congee().checked_add(1) {
                Some(start) => start,
                None => return Vec::new(),
            },
            Bound::Unbounded => 0,
        };
        let (end, include_max) = match range.end_bound() {
            Bound::Included(key) => match key.into_congee().checked_add(1) {
                Some(end) => (end, false),
                None => (usize::MAX, true),
            },
            Bound::Excluded(key) => (key.into_congee(), false),
            Bound::Unbounded => (usize::MAX, true),
        };

        let guard = self.inner.pin();
        let mut raw_values = if start < end {
            // Most generated ranges are narrow. Starting from the full index
            // length makes a point range allocate in proportion to the whole
            // table before Congee examines its bounds.
            let mut capacity = INITIAL_RANGE_CAPACITY;
            loop {
                let mut values = vec![(0, 0); capacity];
                let scanned = self.inner.range(&start, &end, &mut values, &guard);
                if scanned < capacity || capacity > usize::MAX / 2 {
                    values.truncate(scanned);
                    break values;
                }
                capacity *= 2;
            }
        } else {
            Vec::new()
        };

        if include_max && let Some(pointer) = self.inner.get(&usize::MAX, &guard) {
            raw_values.push((usize::MAX, pointer));
        }

        raw_values
            .into_iter()
            .map(|(key, pointer)| {
                // SAFETY: the pinned epoch keeps every returned tree-owned
                // pointer alive until its value has been cloned.
                let value = unsafe { &*std::ptr::with_exposed_provenance::<V>(pointer) };
                (K::from_congee(key), value.clone())
            })
            .collect()
    }

    pub(crate) fn export_topology<T>(
        &mut self,
        mut encode: impl FnMut(&V) -> T,
    ) -> Result<congee::topology::Topology<T>, congee::topology::Error> {
        self.inner.export_topology(|pointer| {
            // SAFETY: every raw payload is a live tree-owned `Arc<V>` pointer,
            // and the exclusive borrow prevents removal while it is cloned.
            unsafe { encode(&*std::ptr::with_exposed_provenance::<V>(pointer)) }
        })
    }

    pub(crate) fn from_topology<T>(
        topology: congee::topology::Topology<T>,
        mut decode: impl FnMut(T) -> V,
    ) -> Result<Self, congee::topology::Error> {
        let drainer = |_key: usize, pointer: usize| {
            // SAFETY: decoded payloads below transfer exactly one `Arc` strong
            // reference to the reconstructed tree.
            drop(unsafe { Self::arc_from_pointer(pointer) });
        };
        let inner = CongeeRaw::from_topology_with_drainer(
            topology,
            DefaultAllocator {},
            |value| Arc::into_raw(Arc::new(decode(value))).expose_provenance(),
            drainer,
        )?;
        let len = inner.keys().len();
        Ok(Self {
            inner,
            mutation: Mutex::new(()),
            len: AtomicUsize::new(len),
            marker: std::marker::PhantomData,
        })
    }
}

impl<K, V> UniqueIndex<K, V> for CongeeIndex<K, V>
where
    K: CongeeKey,
    V: Clone + Debug + Send + Sync + 'static,
{
    #[inline]
    fn get_value(&self, key: &K) -> Option<V> {
        self.with_value(key, Clone::clone)
    }

    #[inline]
    fn with_value<R>(&self, key: &K, read: impl FnOnce(&V) -> R) -> Option<R> {
        let guard = self.inner.pin();
        let pointer = self.inner.get(&key.into_congee(), &guard)?;
        // SAFETY: the epoch guard keeps the tree-owned `Arc<V>` alive for the
        // duration of `read`, and the pointer originated from `Arc::into_raw`.
        let value = unsafe { &*std::ptr::with_exposed_provenance::<V>(pointer) };
        Some(read(value))
    }

    #[inline]
    fn contains_key(&self, key: &K) -> bool {
        let guard = self.inner.pin();
        self.inner.get(&key.into_congee(), &guard).is_some()
    }

    #[inline]
    fn insert_value(&self, key: K, value: V) -> Option<V> {
        let _mutation = self.mutation.lock();
        let guard = self.inner.pin();
        let pointer = Arc::into_raw(Arc::new(value)).expose_provenance();
        match self.inner.insert(key.into_congee(), pointer, &guard) {
            Ok(Some(old)) => Some(Self::retire_old(old, &guard).as_ref().clone()),
            Ok(None) => {
                self.len.fetch_add(1, Ordering::Relaxed);
                None
            }
            Err(_) => {
                // SAFETY: insertion failed, so ownership never transferred.
                drop(unsafe { Self::arc_from_pointer(pointer) });
                Self::allocation_failure()
            }
        }
    }

    #[inline]
    fn insert_value_checked(&self, key: K, value: V) -> Option<()> {
        let _mutation = self.mutation.lock();
        let guard = self.inner.pin();
        let pointer = Arc::into_raw(Arc::new(value)).expose_provenance();
        let result = self
            .inner
            .compute_or_insert(key.into_congee(), |old| old.unwrap_or(pointer), &guard);

        match result {
            Ok(Some(_)) => {
                // The closure returned the existing pointer, so the new value
                // was never installed.
                drop(unsafe { Self::arc_from_pointer(pointer) });
                None
            }
            Ok(None) => {
                self.len.fetch_add(1, Ordering::Relaxed);
                Some(())
            }
            Err(_) => {
                // SAFETY: insertion failed, so ownership never transferred.
                drop(unsafe { Self::arc_from_pointer(pointer) });
                Self::allocation_failure()
            }
        }
    }

    #[inline]
    fn remove_value(&self, key: &K) -> Option<(K, V)> {
        let _mutation = self.mutation.lock();
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
        self.collect_native_range(&(..)).into_iter()
    }

    fn iter_links(&self) -> impl DoubleEndedIterator<Item = V> + '_ {
        self.iter_values().map(|(_, value)| value)
    }

    fn range_values<'a, R>(&'a self, range: R) -> impl DoubleEndedIterator<Item = (K, V)> + 'a
    where
        R: RangeBounds<K> + 'a,
    {
        self.collect_native_range(&range).into_iter()
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
    use std::ops::Bound;
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
    fn native_ranges_preserve_rust_bounds() {
        let index = CongeeIndex::<u64, u64>::default();
        for key in 0..10 {
            assert_eq!(index.insert_value_checked(key, key * 10), Some(()));
        }

        assert_eq!(
            index.range_values(3..7).collect::<Vec<_>>(),
            vec![(3, 30), (4, 40), (5, 50), (6, 60)]
        );
        assert_eq!(
            index
                .range_values((Bound::Excluded(3), Bound::Included(5)))
                .collect::<Vec<_>>(),
            vec![(4, 40), (5, 50)]
        );
        assert_eq!(index.range_values(10..).collect::<Vec<_>>(), Vec::new());
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
