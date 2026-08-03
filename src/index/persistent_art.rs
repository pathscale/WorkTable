//! Persistence-only synchronization and logical change capture for ART indexes.
//!
//! Memory-only Congee and Arctic indexes continue to use their native hot
//! paths. Persisted ART indexes use this wrapper so same-key mutations are
//! sequenced in the same order as their durable logical events. Mutations on
//! different stripes remain concurrent because their Set/Remove records
//! commute during recovery.

use std::array;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, Ordering};

use data_bucket::Link;
use indexset::cdc::change::{ChangeEvent, Id};
use indexset::core::pair::Pair;
use parking_lot::Mutex;

use crate::index::{ArcticIndex, CongeeIndex, UniqueIndex};
use crate::util::OffsetEqLink;
use crate::{ArcticKey, CongeeKey, TableIndexCdc};

const MUTATION_STRIPES: usize = 64;

/// Adds persistence sequencing to a native ART without changing its point-read
/// path or the layout of the underlying ART.
pub struct PersistentArtIndex<I> {
    inner: I,
    next_event_id: AtomicU64,
    mutation_stripes: [Mutex<()>; MUTATION_STRIPES],
}

/// Persisted Arctic index selected by the generated DSL.
pub type PersistentArcticIndex<K, V> = PersistentArtIndex<ArcticIndex<K, V>>;

/// Persisted Congee index selected by the generated DSL.
pub type PersistentCongeeIndex<K, V> = PersistentArtIndex<CongeeIndex<K, V>>;

impl<I: Debug> Debug for PersistentArtIndex<I> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PersistentArtIndex")
            .field("inner", &self.inner)
            .field("next_event_id", &self.next_event_id.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<I: Default> Default for PersistentArtIndex<I> {
    fn default() -> Self {
        Self::from_inner(I::default())
    }
}

impl<I> PersistentArtIndex<I> {
    /// Wraps a reconstructed ART. Event ids restart at zero because startup
    /// recovery checkpoints and clears the preceding WAL before accepting new
    /// operations.
    pub fn from_inner(inner: I) -> Self {
        Self {
            inner,
            next_event_id: AtomicU64::new(0),
            mutation_stripes: array::from_fn(|_| Mutex::new(())),
        }
    }

    /// Returns the native ART, primarily for quiescent checkpoint encoding.
    pub fn into_inner(self) -> I {
        self.inner
    }

    /// Exclusively borrows the native ART.
    pub fn inner_mut(&mut self) -> &mut I {
        &mut self.inner
    }

    /// Borrows the native ART.
    pub fn inner(&self) -> &I {
        &self.inner
    }

    fn mutation_stripe<K: Hash>(&self, key: &K) -> &Mutex<()> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        &self.mutation_stripes[hasher.finish() as usize % MUTATION_STRIPES]
    }

    fn next_event_id(&self) -> Id {
        self.next_event_id.fetch_add(1, Ordering::AcqRel).into()
    }
}

impl<K, V, I> UniqueIndex<K, V> for PersistentArtIndex<I>
where
    K: Clone + Ord + Send + 'static,
    V: Clone + Send + 'static,
    I: UniqueIndex<K, V>,
{
    #[inline]
    fn get_value(&self, key: &K) -> Option<V> {
        self.inner.get_value(key)
    }

    #[inline]
    fn with_value<R>(&self, key: &K, read: impl FnOnce(&V) -> R) -> Option<R> {
        self.inner.with_value(key, read)
    }

    #[inline]
    fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    #[inline]
    fn insert_value(&self, key: K, value: V) -> Option<V> {
        self.inner.insert_value(key, value)
    }

    #[inline]
    fn insert_value_checked(&self, key: K, value: V) -> Option<()> {
        self.inner.insert_value_checked(key, value)
    }

    #[inline]
    fn remove_value(&self, key: &K) -> Option<(K, V)> {
        self.inner.remove_value(key)
    }

    #[inline]
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn iter_values(&self) -> impl DoubleEndedIterator<Item = (K, V)> + '_ {
        self.inner.iter_values()
    }

    fn iter_links(&self) -> impl DoubleEndedIterator<Item = V> + '_ {
        self.inner.iter_links()
    }

    fn range_values<'a, R>(&'a self, range: R) -> impl DoubleEndedIterator<Item = (K, V)> + 'a
    where
        R: RangeBounds<K> + 'a,
    {
        self.inner.range_values(range)
    }

    fn range_links<'a, R>(&'a self, range: R) -> impl DoubleEndedIterator<Item = V> + 'a
    where
        R: RangeBounds<K> + 'a,
    {
        self.inner.range_links(range)
    }
}

macro_rules! impl_persisted_art_cdc {
    ($index:ident, $key_bound:path) => {
        impl<T, const N: usize> TableIndexCdc<T> for PersistentArtIndex<$index<T, OffsetEqLink<N>>>
        where
            T: $key_bound + Eq + Hash,
        {
            fn insert_cdc(&self, value: T, link: Link) -> (Option<Link>, Vec<ChangeEvent<Pair<T, Link>>>) {
                let _sequence_guard = self.mutation_stripe(&value).lock();
                let old = self
                    .inner
                    .insert_value(value.clone(), OffsetEqLink(link))
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
                let _sequence_guard = self.mutation_stripe(&value).lock();
                self.inner
                    .insert_value_checked(value.clone(), OffsetEqLink(link))?;
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
                let _sequence_guard = self.mutation_stripe(&value).lock();
                let Some((key, old)) = self.inner.remove_value(&value) else {
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
    };
}

impl_persisted_art_cdc!(ArcticIndex, ArcticKey);
impl_persisted_art_cdc!(CongeeIndex, CongeeKey);

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::*;

    #[test]
    fn failed_checked_insert_does_not_consume_an_event_id() {
        let index = PersistentArcticIndex::<u64, OffsetEqLink<4096>>::default();
        let link = Link::default();
        assert!(index.insert_checked_cdc(7, link).is_some());
        assert!(index.insert_checked_cdc(7, link).is_none());
        assert_eq!(index.remove_cdc(7, link).1[0].id(), 1.into());
    }

    #[test]
    fn same_key_events_follow_mutation_order() {
        let index = Arc::new(PersistentArcticIndex::<u64, OffsetEqLink<4096>>::default());
        let barrier = Arc::new(Barrier::new(9));
        let mut threads = Vec::new();
        for offset in 0..8 {
            let index = Arc::clone(&index);
            let barrier = Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                index
                    .insert_cdc(
                        11,
                        Link {
                            offset,
                            ..Link::default()
                        },
                    )
                    .1[0]
                    .clone()
            }));
        }
        barrier.wait();
        let mut events = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        events.sort_by_key(ChangeEvent::id);
        let last_link = match events.last().unwrap() {
            ChangeEvent::InsertAt { value, .. } => value.value,
            _ => unreachable!(),
        };
        assert_eq!(index.get_value(&11).unwrap().0, last_link);
    }
}
