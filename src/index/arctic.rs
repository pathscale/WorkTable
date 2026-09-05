//! Arctic adapter for memory-only unique WorkTable indexes.

use std::borrow::Borrow;
use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicUsize, Ordering};

use arctic::{ConcurrentMap, Key, Order};

use super::UniqueIndex;

type ArcticSmr = arctic::concurrent::smr::PsReclaim;

/// Lossless conversion between a WorkTable key and a native Arctic key.
///
/// Keeping this trait local lets generated primary-key newtypes delegate to
/// their underlying integer without implementing Arctic's low-level key API.
pub trait ArcticKey: Clone + Debug + Ord + Send + Sync + 'static {
    type Raw: Key + Clone + Debug + Ord + Send + Sync + 'static;

    fn to_arctic(&self) -> Self::Raw;
    fn from_arctic(value: Self::Raw) -> Self;
}

/// Arctic key used for arbitrary UTF-8 strings.
///
/// Arctic's variable-sized keys require a byte sequence with no zero byte so
/// the tree can append its own logical terminator. Valid UTF-8 never contains
/// `0xff`, therefore adding one to every encoded byte is a lossless,
/// order-preserving mapping into `1..=0xf8`. This includes Rust strings that
/// contain `\0`, without reserving a value or changing their ordering.
pub type ArcticStringKey = arctic::key::BoxedSlice<arctic::key::NonNull>;

fn encode_string(value: &str) -> ArcticStringKey {
    let encoded = value
        .as_bytes()
        .iter()
        .map(|byte| byte.checked_add(1).expect("UTF-8 bytes never reach 0xff"))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    ArcticStringKey::new(encoded).expect("the shifted UTF-8 encoding contains no zero byte")
}

fn decode_string(value: ArcticStringKey) -> String {
    let decoded = value
        .into_boxed_slice()
        .into_vec()
        .into_iter()
        .map(|byte| {
            byte.checked_sub(1)
                .expect("Arctic string encoding contains no zero byte")
        })
        .collect::<Vec<_>>();
    String::from_utf8(decoded).expect("Arctic string key was not encoded from UTF-8")
}

impl ArcticKey for String {
    type Raw = ArcticStringKey;

    #[inline]
    fn to_arctic(&self) -> Self::Raw {
        encode_string(self)
    }

    #[inline]
    fn from_arctic(value: Self::Raw) -> Self {
        decode_string(value)
    }
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

impl ArcticKey for u8 {
    type Raw = u16;

    #[inline]
    fn to_arctic(&self) -> Self::Raw {
        u16::from(*self)
    }

    #[inline]
    fn from_arctic(value: Self::Raw) -> Self {
        value as Self
    }
}

impl ArcticKey for usize {
    type Raw = u64;

    #[inline]
    fn to_arctic(&self) -> Self::Raw {
        *self as u64
    }

    #[inline]
    fn from_arctic(value: Self::Raw) -> Self {
        value as Self
    }
}

impl ArcticKey for i8 {
    type Raw = u16;

    #[inline]
    fn to_arctic(&self) -> Self::Raw {
        u16::from((*self as u8) ^ (1 << 7))
    }

    #[inline]
    fn from_arctic(value: Self::Raw) -> Self {
        ((value as u8) ^ (1 << 7)) as Self
    }
}

impl_arctic_key!(u16, u32, u64, u128);

/// Signed keys, mapped onto the unsigned key space by flipping the sign bit.
///
/// An ART orders keys by the bytes of the key, and two's complement puts that
/// ordering the wrong way round: a negative has its high bit set, so it sorts
/// *after* every positive. XOR with the sign bit corrects exactly that. It is a
/// bijection over the full width and strictly monotonic, which is the only
/// property the tree needs, so `i64::MIN` lands at `0`, `-1` at `0x7fff_..._ffff`,
/// `0` at `0x8000_..._0000` and `i64::MAX` at `u64::MAX`.
///
/// Being a bijection over the *whole* width also preserves adjacency, which
/// keeps excluded range bounds exact in the raw key space.
///
/// `i8` is widened losslessly into Arctic's narrowest raw key, `u16`.
macro_rules! impl_arctic_signed_key {
    ($($ty:ty => $raw:ty),* $(,)?) => {
        $(
            impl ArcticKey for $ty {
                type Raw = $raw;

                #[inline]
                fn to_arctic(&self) -> Self::Raw {
                    (*self as $raw) ^ ((1 as $raw) << (<$raw>::BITS - 1))
                }

                #[inline]
                fn from_arctic(value: Self::Raw) -> Self {
                    (value ^ ((1 as $raw) << (<$raw>::BITS - 1))) as Self
                }
            }
        )*
    };
}

impl_arctic_signed_key!(i16 => u16, i32 => u32, i64 => u64, i128 => u128);

/// Lossless codec for values held inline by Arctic.
#[doc(hidden)]
pub trait ArcticValue: Clone + Debug + Send + Sync + 'static {
    fn into_arctic(self) -> u64;
    fn from_arctic(value: u64) -> Self;
}

impl ArcticValue for u64 {
    #[inline]
    fn into_arctic(self) -> u64 {
        self
    }

    #[inline]
    fn from_arctic(value: u64) -> Self {
        value
    }
}

#[doc(hidden)]
pub fn validate_arctic_link(link: data_bucket::Link) -> eyre::Result<()> {
    if link.offset > u32::from(u16::MAX) || link.length > u32::from(u16::MAX) {
        eyre::bail!(
            "link cannot be represented by Arctic: page {:?}, offset {}, length {}",
            link.page_id,
            link.offset,
            link.length,
        );
    }
    Ok(())
}

fn pack_link(link: data_bucket::Link) -> u64 {
    validate_arctic_link(link).expect("WorkTable validated the link before publishing it to Arctic");
    (u64::from(u32::from(link.page_id)) << 32) | (u64::from(link.offset) << 16) | u64::from(link.length)
}

fn unpack_link(value: u64) -> data_bucket::Link {
    data_bucket::Link {
        page_id: ((value >> 32) as u32).into(),
        offset: ((value >> 16) as u32) & u32::from(u16::MAX),
        length: value as u32 & u32::from(u16::MAX),
    }
}

impl ArcticValue for data_bucket::Link {
    #[inline]
    fn into_arctic(self) -> u64 {
        pack_link(self)
    }

    #[inline]
    fn from_arctic(value: u64) -> Self {
        unpack_link(value)
    }
}

impl<const N: usize> ArcticValue for crate::util::OffsetEqLink<N> {
    #[inline]
    fn into_arctic(self) -> u64 {
        pack_link(self.0)
    }

    #[inline]
    fn from_arctic(value: u64) -> Self {
        Self(unpack_link(value))
    }
}

/// Arctic's lock-free adaptive radix tree with WorkTable's unique-index
/// contract.
///
/// WorkTable links are packed into Arctic's inline 64-bit value. Point
/// operations remain directly backed by Arctic; ordered scans are collected
/// into a stable snapshot to satisfy WorkTable's double-ended query interface.
pub struct ArcticIndex<K: ArcticKey, V: ArcticValue> {
    inner: ConcurrentMap<K::Raw, u64, ArcticSmr>,
    len: AtomicUsize,
    marker: PhantomData<fn() -> V>,
}

/// Owned compatibility view returned by [`ArcticIndex::get`].
///
/// WTI exposes a guarded entry with a `get()` accessor. Arctic values are
/// decoded inline, so this view owns the decoded pair while preserving that
/// small inspection API for backend-agnostic diagnostics and tests.
pub struct ArcticEntry<K, V> {
    pair: indexset::core::pair::Pair<K, V>,
}

impl<K, V> ArcticEntry<K, V> {
    pub fn get(&self) -> &indexset::core::pair::Pair<K, V> {
        &self.pair
    }
}

impl<K: ArcticKey, V: ArcticValue> Debug for ArcticIndex<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArcticIndex")
            .field("len", &self.len.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<K, V> Default for ArcticIndex<K, V>
where
    K: ArcticKey,
    V: ArcticValue,
{
    fn default() -> Self {
        Self {
            inner: ConcurrentMap::default(),
            len: AtomicUsize::new(0),
            marker: PhantomData,
        }
    }
}

impl<K, V> ArcticIndex<K, V>
where
    K: ArcticKey,
    V: ArcticValue,
{
    /// Every entry, ascending.
    ///
    /// An inherent alias for [`UniqueIndex::iter_values`], so this reads the
    /// same as the general-purpose backend does. Without it, code written
    /// against that backend's inherent `iter` does not compile against this
    /// one: a difference in spelling rather than in capability, which costs
    /// the reader a detour and quietly pushes tests towards the one backend
    /// whose name they already know. It caught out the vacuum invariant tests
    /// twice.
    ///
    /// Materialises, exactly as `iter_values` does: the underlying map hands
    /// out entries borrowed from a guard, so they are cloned rather than lent.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (K, V)> + '_ {
        <Self as UniqueIndex<K, V>>::iter_values(self)
    }

    /// WTI-compatible point inspection for code that examines generated
    /// index internals. Normal table reads use [`UniqueIndex::get_value`].
    pub fn get(&self, key: &K) -> Option<ArcticEntry<K, V>> {
        self.get_value(key).map(|value| ArcticEntry {
            pair: indexset::core::pair::Pair {
                key: key.clone(),
                value,
            },
        })
    }

    /// WTI-compatible ordered pair range.
    pub fn range<'a, R>(&'a self, range: R) -> impl DoubleEndedIterator<Item = (K, V)> + 'a
    where
        R: RangeBounds<K> + 'a,
    {
        self.range_values(range)
    }

    /// Heap bytes occupied by Arctic's reachable adaptive nodes.
    pub fn allocated_node_bytes(&self) -> usize {
        self.inner.allocated_node_bytes()
    }

    pub(crate) fn export_topology<T>(
        &mut self,
        mut encode: impl FnMut(&V) -> T,
    ) -> Result<arctic::topology::Topology<T>, arctic::topology::Error>
    where
        K::Raw: arctic::topology::Key,
    {
        self.inner.export_topology(|value| encode(&V::from_arctic(*value)))
    }

    pub(crate) fn from_topology<T>(
        topology: arctic::topology::Topology<T>,
        mut decode: impl FnMut(T) -> V,
    ) -> Result<Self, arctic::topology::Error>
    where
        K::Raw: arctic::topology::Key,
    {
        let inner = ConcurrentMap::from_topology(topology, |value| decode(value).into_arctic())?;
        let len = inner.all().entries(Order::Ascend).count();
        Ok(Self {
            inner,
            len: AtomicUsize::new(len),
            marker: PhantomData,
        })
    }
}

impl<K, V> UniqueIndex<K, V> for ArcticIndex<K, V>
where
    K: ArcticKey,
    V: ArcticValue,
{
    #[inline]
    fn get_value(&self, key: &K) -> Option<V> {
        self.with_value(key, Clone::clone)
    }

    #[inline]
    fn with_value<R>(&self, key: &K, read: impl FnOnce(&V) -> R) -> Option<R> {
        let key = key.to_arctic();
        self.inner.get(key.borrow()).map(|value| {
            let decoded = V::from_arctic(*value);
            read(&decoded)
        })
    }

    #[inline]
    fn contains_key(&self, key: &K) -> bool {
        let key = key.to_arctic();
        self.inner.get(key.borrow()).is_some()
    }

    #[inline]
    fn insert_value(&self, key: K, value: V) -> Option<V> {
        let key = key.to_arctic();
        let updated = self.inner.upsert(key.as_insert(), value.into_arctic());
        let old = updated.old().copied().map(V::from_arctic);
        if old.is_none() {
            self.len.fetch_add(1, Ordering::Relaxed);
        }
        old
    }

    #[inline]
    fn insert_value_checked(&self, key: K, value: V) -> Option<()> {
        let key = key.to_arctic();
        match self.inner.insert(key.as_insert(), value.into_arctic()) {
            Ok(_) => {
                self.len.fetch_add(1, Ordering::Relaxed);
                Some(())
            }
            Err((_old, new)) => {
                let _ = new;
                None
            }
        }
    }

    #[inline]
    fn remove_value(&self, key: &K) -> Option<(K, V)> {
        let raw_key = key.to_arctic();
        let old = self.inner.remove(raw_key.borrow())?;
        self.len.fetch_sub(1, Ordering::Relaxed);
        Some((key.clone(), V::from_arctic(*old)))
    }

    #[inline]
    fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    fn iter_values(&self) -> impl DoubleEndedIterator<Item = (K, V)> + '_ {
        let shard = self.inner.all();
        shard
            .entries(Order::Ascend)
            .map(|(key, value)| (K::from_arctic(key), V::from_arctic(value)))
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
        let lower = match range.start_bound() {
            Bound::Included(key) | Bound::Excluded(key) => Some(key.to_arctic()),
            Bound::Unbounded => None,
        };
        let upper = match range.end_bound() {
            Bound::Included(key) | Bound::Excluded(key) => Some(key.to_arctic()),
            Bound::Unbounded => None,
        };

        macro_rules! collect_range {
            ($native_range:expr) => {{
                self.inner
                    .range($native_range)
                    .entries(Order::Ascend)
                    .map(|(key, value)| (K::from_arctic(key), V::from_arctic(value)))
                    .collect::<Vec<_>>()
            }};
        }

        let values = match (lower, upper) {
            (Some(lower), Some(upper)) if lower <= upper => {
                collect_range!(lower.borrow()..=upper.borrow())
            }
            (Some(_), Some(_)) => Vec::new(),
            (Some(lower), None) => collect_range!(lower.borrow()..),
            (None, Some(upper)) => collect_range!(..=upper.borrow()),
            (None, None) => self
                .inner
                .all()
                .entries(Order::Ascend)
                .map(|(key, value)| (K::from_arctic(key), V::from_arctic(value)))
                .collect(),
        };
        values.into_iter().filter(move |(key, _)| range.contains(key))
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

    use super::{ArcticIndex, UniqueIndex};

    /// Two's complement puts negatives after positives when ordered by their
    /// bytes, which is how an ART orders. If the sign-bit flip were missing or
    /// applied on one side only, this is what would break, and it would break
    /// silently: point lookups would still work.
    #[test]
    fn signed_keys_iterate_in_signed_order() {
        let index = ArcticIndex::<i64, u64>::default();
        let keys = [i64::MIN, -1_000, -1, 0, 1, 1_000, i64::MAX];
        // Inserted out of order, so passing cannot come from insertion order.
        for key in [0, i64::MAX, -1, 1_000, i64::MIN, 1, -1_000] {
            index.insert_value(key, key as u64);
        }

        let seen: Vec<i64> = index.iter_values().map(|(key, _)| key).collect();
        assert_eq!(
            seen, keys,
            "an ART orders by key bytes, so signed order has to be built in"
        );
    }

    /// The case a half-applied transform passes: a range that stays on one side
    /// of zero is fine even when the mapping is wrong, so the range has to
    /// cross the sign boundary to be evidence.
    #[test]
    fn signed_ranges_cross_the_sign_boundary() {
        let index = ArcticIndex::<i32, u64>::default();
        for key in -5..=5 {
            index.insert_value(key, key as u64);
        }

        let spanning: Vec<i32> = index.range_values(-2..=2).map(|(key, _)| key).collect();
        assert_eq!(spanning, vec![-2, -1, 0, 1, 2]);

        let below: Vec<i32> = index.range_values(..0).map(|(key, _)| key).collect();
        assert_eq!(
            below,
            vec![-5, -4, -3, -2, -1],
            "zero must not sort below the negatives"
        );

        // Exclusive bounds are resolved by `next`/`previous` in the raw space.
        // They are only correct because the mapping is onto the whole width.
        let exclusive: Vec<i32> = index
            .range_values((Bound::Excluded(-1), Bound::Excluded(1)))
            .map(|(key, _)| key)
            .collect();
        assert_eq!(exclusive, vec![0], "stepping across the sign boundary must land on 0");
    }

    /// The extremes are where an off-by-one in the flip shows up, and where a
    /// `next`/`previous` that ran outside the mapping's image would.
    #[test]
    fn signed_keys_round_trip_at_the_extremes() {
        let index = ArcticIndex::<i64, u64>::default();
        for key in [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX] {
            index.insert_value(key, 7);
            assert_eq!(index.get_value(&key), Some(7), "{key} did not survive the mapping");
        }

        let from_min: Vec<i64> = index
            .range_values(i64::MIN..=i64::MIN + 1)
            .map(|(key, _)| key)
            .collect();
        assert_eq!(from_min, vec![i64::MIN, i64::MIN + 1]);

        let to_max: Vec<i64> = index.range_values(i64::MAX - 1..).map(|(key, _)| key).collect();
        assert_eq!(to_max, vec![i64::MAX - 1, i64::MAX]);
    }

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
    fn native_ranges_preserve_rust_bounds() {
        let index = ArcticIndex::<u64, u64>::default();
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
    fn arbitrary_strings_round_trip_and_scan_in_utf8_order() {
        let index = ArcticIndex::<String, u64>::default();
        let keys = ["", "\0", "a", "a\0", "aa", "é", "🦀"];
        for (value, key) in keys.iter().enumerate().rev() {
            assert_eq!(index.insert_value_checked((*key).to_owned(), value as u64), Some(()));
        }

        for (value, key) in keys.iter().enumerate() {
            assert_eq!(index.get_value(&(*key).to_owned()), Some(value as u64));
        }
        assert_eq!(
            index.iter_values().map(|(key, _)| key).collect::<Vec<_>>(),
            keys.map(str::to_owned)
        );
        assert_eq!(
            index
                .range_values((Bound::Excluded("a".to_owned()), Bound::Included("é".to_owned())))
                .map(|(key, _)| key)
                .collect::<Vec<_>>(),
            vec!["a\0".to_owned(), "aa".to_owned(), "é".to_owned()]
        );
    }

    #[test]
    fn excluded_bounds_without_neighbours_yield_empty_ranges() {
        let index = ArcticIndex::<u64, u64>::default();
        for key in 0..10 {
            assert_eq!(index.insert_value_checked(key, key * 10), Some(()));
        }

        // `..0` is `Bound::Excluded(0)` above: `previous()` has no value, and
        // the range must be empty, not unbounded (the whole table).
        assert_eq!(index.range_values(..0).collect::<Vec<_>>(), Vec::new());
        // `Excluded(u64::MAX)..` has no successor for the lower bound.
        assert_eq!(
            index
                .range_values((Bound::Excluded(u64::MAX), Bound::Unbounded))
                .collect::<Vec<_>>(),
            Vec::new()
        );
        // Both bounds degenerate at once.
        assert_eq!(
            index
                .range_values((Bound::Excluded(u64::MAX), Bound::Excluded(0)))
                .collect::<Vec<_>>(),
            Vec::new()
        );
        // Normal exclusive bounds keep working.
        assert_eq!(index.range_values(..1).collect::<Vec<_>>(), vec![(0, 0)]);
        assert_eq!(
            index
                .range_values((Bound::Excluded(8), Bound::Unbounded))
                .collect::<Vec<_>>(),
            vec![(9, 90)]
        );
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
