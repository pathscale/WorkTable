//! Contiguous rows for consumers that build once and search a frozen slice.
//!
//! [`LinearTable`] is deliberately smaller than a generated `vec: true`
//! table. It preserves insertion order and duplicate keys, and [`rows`](LinearTable::rows)
//! exposes the exact contiguous `[(K, V)]` storage. A caller can sort while
//! building, freeze the table behind an immutable reference, and run an
//! allocation-free binary search over that slice.
//!
//! The type is the retired `worktable-vec::LinearTable` moved into WorkTable.
//! Keeping it here lets crash handlers and other `no_std + alloc` consumers
//! use the stable row shape without taking a second table crate or routing a
//! lookup through WorkTable's paged indexes.

use alloc::vec::Vec;

use crate::vec_hydrate::{Codec, LoadError, RowTooLarge, from_pages, to_pages};

/// Why a uniqueness-checking insert was refused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InsertError<K> {
    /// The key already belongs to a row.
    DuplicateKey(K),
    /// Reserved for fixed-capacity table implementations that cannot grow.
    ///
    /// [`LinearTable`] uses `Vec` growth and does not emit this variant. It is
    /// retained for source compatibility with the retired standalone type.
    OutOfMemory(K),
}

/// Ordered, contiguous rows with a linear point lookup.
///
/// [`push`](Self::push) preserves duplicates. [`insert`](Self::insert) is the
/// explicit uniqueness-checking alternative. The type has the same layout as
/// its row vector, so exposing the immutable slice adds no index, lock, or
/// allocation to a read path.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[repr(transparent)]
pub struct LinearTable<K, V> {
    rows: Vec<(K, V)>,
}

/// A name for [`LinearTable`] that emphasizes its exact Vec-backed shape.
pub type VecTable<K, V> = LinearTable<K, V>;

impl<K, V> LinearTable<K, V> {
    /// An empty table.
    #[must_use]
    pub const fn new() -> Self {
        Self { rows: Vec::new() }
    }

    /// An empty table with room for at least `capacity` rows.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: Vec::with_capacity(capacity),
        }
    }

    /// Append a row without rejecting a duplicate key.
    ///
    /// Returns the row's insertion-order position.
    pub fn push(&mut self, key: K, value: V) -> usize {
        let row = self.rows.len();
        self.rows.push((key, value));
        row
    }

    /// A row by insertion-order position.
    #[must_use]
    pub fn get_row(&self, row: usize) -> Option<&(K, V)> {
        self.rows.get(row)
    }

    /// A mutable row by insertion-order position.
    pub fn get_row_mut(&mut self, row: usize) -> Option<&mut (K, V)> {
        self.rows.get_mut(row)
    }

    /// Every row as one contiguous slice.
    #[must_use]
    pub fn as_slice(&self) -> &[(K, V)] {
        self.rows.as_slice()
    }

    /// Every row as one mutable contiguous slice.
    pub fn as_mut_slice(&mut self) -> &mut [(K, V)] {
        self.rows.as_mut_slice()
    }

    /// Every row in insertion order.
    pub fn iter(&self) -> core::slice::Iter<'_, (K, V)> {
        self.rows.iter()
    }

    /// Every row mutably, in insertion order.
    pub fn iter_mut(&mut self) -> core::slice::IterMut<'_, (K, V)> {
        self.rows.iter_mut()
    }

    /// Reserve room for at least `additional` more rows.
    pub fn reserve(&mut self, additional: usize) {
        self.rows.reserve(additional);
    }

    /// The current row capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.rows.capacity()
    }

    /// Consume the table and return its rows without copying.
    #[must_use]
    pub fn into_rows(self) -> Vec<(K, V)> {
        self.rows
    }

    /// Every row as one contiguous slice.
    ///
    /// This is the frozen query seam: a caller that has finished building the
    /// table can keep only `&LinearTable` and binary-search this slice without
    /// allocation or internal mutation.
    #[must_use]
    pub fn rows(&self) -> &[(K, V)] {
        &self.rows
    }

    /// Number of rows, including duplicate keys appended with [`push`](Self::push).
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the table has no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl<K, V> LinearTable<K, V>
where
    K: Eq,
{
    /// Append a row only when its key is absent.
    pub fn insert(&mut self, key: K, value: V) -> Result<usize, InsertError<K>> {
        if self.rows.iter().any(|(present, _)| present == &key) {
            return Err(InsertError::DuplicateKey(key));
        }
        Ok(self.push(key, value))
    }

    /// The first value with this key.
    #[inline]
    #[must_use]
    pub fn select(&self, key: &K) -> Option<&V> {
        self.rows
            .iter()
            .find(|(present, _)| present == key)
            .map(|(_, value)| value)
    }
}

impl<K, V> LinearTable<K, V>
where
    Vec<(K, V)>: Codec,
    (K, V): Clone,
{
    /// Encode every row with WorkTable's self-contained Vec page codec.
    ///
    /// # Errors
    ///
    /// Refuses a row whose archive does not fit one page body.
    pub fn unload(&self) -> Result<Vec<u8>, RowTooLarge> {
        to_pages(&self.rows)
    }

    /// Encode rows from `first` onward as an independent append segment.
    ///
    /// Independent segments restart page numbering at zero. The codec accepts
    /// a terminal page followed by such a segment, so callers can concatenate
    /// the returned bytes without rewriting the preceding pages. Updates and
    /// deletes still require a full snapshot.
    ///
    /// # Errors
    ///
    /// Refuses a row whose archive does not fit one page body.
    pub fn unload_appending(&self, first: usize) -> Result<Vec<u8>, RowTooLarge> {
        to_pages(&self.rows[first.min(self.rows.len())..])
    }

    /// Rebuild the contiguous rows from WorkTable Vec pages.
    ///
    /// # Errors
    ///
    /// Refuses incomplete, corrupt, foreign-version, foreign-schema, or
    /// undecodable pages with the codec's page-specific [`LoadError`].
    pub fn load(bytes: &[u8]) -> Result<Self, LoadError> {
        Ok(Self {
            rows: from_pages(bytes)?,
        })
    }
}

impl<K, V> From<Vec<(K, V)>> for LinearTable<K, V> {
    fn from(rows: Vec<(K, V)>) -> Self {
        Self { rows }
    }
}

impl<K, V> AsRef<[(K, V)]> for LinearTable<K, V> {
    fn as_ref(&self) -> &[(K, V)] {
        self.as_slice()
    }
}
