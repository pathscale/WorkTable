use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::mem_stat::MemStat;

/// Compact position used by generated columnar storage.
///
/// This is supplemental metadata, never a replacement for a WorkTable primary
/// key. Slot IDs are not sort keys and are not durable identities.
pub trait ColumnSlotId: Copy + Debug + Eq + Ord + Hash + Send + Sync + MemStat + 'static {
    const BITS: u8;

    fn try_from_position(position: u64) -> Option<Self>;
    fn position(self) -> u64;

    fn slot(self) -> usize {
        usize::try_from(self.position()).expect("column slot ID exceeds this target's address space")
    }
}

macro_rules! column_slot_id {
    ($name:ident, $inner:ty, $bits:literal) => {
        #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name($inner);

        impl ColumnSlotId for $name {
            const BITS: u8 = $bits;

            fn try_from_position(position: u64) -> Option<Self> {
                <$inner>::try_from(position).ok().map(Self)
            }

            fn position(self) -> u64 {
                self.0 as u64
            }
        }

        impl MemStat for $name {
            fn heap_size(&self) -> usize {
                0
            }

            fn used_size(&self) -> usize {
                0
            }
        }
    };
}

column_slot_id!(ColumnSlotId8, u8, 8);
column_slot_id!(ColumnSlotId16, u16, 16);
column_slot_id!(ColumnSlotId32, u32, 32);
column_slot_id!(ColumnSlotId64, u64, 64);

static NEXT_COLUMNAR_INCARNATION: AtomicU64 = AtomicU64::new(1);

/// Returns a process-local table incarnation used to invalidate retained
/// columnar references when a table is rebuilt or reopened.
#[doc(hidden)]
pub fn next_columnar_incarnation() -> u64 {
    NEXT_COLUMNAR_INCARNATION
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| value.checked_add(1))
        .expect("columnar table incarnation space is exhausted")
}

/// Identity carried by generated columnar query results.
///
/// The primary key remains authoritative. The slot, generation, and table
/// incarnation are private validation metadata and are deliberately not
/// serializable or exposed as ordering keys.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnarRowRef<PrimaryKey, SlotId = ColumnSlotId32> {
    primary_key: PrimaryKey,
    slot_id: SlotId,
    generation: u64,
    incarnation: u64,
}

impl<PrimaryKey, SlotId> ColumnarRowRef<PrimaryKey, SlotId> {
    /// Returns the authoritative WorkTable primary key.
    pub fn primary_key(&self) -> &PrimaryKey {
        &self.primary_key
    }

    /// Constructor used by generated WorkTable code.
    #[doc(hidden)]
    pub fn __new(primary_key: PrimaryKey, slot_id: SlotId, generation: u64, incarnation: u64) -> Self {
        Self {
            primary_key,
            slot_id,
            generation,
            incarnation,
        }
    }

    #[doc(hidden)]
    pub fn __slot_id(&self) -> SlotId
    where
        SlotId: Copy,
    {
        self.slot_id
    }

    #[doc(hidden)]
    pub fn __generation(&self) -> u64 {
        self.generation
    }

    #[doc(hidden)]
    pub fn __incarnation(&self) -> u64 {
        self.incarnation
    }
}

impl<PrimaryKey: MemStat, SlotId: MemStat> MemStat for ColumnarRowRef<PrimaryKey, SlotId> {
    fn heap_size(&self) -> usize {
        self.primary_key.heap_size() + self.slot_id.heap_size()
    }

    fn used_size(&self) -> usize {
        self.primary_key.used_size() + self.slot_id.used_size()
    }
}

/// Compression used by a generated columnar field.
///
/// Mutable chunks are currently unencoded; unsupported policies are rejected
/// by the macro instead of being accepted as inert configuration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ColumnCompression {
    #[default]
    None,
}

impl ColumnCompression {
    pub fn is_encoded(self) -> bool {
        false
    }
}

impl MemStat for ColumnCompression {
    fn heap_size(&self) -> usize {
        0
    }

    fn used_size(&self) -> usize {
        0
    }
}

/// Chunked storage for one generated columnar field.
#[derive(Debug)]
pub struct ColumnarColumn<T> {
    chunk_rows: usize,
    compression: ColumnCompression,
    chunks: Vec<Vec<Option<T>>>,
}

impl<T> ColumnarColumn<T> {
    pub fn new(chunk_rows: usize, compression: ColumnCompression) -> Self {
        assert!(chunk_rows > 0, "columnar chunks cannot be empty");
        Self {
            chunk_rows,
            compression,
            chunks: Vec::new(),
        }
    }

    pub fn chunk_rows(&self) -> usize {
        self.chunk_rows
    }

    pub fn compression(&self) -> ColumnCompression {
        self.compression
    }

    pub fn set<SlotId: ColumnSlotId>(&mut self, slot_id: SlotId, value: T) {
        let row = slot_id.slot();
        let chunk_index = row / self.chunk_rows;
        let offset = row % self.chunk_rows;
        while self.chunks.len() <= chunk_index {
            self.chunks.push(Vec::new());
        }
        let chunk = &mut self.chunks[chunk_index];
        if chunk.len() <= offset {
            chunk.resize_with(offset + 1, || None);
        }
        chunk[offset] = Some(value);
    }

    pub fn remove<SlotId: ColumnSlotId>(&mut self, slot_id: SlotId) -> Option<T> {
        let row = slot_id.slot();
        self.chunks
            .get_mut(row / self.chunk_rows)
            .and_then(|chunk| chunk.get_mut(row % self.chunk_rows))
            .and_then(Option::take)
    }

    pub fn get<SlotId: ColumnSlotId>(&self, slot_id: SlotId) -> Option<&T> {
        let row = slot_id.slot();
        self.chunks
            .get(row / self.chunk_rows)
            .and_then(|chunk| chunk.get(row % self.chunk_rows))
            .and_then(Option::as_ref)
    }

    pub fn iter<SlotId: ColumnSlotId>(&self) -> impl Iterator<Item = (SlotId, &T)> {
        let chunk_rows = self.chunk_rows;
        self.chunks.iter().enumerate().flat_map(move |(chunk_index, chunk)| {
            chunk.iter().enumerate().filter_map(move |(offset, value)| {
                value.as_ref().map(|value| {
                    let position = (chunk_index * chunk_rows + offset) as u64;
                    let slot_id = SlotId::try_from_position(position)
                        .expect("stored column position fits its configured column slot ID");
                    (slot_id, value)
                })
            })
        })
    }
}

impl<T: MemStat> MemStat for ColumnarColumn<T> {
    fn heap_size(&self) -> usize {
        self.chunks.heap_size()
    }

    fn used_size(&self) -> usize {
        self.chunks.used_size()
    }
}

/// Ordered metadata for one generated `columnar_indexes` declaration.
#[derive(Debug)]
pub struct ClusteredColumnarIndex<K, SlotId = ColumnSlotId32> {
    rows: BTreeMap<K, BTreeSet<SlotId>>,
}

impl<K, SlotId> Default for ClusteredColumnarIndex<K, SlotId> {
    fn default() -> Self {
        Self { rows: BTreeMap::new() }
    }
}

impl<K: Ord, SlotId: Ord + Copy> ClusteredColumnarIndex<K, SlotId> {
    pub fn insert(&mut self, key: K, slot_id: SlotId) {
        self.rows.entry(key).or_default().insert(slot_id);
    }

    pub fn remove(&mut self, key: &K, slot_id: SlotId) {
        let remove_key = self.rows.get_mut(key).is_some_and(|rows| {
            rows.remove(&slot_id);
            rows.is_empty()
        });
        if remove_key {
            self.rows.remove(key);
        }
    }

    pub fn exact(&self, key: &K) -> Vec<SlotId> {
        self.rows
            .get(key)
            .map(|rows| rows.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn ordered_slot_ids(&self) -> Vec<SlotId> {
        self.rows.values().flat_map(|rows| rows.iter().copied()).collect()
    }
}

impl<K: MemStat + Ord, SlotId: MemStat + Ord> MemStat for ClusteredColumnarIndex<K, SlotId> {
    fn heap_size(&self) -> usize {
        self.rows.heap_size()
    }

    fn used_size(&self) -> usize {
        self.rows.used_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_are_addressed_by_configured_slot_id() {
        let mut column = ColumnarColumn::new(2, ColumnCompression::None);
        column.set(ColumnSlotId16(3), 30);
        column.set(ColumnSlotId16(0), 10);

        assert_eq!(column.chunk_rows(), 2);
        assert_eq!(column.get(ColumnSlotId16(3)), Some(&30));
        assert_eq!(
            column
                .iter::<ColumnSlotId16>()
                .map(|(id, value)| (id.0, *value))
                .collect::<Vec<_>>(),
            [(0, 10), (3, 30)]
        );

        assert_eq!(column.remove(ColumnSlotId16(0)), Some(10));
        assert!(column.get(ColumnSlotId16(0)).is_none());
    }

    #[test]
    fn clustered_index_preserves_key_order() {
        let mut index = ClusteredColumnarIndex::default();
        index.insert((2, 1), ColumnSlotId8(1));
        index.insert((1, 9), ColumnSlotId8(2));
        index.insert((1, 9), ColumnSlotId8(0));

        assert_eq!(index.exact(&(1, 9)), [ColumnSlotId8(0), ColumnSlotId8(2)]);
        assert_eq!(
            index.ordered_slot_ids(),
            [ColumnSlotId8(0), ColumnSlotId8(2), ColumnSlotId8(1)]
        );
    }

    #[test]
    fn widths_have_expected_capacity_boundaries() {
        assert_eq!(ColumnSlotId8::try_from_position(255), Some(ColumnSlotId8(255)));
        assert_eq!(ColumnSlotId8::try_from_position(256), None);
        assert_eq!(ColumnSlotId16::try_from_position(65_535), Some(ColumnSlotId16(65_535)));
        assert_eq!(ColumnSlotId16::try_from_position(65_536), None);
        assert_eq!(
            ColumnSlotId32::try_from_position(u32::MAX as u64),
            Some(ColumnSlotId32(u32::MAX))
        );
        assert_eq!(ColumnSlotId32::try_from_position(u32::MAX as u64 + 1), None);
        assert_eq!(
            ColumnSlotId64::try_from_position(u64::MAX),
            Some(ColumnSlotId64(u64::MAX))
        );
    }
}
