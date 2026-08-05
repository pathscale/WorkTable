use std::collections::{BTreeMap, BTreeSet};

use crate::mem_stat::MemStat;

/// A stable logical row identifier used by generated columnar replicas.
///
/// It is deliberately independent of [`data_bucket::Link`]: vacuum may move a
/// row between physical pages without changing its columnar identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnRowId(u64);

impl ColumnRowId {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl MemStat for ColumnRowId {
    fn heap_size(&self) -> usize {
        0
    }

    fn used_size(&self) -> usize {
        0
    }
}

/// Compression requested for a generated columnar field.
///
/// The first implementation stores mutable chunks without encoding them.
/// `Auto` therefore resolves to `None`; the explicit variants are retained in
/// metadata so immutable/sealed-chunk codecs can be added without changing the
/// macro syntax.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ColumnCompression {
    None,
    #[default]
    Auto,
    Delta,
    Rle,
    Dictionary,
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

/// Chunked, row-id-addressed storage for one generated columnar field.
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

    pub fn set(&mut self, row_id: ColumnRowId, value: T) {
        let row = row_id.get() as usize;
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

    pub fn remove(&mut self, row_id: ColumnRowId) -> Option<T> {
        let row = row_id.get() as usize;
        self.chunks
            .get_mut(row / self.chunk_rows)
            .and_then(|chunk| chunk.get_mut(row % self.chunk_rows))
            .and_then(Option::take)
    }

    pub fn get(&self, row_id: ColumnRowId) -> Option<&T> {
        let row = row_id.get() as usize;
        self.chunks
            .get(row / self.chunk_rows)
            .and_then(|chunk| chunk.get(row % self.chunk_rows))
            .and_then(Option::as_ref)
    }

    pub fn iter(&self) -> impl Iterator<Item = (ColumnRowId, &T)> {
        let chunk_rows = self.chunk_rows;
        self.chunks.iter().enumerate().flat_map(move |(chunk_index, chunk)| {
            chunk.iter().enumerate().filter_map(move |(offset, value)| {
                value.as_ref().map(|value| {
                    let row = chunk_index * chunk_rows + offset;
                    (ColumnRowId::new(row as u64), value)
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
pub struct ClusteredColumnarIndex<K> {
    rows: BTreeMap<K, BTreeSet<ColumnRowId>>,
}

impl<K> Default for ClusteredColumnarIndex<K> {
    fn default() -> Self {
        Self { rows: BTreeMap::new() }
    }
}

impl<K: Ord> ClusteredColumnarIndex<K> {
    pub fn insert(&mut self, key: K, row_id: ColumnRowId) {
        self.rows.entry(key).or_default().insert(row_id);
    }

    pub fn remove(&mut self, key: &K, row_id: ColumnRowId) {
        let remove_key = self.rows.get_mut(key).is_some_and(|rows| {
            rows.remove(&row_id);
            rows.is_empty()
        });
        if remove_key {
            self.rows.remove(key);
        }
    }

    pub fn exact(&self, key: &K) -> Vec<ColumnRowId> {
        self.rows
            .get(key)
            .map(|rows| rows.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn ordered_row_ids(&self) -> Vec<ColumnRowId> {
        self.rows.values().flat_map(|rows| rows.iter().copied()).collect()
    }
}

impl<K: MemStat + Ord> MemStat for ClusteredColumnarIndex<K> {
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
    fn chunks_are_addressed_by_stable_row_id() {
        let mut column = ColumnarColumn::new(2, ColumnCompression::Auto);
        column.set(ColumnRowId::new(3), 30);
        column.set(ColumnRowId::new(0), 10);

        assert_eq!(column.chunk_rows(), 2);
        assert_eq!(column.get(ColumnRowId::new(3)), Some(&30));
        assert_eq!(
            column.iter().map(|(id, value)| (id.get(), *value)).collect::<Vec<_>>(),
            [(0, 10), (3, 30)]
        );

        assert_eq!(column.remove(ColumnRowId::new(0)), Some(10));
        assert!(column.get(ColumnRowId::new(0)).is_none());
    }

    #[test]
    fn clustered_index_preserves_key_order() {
        let mut index = ClusteredColumnarIndex::default();
        index.insert((2, 1), ColumnRowId::new(1));
        index.insert((1, 9), ColumnRowId::new(2));
        index.insert((1, 9), ColumnRowId::new(0));

        assert_eq!(index.exact(&(1, 9)), [ColumnRowId::new(0), ColumnRowId::new(2)]);
        assert_eq!(
            index.ordered_row_ids(),
            [ColumnRowId::new(0), ColumnRowId::new(2), ColumnRowId::new(1)]
        );
    }
}
