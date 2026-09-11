use alloc::{string::String, string::ToString, vec::Vec};
use core::fmt::{self, Debug, Display, Formatter};
#[cfg(not(feature = "std"))]
use ordered_float::FloatCore;

use crate::in_memory::{RowWrapper, StorableRow};
use crate::mem_stat::MemStat;
use crate::util::OffsetEqLink;
use crate::{TableSecondaryIndexInfo, UniqueIndex, WorkTable};

#[derive(Debug)]
pub struct SystemInfo {
    pub table_name: &'static str,
    pub page_count: usize,
    pub row_count: usize,
    pub empty_slots: u64,
    pub memory_usage_bytes: u64,
    pub idx_size: usize,
    pub indexes_info: Vec<IndexInfo>,
}

#[derive(Debug)]
pub struct IndexInfo {
    pub name: String,
    pub index_type: IndexKind,
    pub key_count: usize,
    pub capacity: usize,
    pub heap_size: usize,
    pub used_size: usize,
    pub node_count: usize,
}

#[derive(Debug)]
pub enum IndexKind {
    Unique,
    NonUnique,
}

impl Display for IndexKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unique => write!(f, "unique"),
            Self::NonUnique => write!(f, "non unique"),
        }
    }
}

impl<
    Row,
    PrimaryKey,
    AvailableTypes,
    AvailableIndexes,
    SecondaryIndexes,
    LockType,
    PkGen,
    const DATA_LENGTH: usize,
    PkMap,
> WorkTable<Row, PrimaryKey, AvailableTypes, AvailableIndexes, SecondaryIndexes, LockType, PkGen, DATA_LENGTH, PkMap>
where
    PrimaryKey: Debug + Clone + Ord + Send + 'static + core::hash::Hash,
    Row: StorableRow + Send + Clone + 'static,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
    PkMap: UniqueIndex<PrimaryKey, OffsetEqLink<DATA_LENGTH>>,
    SecondaryIndexes: MemStat + TableSecondaryIndexInfo,
{
    /// Rows currently in the table.
    ///
    /// Separate from [`Self::system_info`] because callers that want only this
    /// were paying for the page walk, the empty-link count and the index info
    /// to read one `usize`.
    pub fn row_count(&self) -> usize {
        self.primary_index.pk_map.len()
    }

    /// Row bytes plus secondary index bytes.
    ///
    /// The same figure `system_info` reports, without building the rest of it.
    pub fn used_bytes(&self) -> u64 {
        self.data.used_bytes() + self.indexes.heap_size() as u64
    }

    pub fn system_info(&self) -> SystemInfo {
        let page_count = self.data.get_page_count();
        let row_count = self.primary_index.pk_map.len();

        let empty_links = self.data.empty_links_count();

        let memory_usage_bytes = self.data.used_bytes();

        let idx_size = self.indexes.heap_size();

        SystemInfo {
            table_name: self.table_name,
            page_count,
            row_count,
            empty_slots: empty_links as u64,
            memory_usage_bytes,
            idx_size,
            indexes_info: self.indexes.index_info(),
        }
    }
}

impl Display for SystemInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mem_fmt = fmt_bytes(self.memory_usage_bytes as usize);
        let idx_fmt = fmt_bytes(self.idx_size);
        let total_fmt = fmt_bytes(self.memory_usage_bytes as usize + self.idx_size);

        writeln!(f, "┌──────────────────────────────┐")?;
        writeln!(f, " \t Table Name: {:<5}", self.table_name)?;
        writeln!(f, "└──────────────────────────────┘")?;
        writeln!(
            f,
            "Rows: {}   Pages: {}   Empty slots: {}",
            self.row_count, self.page_count, self.empty_slots
        )?;
        writeln!(
            f,
            "Allocated Memory: {mem_fmt} (data) + {idx_fmt} (indexes) = {total_fmt} total\n"
        )?;

        // **Padded by hand rather than by a table crate.**
        //
        // This used to be `prettytable-rs`, which reaches `csv` and then
        // `memchr` and fails to build without `std` in 505 places. Every
        // alternative measured puts its usable API behind `std` too: `tabled`
        // compiles without `std` but exposes no `Table` at all in that mode,
        // and `comfy-table` and `ascii_table` do not compile. Seven columns of
        // short strings are not worth a dependency that decides whether this
        // crate can be embedded.
        let mut rows: Vec<[String; COLUMNS]> = Vec::with_capacity(self.indexes_info.len() + 1);
        rows.push([
            "Index".to_string(),
            "Type".to_string(),
            "Keys".to_string(),
            "Capacity".to_string(),
            "Node Count".to_string(),
            "Heap".to_string(),
            "Used".to_string(),
        ]);
        for idx in &self.indexes_info {
            rows.push([
                idx.name.to_string(),
                idx.index_type.to_string(),
                idx.key_count.to_string(),
                idx.capacity.to_string(),
                idx.node_count.to_string(),
                fmt_bytes(idx.heap_size),
                fmt_bytes(idx.used_size),
            ]);
        }

        // Width by character count, not byte length: an index named with any
        // multi-byte character would otherwise pad short and skew every column
        // after it.
        let mut widths = [0usize; COLUMNS];
        for row in &rows {
            for (width, cell) in widths.iter_mut().zip(row) {
                *width = (*width).max(cell.chars().count());
            }
        }

        for (index, row) in rows.iter().enumerate() {
            for (column, (cell, width)) in row.iter().zip(&widths).enumerate() {
                if column + 1 == COLUMNS {
                    write!(f, "{cell}")?;
                } else {
                    let padding = width - cell.chars().count();
                    write!(f, "{cell}{:padding$}{COLUMN_GAP}", "")?;
                }
            }
            writeln!(f)?;
            // A rule under the header, and nothing between the rows: the same
            // shape `FORMAT_NO_BORDER_LINE_SEPARATOR` produced.
            if index == 0 {
                let rule: usize = widths.iter().sum::<usize>() + COLUMN_GAP.len() * (COLUMNS - 1);
                writeln!(f, "{:-<rule$}", "")?;
            }
        }

        Ok(())
    }
}

/// Columns in the per-index table below: name, type, keys, capacity, node
/// count, heap, used.
const COLUMNS: usize = 7;

/// Spaces between one column and the next.
const COLUMN_GAP: &str = "  ";

fn fmt_bytes(bytes: usize) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * KB;
    const GB: f64 = 1024.0 * MB;

    let b = bytes as f64;

    let (value, unit) = if b >= GB {
        (b / GB, "GB")
    } else if b >= MB {
        (b / MB, "MB")
    } else if b >= KB {
        (b / KB, "KB")
    } else {
        return format!("{bytes} B");
    };

    if (value.fract() * 100.0).round() == 0.0 {
        format!("{value:.0} {unit}")
    } else {
        format!("{value:.2} {unit}")
    }
}
