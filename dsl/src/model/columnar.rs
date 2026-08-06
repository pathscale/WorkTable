use proc_macro2::Ident;

pub const DEFAULT_COLUMNAR_CHUNK_ROWS: usize = 65_536;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ColumnSlotIdType {
    U8,
    U16,
    #[default]
    U32,
    U64,
}

impl ColumnSlotIdType {
    pub(crate) fn type_name(self) -> &'static str {
        match self {
            Self::U8 => "ColumnSlotId8",
            Self::U16 => "ColumnSlotId16",
            Self::U32 => "ColumnSlotId32",
            Self::U64 => "ColumnSlotId64",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ColumnCompression {
    #[default]
    None,
}

impl ColumnCompression {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::None => "none",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnarFieldConfig {
    pub chunk_rows: Option<usize>,
    pub compression: ColumnCompression,
}

impl Default for ColumnarFieldConfig {
    fn default() -> Self {
        Self {
            chunk_rows: None,
            compression: ColumnCompression::None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarIndex {
    pub name: Ident,
    pub cluster_by: Vec<Ident>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnarIndexes {
    pub indexes: indexmap::IndexMap<Ident, ColumnarIndex>,
}
