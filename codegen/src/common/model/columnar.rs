use proc_macro2::Ident;

pub const DEFAULT_COLUMNAR_CHUNK_ROWS: usize = 65_536;

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
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Auto => "auto",
            Self::Delta => "delta",
            Self::Rle => "rle",
            Self::Dictionary => "dictionary",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnarFieldConfig {
    pub chunk_rows: usize,
    pub compression: ColumnCompression,
}

impl Default for ColumnarFieldConfig {
    fn default() -> Self {
        Self {
            chunk_rows: DEFAULT_COLUMNAR_CHUNK_ROWS,
            compression: ColumnCompression::Auto,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarIndex {
    pub name: Ident,
    pub columns: Vec<Ident>,
    pub cluster_by: Vec<Ident>,
}
