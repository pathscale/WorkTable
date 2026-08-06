use proc_macro2::Ident;

use crate::common::model::{ColumnSlotIdType, DEFAULT_COLUMNAR_CHUNK_ROWS};

#[derive(Debug)]
pub struct Config {
    pub page_size: Option<u32>,
    pub row_derives: Vec<Ident>,
    pub columnar_slot_id: ColumnSlotIdType,
    pub columnar_chunk_rows: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            page_size: None,
            row_derives: Vec::new(),
            columnar_slot_id: ColumnSlotIdType::default(),
            columnar_chunk_rows: DEFAULT_COLUMNAR_CHUNK_ROWS,
        }
    }
}
