mod fragmentation_info;

use crate::in_memory::{DataPages, StorableRow};

#[derive(Debug)]
pub struct EmptyDataVacuum<Row: StorableRow, const DATA_LENGTH: usize> {
    data_pages: DataPages<Row, DATA_LENGTH>,
}

impl<Row: StorableRow, const DATA_LENGTH: usize> EmptyDataVacuum<Row, DATA_LENGTH> {
    pub fn new(data_pages: DataPages<Row, DATA_LENGTH>) -> Self {
        Self { data_pages }
    }

    pub fn vacuum_pages() -> eyre::Result<()> {}
}
