use crate::{TableRow, WorkTable};
use crate::in_memory::StorableRow;
use crate::persistence::page::SpaceInfo;
use crate::prelude::TablePrimaryKey;

impl<Row, Pk, I, PkGen, const DATA_LENGTH: usize> WorkTable<Row, Pk, I, PkGen, DATA_LENGTH>
where
    Row: TableRow<Pk>,
    Pk: Clone + Ord + TablePrimaryKey,
    Row: StorableRow,
{
    fn get_space_info(&self) -> SpaceInfo {
        SpaceInfo {
            id: 0.into(),
            page_count: 1,
            name: self.table_name.to_string(),
            primary_key_intervals: vec![],
        }
    }
}