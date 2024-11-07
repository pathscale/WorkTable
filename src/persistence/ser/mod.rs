use std::sync::Arc;
use crate::{TableRow, WorkTable};
use crate::in_memory::StorableRow;
use crate::persistence::page;
use crate::persistence::page::SpaceInfo;
use crate::persistence::space::Space;
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

    pub fn into_space(self: Arc<Self>) -> Space {
        let space_info = self.get_space_info();
        let space_info_page = page::General::from(space_info);
        let header = space_info_page.header;

        let mut space = Space::default();
        space.pages.push(space_info_page)
    }
}