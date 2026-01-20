mod fragmentation_info;
mod lock;
mod page;

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use data_bucket::Link;
use data_bucket::page::PageId;
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;
use rkyv::rancor::Strategy;
use rkyv::ser::Serializer;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Serialize};

use crate::in_memory::{DataPages, GhostWrapper, RowWrapper, StorableRow};
use crate::prelude::{OffsetEqLink, TablePrimaryKey};
use crate::vacuum::fragmentation_info::PageFragmentationInfo;
use crate::vacuum::lock::VacuumLock;
use crate::{
    AvailableIndex, IndexMap, PrimaryIndex, TableRow, TableSecondaryIndex, TableSecondaryIndexCdc,
};

#[derive(Debug)]
pub struct EmptyDataVacuum<
    Row,
    PrimaryKey,
    PkNodeType,
    SecondaryIndexes,
    AvailableTypes,
    AvailableIndexes,
    const DATA_LENGTH: usize,
    SecondaryEvents = (),
> where
    PrimaryKey: Clone + Ord + Send + 'static + std::hash::Hash,
    Row: StorableRow + Send + Clone + 'static,
    PkNodeType: NodeLike<Pair<PrimaryKey, OffsetEqLink<DATA_LENGTH>>> + Send + 'static,
{
    data_pages: Arc<DataPages<Row, DATA_LENGTH>>,
    vacuum_lock: Arc<VacuumLock>,

    primary_index: Arc<PrimaryIndex<PrimaryKey, DATA_LENGTH, PkNodeType>>,
    secondary_indexes: Arc<SecondaryIndexes>,

    phantom_data: PhantomData<(SecondaryEvents, AvailableTypes, AvailableIndexes)>,
}

impl<
    Row,
    PrimaryKey,
    PkNodeType,
    SecondaryIndexes,
    AvailableTypes,
    AvailableIndexes,
    const DATA_LENGTH: usize,
    SecondaryEvents,
>
    EmptyDataVacuum<
        Row,
        PrimaryKey,
        PkNodeType,
        SecondaryIndexes,
        AvailableTypes,
        AvailableIndexes,
        DATA_LENGTH,
        SecondaryEvents,
    >
where
    Row: TableRow<PrimaryKey> + StorableRow + Send + Clone + 'static,
    PrimaryKey: Debug + Clone + Ord + Send + TablePrimaryKey + std::hash::Hash,
    PkNodeType: NodeLike<Pair<PrimaryKey, OffsetEqLink<DATA_LENGTH>>> + Send + 'static,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
    Row: Archive
        + Clone
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    <Row as StorableRow>::WrappedRow: Archive
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    <<Row as StorableRow>::WrappedRow as Archive>::Archived: GhostWrapper,
    SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>
        + TableSecondaryIndexCdc<Row, AvailableTypes, SecondaryEvents, AvailableIndexes>,
    AvailableIndexes: Debug + AvailableIndex,
{
    async fn defragment_page(&self, info: PageFragmentationInfo<DATA_LENGTH>) {
        let mut page_empty_links = self
            .data_pages
            .empty_links_registry()
            .page_links_map
            .get(&info.page_id)
            .map(|(_, l)| *l)
            .collect::<Vec<_>>();
        page_empty_links.sort_by(|l1, l2| l1.offset.cmp(&l2.offset));

        let _lock = self.vacuum_lock.lock_page(info.page_id);
        let mut empty_links_iter = page_empty_links.into_iter();

        let Some(mut current_empty) = empty_links_iter.next() else {
            return;
        };

        let Some(mut next_empty) = empty_links_iter.next() else {
            self.shift_data_in_range(current_empty, None);
            return;
        };

        loop {
            let offset = self.shift_data_in_range(current_empty, Some(next_empty.offset));

            let new_next = empty_links_iter.next();
            match new_next {
                Some(link) => {
                    current_empty = Link {
                        page_id: next_empty.page_id,
                        offset,
                        length: next_empty.length + (next_empty.offset - offset),
                    };
                    next_empty = link;
                }
                None => {
                    self.shift_data_in_range(next_empty, None);
                    break;
                }
            }
        }
    }

    pub fn shift_data_in_range(&self, start_link: Link, end_offset: Option<u32>) -> u32 {
        let page_id = start_link.page_id;
        let page = self
            .data_pages
            .get_page(page_id)
            .expect("should exist as link exists");
        let start_link = OffsetEqLink::<_>(start_link);
        let mut range_iter = self.primary_index.reverse_pk_map.range(start_link..);
        let mut entry_offset = start_link.0.offset;

        while let Some((link, pk)) = range_iter.next() {
            let link_value = link.0;

            if let Some(end) = end_offset {
                if entry_offset + link_value.length >= end {
                    return entry_offset;
                }
            }

            let new_link = Link {
                page_id,
                offset: entry_offset,
                length: link_value.length,
            };

            // TODO: Safety comment
            unsafe {
                page.move_from_to(link_value, new_link)
                    .expect("should use valid links")
            }
            entry_offset += link_value.length;
            self.update_index_after_move(pk.clone(), link_value, new_link);
        }

        entry_offset
    }

    fn update_index_after_move(&self, pk: PrimaryKey, old_link: Link, new_link: Link) {
        let old_offset_link = OffsetEqLink(old_link);
        let new_offset_link = OffsetEqLink(new_link);

        self.primary_index
            .pk_map
            .insert(pk.clone(), new_offset_link);
        self.primary_index.reverse_pk_map.remove(&old_offset_link);
        self.primary_index
            .reverse_pk_map
            .insert(new_offset_link, pk);
        // TODO: update secondary indexes
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::marker::PhantomData;
    use std::sync::Arc;

    use indexset::core::pair::Pair;
    use worktable_codegen::{MemStat, worktable};

    use crate::in_memory::{GhostWrapper, RowWrapper, StorableRow};
    use crate::prelude::*;
    use crate::vacuum::EmptyDataVacuum;
    use crate::vacuum::lock::VacuumLock;

    worktable! (
        name: Test,
        columns: {
            id: u64 primary_key autoincrement,
            test: i64,
            another: u64,
            exchange: String
        },
        indexes: {
            test_idx: test unique,
            exchnage_idx: exchange,
            another_idx: another,
        }
    );

    /// Creates an EmptyDataVacuum instance from a WorkTable
    fn create_vacuum(
        table: &TestWorkTable,
    ) -> EmptyDataVacuum<
        TestRow,
        TestPrimaryKey,
        Vec<Pair<TestPrimaryKey, OffsetEqLink<TEST_INNER_SIZE>>>,
        TestIndex,
        TestAvaiableTypes,
        TestAvailableIndexes,
        TEST_INNER_SIZE,
    > {
        EmptyDataVacuum {
            data_pages: Arc::clone(&table.0.data),
            vacuum_lock: Arc::new(VacuumLock::default()),
            primary_index: Arc::clone(&table.0.primary_index),
            secondary_indexes: Arc::clone(&table.0.indexes),
            phantom_data: PhantomData,
        }
    }

    #[tokio::test]
    async fn test_vacuum_shift_data_in_range_single_gap() {
        let table = TestWorkTable::default();

        let mut ids = HashMap::new();
        for i in 0..10 {
            let row = TestRow {
                id: table.get_next_pk().into(),
                test: i,
                another: i as u64,
                exchange: format!("test{}", i),
            };
            let id = row.id;
            table.insert(row.clone()).unwrap();
            ids.insert(id, row);
        }

        let first_two_ids = ids.keys().take(2).cloned().collect::<Vec<_>>();

        table.delete(first_two_ids[0].into()).await.unwrap();
        table.delete(first_two_ids[1].into()).await.unwrap();

        let vacuum = create_vacuum(&table);

        let per_page_info = table.0.data.empty_links_registry().get_per_page_info();
        let info = per_page_info
            .first()
            .expect("at least one page should exist");
        vacuum.defragment_page(*info).await;

        for (id, expected) in ids.into_iter().skip(2) {
            let row = table.select(id);
            assert_eq!(row, Some(expected));
        }
    }

    #[tokio::test]
    async fn test_vacuum_shift_data_middle_gap() {
        let table = TestWorkTable::default();

        let mut ids = HashMap::new();
        for i in 0..20 {
            let row = TestRow {
                id: table.get_next_pk().into(),
                test: i * 10,
                another: i as u64,
                exchange: format!("test{}", i),
            };
            let id = row.id;
            table.insert(row.clone()).unwrap();
            ids.insert(id, row);
        }

        let ids_to_delete = ids.keys().skip(5).take(2).cloned().collect::<Vec<_>>();

        table.delete(ids_to_delete[0].into()).await.unwrap();
        table.delete(ids_to_delete[1].into()).await.unwrap();

        let vacuum = create_vacuum(&table);

        let per_page_info = table.0.data.empty_links_registry().get_per_page_info();
        let info = per_page_info
            .first()
            .expect("at least one page should exist");
        vacuum.defragment_page(*info).await;

        for (id, expected) in ids
            .into_iter()
            .filter(|(i, _)| *i != ids_to_delete[0] && *i != ids_to_delete[1])
        {
            let row = table.select(id);
            assert_eq!(row, Some(expected));
        }
    }
}
