mod fragmentation_info;
mod lock;
mod page;

use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::Ordering;

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
use crate::{AvailableIndex, PrimaryIndex, TableRow, TableSecondaryIndex, TableSecondaryIndexCdc};

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
    async fn defragment(&self) {
        let per_page_info = self.data_pages.empty_links_registry().get_per_page_info();
        let mut in_migration_pages = VecDeque::new();
        let mut free_pages = vec![];
        let mut defragmented_pages = VecDeque::new();

        let mut info_iter = per_page_info.into_iter();
        while let Some(info) = info_iter.next() {
            let page_id = info.page_id;
            if let Some(id) = defragmented_pages.pop_front() {
                match self.move_data_from(page_id, id).await {
                    (true, true) => {
                        // from moved fully and on to no more space
                        free_pages.push(page_id);
                    }
                    (true, false) => {
                        // from moved fully but to has space
                        free_pages.push(id);
                        defragmented_pages.push_back(id);
                    }
                    (false, true) => {
                        // from was not moved but to have NO space
                        in_migration_pages.push_back(page_id);
                    }
                    (false, false) => unreachable!(
                        "at least one of two situations should appear to break from while cycle"
                    ),
                }
            } else {
                let page_id = info.page_id;
                self.defragment_page(info).await;
                if let Some(id) = in_migration_pages.pop_front() {
                    match self.move_data_from(id, page_id).await {
                        (true, true) => {
                            // from moved fully and on to no more space
                            free_pages.push(id);
                        }
                        (true, false) => {
                            // from moved fully but to has space
                            free_pages.push(id);
                            defragmented_pages.push_back(page_id);
                        }
                        (false, true) => {
                            // from was not moved but to have NO space
                            in_migration_pages.push_back(id);
                        }
                        (false, false) => unreachable!(
                            "at least one of two situations should appear to break from while cycle"
                        ),
                    }
                } else {
                    defragmented_pages.push_back(page_id);
                }
            }
        }

        for in_migration_pages in in_migration_pages {
            let page_start = Link {
                page_id: in_migration_pages,
                offset: 0,
                length: 0,
            };
            self.shift_data_in_range(page_start, None);
        }

        for id in free_pages {
            self.data_pages.mark_page_empty(id)
        }
    }

    async fn move_data_from(&self, from: PageId, to: PageId) -> (bool, bool) {
        let from_lock = self.vacuum_lock.lock_page(from);
        let to_lock = self.vacuum_lock.lock_page(to);

        let to_page = self
            .data_pages
            .get_page(to)
            .expect("should exist as link exists");
        let from_page = self
            .data_pages
            .get_page(from)
            .expect("should exist as link exists");
        let to_free_space = to_page.free_space();

        let page_start = OffsetEqLink::<_>(Link {
            page_id: from,
            offset: 0,
            length: 0,
        });

        let page_end = OffsetEqLink::<_>(Link {
            page_id: from.next(),
            offset: 0,
            length: 0,
        });

        let mut range = self
            .primary_index
            .reverse_pk_map
            .range(page_start..page_end);
        let mut sum_links_len = 0;
        let mut links = vec![];
        let mut from_page_will_be_moved = false;
        let mut to_page_will_be_filled = false;

        loop {
            let Some((next, pk)) = range.next() else {
                from_page_will_be_moved = true;
                break;
            };

            if sum_links_len + next.length > to_free_space as u32 {
                to_page_will_be_filled = true;
                if range.next().is_none() {
                    from_page_will_be_moved = true;
                }
                break;
            }
            sum_links_len += next.length;
            links.push((*next, pk.clone()));
        }

        drop(range);

        for (from_link, pk) in links {
            let raw_data = from_page
                .get_raw_row(from_link.0)
                .expect("link is not bigger than free offset");
            let new_link = to_page
                .save_raw_row(&raw_data)
                .expect("page is not full as checked on links collection");
            self.update_index_after_move(pk, from_link.0, new_link);
        }

        {
            let g = from_lock.read().await;
            g.unlock()
        }
        {
            let g = to_lock.read().await;
            g.unlock()
        }

        (from_page_will_be_moved, to_page_will_be_filled)
    }

    async fn defragment_page(&self, info: PageFragmentationInfo<DATA_LENGTH>) {
        let registry = self.data_pages.empty_links_registry();
        let mut page_empty_links = registry
            .page_links_map
            .get(&info.page_id)
            .map(|(_, l)| *l)
            .collect::<Vec<_>>();
        page_empty_links.sort_by(|l1, l2| l1.offset.cmp(&l2.offset));

        let lock = self.vacuum_lock.lock_page(info.page_id);
        let mut empty_links_iter = page_empty_links.into_iter();

        let Some(mut current_empty) = empty_links_iter.next() else {
            return;
        };
        registry.remove_link(current_empty);

        let Some(mut next_empty) = empty_links_iter.next() else {
            self.shift_data_in_range(current_empty, None);
            return;
        };
        registry.remove_link(next_empty);

        loop {
            let offset = self.shift_data_in_range(current_empty, Some(next_empty.offset));

            let new_next = empty_links_iter.next();
            match new_next {
                Some(link) => {
                    registry.remove_link(link);
                    current_empty = Link {
                        page_id: next_empty.page_id,
                        offset,
                        length: next_empty.length + (next_empty.offset - offset),
                    };
                    next_empty = link;
                }
                None => {
                    let from = Link {
                        page_id: next_empty.page_id,
                        offset,
                        length: next_empty.length + (next_empty.offset - offset),
                    };
                    self.shift_data_in_range(from, None);
                    break;
                }
            }
        }

        let l = lock.read().await;
        l.unlock();
    }

    fn shift_data_in_range(&self, start_link: Link, end_offset: Option<u32>) -> u32 {
        let page_id = start_link.page_id;
        let page = self
            .data_pages
            .get_page(page_id)
            .expect("should exist as link exists");
        let start_link = OffsetEqLink::<_>(start_link);
        let range = if let Some(offset) = end_offset {
            let end = OffsetEqLink::<_>(Link {
                page_id,
                offset,
                length: 0,
            });
            self.primary_index.reverse_pk_map.range(start_link..end)
        } else {
            let end = OffsetEqLink::<_>(Link {
                page_id: page_id.next(),
                offset: 0,
                length: 0,
            });
            self.primary_index.reverse_pk_map.range(start_link..end)
        }
        .map(|(l, pk)| (*l, pk.clone()))
        .collect::<Vec<_>>();
        let mut range_iter = range.into_iter();

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

        if end_offset.is_none() {
            // Is safe as page is locked now and we can get here only if end_offset
            // is not set so we are shifting till page end.
            page.free_offset.store(entry_offset, Ordering::Release);
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

    worktable!(
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

        let mut ids = Vec::new();
        for i in 0..10 {
            let row = TestRow {
                id: table.get_next_pk().into(),
                test: i,
                another: i as u64,
                exchange: format!("test{}", i),
            };
            let id = row.id;
            table.insert(row.clone()).unwrap();
            ids.push((id, row));
        }

        let first_two_ids = ids.iter().take(2).map(|(i, _)| *i).collect::<Vec<_>>();

        table.delete(first_two_ids[0].into()).await.unwrap();
        table.delete(first_two_ids[1].into()).await.unwrap();

        let vacuum = create_vacuum(&table);
        vacuum.defragment().await;

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
        vacuum.defragment().await;

        for (id, expected) in ids
            .into_iter()
            .filter(|(i, _)| *i != ids_to_delete[0] && *i != ids_to_delete[1])
        {
            let row = table.select(id);
            assert_eq!(row, Some(expected));
        }
    }

    #[tokio::test]
    async fn test_vacuum_shift_data_last_records() {
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

        let last_two_ids = ids.keys().skip(8).take(2).cloned().collect::<Vec<_>>();

        table.delete(last_two_ids[1].into()).await.unwrap();
        table.delete(last_two_ids[0].into()).await.unwrap();

        let vacuum = create_vacuum(&table);
        vacuum.defragment().await;

        for (id, expected) in ids
            .into_iter()
            .filter(|(i, _)| *i != last_two_ids[0] && *i != last_two_ids[1])
        {
            let row = table.select(id);
            assert_eq!(row, Some(expected));
        }
    }

    #[tokio::test]
    async fn test_vacuum_shift_data_multiple_gaps() {
        let table = TestWorkTable::default();

        let mut ids = HashMap::new();
        for i in 0..15 {
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

        let ids_to_delete = [1, 3, 5, 7].map(|idx| ids.keys().cloned().nth(idx).unwrap());

        for id in &ids_to_delete {
            table.delete((*id).into()).await.unwrap();
        }

        let vacuum = create_vacuum(&table);
        vacuum.defragment().await;

        for (id, expected) in ids.into_iter().filter(|(i, _)| !ids_to_delete.contains(i)) {
            let row = table.select(id);
            assert_eq!(row, Some(expected));
        }
    }

    #[tokio::test]
    async fn test_vacuum_shift_data_single_record_left() {
        let table = TestWorkTable::default();

        let mut ids = Vec::new();
        for i in 0..5 {
            let row = TestRow {
                id: table.get_next_pk().into(),
                test: i,
                another: i as u64,
                exchange: format!("test{}", i),
            };
            let id = row.id;
            table.insert(row.clone()).unwrap();
            ids.push((id, row));
        }

        let remaining_id = ids[0].0;

        for (id, _) in ids.iter().skip(1) {
            table.delete((*id).into()).await.unwrap();
        }

        let vacuum = create_vacuum(&table);
        vacuum.defragment().await;

        let row = table.select(remaining_id);
        assert_eq!(row, Some(ids[0].1.clone()));
    }

    #[tokio::test]
    async fn test_vacuum_defragment_on_delete_last() {
        let table = TestWorkTable::default();

        let mut ids = Vec::new();
        for i in 0..5 {
            let row = TestRow {
                id: table.get_next_pk().into(),
                test: i,
                another: i as u64,
                exchange: format!("test{}", i),
            };
            let id = row.id;
            table.insert(row.clone()).unwrap();
            ids.push((id, row));
        }

        table.delete(ids.last().unwrap().0.into()).await.unwrap();

        let vacuum = create_vacuum(&table);
        vacuum.defragment().await;

        for (id, expected) in ids.into_iter().take(4) {
            let row = table.select(id);
            assert_eq!(row, Some(expected));
        }
    }

    #[tokio::test]
    async fn test_vacuum_shift_data_variable_string_lengths() {
        let table = TestWorkTable::default();

        let mut ids = HashMap::new();
        let strings = vec![
            "a",
            "bbbb",
            "cccccc",
            "dddddddd",
            "eeeeeeeeee",
            "ffffffffffff",
            "gggggggggggggg",
        ];

        for (i, s) in strings.iter().enumerate() {
            let row = TestRow {
                id: table.get_next_pk().into(),
                test: i as i64,
                another: i as u64,
                exchange: s.to_string(),
            };
            let id = row.id;
            table.insert(row.clone()).unwrap();
            ids.insert(id, row);
        }

        let ids_to_delete = ids.keys().take(3).cloned().collect::<Vec<_>>();

        for id in &ids_to_delete {
            table.delete((*id).into()).await.unwrap();
        }

        let vacuum = create_vacuum(&table);
        vacuum.defragment().await;

        for (id, expected) in ids.into_iter().filter(|(i, _)| !ids_to_delete.contains(i)) {
            let row = table.select(id);
            assert_eq!(row, Some(expected));
        }
    }

    #[tokio::test]
    async fn test_vacuum_insert_after_free_offset_update() {
        let table = TestWorkTable::default();

        let mut original_ids = HashMap::new();
        for i in 0..8 {
            let row = TestRow {
                id: table.get_next_pk().into(),
                test: i,
                another: i as u64,
                exchange: format!("original{}", i),
            };
            let id = row.id;
            table.insert(row.clone()).unwrap();
            original_ids.insert(id, row);
        }

        let ids_to_delete = original_ids.keys().take(3).cloned().collect::<Vec<_>>();
        for id in &ids_to_delete {
            table.delete((*id).into()).await.unwrap();
        }

        let vacuum = create_vacuum(&table);
        vacuum.defragment().await;

        let mut new_ids = HashMap::new();
        for i in 0..3 {
            let row = TestRow {
                id: table.get_next_pk().into(),
                test: 100 + i,
                another: (100 + i) as u64,
                exchange: format!("new{}", i),
            };
            let id = row.id;
            table.insert(row.clone()).unwrap();
            new_ids.insert(id, row);
        }

        for (id, expected) in original_ids
            .into_iter()
            .filter(|(i, _)| !ids_to_delete.contains(i))
        {
            let row = table.select(id);
            assert_eq!(row, Some(expected));
        }

        for (id, expected) in new_ids {
            let row = table.select(id);
            assert_eq!(row, Some(expected));
        }
    }

    #[tokio::test]
    async fn test_vacuum_multi_page_data_migration() {
        let table = TestWorkTable::default();

        let mut ids = Vec::new();
        // row is ~40 bytes so ~409 rows per page
        for i in 0..500 {
            let row = TestRow {
                id: table.get_next_pk().into(),
                test: i,
                another: i as u64,
                exchange: format!("test{}", i),
            };
            let id = row.id;
            table.insert(row.clone()).unwrap();
            ids.push((id, row));
        }

        let ids_to_delete: Vec<_> = ids.iter().map(|(i, _)| *i).take(20).collect();
        for id in &ids_to_delete {
            table.delete((*id).into()).await.unwrap();
        }

        let vacuum = create_vacuum(&table);
        vacuum.defragment().await;

        for (id, expected) in ids.into_iter().filter(|(i, _)| !ids_to_delete.contains(i)) {
            let row = table.select(id);
            assert_eq!(row, Some(expected));
        }
    }

    #[tokio::test]
    async fn test_vacuum_multi_page_alternating_deletes() {
        let table = TestWorkTable::default();

        let mut ids = Vec::new();
        // row is ~40 bytes so ~409 rows per page
        for i in 0..500 {
            let row = TestRow {
                id: table.get_next_pk().into(),
                test: i,
                another: i as u64,
                exchange: format!("test{}", i),
            };
            let id = row.id;
            table.insert(row.clone()).unwrap();
            ids.push((id, row));
        }

        let ids_to_delete: Vec<_> = ids.iter().step_by(20).map(|(id, _)| *id).collect();
        for id in &ids_to_delete {
            table.delete((*id).into()).await.unwrap();
        }

        let vacuum = create_vacuum(&table);
        vacuum.defragment().await;

        for (id, expected) in ids
            .into_iter()
            .filter(|(id, _)| !ids_to_delete.contains(id))
        {
            let row = table.select(id);
            assert_eq!(row, Some(expected));
        }
    }

    #[tokio::test]
    async fn test_vacuum_multi_page_last() {
        let table = TestWorkTable::default();

        let mut ids = Vec::new();
        // row is ~40 bytes so ~409 rows per page
        for i in 0..500 {
            let row = TestRow {
                id: table.get_next_pk().into(),
                test: i,
                another: i as u64,
                exchange: format!("test{}", i),
            };
            let id = row.id;
            table.insert(row.clone()).unwrap();
            ids.push((id, row));
        }

        table.delete(ids.last().unwrap().0.into()).await.unwrap();

        let vacuum = create_vacuum(&table);
        vacuum.defragment().await;

        for (id, expected) in ids.into_iter().take(499) {
            let row = table.select(id);
            assert_eq!(row, Some(expected));
        }
    }

    #[tokio::test]
    async fn test_vacuum_multi_page_free_page() {
        let table = TestWorkTable::default();

        let mut ids = Vec::new();
        // row is ~40 bytes so ~409 rows per page
        for i in 0..1000 {
            let row = TestRow {
                id: table.get_next_pk().into(),
                test: i,
                another: i as u64,
                exchange: format!("test{}", i),
            };
            let id = row.id;
            table.insert(row.clone()).unwrap();
            ids.push((id, row));
        }

        let mut ids_to_delete: Vec<_> = ids.iter().skip(300).take(400).map(|(id, _)| *id).collect();
        // remove last too to trigger vacuum for last page too.
        ids_to_delete.push(ids.last().unwrap().0);
        for id in &ids_to_delete {
            table.delete((*id).into()).await.unwrap();
        }

        let vacuum = create_vacuum(&table);
        vacuum.defragment().await;

        assert!(table.0.data.get_empty_pages().len() > 0);

        for (id, expected) in ids
            .into_iter()
            .filter(|(id, _)| !ids_to_delete.contains(id))
        {
            let row = table.select(id);
            assert_eq!(row, Some(expected));
        }
    }
}
