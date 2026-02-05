use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use data_bucket::Link;
use data_bucket::page::PageId;
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;
use rkyv::rancor::Strategy;
use rkyv::ser::Serializer;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

use crate::in_memory::{ArchivedRowWrapper, DataPages, RowWrapper, StorableRow};
use crate::prelude::{Lock, LockMap, OffsetEqLink, RowLock, TablePrimaryKey};
use crate::vacuum::VacuumStats;
use crate::vacuum::WorkTableVacuum;
use crate::vacuum::fragmentation_info::FragmentationInfo;
use crate::{
    AvailableIndex, PrimaryIndex, TableIndex, TableRow, TableSecondaryIndex, TableSecondaryIndexCdc,
};
use async_trait::async_trait;
use ordered_float::OrderedFloat;
use rkyv::api::high::HighDeserializer;

#[derive(Debug)]
pub struct EmptyDataVacuum<
    Row,
    PrimaryKey,
    PkNodeType,
    SecondaryIndexes,
    AvailableTypes,
    AvailableIndexes,
    LockType,
    const DATA_LENGTH: usize,
    SecondaryEvents = (),
> where
    PrimaryKey: Clone + Ord + Send + 'static + std::hash::Hash,
    Row: StorableRow + Send + Clone + 'static,
    PkNodeType: NodeLike<Pair<PrimaryKey, OffsetEqLink<DATA_LENGTH>>> + Send + 'static,
{
    table_name: &'static str,

    data_pages: Arc<DataPages<Row, DATA_LENGTH>>,

    lock_manager: Arc<LockMap<LockType, PrimaryKey>>,

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
    LockType,
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
        LockType,
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
    <<Row as StorableRow>::WrappedRow as Archive>::Archived: ArchivedRowWrapper
        + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>
        + TableSecondaryIndexCdc<Row, AvailableTypes, SecondaryEvents, AvailableIndexes>,
    AvailableIndexes: Debug + AvailableIndex,
    LockType: RowLock,
{
    /// Creates a new [`EmptyDataVacuum`] from the given [`WorkTable`] components.
    pub fn new(
        table_name: &'static str,
        data_pages: Arc<DataPages<Row, DATA_LENGTH>>,
        lock_manager: Arc<LockMap<LockType, PrimaryKey>>,
        primary_index: Arc<PrimaryIndex<PrimaryKey, DATA_LENGTH, PkNodeType>>,
        secondary_indexes: Arc<SecondaryIndexes>,
    ) -> Self {
        Self {
            table_name,
            data_pages,
            lock_manager,
            primary_index,
            secondary_indexes,
            phantom_data: PhantomData,
        }
    }

    async fn defragment(&self) -> VacuumStats {
        let now = Instant::now();

        let registry = self.data_pages.empty_links_registry();
        let mut per_page_info = registry.get_per_page_info();
        let _registry_lock = registry.lock_vacuum().await;
        per_page_info.sort_by(|l, r| {
            OrderedFloat(l.filled_empty_ratio).cmp(&OrderedFloat(r.filled_empty_ratio))
        });
        let initial_bytes_freed: u64 = per_page_info.iter().map(|i| i.empty_bytes as u64).sum();
        let additional_allocated_page = self.data_pages.allocate_new_or_pop_free();

        let mut free_pages = VecDeque::new();
        let mut defragmented_pages = VecDeque::new();
        free_pages.push_back(additional_allocated_page.id);

        let pages_processed = per_page_info.len();

        let info_iter = per_page_info.into_iter();
        for info in info_iter {
            let page_from = info.page_id;
            loop {
                let page_to = if let Some(id) = defragmented_pages.pop_front() {
                    id
                } else if let Some(id) = free_pages.pop_front() {
                    id
                } else {
                    unreachable!("I hope so")
                };
                match self.move_data_from(page_from, page_to).await {
                    (true, true) => {
                        // from moved fully and on to no more space
                        free_pages.push_back(page_from);
                        self.free_page(page_from);
                        break;
                    }
                    (true, false) => {
                        // from moved fully but to has space
                        free_pages.push_back(page_from);
                        self.free_page(page_from);
                        defragmented_pages.push_back(page_to);
                        break;
                    }
                    (false, true) => {
                        // from was not moved but to have NO space
                        continue;
                    }
                    (false, false) => unreachable!(
                        "at least one of two situations should appear to break from while cycle"
                    ),
                }
            }
            registry.remove_link_for_page(page_from);
        }

        let pages_freed = free_pages.len();
        for id in free_pages {
            self.data_pages.mark_page_empty(id)
        }
        for id in defragmented_pages {
            self.data_pages.mark_page_full(id)
        }

        VacuumStats {
            pages_processed,
            pages_freed,
            bytes_freed: initial_bytes_freed,
            duration_ns: now.elapsed().as_nanos(),
        }
    }

    fn free_page(&self, page_id: PageId) {
        let p = self
            .data_pages
            .get_page(page_id)
            .expect("should exist as called");
        p.reset()
    }

    async fn move_data_from(&self, from: PageId, to: PageId) -> (bool, bool) {
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
            let lock = self.full_row_lock(&pk).await;
            if self
                .data_pages
                .with_ref(from_link.0, |r| r.is_deleted())
                .expect("link should be valid")
            {
                lock.unlock();
                self.lock_manager.remove_with_lock_check(&pk);
                continue;
            }
            let raw_data = from_page
                .get_raw_row(from_link.0)
                .expect("link is not bigger than free offset");
            unsafe {
                self.data_pages
                    .with_mut_ref(from_link.0, |r| r.set_in_vacuum_process())
                    .expect("link should be valid")
            }
            let new_link = to_page
                .save_raw_row(&raw_data)
                .expect("page is not full as checked on links collection");
            self.update_index_after_move(pk.clone(), from_link.0, new_link);
            self.lock_manager.remove_with_lock_check(&pk);
            lock.unlock();
        }

        (from_page_will_be_moved, to_page_will_be_filled)
    }

    async fn full_row_lock(&self, pk: &PrimaryKey) -> Arc<Lock> {
        let lock_id = self.lock_manager.next_id();
        if let Some(lock) = self.lock_manager.get(pk) {
            let mut lock_guard = lock.write().await;
            #[allow(clippy::mutable_key_type)]
            let (locks, op_lock) = lock_guard.lock(lock_id);
            drop(lock_guard);
            futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>()).await;

            op_lock
        } else {
            #[allow(clippy::mutable_key_type)]
            let (lock, op_lock) = LockType::with_lock(lock_id);
            let lock = Arc::new(tokio::sync::RwLock::new(lock));
            let mut guard = lock.write().await;
            if let Some(old_lock) = self.lock_manager.insert(pk.clone(), lock.clone()) {
                let mut old_lock_guard = old_lock.write().await;
                #[allow(clippy::mutable_key_type)]
                let locks = guard.merge(&mut *old_lock_guard);
                drop(old_lock_guard);
                drop(guard);

                futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>()).await;
            }

            op_lock
        }
    }

    fn update_index_after_move(&self, pk: PrimaryKey, old_link: Link, new_link: Link) {
        let row = self
            .data_pages
            .select(new_link)
            .expect("should exist as link was moved correctly");

        self.secondary_indexes
            .reinsert_row(row.clone(), old_link, row, new_link)
            .expect("should be ok as index were no violated");
        self.primary_index.insert(pk.clone(), new_link);
    }
}

#[async_trait]
impl<
    Row,
    PrimaryKey,
    PkNodeType,
    SecondaryIndexes,
    AvailableTypes,
    AvailableIndexes,
    LockType,
    const DATA_LENGTH: usize,
    SecondaryEvents,
> WorkTableVacuum
    for EmptyDataVacuum<
        Row,
        PrimaryKey,
        PkNodeType,
        SecondaryIndexes,
        AvailableTypes,
        AvailableIndexes,
        LockType,
        DATA_LENGTH,
        SecondaryEvents,
    >
where
    Row: TableRow<PrimaryKey> + StorableRow + Send + Sync + Clone + 'static,
    PrimaryKey: Debug + Clone + Ord + Send + Sync + TablePrimaryKey + std::hash::Hash,
    PkNodeType: NodeLike<Pair<PrimaryKey, OffsetEqLink<DATA_LENGTH>>> + Send + Sync + 'static,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
    Row: Archive
        + Clone
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    <Row as StorableRow>::WrappedRow: Archive
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        > + Send
        + Sync,
    <<Row as StorableRow>::WrappedRow as Archive>::Archived: ArchivedRowWrapper
        + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>
        + TableSecondaryIndexCdc<Row, AvailableTypes, SecondaryEvents, AvailableIndexes>
        + Send
        + Sync,
    AvailableIndexes: Debug + AvailableIndex,
    SecondaryEvents: Send + Sync + 'static,
    AvailableTypes: Send + Sync + 'static,
    AvailableIndexes: Send + Sync + 'static,
    LockType: RowLock + Send + Sync,
{
    fn table_name(&self) -> &str {
        self.table_name
    }

    fn analyze_fragmentation(&self) -> FragmentationInfo {
        let per_page_info = self.data_pages.empty_links_registry().get_per_page_info();
        FragmentationInfo::new(self.table_name, per_page_info.len(), per_page_info)
    }

    async fn vacuum(&self) -> eyre::Result<VacuumStats> {
        Ok(self.defragment().await)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use indexset::core::pair::Pair;
    use worktable_codegen::{MemStat, worktable};

    use crate::in_memory::{ArchivedRowWrapper, RowWrapper, StorableRow};
    use crate::prelude::*;
    use crate::vacuum::vacuum::EmptyDataVacuum;

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
    #[allow(clippy::type_complexity)]
    fn create_vacuum(
        table: &TestWorkTable,
    ) -> EmptyDataVacuum<
        TestRow,
        TestPrimaryKey,
        Vec<Pair<TestPrimaryKey, OffsetEqLink<TEST_INNER_SIZE>>>,
        TestIndex,
        TestAvaiableTypes,
        TestAvailableIndexes,
        TestLock,
        TEST_INNER_SIZE,
    > {
        EmptyDataVacuum::new(
            table.name(),
            Arc::clone(&table.0.data),
            Arc::clone(&table.0.lock_manager),
            Arc::clone(&table.0.primary_index),
            Arc::clone(&table.0.indexes),
        )
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

        table.delete(first_two_ids[0]).await.unwrap();
        table.delete(first_two_ids[1]).await.unwrap();

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

        table.delete(ids_to_delete[0]).await.unwrap();
        table.delete(ids_to_delete[1]).await.unwrap();

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

        table.delete(last_two_ids[1]).await.unwrap();
        table.delete(last_two_ids[0]).await.unwrap();

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
            table.delete(*id).await.unwrap();
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
            table.delete(*id).await.unwrap();
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

        table.delete(ids.last().unwrap().0).await.unwrap();

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
        let strings = [
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
            table.delete(*id).await.unwrap();
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
            table.delete(*id).await.unwrap();
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
            table.delete(*id).await.unwrap();
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
            table.delete(*id).await.unwrap();
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

        table.delete(ids.last().unwrap().0).await.unwrap();

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
            table.delete(*id).await.unwrap();
        }

        let vacuum = create_vacuum(&table);
        vacuum.defragment().await;

        assert!(!table.0.data.get_empty_pages().is_empty());

        for (id, expected) in ids
            .into_iter()
            .filter(|(id, _)| !ids_to_delete.contains(id))
        {
            let row = table.select(id);
            assert_eq!(row, Some(expected));
        }
    }
}
