use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use data_bucket::Link;
use data_bucket::page::PageId;
use rkyv::rancor::Strategy;
use rkyv::ser::Serializer;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

use crate::in_memory::{ArchivedRowWrapper, DataPages, RowWrapper, StorableRow};
use crate::lock::{Lock, LockGuard, LockMap, RowLock};
use crate::prelude::{OffsetEqLink, TablePrimaryKey};
use crate::vacuum::VacuumPersistence;
use crate::vacuum::VacuumStats;
use crate::vacuum::WorkTableVacuum;
use crate::vacuum::fragmentation_info::FragmentationInfo;
use crate::{
    AvailableIndex, PrimaryIndex, TableIndex, TableIndexCdc, TableRow, TableSecondaryIndex, TableSecondaryIndexCdc,
    UniqueIndex,
};
use async_trait::async_trait;
use ordered_float::OrderedFloat;
use rkyv::api::high::HighDeserializer;

/// Outcome of a single vacuum candidate move.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateMove {
    /// The row was moved to the destination page.
    Moved,
    /// The candidate was stale (its key moved or the row was deleted); nothing
    /// live remains in the source slot.
    Stale,
    /// The physical move failed; the live row remains on the source page, so
    /// the page must not be reclaimed.
    Failed,
}

#[derive(derive_more::Debug)]
pub struct EmptyDataVacuum<
    Row,
    PrimaryKey,
    PkMap,
    SecondaryIndexes,
    AvailableTypes,
    AvailableIndexes,
    LockType,
    const DATA_LENGTH: usize,
    SecondaryEvents = (),
> where
    PrimaryKey: Clone + Ord + Send + 'static + std::hash::Hash,
    Row: StorableRow + Send + Clone + 'static + Debug,
    PkMap: UniqueIndex<PrimaryKey, OffsetEqLink<DATA_LENGTH>>,
{
    table_name: &'static str,

    data_pages: Arc<DataPages<Row, DATA_LENGTH>>,

    lock_manager: Arc<LockMap<LockType, PrimaryKey>>,

    primary_index: Arc<PrimaryIndex<PrimaryKey, DATA_LENGTH, PkMap>>,
    secondary_indexes: Arc<SecondaryIndexes>,

    /// Persistence sink for row moves. `None` for in-memory tables; persisted
    /// tables must set it so index updates go through CDC and reach disk.
    #[debug(ignore)]
    persistence: Option<Arc<dyn VacuumPersistence<PrimaryKey, SecondaryEvents>>>,

    phantom_data: PhantomData<(SecondaryEvents, AvailableTypes, AvailableIndexes)>,
}

impl<
    Row,
    PrimaryKey,
    PkMap,
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
        PkMap,
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
    PkMap: UniqueIndex<PrimaryKey, OffsetEqLink<DATA_LENGTH>>,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
    Row: Archive
        + Clone
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>
        + Debug,
    <Row as StorableRow>::WrappedRow:
        Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
    <<Row as StorableRow>::WrappedRow as Archive>::Archived:
        ArchivedRowWrapper + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>
        + TableSecondaryIndexCdc<Row, AvailableTypes, SecondaryEvents, AvailableIndexes>,
    AvailableIndexes: Debug + AvailableIndex,
    LockType: RowLock,
    PrimaryIndex<PrimaryKey, DATA_LENGTH, PkMap>: TableIndexCdc<PrimaryKey>,
{
    /// Creates a new [`EmptyDataVacuum`] from the given `WorkTable` components.
    pub fn new(
        table_name: &'static str,
        data_pages: Arc<DataPages<Row, DATA_LENGTH>>,
        lock_manager: Arc<LockMap<LockType, PrimaryKey>>,
        primary_index: Arc<PrimaryIndex<PrimaryKey, DATA_LENGTH, PkMap>>,
        secondary_indexes: Arc<SecondaryIndexes>,
    ) -> Self {
        Self {
            table_name,
            data_pages,
            lock_manager,
            primary_index,
            secondary_indexes,
            persistence: None,
            phantom_data: PhantomData,
        }
    }

    /// Attaches a persistence sink. Index updates for moved rows then use the
    /// CDC mutation variants and their events are queued as persistence
    /// operations. Required for persisted tables.
    pub fn with_persistence(mut self, sink: Arc<dyn VacuumPersistence<PrimaryKey, SecondaryEvents>>) -> Self {
        self.persistence = Some(sink);
        self
    }

    async fn defragment(&self) -> eyre::Result<VacuumStats> {
        let now = Instant::now();

        let registry = self.data_pages.empty_links_registry();
        let mut per_page_info = registry.get_per_page_info();
        let _registry_lock = registry.lock_vacuum().await;

        per_page_info.sort_by_key(|l| OrderedFloat(l.filled_empty_ratio));
        let initial_bytes_freed: u64 = per_page_info.iter().map(|i| i.empty_bytes as u64).sum();
        let additional_allocated_page = self.data_pages.allocate_new_or_pop_free();

        let mut free_pages = VecDeque::new();
        let mut defragmented_pages = VecDeque::new();
        free_pages.push_back(additional_allocated_page.id);
        let mut pages_freed = 0;

        let pages_processed = per_page_info.len();

        let info_iter = per_page_info.into_iter();
        for info in info_iter {
            let page_from = info.page_id;
            if self.data_pages.current_page_id() == page_from {
                // don't touch current page or else inserts will be broken
                continue;
            }
            let mut source_fully_moved = false;
            loop {
                let page_to = if let Some(id) = defragmented_pages.pop_front() {
                    id
                } else if let Some(id) = free_pages.pop_front() {
                    id
                } else {
                    // A source page cannot become a destination until every
                    // reader that could still hold one of its old links has
                    // left the grace period. This call reuses it immediately
                    // when reclamation is safe, or allocates a temporary page
                    // while a pre-existing reader is still active.
                    self.data_pages.allocate_new_or_pop_free().id
                };
                // `page_from` is `mark_page_empty`'d after this loop; it must
                // never also be the destination, or the post-loop reclamation
                // would drop the rows just moved into it. (Review finding F2.)
                debug_assert_ne!(
                    page_from, page_to,
                    "vacuum destination must differ from the source being reclaimed"
                );
                let move_result = match self.move_data_from(page_from, page_to).await {
                    Ok(result) => result,
                    Err(error) => {
                        // Register every staged page before propagating, or
                        // the allocated destinations and the source tails
                        // would leak from the allocator entirely.
                        defragmented_pages.push_back(page_to);
                        if let Err(cleanup_error) = self.finalize_staged_pages(free_pages, defragmented_pages) {
                            tracing::warn!(
                                table = self.table_name,
                                %cleanup_error,
                                "failed to finalize staged vacuum pages after a move error"
                            );
                        }
                        return Err(error);
                    }
                };
                match move_result {
                    (true, true) => {
                        // from moved fully and on to no more space
                        self.data_pages.mark_page_full(page_to);
                        source_fully_moved = true;
                        break;
                    }
                    (true, false) => {
                        // from moved fully but to has space
                        defragmented_pages.push_back(page_to);
                        source_fully_moved = true;
                        break;
                    }
                    (false, true) => {
                        // from was not moved but to have NO space
                        self.data_pages.mark_page_full(page_to);
                        continue;
                    }
                    (false, false) => {
                        // A row failed to move even though the destination
                        // still has space. Keep the destination for later
                        // sources and leave this source page alone: reclaiming
                        // it would drop the live row that stayed behind.
                        defragmented_pages.push_back(page_to);
                        break;
                    }
                }
            }
            if !source_fully_moved {
                continue;
            }
            // Remove the page's empty-link fragments before reclamation can
            // expose the whole page for reuse. Otherwise a concurrent insert
            // could claim a stale fragment between retirement and cleanup.
            registry.remove_link_for_page(page_from);
            if let Some(persistence) = &self.persistence {
                // Queue the durable-free marker before publishing this page to
                // in-memory allocators. Any concurrent reuse is then ordered
                // after the marker and consumes the durable free range again.
                if let Err(error) = persistence.reclaim_pages(vec![page_from]) {
                    if let Err(cleanup_error) = self.finalize_staged_pages(free_pages, defragmented_pages) {
                        tracing::warn!(
                            table = self.table_name,
                            %cleanup_error,
                            "failed to finalize staged vacuum pages after a reclaim error"
                        );
                    }
                    return Err(error.into());
                }
            }
            self.data_pages.mark_page_empty(page_from);
            pages_freed += 1;
        }

        // Leftover free pages were allocated (or popped from the free list) by
        // vacuum itself as scratch destinations; handing them back is not
        // freeing table pages, so they do not count towards `pages_freed`.
        self.finalize_staged_pages(free_pages, defragmented_pages)?;

        Ok(VacuumStats {
            pages_processed,
            pages_freed,
            bytes_freed: initial_bytes_freed,
            duration_ns: now.elapsed().as_nanos(),
        })
    }

    /// Registers the staged destination and free pages back with the
    /// allocator. Shared by the success tail of [`Self::defragment`] and its
    /// error paths, so staged pages are never leaked: allocated destinations
    /// are marked full (their tails go to the empty-links registry) and
    /// unused free pages are reclaimed and marked empty.
    fn finalize_staged_pages(
        &self,
        free_pages: VecDeque<PageId>,
        defragmented_pages: VecDeque<PageId>,
    ) -> eyre::Result<()> {
        if let Some(persistence) = &self.persistence
            && !free_pages.is_empty()
        {
            persistence.reclaim_pages(free_pages.iter().copied().collect())?;
        }
        for id in free_pages {
            self.data_pages.mark_page_empty(id);
        }
        for id in defragmented_pages {
            self.data_pages.mark_page_full(id)
        }
        Ok(())
    }

    async fn move_data_from(&self, from: PageId, to: PageId) -> eyre::Result<(bool, bool)> {
        let to_page = self.data_pages.get_page(to).expect("should exist as link exists");
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

        let mut range = self.primary_index.reverse_pk_map.range(page_start..page_end);
        let mut sum_links_len = 0;
        let mut links = vec![];
        let mut from_page_will_be_moved = false;
        let mut to_page_will_be_filled = false;

        loop {
            let Some((next, pk)) = range.next() else {
                from_page_will_be_moved = true;
                break;
            };

            if next.page_id != from {
                continue;
            }

            if sum_links_len + next.length > to_free_space as u32 {
                // This candidate stays on the source page, so the page must
                // never be reported fully moved in this pass — even when the
                // skipped row was the last one in the range. Reporting it as
                // moved would let `defragment` reclaim the source page with a
                // live, still-indexed row on it.
                to_page_will_be_filled = true;
                break;
            }
            sum_links_len += next.length;
            links.push((next, pk));
        }

        drop(range);

        let mut any_move_failed = false;
        for (from_link, pk) in links {
            if self.move_candidate_if_current(from_link.0, pk, to).await? == CandidateMove::Failed {
                any_move_failed = true;
            }
        }
        if any_move_failed {
            // A live row stayed behind on the source page; it must not be
            // reported fully moved, or `defragment` would reclaim it.
            from_page_will_be_moved = false;
        }

        Ok((from_page_will_be_moved, to_page_will_be_filled))
    }

    /// Moves a reverse-index candidate only if it is still the forward-index
    /// location after the row lock is acquired.
    ///
    /// `move_data_from` snapshots the reverse index before taking per-row
    /// locks. A concurrent reinsert may move the key and recycle the captured
    /// slot for a different row in that interval. Revalidating under the row
    /// lock prevents vacuum from publishing that replacement row under the
    /// stale candidate's primary key.
    async fn move_candidate_if_current(
        &self,
        from_link: Link,
        pk: PrimaryKey,
        to: PageId,
    ) -> eyre::Result<CandidateMove> {
        let lock = self.full_row_lock(&pk).await;
        let _guard = LockGuard::new_with_mutation(lock, self.lock_manager.clone(), pk.clone());

        let current_link: Option<Link> = self.primary_index.pk_map.lookup_for_select(&pk).map(Into::into);
        if current_link != Some(from_link) {
            return Ok(CandidateMove::Stale);
        }

        if self
            .data_pages
            .with_ref(from_link, |r| r.is_deleted())
            .expect("a current primary-index link should be valid")
        {
            return Ok(CandidateMove::Stale);
        }

        let (raw_data, new_link) = match unsafe { self.data_pages.move_row_for_vacuum(from_link, to) } {
            Ok(moved) => moved,
            Err(error) => {
                // Leave the row in place and let the sweep continue; the
                // source page is then reported as not fully moved so it is
                // not reclaimed with this live row on it.
                tracing::warn!(
                    table = self.table_name,
                    ?pk,
                    ?from_link,
                    to_page = ?to,
                    %error,
                    "vacuum failed to move a row; skipping it"
                );
                return Ok(CandidateMove::Failed);
            }
        };
        self.update_index_after_move(pk, from_link, new_link, raw_data)?;
        self.data_pages.retire_published_link(from_link);

        Ok(CandidateMove::Moved)
    }

    async fn full_row_lock(&self, pk: &PrimaryKey) -> Arc<Lock> {
        let lock_id = self.lock_manager.next_id();
        // One atomic acquire, no check-then-act: see LockMap::get_or_insert_with.
        let lock = self.lock_manager.get_or_insert_with(pk.clone(), LockType::new);
        let mut lock_guard = lock.write().await;
        #[allow(clippy::mutable_key_type)]
        let (locks, op_lock) = lock_guard.lock(lock_id);
        drop(lock_guard);
        futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>()).await;

        op_lock
    }

    fn update_index_after_move(
        &self,
        pk: PrimaryKey,
        old_link: Link,
        new_link: Link,
        raw_data: Vec<u8>,
    ) -> eyre::Result<()> {
        let row = self
            .data_pages
            .select(new_link)
            .expect("should exist as link was moved correctly");

        if let Some(persistence) = &self.persistence {
            // Persisted table: mutate indexes through the CDC variants and queue
            // the events with the moved bytes, so the on-disk state follows the
            // move and the event-id stream stays gapless.
            let (secondary_keys_events, res) =
                self.secondary_indexes
                    .reinsert_row_cdc(row.clone(), old_link, row, new_link);
            res.expect("should be ok as index were no violated");
            let (_, primary_key_events) = self.primary_index.insert_cdc(pk.clone(), new_link);
            persistence.apply_move(raw_data, new_link, primary_key_events, secondary_keys_events)?;
        } else {
            self.secondary_indexes
                .reinsert_row(row.clone(), old_link, row, new_link)
                .expect("should be ok as index were no violated");
            self.primary_index.insert(pk.clone(), new_link);
        }
        Ok(())
    }
}

#[async_trait]
impl<
    Row,
    PrimaryKey,
    PkMap,
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
        PkMap,
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
    PkMap: UniqueIndex<PrimaryKey, OffsetEqLink<DATA_LENGTH>> + Send + Sync + 'static,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
    Row: Archive
        + Clone
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>
        + Debug,
    <Row as StorableRow>::WrappedRow: Archive
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>
        + Send
        + Sync,
    <<Row as StorableRow>::WrappedRow as Archive>::Archived:
        ArchivedRowWrapper + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>
        + TableSecondaryIndexCdc<Row, AvailableTypes, SecondaryEvents, AvailableIndexes>
        + Send
        + Sync,
    AvailableIndexes: Debug + AvailableIndex,
    SecondaryEvents: Send + Sync + 'static,
    AvailableTypes: Send + Sync + 'static,
    AvailableIndexes: Send + Sync + 'static,
    LockType: RowLock + Send + Sync,
    PrimaryIndex<PrimaryKey, DATA_LENGTH, PkMap>: TableIndexCdc<PrimaryKey>,
{
    fn table_name(&self) -> &str {
        self.table_name
    }

    fn analyze_fragmentation(&self) -> FragmentationInfo {
        let per_page_info = self.data_pages.empty_links_registry().get_per_page_info();
        FragmentationInfo::new(self.table_name, per_page_info.len(), per_page_info)
    }

    async fn vacuum(&self) -> eyre::Result<VacuumStats> {
        self.defragment().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use data_bucket::Link;
    use data_bucket::page::PageId;
    use worktable_codegen::{MemStat, worktable};

    use crate::in_memory::{ArchivedRowWrapper, RowWrapper, StorableRow};
    use crate::prelude::*;
    use crate::vacuum::vacuum::{CandidateMove, EmptyDataVacuum};

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
        IndexMap<TestPrimaryKey, OffsetEqLink<TEST_INNER_SIZE>>,
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
        vacuum.defragment().await.unwrap();

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
        vacuum.defragment().await.unwrap();

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
        vacuum.defragment().await.unwrap();

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
        vacuum.defragment().await.unwrap();

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
        vacuum.defragment().await.unwrap();

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
        vacuum.defragment().await.unwrap();

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
        vacuum.defragment().await.unwrap();

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
        vacuum.defragment().await.unwrap();

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

        for (id, expected) in original_ids.into_iter().filter(|(i, _)| !ids_to_delete.contains(i)) {
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
        vacuum.defragment().await.unwrap();

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
        vacuum.defragment().await.unwrap();

        for (id, expected) in ids.into_iter().filter(|(id, _)| !ids_to_delete.contains(id)) {
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
        vacuum.defragment().await.unwrap();

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
        vacuum.defragment().await.unwrap();

        assert!(!table.0.data.get_empty_pages().is_empty());

        for (id, expected) in ids.into_iter().filter(|(id, _)| !ids_to_delete.contains(id)) {
            let row = table.select(id);
            assert_eq!(row, Some(expected));
        }
    }

    #[tokio::test]
    async fn vacuum_does_not_reuse_source_pages_during_a_read_grace_period() {
        let table = TestWorkTable::default();
        let mut rows_by_page: HashMap<PageId, Vec<(u64, TestRow, Link)>> = HashMap::new();

        // Two large rows fit on each page. Deleting one from many pages leaves
        // enough fragmented source pages that the old vacuum implementation
        // reset and recycled an earlier source as a later destination.
        for i in 0..40u64 {
            let row = TestRow {
                id: table.get_next_pk().into(),
                test: i as i64,
                another: i,
                exchange: format!("{i:02}-{}", "x".repeat(6_000)),
            };
            let id = row.id;
            table.insert(row.clone()).unwrap();
            let link = table
                .0
                .primary_index
                .pk_map
                .get_value(&TestPrimaryKey::from(id))
                .unwrap()
                .0;
            rows_by_page.entry(link.page_id).or_default().push((id, row, link));
        }

        let current_page = table.0.data.current_page_id();
        let mut protected_rows = Vec::new();
        for (page_id, rows) in rows_by_page {
            if page_id == current_page || rows.len() < 2 {
                continue;
            }

            protected_rows.push(rows[0].clone());
            for (id, _, _) in rows.into_iter().skip(1) {
                table.delete(id).await.unwrap();
            }
        }
        assert!(
            protected_rows.len() >= 3,
            "test setup needs several fragmented source pages"
        );

        // Model a generated reader that already resolved each old physical
        // link, then pauses while vacuum swings the indexes.
        let read_guard = table.0.data.read_guard();
        create_vacuum(&table).defragment().await.unwrap();

        let mut moved = 0;
        for (id, expected, old_link) in &protected_rows {
            let current_link = table
                .0
                .primary_index
                .pk_map
                .get_value(&TestPrimaryKey::from(*id))
                .unwrap()
                .0;
            moved += usize::from(current_link != *old_link);
            assert_eq!(
                table.0.data.select_non_ghosted(*old_link),
                Ok(expected.clone()),
                "a retired source link was reset or republished before the reader left"
            );
        }
        assert!(moved >= 3, "test setup did not exercise enough vacuum moves");

        drop(read_guard);
    }

    #[tokio::test]
    async fn vacuum_stats_do_not_count_vacuums_own_scratch_pages_as_freed() {
        let table = TestWorkTable::default();
        for i in 0..10 {
            let row = TestRow {
                id: table.get_next_pk().into(),
                test: i,
                another: i as u64,
                exchange: format!("test{}", i),
            };
            table.insert(row).unwrap();
        }

        // Nothing was deleted, so there is nothing to free: the page vacuum
        // allocates for itself must not be reported as a freed page.
        let stats = create_vacuum(&table).defragment().await.unwrap();
        assert_eq!(
            stats.pages_freed, 0,
            "vacuum's own scratch allocation must not count as freed"
        );
    }

    #[tokio::test]
    async fn move_data_from_keeps_source_unmoved_when_its_last_row_is_skipped_for_space() {
        let table = TestWorkTable::default();

        // Two large rows fit on each page.
        let mut rows = Vec::new();
        for i in 0..4u64 {
            let row = TestRow {
                id: table.get_next_pk().into(),
                test: i as i64,
                another: i,
                exchange: format!("{i:02}-{}", "x".repeat(6_000)),
            };
            table.insert(row.clone()).unwrap();
            rows.push(row);
        }

        let link_of = |id: u64| {
            table
                .0
                .primary_index
                .pk_map
                .get_value(&TestPrimaryKey::from(id))
                .unwrap()
                .0
        };
        let source_page = link_of(rows[0].id).page_id;
        let dest_page = link_of(rows[2].id).page_id;
        assert_ne!(source_page, dest_page, "test setup needs two distinct pages");

        // Leave exactly one live row on the source page. The destination still
        // holds two large rows, so its remaining free space cannot fit that
        // survivor: the last (and only) candidate fails the free-space check.
        table.delete(rows[1].id).await.unwrap();

        let vacuum = create_vacuum(&table);
        let (from_moved, to_filled) = vacuum.move_data_from(source_page, dest_page).await.unwrap();

        assert!(to_filled, "destination must be reported out of space");
        assert!(
            !from_moved,
            "a source page whose live row was skipped for space must not be reported fully moved, \
             or defragment would reclaim it with the row still indexed"
        );
        assert_eq!(table.select(rows[0].id), Some(rows[0].clone()));
        assert_eq!(
            link_of(rows[0].id).page_id,
            source_page,
            "the skipped row must stay in place"
        );
    }

    #[tokio::test]
    async fn a_failed_row_move_is_skipped_and_leaves_the_row_in_place() {
        let table = TestWorkTable::default();

        // Two large rows fit on each page.
        let mut rows = Vec::new();
        for i in 0..4u64 {
            let row = TestRow {
                id: table.get_next_pk().into(),
                test: i as i64,
                another: i,
                exchange: format!("{i:02}-{}", "x".repeat(6_000)),
            };
            table.insert(row.clone()).unwrap();
            rows.push(row);
        }

        let link_of = |id: u64| {
            table
                .0
                .primary_index
                .pk_map
                .get_value(&TestPrimaryKey::from(id))
                .unwrap()
                .0
        };
        let source_link = link_of(rows[0].id);
        let dest_page = link_of(rows[2].id).page_id;
        assert_ne!(source_link.page_id, dest_page, "test setup needs two distinct pages");

        // The destination holds two large rows and cannot fit another one, so
        // the physical move must fail. The failure has to be reported (not
        // panic) and the row must stay in place, still selectable.
        let outcome = create_vacuum(&table)
            .move_candidate_if_current(source_link, TestPrimaryKey::from(rows[0].id), dest_page)
            .await
            .unwrap();

        assert_eq!(outcome, CandidateMove::Failed);
        assert_eq!(table.select(rows[0].id), Some(rows[0].clone()));
        assert_eq!(
            link_of(rows[0].id),
            source_link,
            "a failed move must leave the row in place"
        );
    }

    #[tokio::test]
    async fn vacuum_skips_a_stale_candidate_after_its_link_is_reused() {
        let table = TestWorkTable::default();
        let target = TestRow {
            id: table.get_next_pk().into(),
            test: 10,
            another: 10,
            exchange: "target00".to_string(),
        };
        let target_id = target.id;
        table.insert(target).unwrap();

        // Model the reverse-index snapshot taken before vacuum waits for the
        // row lock.
        let stale_link = table
            .0
            .primary_index
            .pk_map
            .get_value(&TestPrimaryKey::from(target_id))
            .unwrap()
            .0;

        // A same-sized reinsert moves the target and retires its old slot.
        let updated_target = TestRow {
            id: target_id,
            test: 11,
            another: 11,
            exchange: "updated0".to_string(),
        };
        table.update(updated_target.clone()).await.unwrap();
        let current_target_link = table
            .0
            .primary_index
            .pk_map
            .get_value(&TestPrimaryKey::from(target_id))
            .unwrap()
            .0;
        assert_ne!(current_target_link, stale_link);

        // Reuse the retired physical slot for a different row. Without the
        // post-lock forward-index check, vacuum would move this row and bind
        // it to `target_id`.
        let replacement = TestRow {
            id: table.get_next_pk().into(),
            test: 12,
            another: 12,
            exchange: "reused00".to_string(),
        };
        let replacement_id = replacement.id;
        table.insert(replacement.clone()).unwrap();
        let replacement_link = table
            .0
            .primary_index
            .pk_map
            .get_value(&TestPrimaryKey::from(replacement_id))
            .unwrap()
            .0;
        assert_eq!(replacement_link, stale_link, "test setup must recycle the stale slot");

        let destination = table.0.data.allocate_new_or_pop_free().id;
        let moved = create_vacuum(&table)
            .move_candidate_if_current(stale_link, TestPrimaryKey::from(target_id), destination)
            .await
            .unwrap();

        assert_eq!(
            moved,
            CandidateMove::Stale,
            "vacuum must reject a candidate whose key moved"
        );
        assert_eq!(table.select(target_id), Some(updated_target));
        assert_eq!(table.select(replacement_id), Some(replacement));
    }
}
