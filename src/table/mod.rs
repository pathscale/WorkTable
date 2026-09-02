pub mod select;
pub mod system_info;
pub mod vacuum;

use crate::in_memory::{ArchivedRowWrapper, DataPages, RowWrapper, StorableRow};
use crate::persistence::{AcknowledgeOperation, InsertOperation, Operation, PersistenceLoadError};
use crate::prelude::{Link, LockMap, OperationId, PrimaryKeyGeneratorState};
use crate::primary_key::{PrimaryKeyGenerator, TablePrimaryKey};
use crate::util::OffsetEqLink;
use crate::{
    AvailableIndex, IndexError, IndexMap, PrimaryIndex, TableIndex, TableIndexCdc, TableRow, TableSecondaryIndex,
    TableSecondaryIndexCdc, TableSecondaryIndexEventsOps, UniqueIndex, convert_change_events, in_memory,
};
use data_bucket::INNER_PAGE_SIZE;
use derive_more::{Display, Error, From};
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
#[cfg(feature = "perf_measurements")]
use performance_measurement_codegen::performance_measurement;
use rkyv::api::high::HighDeserializer;
use rkyv::rancor::Strategy;
use rkyv::ser::Serializer;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Portable, Serialize};
use std::collections::HashSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug)]
pub struct WorkTable<
    Row,
    PrimaryKey,
    AvailableTypes = (),
    AvailableIndexes = (),
    SecondaryIndexes = (),
    LockType = (),
    PkGen = <PrimaryKey as TablePrimaryKey>::Generator,
    const DATA_LENGTH: usize = INNER_PAGE_SIZE,
    PkMap = IndexMap<PrimaryKey, OffsetEqLink<DATA_LENGTH>>,
> where
    PrimaryKey: Clone + Ord + Send + 'static + std::hash::Hash,
    Row: StorableRow + Send + Clone + 'static,
    PkMap: crate::UniqueIndex<PrimaryKey, OffsetEqLink<DATA_LENGTH>>,
{
    pub data: Arc<DataPages<Row, DATA_LENGTH>>,

    pub primary_index: Arc<PrimaryIndex<PrimaryKey, DATA_LENGTH, PkMap>>,

    pub indexes: Arc<SecondaryIndexes>,

    pub pk_gen: PkGen,

    pub lock_manager: Arc<LockMap<LockType, PrimaryKey>>,

    pub table_name: &'static str,

    pub pk_phantom: PhantomData<(AvailableTypes, AvailableIndexes)>,
}

// Manual implementations to avoid unneeded trait bounds.
impl<
    Row,
    PrimaryKey,
    AvailableTypes,
    AvailableIndexes,
    SecondaryIndexes,
    LockType,
    PkGen,
    const DATA_LENGTH: usize,
    PkMap,
> Default
    for WorkTable<
        Row,
        PrimaryKey,
        AvailableTypes,
        AvailableIndexes,
        SecondaryIndexes,
        LockType,
        PkGen,
        DATA_LENGTH,
        PkMap,
    >
where
    PrimaryKey: Debug + Clone + Ord + Send + TablePrimaryKey + std::hash::Hash,
    SecondaryIndexes: Default,
    PkGen: Default,
    PkMap: crate::UniqueIndex<PrimaryKey, OffsetEqLink<DATA_LENGTH>>,
    Row: StorableRow + Send + Clone + 'static,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
{
    fn default() -> Self {
        Self {
            data: Arc::new(DataPages::new()),
            primary_index: Arc::new(PrimaryIndex::<PrimaryKey, DATA_LENGTH, PkMap>::default()),
            indexes: Arc::new(SecondaryIndexes::default()),
            pk_gen: Default::default(),
            lock_manager: Default::default(),
            table_name: "",
            pk_phantom: PhantomData,
        }
    }
}

impl<
    Row,
    PrimaryKey,
    AvailableTypes,
    AvailableIndexes,
    SecondaryIndexes,
    LockType,
    PkGen,
    const DATA_LENGTH: usize,
    PkMap,
> WorkTable<Row, PrimaryKey, AvailableTypes, AvailableIndexes, SecondaryIndexes, LockType, PkGen, DATA_LENGTH, PkMap>
where
    Row: TableRow<PrimaryKey>,
    PrimaryKey: Debug + Clone + Ord + Send + TablePrimaryKey + std::hash::Hash,
    PkMap: crate::UniqueIndex<PrimaryKey, OffsetEqLink<DATA_LENGTH>>,
    Row: StorableRow + Send + Clone + 'static,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
{
    /// Audits the persisted primary-index/data boundary before a table is made
    /// available to callers.
    ///
    /// This load-only scan prevents a torn index link from turning zeroed or
    /// unrelated bytes into a plausible row. It deliberately does not run on
    /// steady-state operations.
    pub fn validate_persisted_state(&self, path: impl AsRef<Path>) -> Result<(), PersistenceLoadError>
    where
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>
            + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    {
        let path = path.as_ref();
        let mut links = HashSet::with_capacity(self.primary_index.pk_map.len());

        for (primary_key, offset_link) in self.primary_index.pk_map.iter_values() {
            if !links.insert(offset_link) {
                return Err(PersistenceLoadError::corrupt(
                    path,
                    format!("multiple primary keys reference physical link {:?}", offset_link.0),
                ));
            }

            let row = self.data.select_non_ghosted_checked(offset_link.0).map_err(|error| {
                PersistenceLoadError::corrupt(
                    path,
                    format!("primary key {primary_key:?} references an invalid row: {error}"),
                )
            })?;
            if row.get_primary_key() != primary_key {
                return Err(PersistenceLoadError::corrupt(
                    path,
                    format!("row at {:?} does not match primary key {primary_key:?}", offset_link.0),
                ));
            }

            let reverse_key = self.primary_index.reverse_pk_map.get_value(&offset_link);
            if reverse_key.as_ref() != Some(&primary_key) {
                return Err(PersistenceLoadError::corrupt(
                    path,
                    format!("reverse primary index does not match link {:?}", offset_link.0),
                ));
            }
        }

        if self.primary_index.reverse_pk_map.len() != links.len() {
            return Err(PersistenceLoadError::corrupt(
                path,
                "forward and reverse primary indexes contain different numbers of entries",
            ));
        }

        for (offset_link, primary_key) in self.primary_index.reverse_pk_map.iter_values() {
            if self.primary_index.pk_map.get_value(&primary_key) != Some(offset_link) {
                return Err(PersistenceLoadError::corrupt(
                    path,
                    format!("forward primary index does not match link {:?}", offset_link.0),
                ));
            }
        }

        Ok(())
    }

    pub fn get_next_pk(&self) -> PrimaryKey
    where
        PkGen: PrimaryKeyGenerator<PrimaryKey>,
    {
        self.pk_gen.next()
    }

    /// Reserves `count` consecutive primary keys on autoincrement tables.
    ///
    /// The returned half-open range is expressed in the raw column type so a
    /// caller can iterate it directly while pre-assigning contiguous keys to a
    /// batch of rows for `insert_many`. Interleaved [`Self::get_next_pk`]
    /// calls keep working and never overlap a reservation.
    pub fn reserve_pks<Raw>(&self, count: usize) -> std::ops::Range<Raw>
    where
        PkGen: crate::primary_key::PrimaryKeyGeneratorRange<Raw>,
    {
        self.pk_gen.reserve(count)
    }

    /// Selects `Row` from table identified with provided primary key. Returns `None` if no value presented.
    #[cfg_attr(feature = "perf_measurements", performance_measurement(prefix_name = "WorkTable"))]
    pub fn select(&self, pk: PrimaryKey) -> Option<Row>
    where
        LockType: 'static,
        Row: Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived:
            Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        let _read_guard = self.data.read_guard();
        for _ in 0..64 {
            let link = self.primary_index.pk_map.lookup_for_select(&pk).map(Into::into)?;
            if let Ok(row) = self.data.select_non_ghosted(link) {
                return Some(row);
            }

            let current_link: Option<Link> = self.primary_index.pk_map.lookup_for_select(&pk).map(Into::into);
            if current_link == Some(link) {
                return None;
            }
            std::hint::spin_loop();
        }
        None
    }

    #[cfg_attr(feature = "perf_measurements", performance_measurement(prefix_name = "WorkTable"))]
    pub fn insert(&self, row: Row) -> Result<PrimaryKey, WorkTableError>
    where
        Row: Archive
            + Clone
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <Row as StorableRow>::WrappedRow:
            Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: ArchivedRowWrapper
            + Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
        PrimaryKey: Clone,
        AvailableTypes: 'static,
        AvailableIndexes: AvailableIndex,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>,
        LockType: 'static,
    {
        let pk = row.get_primary_key().clone();
        let _mutation_guard = self.lock_manager.mutation_guard(&pk);
        self.insert_locked(row)
    }

    /// `insert`, for a caller already holding this key's mutation gate.
    ///
    /// `upsert` needs this. It takes the full row lock, finds the key absent,
    /// and wants to insert without letting go: releasing first is what opens a
    /// window another writer can win, which is why that path carries a retry
    /// loop and a backoff. It could not hold on, because `insert` re-acquires
    /// the same per-key gate the guard already holds and would deadlock
    /// against itself.
    ///
    /// Splitting the acquisition off is the whole fix, and it is the shape
    /// `update_with_guard` already uses. Making `insert` async does not help
    /// here and makes it worse: the drop-to-insert window would then contain
    /// an await, so a task can be descheduled inside it and the race widens
    /// from a few hundred nanoseconds of straight-line code to a scheduling
    /// quantum.
    ///
    /// # Correctness
    ///
    /// The caller must already hold the mutation gate for this row's primary
    /// key. Calling it without one drops the serialisation every other write
    /// on this table has.
    pub fn insert_locked(&self, row: Row) -> Result<PrimaryKey, WorkTableError>
    where
        Row: Archive
            + Clone
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <Row as StorableRow>::WrappedRow:
            Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: ArchivedRowWrapper
            + Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
        PrimaryKey: Clone,
        AvailableTypes: 'static,
        AvailableIndexes: AvailableIndex,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>,
        LockType: 'static,
    {
        let pk = row.get_primary_key().clone();
        let link = self.data.insert(row.clone()).map_err(WorkTableError::PagesError)?;
        if self.primary_index.insert_checked(pk.clone(), link).is_none() {
            self.data.delete(link).map_err(WorkTableError::PagesError)?;
            return Err(WorkTableError::PrimaryAlreadyExists);
        };
        if let Err(e) = self.indexes.save_row(row.clone(), link) {
            return match e {
                IndexError::AlreadyExists { at, inserted_already } => {
                    self.primary_index.remove(&pk, link);
                    self.indexes.delete_from_indexes(row, link, inserted_already)?;
                    self.data.delete(link).map_err(WorkTableError::PagesError)?;

                    Err(WorkTableError::AlreadyExists(at.to_string_value()))
                }
                IndexError::NotFound => {
                    // Mirror the AlreadyExists arm. Returning without rollback
                    // left the primary key permanently bound to a ghosted row
                    // and partial secondary entries behind. `NotFound` carries
                    // no list of touched indexes, so unwind by the row's own
                    // keys: every entry save_row can have inserted holds this
                    // row's values and link.
                    self.primary_index.remove(&pk, link);
                    self.indexes.delete_row(row, link)?;
                    self.data.delete(link).map_err(WorkTableError::PagesError)?;

                    Err(WorkTableError::NotFound)
                }
            };
        }
        unsafe {
            self.data
                .with_mut_ref(link, |r| r.unghost())
                .map_err(WorkTableError::PagesError)?
        }

        Ok(pk)
    }

    /// Deletes every row named by `pks`, ghosting them behind one grace marker.
    ///
    /// Deleting is already a bit flip rather than a move: the row is marked
    /// deleted in place, its index entries are removed, and its storage becomes
    /// reusable once no reader can still reach it. Vacuum is what later
    /// compacts the pages and hands whole ones back. This batches that, and the
    /// batching is worth having for one specific reason: the per-row cost is
    /// dominated by the domain advance each retirement takes, not by the bit
    /// flip, so `n` deletes cost `n` advances where a batch costs one.
    ///
    /// Unlike [`Self::insert_many`] this is **not** all-or-nothing, and the
    /// difference is deliberate. A rejected insert has published nothing, so
    /// unwinding restores a state that was real. A delete that fails partway
    /// has already removed index entries and ghosted rows, and those rows are
    /// genuinely gone; resurrecting them would mean re-publishing index entries
    /// for storage that is queued for reuse. So the batch reports what it
    /// deleted and stops at the first failure, rather than pretending it can
    /// rewind.
    ///
    /// A primary key that is not present is skipped rather than failing the
    /// batch. Callers evicting a generation do not generally know which of its
    /// keys a concurrent writer has already removed, and making them find out
    /// first would be a race they cannot win.
    ///
    /// Returns the keys actually deleted, in the order given.
    pub fn delete_many(&self, pks: Vec<PrimaryKey>) -> Result<Vec<PrimaryKey>, BatchDeleteError<PrimaryKey>>
    where
        Row: Archive
            + Clone
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <Row as StorableRow>::WrappedRow:
            Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: ArchivedRowWrapper
            + Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
        PrimaryKey: Clone,
        AvailableTypes: 'static,
        AvailableIndexes: AvailableIndex,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>,
        LockType: 'static,
    {
        if pks.is_empty() {
            return Ok(Vec::new());
        }
        // Stripe-ordered, exactly as `insert_many` takes them, so a batch
        // delete and a batch insert cannot deadlock against each other.
        let _mutation_guards = self.lock_manager.mutation_guards(pks.iter());

        let mut deleted: Vec<PrimaryKey> = Vec::with_capacity(pks.len());
        let mut links: Vec<Link> = Vec::with_capacity(pks.len());

        for pk in &pks {
            let Some(link) = self.primary_index.pk_map.get_value(pk).map(Into::into) else {
                // Already gone. Not an error: see above.
                continue;
            };
            // Read at the link rather than by key: the link is already in
            // hand, and `select` would spend a second primary-key lookup to
            // find it again. `select_non_ghosted` keeps the check that matters,
            // which is that low-level staged or hydrated state can publish
            // index reachability before clearing a row's ghost bit.
            let Ok(row) = self.data.select_non_ghosted(link) else {
                continue;
            };

            // Index removals run BEFORE the rows are ghosted, and for the same
            // reason the single-row path gives: insert publishes data first and
            // indexes second, so tearing down in the reverse order guarantees
            // no index entry ever resolves to storage that has been freed or
            // reused.
            if let Err(source) = self.indexes.delete_row(row, link) {
                // Finish the rows that already succeeded before leaving.
                //
                // Each of them is out of every index but not yet ghosted,
                // because ghosting is deferred to one pass below. Returning
                // here without it would leave them allocated, live in the page
                // layer, and unreachable through any index: leaked for the
                // lifetime of the table, and contradicting the documented
                // promise that a failed batch's earlier rows are genuinely
                // gone and queued for reuse.
                //
                // A failure here is reported in preference to a failure in the
                // cleanup: the caller's key is the more useful diagnosis, and
                // the cleanup failing means the pages were already unusable.
                let _ = self.data.delete_many(&links);
                return Err(BatchDeleteError::Key {
                    key: pk.clone(),
                    deleted: deleted.len(),
                    source: WorkTableError::from(source),
                });
            }
            self.primary_index.remove(pk, link);
            links.push(link);
            deleted.push(pk.clone());
        }

        // One ghosting pass, one grace marker, one reclaim.
        if let Err(source) = self.data.delete_many(&links) {
            return Err(BatchDeleteError::Table(WorkTableError::PagesError(source)));
        }

        Ok(deleted)
    }

    /// Deletes every row whose primary key falls in `range`.
    ///
    /// The shape bulk eviction actually has. A caller dropping a generation
    /// knows the span it wants gone, not the individual keys, and making them
    /// enumerate the span first means walking the primary index by hand and
    /// then handing the result straight back.
    ///
    /// The span is collected from the primary index in one ordered walk, and
    /// the delete then runs exactly as [`Self::delete_many`]: keys are still
    /// resolved under their mutation guards, because the set can change between
    /// the walk and the delete and a key that has since gone is skipped rather
    /// than failing the batch. So this is one walk instead of the caller's, not
    /// a way to skip the per-key work that keeps the delete correct.
    ///
    /// Returns the keys actually deleted, in key order.
    pub fn delete_range<R>(&self, range: R) -> Result<Vec<PrimaryKey>, BatchDeleteError<PrimaryKey>>
    where
        R: std::ops::RangeBounds<PrimaryKey>,
        Row: Archive
            + Clone
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <Row as StorableRow>::WrappedRow:
            Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: ArchivedRowWrapper
            + Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
        PrimaryKey: Clone,
        AvailableTypes: 'static,
        AvailableIndexes: AvailableIndex,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>,
        LockType: 'static,
    {
        // Owned bounds, because the span is walked twice and `R` is consumed.
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();

        // First walk: learn which keys the span holds. Nothing is trusted from
        // this pass except the key set, which is what the guards are for.
        let keys: Vec<PrimaryKey> = self
            .primary_index
            .pk_map
            .range_values((start.clone(), end.clone()))
            .map(|(key, _)| key)
            .collect();
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let _mutation_guards = self.lock_manager.mutation_guards(keys.iter());

        // Second walk, under the guards. Links for guarded keys cannot move
        // and are used directly; keys that appeared since the first walk are
        // filtered out below, because no guard covers them. This is the whole
        // point: `k` individual lookups cost
        // `k` times `O(log n)` in table size, one walk costs `O(log n + k)`, so
        // the saving grows with both the batch and the table. A first version
        // walked and then looked every key up again, which is strictly more
        // work than `delete_many` and gave this no reason to exist.
        let pinned: Vec<(PrimaryKey, Link)> = self
            .primary_index
            .pk_map
            .range_values((start, end))
            .map(|(key, link)| (key, link.into()))
            .collect();

        let mut deleted: Vec<PrimaryKey> = Vec::with_capacity(pinned.len());
        let mut links: Vec<Link> = Vec::with_capacity(pinned.len());
        for (key, link) in pinned {
            // Only keys the guards actually cover.
            //
            // The guards were taken over the first walk's key set. A key
            // inserted into the span between the two walks appears in this one
            // while hashing to a stripe nobody locked, so deleting it would
            // tear down its indexes and storage with none of the mutation
            // serialisation every other write on this table has, racing the
            // publication that is still in flight.
            //
            // `keys` came from an ordered walk, so it is sorted and this is a
            // binary search rather than a set allocation.
            if keys.binary_search(&key).is_err() {
                continue;
            }
            // Read at the link, as above: the walk already produced it.
            let Ok(row) = self.data.select_non_ghosted(link) else {
                continue;
            };
            if let Err(source) = self.indexes.delete_row(row, link) {
                // As in `delete_many`: ghost what already succeeded rather
                // than leaving those rows out of every index and still
                // allocated.
                let _ = self.data.delete_many(&links);
                return Err(BatchDeleteError::Key {
                    key,
                    deleted: deleted.len(),
                    source: WorkTableError::from(source),
                });
            }
            self.primary_index.remove(&key, link);
            links.push(link);
            deleted.push(key);
        }

        if let Err(source) = self.data.delete_many(&links) {
            return Err(BatchDeleteError::Table(WorkTableError::PagesError(source)));
        }

        Ok(deleted)
    }

    /// Inserts every row of `rows`, all or nothing.
    ///
    /// Rows are first staged ghosted (invisible to lock-free readers), every
    /// primary and secondary index check runs while they are still invisible,
    /// and only a fully validated batch is made visible. Any rejected row — a
    /// primary key duplicate, a unique collision on any index (including with
    /// another row of the same batch), or a page error — rejects the whole
    /// batch and unwinds the staged rows, their index entries and the primary
    /// key map without a value ever having been exposed.
    ///
    /// After `Ok`, every row is visible to reads.
    pub fn insert_many(&self, rows: Vec<Row>) -> Result<Vec<PrimaryKey>, BatchInsertError>
    where
        Row: Archive
            + Clone
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <Row as StorableRow>::WrappedRow:
            Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: ArchivedRowWrapper
            + Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
        PrimaryKey: Clone,
        AvailableTypes: 'static,
        AvailableIndexes: AvailableIndex,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>,
        LockType: 'static,
    {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let pks: Vec<PrimaryKey> = rows.iter().map(|row| row.get_primary_key().clone()).collect();
        let _mutation_guards = self.lock_manager.mutation_guards(pks.iter());

        let mut links: Vec<Link> = Vec::with_capacity(rows.len());
        for (row_index, row) in rows.iter().enumerate() {
            let source = match self.stage_batch_row(row, &pks[row_index]) {
                Ok(link) => {
                    links.push(link);
                    continue;
                }
                Err(source) => source,
            };
            return Err(match self.unwind_batch_rows(&rows, &pks, &links) {
                Ok(()) => BatchInsertError::Row { row_index, source },
                // An unwind failure leaves no single offending row to name.
                Err(unwind_error) => BatchInsertError::Table(unwind_error),
            });
        }

        for link in links.iter() {
            let unghosted = unsafe { self.data.with_mut_ref(*link, |r| r.unghost()) };
            if let Err(e) = unghosted {
                // Practically unreachable: the link was created above. Rows
                // before this one were already made visible, so unwinding
                // everything deletes them again rather than leaving a torn
                // batch behind.
                let _ = self.unwind_batch_rows(&rows, &pks, &links);
                return Err(BatchInsertError::Table(WorkTableError::PagesError(e)));
            }
        }

        Ok(pks)
    }

    /// Stages one batch row ghosted: data slot, primary key mapping and every
    /// secondary index entry. On rejection its own partial state is unwound
    /// and the row-level error is returned.
    fn stage_batch_row(&self, row: &Row, pk: &PrimaryKey) -> Result<Link, WorkTableError>
    where
        Row: Archive
            + Clone
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <Row as StorableRow>::WrappedRow:
            Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: ArchivedRowWrapper
            + Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
        PrimaryKey: Clone,
        AvailableTypes: 'static,
        AvailableIndexes: AvailableIndex,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>,
    {
        let link = self.data.insert(row.clone()).map_err(WorkTableError::PagesError)?;
        if self.primary_index.insert_checked(pk.clone(), link).is_none() {
            self.data.delete(link).map_err(WorkTableError::PagesError)?;
            return Err(WorkTableError::PrimaryAlreadyExists);
        }
        if let Err(e) = self.indexes.save_row(row.clone(), link) {
            return match e {
                IndexError::AlreadyExists { at, inserted_already } => {
                    self.primary_index.remove(pk, link);
                    self.indexes.delete_from_indexes(row.clone(), link, inserted_already)?;
                    self.data.delete(link).map_err(WorkTableError::PagesError)?;
                    Err(WorkTableError::AlreadyExists(at.to_string_value()))
                }
                IndexError::NotFound => {
                    self.primary_index.remove(pk, link);
                    self.indexes.delete_row(row.clone(), link)?;
                    self.data.delete(link).map_err(WorkTableError::PagesError)?;
                    Err(WorkTableError::NotFound)
                }
            };
        }
        Ok(link)
    }

    /// Unwinds fully staged batch rows, newest first, in the single-row
    /// rollback order: index entries and primary key mapping before the data
    /// slot, so a link is never reusable while an index entry can still
    /// resolve to it. Keeps unwinding on error and reports the first one.
    fn unwind_batch_rows(&self, rows: &[Row], pks: &[PrimaryKey], links: &[Link]) -> Result<(), WorkTableError>
    where
        Row: Archive
            + Clone
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <Row as StorableRow>::WrappedRow:
            Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: ArchivedRowWrapper
            + Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
        PrimaryKey: Clone,
        AvailableTypes: 'static,
        AvailableIndexes: AvailableIndex,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>,
    {
        let mut first_error: Option<WorkTableError> = None;
        for index in (0..links.len()).rev() {
            let link = links[index];
            self.primary_index.remove(&pks[index], link);
            if let Err(e) = self.indexes.delete_row(rows[index].clone(), link) {
                if first_error.is_none() {
                    first_error = Some(e.into());
                }
                continue;
            }
            if let Err(e) = self.data.delete(link)
                && first_error.is_none()
            {
                first_error = Some(WorkTableError::PagesError(e));
            }
        }
        match first_error {
            None => Ok(()),
            Some(error) => Err(error),
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn insert_cdc<SecondaryEvents>(
        &self,
        row: Row,
    ) -> (
        Option<Operation<<PkGen as PrimaryKeyGeneratorState>::State, PrimaryKey, SecondaryEvents>>,
        Result<PrimaryKey, WorkTableError>,
    )
    where
        Row: Archive
            + Clone
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <Row as StorableRow>::WrappedRow:
            Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: ArchivedRowWrapper
            + Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
        PrimaryKey: Clone,
        SecondaryEvents: Debug + Default + Clone + TableSecondaryIndexEventsOps<AvailableIndexes>,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>
            + TableSecondaryIndexCdc<Row, AvailableTypes, SecondaryEvents, AvailableIndexes>,
        PkGen: PrimaryKeyGeneratorState,
        <PkGen as PrimaryKeyGeneratorState>::State: Debug,
        AvailableIndexes: Debug + AvailableIndex,
        PrimaryIndex<PrimaryKey, DATA_LENGTH, PkMap>: TableIndexCdc<PrimaryKey>,
    {
        let pk = row.get_primary_key().clone();
        let _mutation_guard = self.lock_manager.mutation_guard(&pk);

        let (link, _) = match self.data.insert_cdc(row.clone()) {
            Ok(result) => result,
            Err(e) => return (None, Err(WorkTableError::PagesError(e))),
        };

        let primary_key_events = self.primary_index.insert_checked_cdc(pk.clone(), link);
        let Some(primary_key_events) = primary_key_events else {
            if let Err(e) = self.data.delete(link) {
                return (None, Err(WorkTableError::PagesError(e)));
            }
            return (None, Err(WorkTableError::PrimaryAlreadyExists));
        };
        let primary_key_events = convert_change_events(primary_key_events);

        let (secondary_events, indexes_res) = self.indexes.save_row_cdc(row.clone(), link);
        if let Err(e) = indexes_res {
            let (ack_op, error) = match e {
                IndexError::AlreadyExists { at, inserted_already } => {
                    let (_, rollback_pk_events) = self.primary_index.remove_cdc(pk.clone(), link);
                    let rollback_pk_events = convert_change_events(rollback_pk_events);

                    let (rollback_secondary_events, _) =
                        self.indexes
                            .delete_from_indexes_cdc(row.clone(), link, inserted_already);

                    let mut merged_primary_events = primary_key_events.clone();
                    merged_primary_events.extend(rollback_pk_events);

                    let mut merged_secondary_events = secondary_events.clone();
                    merged_secondary_events.extend(rollback_secondary_events);

                    let ack_op = Operation::Acknowledge(AcknowledgeOperation {
                        id: OperationId::Single(Uuid::now_v7()),
                        primary_key_events: merged_primary_events,
                        secondary_keys_events: merged_secondary_events,
                    });

                    if let Err(e) = self.data.delete(link) {
                        (ack_op, WorkTableError::PagesError(e))
                    } else {
                        (ack_op, WorkTableError::AlreadyExists(at.to_string_value()))
                    }
                }
                IndexError::NotFound => {
                    // Mirror the AlreadyExists arm: roll the primary index and
                    // the row's secondary entries back and release the data
                    // slot, so no permanently half-committed state remains.
                    let (_, rollback_pk_events) = self.primary_index.remove_cdc(pk.clone(), link);
                    let rollback_pk_events = convert_change_events(rollback_pk_events);

                    let (rollback_secondary_events, _) = self.indexes.delete_row_cdc(row.clone(), link);

                    let mut merged_primary_events = primary_key_events.clone();
                    merged_primary_events.extend(rollback_pk_events);

                    let mut merged_secondary_events = secondary_events.clone();
                    merged_secondary_events.extend(rollback_secondary_events);

                    let ack_op = Operation::Acknowledge(AcknowledgeOperation {
                        id: OperationId::Single(Uuid::now_v7()),
                        primary_key_events: merged_primary_events,
                        secondary_keys_events: merged_secondary_events,
                    });

                    if let Err(e) = self.data.delete(link) {
                        (ack_op, WorkTableError::PagesError(e))
                    } else {
                        (ack_op, WorkTableError::NotFound)
                    }
                }
            };
            return (Some(ack_op), Err(error));
        }

        unsafe {
            if let Err(e) = self.data.with_mut_ref(link, |r| r.unghost()) {
                let ack_op = Operation::Acknowledge(AcknowledgeOperation {
                    id: OperationId::Single(Uuid::now_v7()),
                    primary_key_events: primary_key_events.clone(),
                    secondary_keys_events: secondary_events.clone(),
                });
                return (Some(ack_op), Err(WorkTableError::PagesError(e)));
            }
        }

        let bytes = match self.data.select_raw(link) {
            Ok(bytes) => bytes,
            Err(e) => {
                let ack_op = Operation::Acknowledge(AcknowledgeOperation {
                    id: OperationId::Single(Uuid::now_v7()),
                    primary_key_events: primary_key_events.clone(),
                    secondary_keys_events: secondary_events.clone(),
                });
                return (Some(ack_op), Err(WorkTableError::PagesError(e)));
            }
        };

        let op = Operation::Insert(InsertOperation {
            id: OperationId::Single(Uuid::now_v7()),
            pk_gen_state: self.pk_gen.get_state(),
            primary_key_events,
            secondary_keys_events: secondary_events,
            bytes,
            link,
        });

        (Some(op), Ok(pk))
    }

    /// CDC variant of [`Self::insert_many`], for persisted tables.
    ///
    /// On success it returns one `Insert` operation per row, all sharing a
    /// single `OperationId::Multi`, so the persistence analyzer coalesces the
    /// whole batch into one engine application while the CDC event-id stream
    /// stays positional and gap-free. On rejection the in-memory state is
    /// unwound exactly like `insert_many` and a single `Acknowledge`
    /// operation is returned carrying every forward and rollback event, so
    /// the persistence layer accounts for all consumed event ids without
    /// applying any of them.
    #[allow(clippy::type_complexity)]
    pub fn insert_many_cdc<SecondaryEvents>(
        &self,
        rows: Vec<Row>,
    ) -> (
        Vec<Operation<<PkGen as PrimaryKeyGeneratorState>::State, PrimaryKey, SecondaryEvents>>,
        Result<Vec<PrimaryKey>, BatchInsertError>,
    )
    where
        Row: Archive
            + Clone
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <Row as StorableRow>::WrappedRow:
            Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: ArchivedRowWrapper
            + Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
        PrimaryKey: Clone,
        SecondaryEvents: Debug + Default + Clone + TableSecondaryIndexEventsOps<AvailableIndexes>,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>
            + TableSecondaryIndexCdc<Row, AvailableTypes, SecondaryEvents, AvailableIndexes>,
        PkGen: PrimaryKeyGeneratorState,
        <PkGen as PrimaryKeyGeneratorState>::State: Debug,
        AvailableIndexes: Debug + AvailableIndex,
        PrimaryIndex<PrimaryKey, DATA_LENGTH, PkMap>: TableIndexCdc<PrimaryKey>,
    {
        if rows.is_empty() {
            return (Vec::new(), Ok(Vec::new()));
        }
        let pks: Vec<PrimaryKey> = rows.iter().map(|row| row.get_primary_key().clone()).collect();
        let _mutation_guards = self.lock_manager.mutation_guards(pks.iter());

        let mut links: Vec<Link> = Vec::with_capacity(rows.len());
        let mut forward_primary: Vec<Vec<ChangeEvent<Pair<PrimaryKey, Link>>>> = Vec::with_capacity(rows.len());
        let mut forward_secondary: Vec<SecondaryEvents> = Vec::with_capacity(rows.len());

        // Merges every event gathered so far — forward events of staged rows,
        // the failing row's own merged events and the prefix rollback events —
        // into one Acknowledge operation.
        let fail = |this: &Self,
                    links: &[Link],
                    forward_primary: Vec<Vec<ChangeEvent<Pair<PrimaryKey, Link>>>>,
                    forward_secondary: Vec<SecondaryEvents>,
                    own_primary: Vec<ChangeEvent<Pair<PrimaryKey, Link>>>,
                    own_secondary: Option<SecondaryEvents>,
                    error: BatchInsertError| {
            let mut merged_primary: Vec<ChangeEvent<Pair<PrimaryKey, Link>>> =
                forward_primary.into_iter().flatten().collect();
            let mut merged_secondary = SecondaryEvents::default();
            for events in forward_secondary {
                merged_secondary.extend(events);
            }
            merged_primary.extend(own_primary);
            if let Some(own_secondary) = own_secondary {
                merged_secondary.extend(own_secondary);
            }

            let mut unwind_error: Option<WorkTableError> = None;
            for index in (0..links.len()).rev() {
                let link = links[index];
                let (_, rollback_primary) = this.primary_index.remove_cdc(pks[index].clone(), link);
                merged_primary.extend(convert_change_events(rollback_primary));
                let (rollback_secondary, _) = this.indexes.delete_row_cdc(rows[index].clone(), link);
                merged_secondary.extend(rollback_secondary);
                if let Err(e) = this.data.delete(link)
                    && unwind_error.is_none()
                {
                    unwind_error = Some(WorkTableError::PagesError(e));
                }
            }

            let ack_op = Operation::Acknowledge(AcknowledgeOperation {
                id: OperationId::Single(Uuid::now_v7()),
                primary_key_events: merged_primary,
                secondary_keys_events: merged_secondary,
            });
            let error = match unwind_error {
                // An unwind failure leaves no single offending row to name.
                Some(unwind_error) => BatchInsertError::Table(unwind_error),
                None => error,
            };
            (vec![ack_op], Err(error))
        };

        for (row_index, row) in rows.iter().enumerate() {
            let link = match self.data.insert(row.clone()) {
                Ok(link) => link,
                Err(e) => {
                    return fail(
                        self,
                        &links,
                        forward_primary,
                        forward_secondary,
                        vec![],
                        None,
                        BatchInsertError::Row {
                            row_index,
                            source: WorkTableError::PagesError(e),
                        },
                    );
                }
            };

            let Some(primary_key_events) = self.primary_index.insert_checked_cdc(pks[row_index].clone(), link) else {
                let source = match self.data.delete(link) {
                    Ok(()) => WorkTableError::PrimaryAlreadyExists,
                    Err(e) => WorkTableError::PagesError(e),
                };
                return fail(
                    self,
                    &links,
                    forward_primary,
                    forward_secondary,
                    vec![],
                    None,
                    BatchInsertError::Row { row_index, source },
                );
            };
            let mut primary_key_events = convert_change_events(primary_key_events);

            let (mut secondary_events, indexes_res) = self.indexes.save_row_cdc(row.clone(), link);
            if let Err(e) = indexes_res {
                let source = match e {
                    IndexError::AlreadyExists { at, inserted_already } => {
                        let (_, rollback_primary) = self.primary_index.remove_cdc(pks[row_index].clone(), link);
                        primary_key_events.extend(convert_change_events(rollback_primary));
                        let (rollback_secondary, _) =
                            self.indexes
                                .delete_from_indexes_cdc(row.clone(), link, inserted_already);
                        secondary_events.extend(rollback_secondary);
                        match self.data.delete(link) {
                            Ok(()) => WorkTableError::AlreadyExists(at.to_string_value()),
                            Err(e) => WorkTableError::PagesError(e),
                        }
                    }
                    IndexError::NotFound => {
                        let (_, rollback_primary) = self.primary_index.remove_cdc(pks[row_index].clone(), link);
                        primary_key_events.extend(convert_change_events(rollback_primary));
                        let (rollback_secondary, _) = self.indexes.delete_row_cdc(row.clone(), link);
                        secondary_events.extend(rollback_secondary);
                        match self.data.delete(link) {
                            Ok(()) => WorkTableError::NotFound,
                            Err(e) => WorkTableError::PagesError(e),
                        }
                    }
                };
                return fail(
                    self,
                    &links,
                    forward_primary,
                    forward_secondary,
                    primary_key_events,
                    Some(secondary_events),
                    BatchInsertError::Row { row_index, source },
                );
            }

            links.push(link);
            forward_primary.push(primary_key_events);
            forward_secondary.push(secondary_events);
        }

        // Every check passed: the batch can no longer be rejected. Make the
        // rows visible and emit their persistence operations under shared
        // Multi ids so the analyzer coalesces whole groups into single engine
        // applications. Group size is capped: the analyzer's bookkeeping
        // table indexes rows by operation id in a multimap whose removals
        // scan all entries of an equal key, so an unbounded group makes the
        // analyzer drain quadratic in batch size. Chunk ids come from
        // `Uuid::now_v7`, whose shared process context keeps them
        // creation-ordered, so cross-chunk event order survives the
        // analyzer's operation-id sort.
        const PERSIST_GROUP_ROWS: usize = 1024;
        let mut batch_id = Uuid::now_v7();
        let mut ops = Vec::with_capacity(links.len());
        for (row_index, link) in links.iter().enumerate() {
            if row_index != 0 && row_index % PERSIST_GROUP_ROWS == 0 {
                batch_id = Uuid::now_v7();
            }
            let published = unsafe { self.data.with_mut_ref(*link, |r| r.unghost()) };
            let bytes = match published
                .map_err(WorkTableError::PagesError)
                .and_then(|()| self.data.select_raw(*link).map_err(WorkTableError::PagesError))
            {
                Ok(bytes) => bytes,
                Err(source) => {
                    // Practically unreachable: the link was created above.
                    // Rows before `row_index` were already made visible, so
                    // unwind everything (deleting them again) rather than
                    // leaving a torn batch behind, and hand back every event
                    // consumed so far in one Acknowledge. Events already moved
                    // into built operations are restored first so the
                    // Acknowledge accounts for the complete event-id stream.
                    for (built_index, op) in ops.into_iter().enumerate() {
                        if let Operation::Insert(insert) = op {
                            forward_primary[built_index] = insert.primary_key_events;
                            forward_secondary[built_index] = insert.secondary_keys_events;
                        }
                    }
                    return fail(
                        self,
                        &links,
                        forward_primary,
                        forward_secondary,
                        vec![],
                        None,
                        BatchInsertError::Table(source),
                    );
                }
            };
            ops.push(Operation::Insert(InsertOperation {
                id: OperationId::Multi(batch_id),
                pk_gen_state: self.pk_gen.get_state(),
                primary_key_events: std::mem::take(&mut forward_primary[row_index]),
                secondary_keys_events: std::mem::take(&mut forward_secondary[row_index]),
                bytes,
                link: *link,
            }));
        }

        (ops, Ok(pks))
    }

    /// Reinserts provided row with updating indexes and saving it's data in new
    /// place. Is used to not delete and insert because this situation causes
    /// a possible gap when row doesn't exist.
    ///
    /// For reinsert it's ok that part of indexes will lead to old row and other
    /// part is for new row. Goal is to make `PrimaryKey` of the row always
    /// acceptable. As for reinsert `PrimaryKey` will be same for both old and
    /// new [`Link`]'s, goal will be achieved.
    ///
    /// [`Link`]: data_bucket::Link
    pub async fn reinsert(&self, row_old: Row, row_new: Row) -> Result<PrimaryKey, WorkTableError>
    where
        Row: Archive
            + Clone
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <Row as StorableRow>::WrappedRow:
            Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: ArchivedRowWrapper
            + Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
        PrimaryKey: Clone,
        AvailableTypes: 'static,
        AvailableIndexes: Debug + AvailableIndex,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>,
        LockType: 'static,
    {
        let pk = row_new.get_primary_key().clone();
        if pk != row_old.get_primary_key() {
            return Err(WorkTableError::PrimaryUpdateTry);
        }
        let old_link: Link = self
            .primary_index
            .pk_map
            .get_value(&pk)
            .map(Into::into)
            .ok_or(WorkTableError::NotFound)?;
        let new_link = self.data.insert(row_new.clone()).map_err(WorkTableError::PagesError)?;
        // The new row must be visible before reinsert_row runs: reinsert_row
        // swings unchanged unique entries onto new_link, and pointing them at
        // a ghosted row makes select-by-unique transiently return nothing for
        // a row that exists. The primary index stays on the old link until
        // every index check has passed, so a reader by primary key can never
        // observe the values of a reinsert that is rolled back.
        unsafe {
            self.data
                .with_mut_ref(new_link, |r| r.unghost())
                .map_err(WorkTableError::PagesError)?
        }

        let indexes_res = self.indexes.reinsert_row(row_old, old_link, row_new.clone(), new_link);
        if let Err(e) = indexes_res {
            return match e {
                IndexError::AlreadyExists { at, inserted_already } => {
                    self.indexes.delete_from_indexes(row_new, new_link, inserted_already)?;
                    self.data.delete(new_link).map_err(WorkTableError::PagesError)?;

                    Err(WorkTableError::AlreadyExists(at.to_string_value()))
                }
                IndexError::NotFound => {
                    // The primary index was never swung and the new row is
                    // still ghosted, so no reader can observe it; release the
                    // new data slot instead of leaking it. Partially applied
                    // secondary entries cannot be unwound here: `NotFound`
                    // carries no list of touched indexes, and removing by the
                    // new row's keys would delete the old row's live entries
                    // for unchanged unique values. No current index
                    // implementation returns `NotFound` from reinsert_row.
                    self.data.delete(new_link).map_err(WorkTableError::PagesError)?;

                    Err(WorkTableError::NotFound)
                }
            };
        }
        self.primary_index.insert(pk.clone(), new_link);
        self.data.delete(old_link).map_err(WorkTableError::PagesError)?;
        Ok(pk)
    }

    #[allow(clippy::type_complexity)]
    pub fn reinsert_cdc<SecondaryEvents>(
        &self,
        row_old: Row,
        row_new: Row,
    ) -> (
        Option<Operation<<PkGen as PrimaryKeyGeneratorState>::State, PrimaryKey, SecondaryEvents>>,
        Result<PrimaryKey, WorkTableError>,
    )
    where
        Row: Archive
            + Clone
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <Row as StorableRow>::WrappedRow:
            Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: ArchivedRowWrapper
            + Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
        PrimaryKey: Clone,
        SecondaryEvents: Debug + Default + Clone + TableSecondaryIndexEventsOps<AvailableIndexes>,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>
            + TableSecondaryIndexCdc<Row, AvailableTypes, SecondaryEvents, AvailableIndexes>,
        PkGen: PrimaryKeyGeneratorState,
        AvailableIndexes: Debug + AvailableIndex,
        PrimaryIndex<PrimaryKey, DATA_LENGTH, PkMap>: TableIndexCdc<PrimaryKey>,
    {
        let pk = row_new.get_primary_key().clone();
        if pk != row_old.get_primary_key() {
            return (None, Err(WorkTableError::PrimaryUpdateTry));
        }

        // Get old link - if not found, no events to acknowledge
        let old_link = match self.primary_index.pk_map.get_value(&pk) {
            Some(v) => v.into(),
            None => return (None, Err(WorkTableError::NotFound)),
        };

        // Insert new data - if this fails, no events to acknowledge
        let (new_link, _) = match self.data.insert_cdc(row_new.clone()) {
            Ok(result) => result,
            Err(e) => return (None, Err(WorkTableError::PagesError(e))),
        };

        // The new row must be visible before reinsert_row_cdc runs: it swings
        // unchanged unique entries onto new_link, and pointing them at a
        // ghosted row makes select-by-unique transiently return nothing for a
        // row that exists. The primary index keeps the old link until every
        // index check has passed, so a reader by primary key can never observe
        // a reinsert that is then rolled back.
        unsafe {
            if let Err(e) = self.data.with_mut_ref(new_link, |r| r.unghost()) {
                return (None, Err(WorkTableError::PagesError(e)));
            }
        }

        let (secondary_events, indexes_res) =
            self.indexes
                .reinsert_row_cdc(row_old, old_link, row_new.clone(), new_link);

        if let Err(e) = indexes_res {
            let (ack_op, error) = match e {
                IndexError::AlreadyExists { at, inserted_already } => {
                    // Rollback: generate CDC events for cleaning up new secondary indexes.
                    // The primary index was never swung, so there are no
                    // primary events to merge or to roll back.
                    let (rollback_secondary_events, _) =
                        self.indexes
                            .delete_from_indexes_cdc(row_new, new_link, inserted_already);

                    let mut merged_secondary_events = secondary_events.clone();
                    merged_secondary_events.extend(rollback_secondary_events);

                    let ack_op = Operation::Acknowledge(AcknowledgeOperation {
                        id: OperationId::Single(Uuid::now_v7()),
                        primary_key_events: vec![],
                        secondary_keys_events: merged_secondary_events,
                    });

                    if let Err(e) = self.data.delete(new_link) {
                        (ack_op, WorkTableError::PagesError(e))
                    } else {
                        (ack_op, WorkTableError::AlreadyExists(at.to_string_value()))
                    }
                }
                IndexError::NotFound => {
                    // As in `reinsert`: the primary index was never swung and
                    // the new row is still ghosted, so releasing the new data
                    // slot is the whole reachable rollback. `NotFound` carries
                    // no list of touched indexes, so partially applied
                    // secondary entries cannot be unwound precisely; no
                    // current index implementation returns it.
                    let ack_op = Operation::Acknowledge(AcknowledgeOperation {
                        id: OperationId::Single(Uuid::now_v7()),
                        primary_key_events: vec![],
                        secondary_keys_events: secondary_events.clone(),
                    });
                    if let Err(e) = self.data.delete(new_link) {
                        (ack_op, WorkTableError::PagesError(e))
                    } else {
                        (ack_op, WorkTableError::NotFound)
                    }
                }
            };
            return (Some(ack_op), Err(error));
        }

        // All index checks passed: swing the primary index to the new row.
        let (_, primary_key_events) = self.primary_index.insert_cdc(pk.clone(), new_link);
        let primary_key_events = convert_change_events(primary_key_events);

        // Delete old data
        if let Err(e) = self.data.delete(old_link) {
            let ack_op = Operation::Acknowledge(AcknowledgeOperation {
                id: OperationId::Single(Uuid::now_v7()),
                primary_key_events: primary_key_events.clone(),
                secondary_keys_events: secondary_events.clone(),
            });
            return (Some(ack_op), Err(WorkTableError::PagesError(e)));
        }

        // Get raw bytes for persistence
        let bytes = match self.data.select_raw(new_link) {
            Ok(bytes) => bytes,
            Err(e) => {
                let ack_op = Operation::Acknowledge(AcknowledgeOperation {
                    id: OperationId::Single(Uuid::now_v7()),
                    primary_key_events: primary_key_events.clone(),
                    secondary_keys_events: secondary_events.clone(),
                });
                return (Some(ack_op), Err(WorkTableError::PagesError(e)));
            }
        };

        let op = Operation::Insert(InsertOperation {
            id: OperationId::Single(Uuid::now_v7()),
            pk_gen_state: self.pk_gen.get_state(),
            primary_key_events,
            secondary_keys_events: secondary_events,
            bytes,
            link: new_link,
        });

        (Some(op), Ok(pk))
    }
}

/// Error returned by `insert_many`.
///
/// A rejected batch names the offending row, and `source` carries the reason,
/// including which unique index collided for `AlreadyExists` rejections.
#[derive(Debug, Display, Error)]
pub enum BatchInsertError {
    /// One row was rejected and the whole batch was rolled back.
    #[display("batch insert rejected at row {row_index}: {source}")]
    Row {
        /// Position of the rejected row in the `rows` argument.
        row_index: usize,
        source: WorkTableError,
    },
    /// The batch failed for a reason not attributable to a single row, such
    /// as a persistence shutdown or an error while unwinding.
    #[display("{_0}")]
    Table(WorkTableError),
}

/// Why a [`WorkTable::delete_many`] stopped.
///
/// It carries how many keys were deleted before the failure, because a bulk
/// delete does not roll back: those rows are gone, and a caller retrying the
/// batch needs to know the prefix already succeeded rather than assume nothing
/// happened.
#[derive(Debug, Display, Error)]
pub enum BatchDeleteError<PrimaryKey: std::fmt::Debug> {
    /// One key could not be deleted. Everything before it was.
    #[display("batch delete stopped at {key:?} after {deleted} deleted: {source}")]
    Key {
        /// The key that failed.
        key: PrimaryKey,
        /// How many keys were deleted before this one.
        deleted: usize,
        source: WorkTableError,
    },
    /// The batch failed for a reason not attributable to a single key.
    #[display("{_0}")]
    Table(WorkTableError),
}

#[derive(Debug, Display, Error, From)]
pub enum WorkTableError {
    NotFound,
    #[display("Value already exists for `{}` index", _0)]
    AlreadyExists(#[error(not(source))] String),
    #[display("Row with this primary key already exists")]
    PrimaryAlreadyExists,
    SerializeError,
    SecondaryIndexError,
    PrimaryUpdateTry,
    PagesError(in_memory::PagesExecutionError),
    #[display("{}", _0)]
    PersistenceError(#[error(not(source))] std::sync::Arc<crate::persistence::PersistenceError>),
}
