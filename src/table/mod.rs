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

    pub update_state: IndexMap<PrimaryKey, Row>,

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
            update_state: IndexMap::default(),
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

        // Match the plain `insert` path: keep the new row ghosted and the
        // primary index on the old link until every index check has passed.
        // Unghosting or swinging the primary index earlier exposes a
        // never-committed update to lock-free readers when a secondary
        // unique-index check then fails and the reinsert is rolled back.
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
        unsafe {
            self.data
                .with_mut_ref(new_link, |r| r.unghost())
                .map_err(WorkTableError::PagesError)?
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

        // Update secondary indexes first. As in `insert_cdc`, the new row
        // stays ghosted and the primary index keeps the old link until every
        // index check has passed, so lock-free readers can never observe a
        // reinsert that is then rolled back.
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

        // All index checks passed: make the new row visible and swing the
        // primary index to it.
        unsafe {
            if let Err(e) = self.data.with_mut_ref(new_link, |r| r.unghost()) {
                let ack_op = Operation::Acknowledge(AcknowledgeOperation {
                    id: OperationId::Single(Uuid::now_v7()),
                    primary_key_events: vec![],
                    secondary_keys_events: secondary_events.clone(),
                });
                return (Some(ack_op), Err(WorkTableError::PagesError(e)));
            }
        }

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
