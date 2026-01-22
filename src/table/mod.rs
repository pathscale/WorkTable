pub mod select;
pub mod system_info;
pub mod vacuum;

use crate::in_memory::{DataPages, GhostWrapper, RowWrapper, StorableRow};
use crate::lock::WorkTableLock;
use crate::persistence::{InsertOperation, Operation};
use crate::prelude::{Link, OperationId, PrimaryKeyGeneratorState};
use crate::primary_key::{PrimaryKeyGenerator, TablePrimaryKey};
use crate::util::OffsetEqLink;
use crate::{
    AvailableIndex, IndexError, IndexMap, PrimaryIndex, TableIndex, TableIndexCdc, TableRow,
    TableSecondaryIndex, TableSecondaryIndexCdc, convert_change_events, in_memory,
};
use data_bucket::INNER_PAGE_SIZE;
use derive_more::{Display, Error, From};
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;
#[cfg(feature = "perf_measurements")]
use performance_measurement_codegen::performance_measurement;
use rkyv::api::high::HighDeserializer;
use rkyv::rancor::Strategy;
use rkyv::ser::Serializer;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use std::fmt::Debug;
use std::marker::PhantomData;
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
    PkNodeType = Vec<Pair<PrimaryKey, OffsetEqLink<DATA_LENGTH>>>,
> where
    PrimaryKey: Clone + Ord + Send + 'static + std::hash::Hash,
    Row: StorableRow + Send + Clone + 'static,
    PkNodeType: NodeLike<Pair<PrimaryKey, OffsetEqLink<DATA_LENGTH>>> + Send + 'static,
{
    pub data: Arc<DataPages<Row, DATA_LENGTH>>,

    pub primary_index: Arc<PrimaryIndex<PrimaryKey, DATA_LENGTH, PkNodeType>>,

    pub indexes: Arc<SecondaryIndexes>,

    pub pk_gen: PkGen,

    pub lock_manager: Arc<WorkTableLock<LockType, PrimaryKey>>,

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
    PkNodeType,
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
        PkNodeType,
    >
where
    PrimaryKey: Debug + Clone + Ord + Send + TablePrimaryKey + std::hash::Hash,
    SecondaryIndexes: Default,
    PkGen: Default,
    PkNodeType: NodeLike<Pair<PrimaryKey, OffsetEqLink<DATA_LENGTH>>> + Send + 'static,
    Row: StorableRow + Send + Clone + 'static,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
{
    fn default() -> Self {
        Self {
            data: Arc::new(DataPages::new()),
            primary_index: Arc::new(PrimaryIndex::default()),
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
    PkNodeType,
>
    WorkTable<
        Row,
        PrimaryKey,
        AvailableTypes,
        AvailableIndexes,
        SecondaryIndexes,
        LockType,
        PkGen,
        DATA_LENGTH,
        PkNodeType,
    >
where
    Row: TableRow<PrimaryKey>,
    PrimaryKey: Debug + Clone + Ord + Send + TablePrimaryKey + std::hash::Hash,
    PkNodeType: NodeLike<Pair<PrimaryKey, OffsetEqLink<DATA_LENGTH>>> + Send + 'static,
    Row: StorableRow + Send + Clone + 'static,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
{
    pub fn get_next_pk(&self) -> PrimaryKey
    where
        PkGen: PrimaryKeyGenerator<PrimaryKey>,
    {
        self.pk_gen.next()
    }

    /// Selects `Row` from table identified with provided primary key. Returns `None` if no value presented.
    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "WorkTable")
    )]
    pub async fn select(&self, pk: PrimaryKey) -> Option<Row>
    where
        LockType: 'static,
        Row: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived:
            Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        let mut link: Option<Link> = self
            .primary_index
            .pk_map
            .get(&pk)
            .map(|v| v.get().value.into());
        if let Some(l) = link {
            if self.lock_manager.await_page_lock(l.page_id).await {
                // We waited for vacuum to complete, need to re-lookup the link
                link = self
                    .primary_index
                    .pk_map
                    .get(&pk)
                    .map(|v| v.get().value.into());
            }
        }
        if let Some(link) = link {
            self.data.select(link).ok()
        } else {
            None
        }
    }

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "WorkTable")
    )]
    pub fn insert(&self, row: Row) -> Result<PrimaryKey, WorkTableError>
    where
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
        PrimaryKey: Clone,
        AvailableTypes: 'static,
        AvailableIndexes: AvailableIndex,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>,
        LockType: 'static,
    {
        let pk = row.get_primary_key().clone();
        let link = self
            .data
            .insert(row.clone())
            .map_err(WorkTableError::PagesError)?;
        if self
            .primary_index
            .insert_checked(pk.clone(), link)
            .is_none()
        {
            self.data.delete(link).map_err(WorkTableError::PagesError)?;
            return Err(WorkTableError::AlreadyExists("Primary".to_string()));
        };
        if let Err(e) = self.indexes.save_row(row.clone(), link) {
            return match e {
                IndexError::AlreadyExists {
                    at,
                    inserted_already,
                } => {
                    self.data.delete(link).map_err(WorkTableError::PagesError)?;
                    self.primary_index.remove(&pk, link);
                    self.indexes
                        .delete_from_indexes(row, link, inserted_already)?;

                    Err(WorkTableError::AlreadyExists(at.to_string_value()))
                }
                IndexError::NotFound => Err(WorkTableError::NotFound),
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
    ) -> Result<
        (
            PrimaryKey,
            Operation<<PkGen as PrimaryKeyGeneratorState>::State, PrimaryKey, SecondaryEvents>,
        ),
        WorkTableError,
    >
    where
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
        PrimaryKey: Clone,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>
            + TableSecondaryIndexCdc<Row, AvailableTypes, SecondaryEvents, AvailableIndexes>,
        PkGen: PrimaryKeyGeneratorState,
        AvailableIndexes: Debug + AvailableIndex,
    {
        let pk = row.get_primary_key().clone();
        let (link, _) = self
            .data
            .insert_cdc(row.clone())
            .map_err(WorkTableError::PagesError)?;
        let primary_key_events = self.primary_index.insert_checked_cdc(pk.clone(), link);
        let Some(primary_key_events) = primary_key_events else {
            self.data.delete(link).map_err(WorkTableError::PagesError)?;
            return Err(WorkTableError::AlreadyExists("Primary".to_string()));
        };
        let primary_key_events = convert_change_events(primary_key_events);
        let indexes_res = self.indexes.save_row_cdc(row.clone(), link);
        if let Err(e) = indexes_res {
            return match e {
                IndexError::AlreadyExists {
                    at,
                    inserted_already,
                } => {
                    self.data.delete(link).map_err(WorkTableError::PagesError)?;
                    self.primary_index.remove(&pk, link);
                    self.indexes
                        .delete_from_indexes(row, link, inserted_already)?;

                    Err(WorkTableError::AlreadyExists(at.to_string_value()))
                }
                IndexError::NotFound => Err(WorkTableError::NotFound),
            };
        }
        unsafe {
            self.data
                .with_mut_ref(link, |r| r.unghost())
                .map_err(WorkTableError::PagesError)?
        }
        let bytes = self
            .data
            .select_raw(link)
            .map_err(WorkTableError::PagesError)?;

        let op = Operation::Insert(InsertOperation {
            id: OperationId::Single(Uuid::now_v7()),
            pk_gen_state: self.pk_gen.get_state(),
            primary_key_events,
            secondary_keys_events: indexes_res.expect("was checked before"),
            bytes,
            link,
        });

        Ok((pk, op))
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
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <Row as StorableRow>::WrappedRow: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: GhostWrapper,
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
        let mut old_link: Link = self
            .primary_index
            .pk_map
            .get(&pk)
            .map(|v| v.get().value.into())
            .ok_or(WorkTableError::NotFound)?;
        if self.lock_manager.await_page_lock(old_link.page_id).await {
            // We waited for vacuum to complete, need to re-lookup the link
            old_link = self
                .primary_index
                .pk_map
                .get(&pk)
                .map(|v| v.get().value.into())
                .ok_or(WorkTableError::NotFound)?;
        }
        let new_link = self
            .data
            .insert(row_new.clone())
            .map_err(WorkTableError::PagesError)?;
        unsafe {
            self.data
                .with_mut_ref(new_link, |r| r.unghost())
                .map_err(WorkTableError::PagesError)?
        }
        self.primary_index.insert(pk.clone(), new_link);

        let indexes_res = self
            .indexes
            .reinsert_row(row_old, old_link, row_new.clone(), new_link);
        if let Err(e) = indexes_res {
            return match e {
                IndexError::AlreadyExists {
                    at,
                    inserted_already,
                } => {
                    self.primary_index.insert(pk.clone(), old_link);
                    self.indexes
                        .delete_from_indexes(row_new, new_link, inserted_already)?;
                    self.data
                        .delete(new_link)
                        .map_err(WorkTableError::PagesError)?;

                    Err(WorkTableError::AlreadyExists(at.to_string_value()))
                }
                IndexError::NotFound => Err(WorkTableError::NotFound),
            };
        }
        self.data
            .delete(old_link)
            .map_err(WorkTableError::PagesError)?;
        Ok(pk)
    }

    #[allow(clippy::type_complexity)]
    pub fn reinsert_cdc<SecondaryEvents>(
        &self,
        row_old: Row,
        row_new: Row,
    ) -> Result<
        (
            PrimaryKey,
            Operation<<PkGen as PrimaryKeyGeneratorState>::State, PrimaryKey, SecondaryEvents>,
        ),
        WorkTableError,
    >
    where
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
        PrimaryKey: Clone,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>
            + TableSecondaryIndexCdc<Row, AvailableTypes, SecondaryEvents, AvailableIndexes>,
        PkGen: PrimaryKeyGeneratorState,
        AvailableIndexes: Debug + AvailableIndex,
    {
        let pk = row_new.get_primary_key().clone();
        if pk != row_old.get_primary_key() {
            return Err(WorkTableError::PrimaryUpdateTry);
        }
        let old_link = self
            .primary_index
            .pk_map
            .get(&pk)
            .map(|v| v.get().value.into())
            .ok_or(WorkTableError::NotFound)?;
        let (new_link, _) = self
            .data
            .insert_cdc(row_new.clone())
            .map_err(WorkTableError::PagesError)?;
        unsafe {
            self.data
                .with_mut_ref(new_link, |r| r.unghost())
                .map_err(WorkTableError::PagesError)?
        }
        let (_, primary_key_events) = self.primary_index.insert_cdc(pk.clone(), new_link);
        let primary_key_events = convert_change_events(primary_key_events);
        let indexes_res =
            self.indexes
                .reinsert_row_cdc(row_old, old_link, row_new.clone(), new_link);
        if let Err(e) = indexes_res {
            return match e {
                IndexError::AlreadyExists {
                    at,
                    inserted_already,
                } => {
                    self.primary_index.insert(pk.clone(), old_link);
                    self.indexes
                        .delete_from_indexes(row_new, new_link, inserted_already)?;
                    self.data
                        .delete(new_link)
                        .map_err(WorkTableError::PagesError)?;

                    Err(WorkTableError::AlreadyExists(at.to_string_value()))
                }
                IndexError::NotFound => Err(WorkTableError::NotFound),
            };
        }

        self.data
            .delete(old_link)
            .map_err(WorkTableError::PagesError)?;
        let bytes = self
            .data
            .select_raw(new_link)
            .map_err(WorkTableError::PagesError)?;

        let op = Operation::Insert(InsertOperation {
            id: OperationId::Single(Uuid::now_v7()),
            pk_gen_state: self.pk_gen.get_state(),
            primary_key_events,
            secondary_keys_events: indexes_res.expect("was checked just before"),
            bytes,
            link: new_link,
        });

        Ok((pk, op))
    }
}

#[derive(Debug, Display, Error, From)]
pub enum WorkTableError {
    NotFound,
    #[display("Value already exists for `{}` index", _0)]
    AlreadyExists(#[error(not(source))] String),
    SerializeError,
    SecondaryIndexError,
    PrimaryUpdateTry,
    PagesError(in_memory::PagesExecutionError),
}
