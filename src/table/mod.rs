pub mod select;
pub mod system_info;

use std::fmt::Debug;
use std::marker::PhantomData;

use crate::in_memory::{DataPages, RowWrapper, StorableRow};
use crate::lock::LockMap;
use crate::persistence::{InsertOperation, Operation};
use crate::prelude::{OperationId, PrimaryKeyGeneratorState};
use crate::primary_key::{PrimaryKeyGenerator, TablePrimaryKey};
use crate::{
    in_memory, IndexError, IndexMap, TableRow, TableSecondaryIndex, TableSecondaryIndexCdc,
};
use data_bucket::{Link, INNER_PAGE_SIZE};
use derive_more::{Display, Error, From};
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;
#[cfg(feature = "perf_measurements")]
use performance_measurement_codegen::performance_measurement;
use rkyv::api::high::HighDeserializer;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
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
    PkNodeType = Vec<Pair<PrimaryKey, Link>>,
    const DATA_LENGTH: usize = INNER_PAGE_SIZE,
> where
    PrimaryKey: Clone + Ord + Send + 'static + std::hash::Hash,
    Row: StorableRow,
    PkNodeType: NodeLike<Pair<PrimaryKey, Link>> + Send + 'static,
{
    pub data: DataPages<Row, DATA_LENGTH>,

    pub pk_map: IndexMap<PrimaryKey, Link, PkNodeType>,

    pub indexes: SecondaryIndexes,

    pub pk_gen: PkGen,

    pub lock_map: LockMap<LockType, PrimaryKey>,

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
        PkNodeType,
        const DATA_LENGTH: usize,
    > Default
    for WorkTable<
        Row,
        PrimaryKey,
        AvailableTypes,
        AvailableIndexes,
        SecondaryIndexes,
        LockType,
        PkGen,
        PkNodeType,
        DATA_LENGTH,
    >
where
    PrimaryKey: Clone + Ord + Send + TablePrimaryKey + std::hash::Hash,
    SecondaryIndexes: Default,
    PkGen: Default,
    PkNodeType: NodeLike<Pair<PrimaryKey, Link>> + Send + 'static,
    Row: StorableRow,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
{
    fn default() -> Self {
        Self {
            data: DataPages::new(),
            pk_map: IndexMap::default(),
            indexes: SecondaryIndexes::default(),
            pk_gen: Default::default(),
            lock_map: LockMap::new(),
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
        PkNodeType,
        const DATA_LENGTH: usize,
    >
    WorkTable<
        Row,
        PrimaryKey,
        AvailableTypes,
        AvailableIndexes,
        SecondaryIndexes,
        LockType,
        PkGen,
        PkNodeType,
        DATA_LENGTH,
    >
where
    Row: TableRow<PrimaryKey>,
    PrimaryKey: Debug + Clone + Ord + Send + TablePrimaryKey + std::hash::Hash,
    PkNodeType: NodeLike<Pair<PrimaryKey, Link>> + Send + 'static,
    Row: StorableRow,
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
    pub fn select(&self, pk: PrimaryKey) -> Option<Row>
    where
        LockType: 'static,
        Row: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived:
            Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        let link = self.pk_map.get(&pk).map(|v| v.get().value)?;
        self.data.select(link).ok()
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
        PrimaryKey: Clone,
        AvailableTypes: 'static,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>,
        LockType: 'static,
    {
        let pk = row.get_primary_key().clone();
        let link = self
            .data
            .insert(row.clone())
            .map_err(WorkTableError::PagesError)?;
        if self
            .pk_map
            .insert(pk.clone(), link)
            .map_or(Ok(()), |_| Err(WorkTableError::AlreadyExists))
            .is_err()
        {
            self.data.delete(link).map_err(WorkTableError::PagesError)?;
            return Err(WorkTableError::AlreadyExists);
        };
        if let Err(e) = self.indexes.save_row(row.clone(), link) {
            return match e {
                IndexError::AlreadyExists {
                    at: _,
                    inserted_already,
                } => {
                    self.data.delete(link).map_err(WorkTableError::PagesError)?;
                    self.pk_map.remove(&pk);
                    self.indexes
                        .delete_from_indexes(row, link, inserted_already)?;

                    Err(WorkTableError::AlreadyExists)
                }
                IndexError::NotFound => Err(WorkTableError::NotFound),
            };
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
        PrimaryKey: Clone,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>
            + TableSecondaryIndexCdc<Row, AvailableTypes, SecondaryEvents, AvailableIndexes>,
        PkGen: PrimaryKeyGeneratorState,
        AvailableIndexes: Debug,
    {
        let pk = row.get_primary_key().clone();
        let (link, bytes) = self
            .data
            .insert_cdc(row.clone())
            .map_err(WorkTableError::PagesError)?;
        let (exists, primary_key_events) = self.pk_map.insert_cdc(pk.clone(), link);
        if exists.is_some() {
            self.data.delete(link).map_err(WorkTableError::PagesError)?;
            return Err(WorkTableError::AlreadyExists);
        }
        let indexes_res = self.indexes.save_row_cdc(row.clone(), link);
        if let Err(e) = indexes_res {
            return match e {
                IndexError::AlreadyExists {
                    at: _,
                    inserted_already,
                } => {
                    self.data.delete(link).map_err(WorkTableError::PagesError)?;
                    self.pk_map.remove(&pk);
                    self.indexes
                        .delete_from_indexes(row, link, inserted_already)?;

                    Err(WorkTableError::AlreadyExists)
                }
                IndexError::NotFound => Err(WorkTableError::NotFound),
            };
        }

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
}

#[derive(Debug, Display, Error, From)]
pub enum WorkTableError {
    NotFound,
    AlreadyExists,
    SerializeError,
    PagesError(in_memory::PagesExecutionError),
}

#[cfg(test)]
mod tests {
    // mod eyre {
    //     use eyre::*;
    //     use worktable_codegen::worktable;
    //
    //     use crate::prelude::*;
    //
    //     worktable! (
    //         name: Test,
    //         columns: {
    //             id: u64 primary_key,
    //             test: u64
    //         }
    //     );
    //
    //     #[test]
    //     fn test() {
    //         let table = TestWorkTable::default();
    //         let row = TestRow {
    //             id: 1,
    //             test: 1,
    //         };
    //         let pk = table.insert::<{ crate::table::tests::tuple_primary_key::TestRow::ROW_SIZE }>(row.clone()).unwrap();
    //         let selected_row = table.select(pk).unwrap();
    //
    //         assert_eq!(selected_row, row);
    //         assert!(table.select((1, 0).into()).is_none())
    //     }
    // }
}
