#![doc = include_str!("../docs/crate.md")]

pub mod in_memory;
mod index;
pub mod lock;
mod mem_stat;
pub mod migration;
pub mod partition;
pub mod persistence;
mod primary_key;
mod row;
mod table;
mod util;

#[cfg(feature = "s3-support")]
pub mod features;

pub use index::*;
pub use persistence::{LoadMode, PersistedWorkTable, PersistenceConfig, PersistenceLoadError};
pub use row::*;
pub use table::*;

pub use data_bucket;
pub use worktable_codegen::migration_engine;
pub use worktable_codegen::worktable;
pub use worktable_codegen::worktable_version;

#[cfg(feature = "s3-support")]
pub use worktable_codegen::s3_sync_persistence;

pub mod prelude {
    pub use crate::in_memory::{ArchivedRowWrapper, Data, DataPages, Query, RowWrapper, StorableRow};
    pub use crate::lock::FullRowLock;
    pub use crate::lock::{Lock, RowLock};
    pub use crate::lock::{LockAcquirer, LockGuard, LockMap, PendingLock};
    pub use crate::mem_stat::MemStat;
    pub use crate::partition::{MAX_PARTITIONS, PartitionError, PartitionSet};
    pub use crate::persistence::{
        AcknowledgeOperation, ArtPersistenceKey, DeleteOperation, DiskConfig, DiskPersistenceEngine,
        IndexTableOfContents, InsertOperation, LoadMode, Operation, OperationId, PersistedWorkTable, PersistenceConfig,
        PersistenceEngine, PersistenceError, PersistenceIndexCorruption, PersistenceLoadError, PersistenceMonitor,
        PersistenceResult, PersistenceState, PersistenceTask, ReadOnlyPersistenceEngine, SpaceArcticIndex,
        SpaceCongeeIndex, SpaceData, SpaceDataOps, SpaceIndex, SpaceIndexOps, SpaceIndexUnsized, SpaceLogicalIndex,
        SpaceLogicalIndexUnsized, SpaceSecondaryIndexOps, TocEntryOversizedError, UpdateOperation,
        load_persisted_state, map_index_pages_to_toc_and_general, map_unsized_index_pages_to_toc_and_general,
        reconstruct_multi_index_nodes, validate_events,
    };
    pub use crate::primary_key::{PrimaryKeyGenerator, PrimaryKeyGeneratorState, TablePrimaryKey};
    pub use crate::table::select::{Order, QueryParams, SelectQueryBuilder, SelectQueryExecutor};
    pub use crate::table::system_info::{IndexInfo, IndexKind, SystemInfo};
    pub use crate::util::{OffsetEqLink, OrderedF32Def, OrderedF64Def};
    pub use crate::{
        ArcticIndex, ArcticKey, ArcticMultiIndex, AvailableIndex, CongeeIndex, CongeeKey, Difference, IndexError, IndexMap,
        IndexMultiMap, MultiPairRecreate, PersistentArcticIndex, PersistentArtIndex, PersistentCongeeIndex,
        PersistentWtiIndex, PrimaryIndex, TableIndex, TableIndexCdc, TableRow, TableSecondaryIndex,
        TableSecondaryIndexCdc, TableSecondaryIndexEventsOps, TableSecondaryIndexInfo, UniqueIndex, UnsizedNode,
        UpstreamIndexMap, UpstreamIndexPair, WorkTable, WorkTableError, vacuum::EmptyDataVacuum,
        vacuum::VacuumPersistence, vacuum::WorkTableVacuum,
    };
    pub use data_bucket::{
        DATA_VERSION, DataPage, GENERAL_HEADER_SIZE, GeneralHeader, GeneralPage, INNER_PAGE_SIZE, IndexPage, Interval,
        Link, PAGE_SIZE, PageType, Persistable, PersistableIndex, SizeMeasurable, SizeMeasure, SpaceInfoPage,
        TableOfContentsPage, UnsizedIndexPage, VariableSizeMeasurable, VariableSizeMeasure, align,
        get_index_page_size_from_data_length, map_data_pages_to_general, parse_data_page, parse_page, persist_page,
        seek_to_page_start, update_at,
    };
    pub use derive_more::{Display as MoreDisplay, From, Into};
    pub use indexset::{
        cdc::change::{ChangeEvent as IndexChangeEvent, Id as IndexChangeEventId},
        core::{multipair::MultiPair as IndexMultiPair, pair::Pair as IndexPair},
    };
    pub use ordered_float::OrderedFloat;
    pub use parking_lot::RwLock as ParkingRwLock;
    pub use worktable_codegen::{MemStat, PersistIndex, PersistTable};

    pub const WT_INDEX_EXTENSION: &str = ".wt.idx";
    pub const WT_DATA_EXTENSION: &str = ".wt.data";

    #[cfg(feature = "s3-support")]
    pub use crate::features::{S3Config, S3DiskConfig, S3SyncDiskPersistenceEngine};
}
