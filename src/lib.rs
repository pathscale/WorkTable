#![cfg_attr(not(feature = "std"), no_std)]
#![doc = include_str!("../docs/crate.md")]

#[macro_use]
extern crate alloc;

/// Generated code names `worktable::` paths, which must also resolve inside
/// this crate, where `worktable!` is invoked for the persistence queue.
extern crate self as worktable;

pub mod in_memory;
mod index;
pub mod lock;
mod mem_stat;
#[cfg(feature = "std")]
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
#[cfg(feature = "std")]
pub use persistence::{
    LoadMode, PersistedWorkTable, PersistenceConfig, PersistenceLoadError, UnloadFailure, UnloadReport,
};
pub use row::*;
pub use table::*;

pub use data_bucket;
pub use worktable_codegen::migration_engine;
pub use worktable_codegen::worktable;
pub use worktable_codegen::worktable_version;
/// The schema language, so the declaration each table embeds can be read
/// without taking a second dependency and matching its version by hand.
pub use worktable_dsl;

#[cfg(feature = "s3-support")]
pub use worktable_codegen::s3_sync_persistence;

pub mod prelude {
    pub use alloc::collections::BTreeMap;
    pub use alloc::sync::Arc;
    pub use alloc::vec::IntoIter;
    pub use hashbrown::{HashMap, HashSet};

    pub use crate::in_memory::{ArchivedRowWrapper, Data, DataPages, Query, RowWrapper, StorableRow};
    pub use crate::lock::FullRowLock;
    pub use crate::lock::{Lock, RowLock};
    pub use crate::lock::{LockAcquirer, LockGuard, LockMap, PendingLock};
    pub use crate::mem_stat::MemStat;
    pub use crate::partition::{MAX_PARTITIONS, PartRef, PartitionError, PartitionSet};
    pub use crate::persistence::{AcknowledgeOperation, DeleteOperation, InsertOperation, Operation, OperationId};
    pub use crate::persistence::{OperationType, UpdateOperation, validate_events};
    #[cfg(feature = "std")]
    pub use crate::persistence::{
        ArtPersistenceKey, DiskConfig, DiskPersistenceEngine, IndexTableOfContents, LoadMode, PersistedWorkTable,
        PersistenceConfig, PersistenceEngine, PersistenceError, PersistenceIndexCorruption, PersistenceLoadError,
        PersistenceMonitor, PersistenceResult, PersistenceState, PersistenceTask, ReadOnlyPersistenceEngine,
        SpaceArcticIndex, SpaceArcticMultiIndex, SpaceArcticStringIndex, SpaceCongeeIndex, SpaceData, SpaceDataOps,
        SpaceIndex, SpaceIndexOps, SpaceIndexUnsized, SpaceLogicalIndex, SpaceLogicalIndexUnsized,
        SpaceLogicalMultiIndex, SpaceLogicalMultiIndexUnsized, SpaceSecondaryIndexOps, TocEntryOversizedError,
        UnloadFailure, UnloadReport, load_persisted_state, map_index_pages_to_toc_and_general,
        map_unsized_index_pages_to_toc_and_general, reconstruct_multi_index_nodes,
    };
    pub use crate::primary_key::{
        PrimaryKeyGenerator, PrimaryKeyGeneratorRange, PrimaryKeyGeneratorState, TablePrimaryKey,
    };
    pub use crate::table::select::{Order, QueryParams, SelectQueryBuilder, SelectQueryExecutor};
    pub use crate::table::system_info::{IndexInfo, IndexKind, SystemInfo};
    pub use crate::util::{OffsetEqLink, OrderedF32Def, OrderedF64Def};
    pub use crate::{
        ArcticEntry, ArcticIndex, ArcticKey, ArcticMultiIndex, ArcticStringKey, AvailableIndex, BatchDeleteError,
        BatchInsertError, CongeeIndex, CongeeKey, Difference, IndexError, IndexMap, IndexMultiMap, MultiPairRecreate,
        PersistentArcticIndex, PersistentArcticMultiIndex, PersistentArtIndex, PersistentCongeeIndex,
        PersistentWtiIndex, PrimaryIndex, TableIndex, TableIndexCdc, TableRow, TableSecondaryIndex,
        TableSecondaryIndexCdc, TableSecondaryIndexEventsOps, TableSecondaryIndexInfo, UniqueIndex, UnsizedNode,
        UpstreamIndexMap, UpstreamIndexPair, WorkTable, WorkTableError, validate_arctic_link,
    };
    #[cfg(feature = "std")]
    pub use crate::{vacuum::EmptyDataVacuum, vacuum::VacuumPersistence, vacuum::WorkTableVacuum};
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
