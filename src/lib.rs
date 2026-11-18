#![cfg_attr(not(any(test, feature = "std")), no_std)]
#![doc = include_str!("../docs/crate.md")]

#[macro_use]
extern crate alloc;

/// Generated code names `worktable::` paths, which must also resolve inside
/// this crate, where `worktable!` is invoked for the persistence queue.
extern crate self as worktable;

pub use data_bucket::eyre;

pub(crate) fn snake_to_pascal(input: &str) -> alloc::string::String {
    let mut out = alloc::string::String::with_capacity(input.len());
    for part in input.split('_') {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            for uc in first.to_uppercase() {
                out.push(uc);
            }
            for ch in chars {
                for lc in ch.to_lowercase() {
                    out.push(lc);
                }
            }
        }
    }
    out
}

#[doc(hidden)]
pub mod uuid_compat {
    pub use ::uuid::*;

    #[cfg(not(target_os = "none"))]
    pub use ::uuid::Uuid;

    #[cfg(target_os = "none")]
    pub type Uuid = ::uuid::Uuid;

    #[cfg(not(target_os = "none"))]
    pub fn now_v7() -> ::uuid::Uuid {
        crate::persistence::operation::new_operation_uuid()
    }

    #[cfg(target_os = "none")]
    pub fn now_v7() -> ::uuid::Uuid {
        crate::persistence::operation::new_operation_uuid()
    }
}

/// A fixed-capacity table whose rows are claimed without blocking.
pub mod atomic_key_table;
mod columnar;
pub mod fsx;
pub mod in_memory;
mod index;
pub mod linear_table;
pub mod lock;
mod mem_stat;
pub mod migration;
pub mod partition;
pub mod persistence;
/// Which async runtime a table's work runs on.
pub mod runtime;
mod storage_catalog;

mod primary_key;
mod row;
mod table;
mod util;
/// The page codec behind `storage: vec` plus `persist: true`.
pub mod vec_hydrate;

#[cfg(feature = "s3-support")]
pub mod features;
#[cfg(feature = "s3-support")]
pub use features::{DatabaseS3DiskConfig, DatabaseS3PersistenceEngine, LegacySnapshotConfig, RestoredLegacySnapshot};

pub use columnar::{
    ClusteredColumnarIndex, ColumnCompression, ColumnSlotId, ColumnSlotId8, ColumnSlotId16, ColumnSlotId32,
    ColumnSlotId64, ColumnarColumn, ColumnarRowRef, next_columnar_incarnation,
};
pub use index::*;
pub use linear_table::{InsertError as LinearInsertError, LinearTable, VecTable};
pub use persistence::{
    LoadMode, PersistedWorkTable, PersistenceConfig, PersistenceLoadError, UnloadFailure, UnloadReport,
};
pub use row::*;
pub use storage_catalog::{Database, DatabaseCatalog, GeneratedSystemCatalog, SystemCatalogView};
#[cfg(feature = "s3-support")]
pub type S3Database = Database<data_bucket::storage::s3::S3PageStore>;
pub use table::*;

pub use data_bucket;
pub use worktable_codegen::migration_engine;
/// Declares the process's runtime profiles. See `runtime::Profile`.
pub use worktable_codegen::runtimes;
pub use worktable_codegen::worktable;
pub use worktable_codegen::worktable_version;
/// The schema language, so the declaration each table embeds can be read
/// without taking a second dependency and matching its version by hand.
#[cfg(not(target_os = "none"))]
pub use worktable_dsl;

#[cfg(feature = "s3-support")]
pub use worktable_codegen::database_s3_persistence;
#[cfg(feature = "s3-support")]
pub use worktable_codegen::s3_sync_persistence;

/// Emits its body unconditionally.
///
/// Retained as a compatibility macro for generated code.
#[macro_export]
#[doc(hidden)]
macro_rules! __wt_if_std {
    ($($item:tt)*) => { $($item)* };
}

pub mod prelude {
    /// The filesystem this crate goes through. Generated code opens files by
    /// this path, so a consumer of `worktable!` gets the same backend the
    /// crate itself uses without naming it.
    pub use crate::fsx;
    pub use eko::compat::fs::symlink_metadata as fs_symlink_metadata;
    pub use eko::compat::io::ErrorKind as FsErrorKind;
    pub use eko::compat::path::Path as FsPath;
    /// The runtime a table names, and the registry of pool flavors it can
    /// pick between. `worktable!` emits these type names, so they have to
    /// resolve in the consumer's crate for the same reason `fsx` does.
    pub use crate::runtime::{
        Elapsed, FLAVOR_COUNT, Flavor, FlavorMarker, Locality, LowLatency, NagoyaRt, Profile, Runtime,
        RuntimeJoinHandle, RuntimeNotified, RuntimeNotify, RuntimeRwLock, RuntimeSemaphore, RuntimeSemaphorePermit,
        RuntimeUnpinned, SharedSlot, Spread, TableRuntime, Throughput, Tuning, WideInjector, describe_tuning,
        engine_executor, engine_flavor, env_override, executor_for_flavor, parse_selection,
    };
    #[cfg(feature = "tokio-runtime")]
    pub use crate::runtime::{TokioJoinHandle, TokioRt};
    /// The three async primitives generated code awaits on. Re-exported for
    /// the same reason `fsx` is: `worktable!` expands inside the consumer's
    /// crate, so every path it emits has to resolve there. Emitting `tokio::`
    /// made a whole runtime part of the macro's contract, and every consumer
    /// carried it whether or not they ran one.
    pub use nagoya::{sleep, timeout, yield_now};

    pub use alloc::boxed::Box;
    /// The `BTreeMap` entry, under a name a macro expansion can write.
    ///
    /// A `storage: vec` table needs it to refuse a duplicate key in one traversal
    /// rather than a `contains_key` followed by an `insert`. The path is
    /// re-exported rather than emitted, for the same reason everything else
    /// here is: `alloc::` does not resolve in a consumer that never declared
    /// `extern crate alloc`.
    pub use alloc::collections::btree_map::Entry as BTreeMapEntry;
    pub use alloc::collections::{BTreeMap, BTreeSet};
    /// What `using fxhash` stores.
    ///
    /// `hashbrown` rather than `std::collections::HashMap`, because this crate
    /// is `no_std` and `std`'s map is not reachable from one; `FxBuildHasher`
    /// rather than SipHash, because the keys here are already-checked column
    /// values and not adversarial input, and SipHash is most of a hash map's
    /// lookup cost.
    ///
    /// Re-exported rather than emitted for the same reason as everything else
    /// in this module: a consumer that never depended on `hashbrown` or
    /// `rustc-hash` still has to be able to compile the expansion.
    pub use hashbrown::HashMap as FxHashMapInner;
    /// The entry API, so a `vec: true` insert can refuse a duplicate key in one
    /// traversal rather than a `contains_key` followed by an `insert`.
    pub use hashbrown::hash_map::Entry as HashMapEntry;
    pub use rustc_hash::FxBuildHasher;

    /// A hash map from a column value to a row position.
    ///
    /// Point operations only: it cannot answer a range, which is why the
    /// generator refuses to emit `range` and `range_by_` on a table whose index
    /// is one of these, and why `using fxhash` is accepted on `vec: true` and
    /// refused on a paged table.
    pub type FxHashMap<K, V> = hashbrown::HashMap<K, V, rustc_hash::FxBuildHasher>;
    pub use alloc::sync::Arc;
    /// `Vec` and `vec!` for the same reason as `Arc` above: a `no_std`
    /// consumer has neither in scope, and the expansion uses both.
    pub use alloc::vec;
    pub use alloc::vec::{IntoIter, Vec};
    /// The one combinator generated code awaits on, re-exported for the same
    /// reason as `sleep` and `timeout`: emitting `futures::` made that crate
    /// part of the macro's contract, so every consumer had to depend on it.
    pub use futures::future::join_all;
    pub use hashbrown::{HashMap, HashSet};

    pub use crate::atomic_key_table::AtomicKeyTable;
    pub use crate::in_memory::{
        ArchivedRowWrapper, Data, DataPages, InlineArchived, Query, RowWrapper, SelectRef, StorableRow,
    };
    pub use crate::lock::FullRowLock;
    pub use crate::lock::LockMap;
    pub use crate::lock::{Lock, RowLock};
    // `LockAcquirer`, `LockGuard` and `PendingLock` borrow their `LockMap` as a
    // raw pointer rather than an owning `Arc` (see the "Borrowed guards" note
    // on `LockMap`), so holding one past the map's last `Arc` is undefined
    // behaviour. They are not a user-facing API: the only callers are the
    // operation bodies this crate generates, which is why they are re-exported
    // here for that generated code and `#[doc(hidden)]` at their definitions.
    #[doc(hidden)]
    pub use crate::lock::{LockAcquirer, LockGuard, PendingLock};
    pub use crate::mem_stat::MemStat;
    pub use crate::partition::{DenseError, DenseRows, MAX_PARTITIONS, PartRef, PartitionError, PartitionSet};
    pub use crate::persistence::{
        AcknowledgeOperation, ArtPersistenceKey, DeleteOperation, DiskConfig, DiskPersistenceEngine,
        IndexTableOfContents, InsertOperation, LoadMode, Operation, OperationId, OperationType, PersistedWorkTable,
        PersistenceConfig, PersistenceEngine, PersistenceError, PersistenceIndexCorruption, PersistenceLoadError,
        PersistenceMonitor, PersistenceResult, PersistenceState, PersistenceTask, ReadOnlyPersistenceEngine,
        SpaceArcticIndex, SpaceArcticMultiIndex, SpaceArcticStringIndex, SpaceCongeeIndex, SpaceData, SpaceDataOps,
        SpaceIndex, SpaceIndexOps, SpaceIndexUnsized, SpaceLogicalIndex, SpaceLogicalIndexUnsized,
        SpaceLogicalMultiIndex, SpaceLogicalMultiIndexUnsized, SpaceSecondaryIndexOps, TocEntryOversizedError,
        UnloadFailure, UnloadReport, UpdateOperation, load_persisted_state, map_index_pages_to_toc_and_general,
        map_unsized_index_pages_to_toc_and_general, reconstruct_multi_index_nodes, validate_events,
    };
    pub use crate::primary_key::{
        PrimaryKeyGenerator, PrimaryKeyGeneratorRange, PrimaryKeyGeneratorState, TablePrimaryKey,
    };
    pub use crate::table::select::{
        Order, QueryParams, SelectQueryAsyncExecutor, SelectQueryBuilder, SelectQueryExecutor, SelectQueryFuture,
    };
    pub use crate::table::system_info::{IndexInfo, IndexKind, SystemInfo};
    pub use crate::util::{OffsetEqLink, OrderedF32Def, OrderedF64Def};
    /// The page codec a `storage: vec` table unloads and loads through.
    pub use crate::vec_hydrate::{Codec, LoadError, NotAnArchive, RowTooLarge, from_pages, to_pages};
    #[allow(unused_imports)]
    pub use crate::{};
    pub use crate::{
        ArcticEntry, ArcticIndex, ArcticKey, ArcticMultiIndex, ArcticStringKey, AvailableIndex, BatchDeleteError,
        BatchInsertError, ClusteredColumnarIndex, ColumnCompression, ColumnSlotId, ColumnSlotId8, ColumnSlotId16,
        ColumnSlotId32, ColumnSlotId64, ColumnarColumn, ColumnarRowRef, CongeeIndex, CongeeKey, Database,
        DatabaseCatalog, Difference, GeneratedSystemCatalog, IndexError, IndexMap, IndexMultiMap, MultiPairRecreate,
        PersistentArcticIndex, PersistentArcticMultiIndex, PersistentArtIndex, PersistentCongeeIndex,
        PersistentWtiIndex, PrimaryIndex, TableIndex, TableIndexCdc, TableRow, TableSecondaryIndex,
        TableSecondaryIndexCdc, TableSecondaryIndexEventsOps, TableSecondaryIndexInfo, UniqueIndex, UnsizedNode,
        WorkTable, WorkTableError, next_columnar_incarnation, validate_arctic_link,
    };
    #[cfg(feature = "s3-support")]
    pub use crate::{DatabaseS3DiskConfig, DatabaseS3PersistenceEngine, S3Database};
    pub use crate::{LinearInsertError, LinearTable, VecTable};
    /// The upstream IndexSet backend, when the `vanilla-index` feature selects it.
    #[cfg(feature = "vanilla-index")]
    pub use crate::{UpstreamIndexMap, UpstreamIndexPair};
    pub use crate::{
        vacuum::EmptyDataVacuum, vacuum::VacuumPacing, vacuum::VacuumPersistence, vacuum::WorkTableVacuum,
    };
    /// `eyre` and `uuid`, for the same reason as `rkyv` above: `worktable!`
    /// expands in the consumer's crate, so every path it emits has to resolve
    /// there. Emitting a bare `eyre::` made that crate part of the macro's
    /// contract, and a consumer who never mentions eyre had to depend on it
    /// anyway to compile a table declaration.
    pub use crate::eyre;
    pub use ::uuid;
    pub use data_bucket::{
        DATA_VERSION, DataPage, GENERAL_HEADER_SIZE, GeneralHeader, GeneralPage, INNER_PAGE_SIZE, IndexPage, Interval,
        Link, PAGE_SIZE, PageType, Persistable, PersistableIndex, SizeMeasurable, SizeMeasure, SpaceInfoPage,
        TableOfContentsPage, UnsizedIndexPage, VariableSizeMeasurable, VariableSizeMeasure, align,
        data_page_row_capacity, map_data_pages_to_general, parse_data_page, parse_page, persist_page,
        seek_to_page_start, update_at,
    };
    pub use derive_more::{Display as MoreDisplay, From, Into};
    pub use indexset::{
        cdc::change::{ChangeEvent as IndexChangeEvent, Id as IndexChangeEventId},
        core::{multipair::MultiPair as IndexMultiPair, pair::Pair as IndexPair},
    };
    pub use ordered_float::OrderedFloat;
    pub use parking_lot::RwLock as ParkingRwLock;
    pub use parking_lot::RwLockReadGuard as ParkingRwLockReadGuard;
    /// rkyv itself, so a generated row can derive its traits without the
    /// consumer declaring rkyv. `worktable!`'s paged path still emits a bare
    /// `rkyv::` and is the remaining half of that leak.
    pub use rkyv;

    /// Node capacity representable by the persisted index's u16 slot format.
    pub fn get_index_page_size_from_data_length<T: Default + SizeMeasurable>(length: usize) -> usize {
        data_bucket::get_index_page_size_from_data_length::<T>(length).min(usize::from(u16::MAX))
    }
    pub use worktable_codegen::{MemStat, PersistIndex, PersistTable};

    pub const WT_INDEX_EXTENSION: &str = ".wt.idx";
    pub const WT_DATA_EXTENSION: &str = ".wt.data";

    #[cfg(feature = "s3-support")]
    pub use crate::features::{
        ExclusiveFencedLegacyDestination, LegacyPublicationSummary, LegacySnapshotConfig, LegacySnapshotStore,
        MigrationCaptureLimits, MigrationCaptureOutcome, MigrationCaptureProvenance, RestoredLegacySnapshot, S3Config,
        S3DiskConfig, S3SyncDiskPersistenceEngine,
    };
}
