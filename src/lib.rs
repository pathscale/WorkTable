#![cfg_attr(not(feature = "std"), no_std)]
#![doc = include_str!("../docs/crate.md")]

#[macro_use]
extern crate alloc;

/// Generated code names `worktable::` paths, which must also resolve inside
/// this crate, where `worktable!` is invoked for the persistence queue.
extern crate self as worktable;

/// A fixed-capacity table whose rows are claimed without blocking.
pub mod atomic_key_table;
mod columnar;
#[cfg(feature = "std")]
pub mod fsx;
pub mod in_memory;
mod index;
pub mod lock;
mod mem_stat;
#[cfg(feature = "std")]
pub mod migration;
pub mod partition;
pub mod persistence;
/// Which async runtime a table's work runs on.
///
/// The module is available to a `no_std` build even though the backends inside
/// it are not. The trait and the profile machinery are types a table names, and
/// a table names them whether or not it ever spawns; only the impls need
/// threads, and those are gated within.
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
pub use features::{DatabaseS3DiskConfig, DatabaseS3PersistenceEngine};

pub use columnar::{
    ClusteredColumnarIndex, ColumnCompression, ColumnSlotId, ColumnSlotId8, ColumnSlotId16, ColumnSlotId32,
    ColumnSlotId64, ColumnarColumn, ColumnarRowRef, next_columnar_incarnation,
};
pub use index::*;
#[cfg(feature = "std")]
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
#[cfg(feature = "std")]
pub use worktable_dsl;

#[cfg(feature = "s3-support")]
pub use worktable_codegen::database_s3_persistence;
#[cfg(feature = "s3-support")]
pub use worktable_codegen::s3_sync_persistence;

/// Emits its body only when `worktable` itself was built with `std`.
///
/// `worktable!` expands in the consumer's crate, so a `#[cfg(feature = "std")]`
/// it emits would test the *consumer's* feature of that name, which is a
/// different flag or no flag at all. This macro is expanded here, against this
/// crate's features, and so says what the macro actually needs to ask: does the
/// `worktable` I am generating against have a disk and threads?
///
/// It exists for the generated `vacuum` method and the `ArtPersistenceKey`
/// impl. Both name types that are std-only for real reasons rather than by
/// grouping: `EmptyDataVacuum` is not empty despite the name and holds the
/// data pages, lock manager and persistence sink.
#[cfg(feature = "std")]
#[macro_export]
#[doc(hidden)]
macro_rules! __wt_if_std {
    ($($item:tt)*) => { $($item)* };
}

#[cfg(not(feature = "std"))]
#[macro_export]
#[doc(hidden)]
macro_rules! __wt_if_std {
    ($($item:tt)*) => {};
}

pub mod prelude {
    /// The filesystem this crate goes through. Generated code opens files by
    /// this path, so a consumer of `worktable!` gets the same backend the
    /// crate itself uses without naming it.
    #[cfg(feature = "std")]
    pub use crate::fsx;
    /// The runtime a table names, and the registry of pool flavors it can
    /// pick between. `worktable!` emits these type names, so they have to
    /// resolve in the consumer's crate for the same reason `fsx` does.
    pub use crate::runtime::{
        Elapsed, FLAVOR_COUNT, Flavor, FlavorMarker, Profile, Runtime, RuntimeJoinHandle, RuntimeNotified,
        RuntimeNotify, RuntimeRwLock, RuntimeSemaphore, RuntimeSemaphorePermit, RuntimeUnpinned, TableRuntime, Tuning,
    };
    /// The house backend and its pool flavors, plus the process-level
    /// selection a benchmark reads. Gated with the backends themselves: a
    /// `no_std` build has the trait but nothing that spawns.
    #[cfg(feature = "std")]
    pub use crate::runtime::{
        Locality, LowLatency, NagoyaRt, SharedSlot, Spread, Throughput, WideInjector, describe_tuning, engine_executor,
        engine_flavor, env_override, executor_for_flavor, parse_selection,
    };
    #[cfg(all(feature = "std", feature = "tokio-runtime"))]
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
    pub use crate::in_memory::{ArchivedRowWrapper, Data, DataPages, Query, RowWrapper, StorableRow};
    pub use crate::lock::FullRowLock;
    pub use crate::lock::{Lock, RowLock};
    pub use crate::lock::{LockAcquirer, LockGuard, LockMap, PendingLock};
    pub use crate::mem_stat::MemStat;
    pub use crate::partition::{DenseError, DenseRows, MAX_PARTITIONS, PartRef, PartitionError, PartitionSet};
    pub use crate::persistence::{AcknowledgeOperation, DeleteOperation, InsertOperation, Operation, OperationId};
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
    pub use crate::persistence::{OperationType, UpdateOperation, validate_events};
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
    /// The upstream IndexSet backend, when the `vanilla-index` feature selects it.
    #[cfg(feature = "vanilla-index")]
    pub use crate::{UpstreamIndexMap, UpstreamIndexPair};
    #[cfg(feature = "std")]
    pub use crate::{
        vacuum::EmptyDataVacuum, vacuum::VacuumPacing, vacuum::VacuumPersistence, vacuum::WorkTableVacuum,
    };
    /// `eyre` and `uuid`, for the same reason as `rkyv` above: `worktable!`
    /// expands in the consumer's crate, so every path it emits has to resolve
    /// there. Emitting a bare `eyre::` made that crate part of the macro's
    /// contract, and a consumer who never mentions eyre had to depend on it
    /// anyway to compile a table declaration.
    #[cfg(feature = "std")]
    pub use ::eyre;
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
    pub use crate::features::{S3Config, S3DiskConfig, S3SyncDiskPersistenceEngine};
}
