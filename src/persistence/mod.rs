use std::future::Future;

use data_bucket::page::PageId;

use crate::persistence::operation::BatchOperation;

pub use engine::DiskConfig;
pub use engine::DiskPersistenceEngine;
pub use error::{
    PersistenceError, PersistenceIndexCorruption, PersistenceLoadError, PersistenceResult, PersistenceState,
    load_persisted_state,
};
pub use operation::{
    AcknowledgeOperation, DeleteOperation, InsertOperation, Operation, OperationId, OperationType, UpdateOperation,
    validate_events,
};
pub use readonly_engine::ReadOnlyPersistenceEngine;
pub use space::{
    ArtPersistenceKey, IndexTableOfContents, SpaceArcticIndex, SpaceCongeeIndex, SpaceData, SpaceDataOps, SpaceIndex,
    SpaceIndexOps, SpaceIndexUnsized, SpaceLogicalIndex, SpaceLogicalIndexUnsized, SpaceSecondaryIndexOps,
    TocEntryOversizedError, map_index_pages_to_toc_and_general, map_unsized_index_pages_to_toc_and_general,
    reconstruct_multi_index_nodes,
};
pub use task::{PersistenceMonitor, PersistenceTask};

mod engine;
mod error;
pub mod operation;
mod readonly_engine;
mod space;
mod task;

// TODO: remove this
pub trait PersistenceConfig {
    fn table_path(&self) -> &str;

    fn version(&self) -> u32;
}

/// Controls the consistency checks applied while loading persisted state.
///
/// Normal application opens must use [`LoadMode::Strict`], which is also the
/// default used by [`PersistedWorkTable::load`]. Recovery tools may use
/// [`LoadMode::Recovery`] on a private copy of a rejected store to read rows
/// through a surviving index and rebuild them into a fresh table.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LoadMode {
    /// Reject disagreement between the primary index, secondary indexes, and
    /// rows before exposing the table.
    #[default]
    Strict,
    /// Permit indexes to contain different sets of otherwise valid rows.
    ///
    /// This mode does not disable file parsing, checked row decoding, or
    /// per-entry key/link validation. It is only for offline recovery; never
    /// use the returned table to serve live traffic.
    Recovery,
}

pub trait PersistedWorkTable<E>: Sized
where
    E: Send,
{
    fn new(engine: E) -> impl Future<Output = eyre::Result<Self>> + Send;

    fn load(engine: E) -> impl Future<Output = eyre::Result<Self>> + Send;

    /// Loads a table with an explicit consistency policy.
    ///
    /// The compatibility default forwards to [`Self::load`], so custom
    /// persistence implementations remain strict unless they deliberately
    /// implement recovery semantics. WorkTable's generated disk and read-only
    /// implementations override this method.
    fn load_with(engine: E, _mode: LoadMode) -> impl Future<Output = eyre::Result<Self>> + Send {
        Self::load(engine)
    }
}

pub trait PersistenceEngine<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents, AvailableIndexes> {
    type Config: PersistenceConfig;

    fn new(config: Self::Config) -> impl Future<Output = eyre::Result<Self>> + Send
    where
        Self: Sized;

    fn apply_operation(
        &mut self,
        op: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents>,
    ) -> impl Future<Output = eyre::Result<()>> + Send;

    fn apply_batch_operation(
        &mut self,
        batch_op: BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents, AvailableIndexes>,
    ) -> impl Future<Output = eyre::Result<()>> + Send;

    /// Persists whole data pages made reusable by vacuum.
    ///
    /// The persistence task invokes this only after every row move queued
    /// before the reclamation barrier has reached the engine. Custom engines
    /// that do not manage data pages may keep the default no-op.
    fn reclaim_data_pages(&mut self, _page_ids: Vec<PageId>) -> impl Future<Output = eyre::Result<()>> + Send {
        async { Ok(()) }
    }

    /// Installs the generated table schema and rejects a non-empty schema that
    /// belongs to a different table shape.
    /// Custom engines may keep the default no-op when they do not expose
    /// WorkTable data files.
    fn ensure_schema(
        &mut self,
        _row_schema: Vec<(String, String)>,
        _primary_key_fields: Vec<String>,
        _secondary_index_types: Vec<(String, String)>,
    ) -> impl Future<Output = eyre::Result<()>> + Send {
        async { Ok(()) }
    }

    /// Validates the generated schema while loading an existing store.
    ///
    /// Disk engines leave legacy stores with empty schema metadata unchanged,
    /// so opening an old database does not mutate it as a side effect. A newly
    /// created store may install its schema from this method. Custom engines
    /// may keep the default no-op.
    fn validate_schema(
        &mut self,
        _row_schema: Vec<(String, String)>,
        _primary_key_fields: Vec<String>,
        _secondary_index_types: Vec<(String, String)>,
    ) -> impl Future<Output = eyre::Result<()>> + Send {
        async { Ok(()) }
    }

    fn config(&self) -> &Self::Config;
}
