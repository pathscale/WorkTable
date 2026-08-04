use std::future::Future;

use crate::persistence::operation::BatchOperation;

pub use engine::DiskConfig;
pub use engine::DiskPersistenceEngine;
pub use error::{PersistenceError, PersistenceResult, PersistenceState};
pub use operation::{
    AcknowledgeOperation, DeleteOperation, InsertOperation, Operation, OperationId, OperationType, UpdateOperation,
    validate_events,
};
pub use readonly_engine::ReadOnlyPersistenceEngine;
pub use space::{
    ArtPersistenceKey, IndexTableOfContents, SpaceArcticIndex, SpaceCongeeIndex, SpaceData, SpaceDataOps, SpaceIndex,
    SpaceIndexOps, SpaceIndexUnsized, SpaceSecondaryIndexOps, map_index_pages_to_toc_and_general,
    map_unsized_index_pages_to_toc_and_general, reconstruct_multi_index_nodes,
};
pub use task::PersistenceTask;

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

pub trait PersistedWorkTable<E>: Sized
where
    E: Send,
{
    fn new(engine: E) -> impl Future<Output = eyre::Result<Self>> + Send;

    fn load(engine: E) -> impl Future<Output = eyre::Result<Self>> + Send;
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
