use std::future::Future;

use crate::persistence::operation::BatchOperation;

pub use engine::DiskConfig;
pub use engine::DiskPersistenceEngine;
pub use operation::{
    validate_events, DeleteOperation, InsertOperation, Operation, OperationId, OperationType,
    UpdateOperation,
};
pub use space::{
    map_index_pages_to_toc_and_general, map_unsized_index_pages_to_toc_and_general, IndexTableOfContents, SpaceData, SpaceDataOps, SpaceIndex,
    SpaceIndexOps, SpaceIndexUnsized,
    SpaceSecondaryIndexOps,
};
pub use task::PersistenceTask;

mod engine;
pub mod operation;
mod space;
mod task;

/// Trait for persistence configuration types.
/// Provides access to the table-specific path for file operations.
pub trait PersistenceConfig {
    /// Returns the table-specific path for this worktable
    fn table_path(&self) -> &str;
}

/// Trait for worktables that can be created/loaded from a persistence engine.
/// The engine must implement PersistenceEngine and provide access to its config.
pub trait PersistedWorkTable<E>: Sized
where
    E: Send,
{
    /// Create a new empty worktable with the given engine
    fn new(engine: E) -> impl Future<Output = eyre::Result<Self>> + Send;

    /// Load worktable from disk using the engine's config to find files.
    /// Falls back to `new()` if no files exist.
    fn load(engine: E) -> impl Future<Output = eyre::Result<Self>> + Send;
}

pub trait PersistenceEngine<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents, AvailableIndexes>
{
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
        batch_op: BatchOperation<
            PrimaryKeyGenState,
            PrimaryKey,
            SecondaryIndexEvents,
            AvailableIndexes,
        >,
    ) -> impl Future<Output = eyre::Result<()>> + Send;

    /// Returns the configuration used to create this engine
    fn config(&self) -> &Self::Config;
}
