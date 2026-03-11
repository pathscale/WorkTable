use std::future::Future;

use crate::persistence::operation::BatchOperation;

pub use engine::DiskConfig;
pub use engine::DiskPersistenceEngine;
pub use operation::{
    DeleteOperation, InsertOperation, Operation, OperationId, OperationType, UpdateOperation,
    validate_events,
};
pub use space::{
    IndexTableOfContents, SpaceData, SpaceDataOps, SpaceIndex, SpaceIndexOps, SpaceIndexUnsized,
    SpaceSecondaryIndexOps, map_index_pages_to_toc_and_general,
    map_unsized_index_pages_to_toc_and_general,
};
pub use task::PersistenceTask;

mod engine;
pub mod operation;
mod space;
mod task;

// TODO: remove this
pub trait PersistenceConfig {
    fn table_path(&self) -> &str;
}

pub trait PersistedWorkTable<E>: Sized
where
    E: Send,
{
    fn new(engine: E) -> impl Future<Output = eyre::Result<Self>> + Send;

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

    fn config(&self) -> &Self::Config;
}
