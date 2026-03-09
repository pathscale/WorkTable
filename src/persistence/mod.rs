mod engine;
mod manager;
pub mod operation;
mod space;
mod task;

use crate::persistence::operation::BatchOperation;
pub use engine::DiskPersistenceEngine;
pub use manager::DiskConfig;
pub use operation::{
    DeleteOperation, InsertOperation, Operation, OperationId, OperationType, UpdateOperation,
    validate_events,
};
pub use space::{
    IndexTableOfContents, SpaceData, SpaceDataOps, SpaceIndex, SpaceIndexOps, SpaceIndexUnsized,
    SpaceSecondaryIndexOps, map_index_pages_to_toc_and_general,
    map_unsized_index_pages_to_toc_and_general,
};
use std::future::Future;
pub use task::PersistenceTask;

// Re-export for backward compatibility
pub use DiskConfig as PersistenceConfig;

pub trait PersistenceEngine<
    PrimaryKeyGenState,
    PrimaryKey,
    SecondaryIndexEvents,
    AvailableIndexes,
>
{
    type Config;

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
}
