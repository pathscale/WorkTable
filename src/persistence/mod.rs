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

pub trait PersistenceEngine<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents, AvailableIndexes>
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
