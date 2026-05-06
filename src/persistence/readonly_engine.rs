use std::fmt::Debug;
use std::hash::Hash;

use crate::TableSecondaryIndexEventsOps;
use crate::persistence::operation::{BatchOperation, Operation};
use crate::persistence::{PersistenceConfig, PersistenceEngine};
use crate::prelude::{PrimaryKeyGeneratorState, TablePrimaryKey};

#[derive(Debug)]
pub struct ReadOnlyPersistenceEngine<C: PersistenceConfig> {
    config: C,
}

impl<C: PersistenceConfig + Send> ReadOnlyPersistenceEngine<C> {
    pub async fn create(config: C) -> eyre::Result<Self> {
        Ok(Self { config })
    }

    pub fn config_ref(&self) -> &C {
        &self.config
    }
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents, AvailableIndexes, C>
    PersistenceEngine<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents, AvailableIndexes>
    for ReadOnlyPersistenceEngine<C>
where
    C: PersistenceConfig + Send,
    PrimaryKey: Clone + Debug + Ord + TablePrimaryKey + Send,
    <PrimaryKey as TablePrimaryKey>::Generator: PrimaryKeyGeneratorState,
    SecondaryIndexEvents: Clone + Debug + Default + TableSecondaryIndexEventsOps<AvailableIndexes> + Send,
    PrimaryKeyGenState: Clone + Debug + Send,
    AvailableIndexes: Clone + Copy + Debug + Eq + Hash + Send,
{
    type Config = C;

    async fn new(config: Self::Config) -> eyre::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self { config })
    }

    async fn apply_operation(
        &mut self,
        _op: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents>,
    ) -> eyre::Result<()> {
        Ok(())
    }

    async fn apply_batch_operation(
        &mut self,
        _batch_op: BatchOperation<
            PrimaryKeyGenState,
            PrimaryKey,
            SecondaryIndexEvents,
            AvailableIndexes,
        >,
    ) -> eyre::Result<()> {
        Ok(())
    }

    fn config(&self) -> &Self::Config {
        &self.config
    }
}
