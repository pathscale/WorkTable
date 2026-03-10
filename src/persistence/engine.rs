use std::fmt::Debug;
use std::fs;
use std::hash::Hash;
use std::marker::PhantomData;
use std::path::Path;

use futures::future::Either;
use futures::stream::FuturesUnordered;
use futures::StreamExt;

use crate::persistence::operation::{BatchOperation, Operation};
use crate::persistence::{PersistenceEngine, SpaceDataOps, SpaceIndexOps, SpaceSecondaryIndexOps};
use crate::prelude::{PrimaryKeyGeneratorState, TablePrimaryKey};
use crate::TableSecondaryIndexEventsOps;

#[derive(Debug, Clone)]
pub struct DiskConfig {
    pub config_path: String,
    pub tables_path: String,
}

impl DiskConfig {
    pub fn new<S1: Into<String>, S2: Into<String>>(config_path: S1, table_files_dir: S2) -> Self {
        Self {
            config_path: config_path.into(),
            tables_path: table_files_dir.into(),
        }
    }
}

#[derive(Debug)]
pub struct DiskPersistenceEngine<
    SpaceData,
    SpacePrimaryIndex,
    SpaceSecondaryIndexes,
    PrimaryKey,
    SecondaryIndexEvents,
    AvailableIndexes,
    PrimaryKeyGenState = <<PrimaryKey as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
>
where
    PrimaryKey: TablePrimaryKey,
    <PrimaryKey as TablePrimaryKey>::Generator: PrimaryKeyGeneratorState
{
    pub data: SpaceData,
    pub primary_index: SpacePrimaryIndex,
    pub secondary_indexes: SpaceSecondaryIndexes,
    phantom_data: PhantomData<(PrimaryKey, SecondaryIndexEvents, PrimaryKeyGenState, AvailableIndexes)>,
}

impl<
    SpaceData,
    SpacePrimaryIndex,
    SpaceSecondaryIndexes,
    PrimaryKey,
    SecondaryIndexEvents,
    AvailableIndexes,
    PrimaryKeyGenState,
> PersistenceEngine<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents, AvailableIndexes>
    for DiskPersistenceEngine<
        SpaceData,
        SpacePrimaryIndex,
        SpaceSecondaryIndexes,
        PrimaryKey,
        SecondaryIndexEvents,
        AvailableIndexes,
        PrimaryKeyGenState,
    >
where
    PrimaryKey: Clone + Debug + Ord + TablePrimaryKey + Send,
    <PrimaryKey as TablePrimaryKey>::Generator: PrimaryKeyGeneratorState,
    SpaceData: SpaceDataOps<PrimaryKeyGenState> + Send,
    SpacePrimaryIndex: SpaceIndexOps<PrimaryKey> + Send,
    SpaceSecondaryIndexes: SpaceSecondaryIndexOps<SecondaryIndexEvents> + Send,
    SecondaryIndexEvents:
        Clone + Debug + Default + TableSecondaryIndexEventsOps<AvailableIndexes> + Send,
    PrimaryKeyGenState: Clone + Debug + Send,
    AvailableIndexes: Clone + Copy + Debug + Eq + Hash + Send,
{
    type Config = DiskConfig;

    async fn new(config: Self::Config) -> eyre::Result<Self>
    where
        Self: Sized,
    {
        let table_path = Path::new(&config.tables_path);
        if !table_path.exists() {
            fs::create_dir_all(table_path)?;
        }

        Ok(Self {
            data: SpaceData::from_table_files_path(config.tables_path.clone()).await?,
            primary_index: SpacePrimaryIndex::primary_from_table_files_path(
                config.tables_path.clone(),
            )
            .await?,
            secondary_indexes: SpaceSecondaryIndexes::from_table_files_path(config.tables_path)
                .await?,
            phantom_data: PhantomData,
        })
    }
    async fn apply_operation(
        &mut self,
        op: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents>,
    ) -> eyre::Result<()> {
        match op {
            Operation::Insert(insert) => {
                self.data
                    .save_data(insert.link, insert.bytes.as_ref())
                    .await?;
                for event in insert.primary_key_events {
                    self.primary_index.process_change_event(event).await?;
                }
                let info = self.data.get_mut_info();
                info.inner.pk_gen_state = insert.pk_gen_state;
                self.data.save_info().await?;
                self.secondary_indexes
                    .process_change_events(insert.secondary_keys_events)
                    .await
            }
            Operation::Update(update) => {
                self.data
                    .save_data(update.link, update.bytes.as_ref())
                    .await?;
                self.secondary_indexes
                    .process_change_events(update.secondary_keys_events)
                    .await
            }
            Operation::Delete(delete) => {
                for event in delete.primary_key_events {
                    self.primary_index.process_change_event(event).await?;
                }
                self.secondary_indexes
                    .process_change_events(delete.secondary_keys_events)
                    .await
            }
        }
    }

    async fn apply_batch_operation(
        &mut self,
        batch_op: BatchOperation<
            PrimaryKeyGenState,
            PrimaryKey,
            SecondaryIndexEvents,
            AvailableIndexes,
        >,
    ) -> eyre::Result<()> {
        let batch_data_op = batch_op.get_batch_data_op()?;

        let (pk_evs, secondary_evs) = batch_op.get_indexes_evs()?;
        {
            let mut futs = FuturesUnordered::new();
            futs.push(Either::Left(Either::Right(
                self.data.save_batch_data(batch_data_op),
            )));
            futs.push(Either::Left(Either::Left(
                self.primary_index.process_change_event_batch(pk_evs),
            )));
            futs.push(Either::Right(
                self.secondary_indexes
                    .process_change_event_batch(secondary_evs),
            ));

            while (futs.next().await).is_some() {}
        }

        if let Some(pk_gen_state_update) = batch_op.get_pk_gen_state()? {
            let info = self.data.get_mut_info();
            info.inner.pk_gen_state = pk_gen_state_update;
            self.data.save_info().await?;
        }

        Ok(())
    }
}
