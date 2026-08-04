use std::fmt::Debug;
use std::fs;
use std::future::Future;
use std::hash::Hash;
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::path::Path;

use futures::future::Either;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

use crate::TableSecondaryIndexEventsOps;
use crate::persistence::operation::{BatchOperation, Operation};
use crate::persistence::{
    PersistenceConfig, PersistenceEngine, PersistenceLoadError, SpaceDataOps, SpaceIndexOps, SpaceSecondaryIndexOps,
};
use crate::prelude::{PrimaryKeyGeneratorState, TablePrimaryKey, WT_DATA_EXTENSION};

fn classify_existing_store_error<T>(path: &str, existed: bool, result: eyre::Result<T>) -> eyre::Result<T> {
    result.map_err(|error| {
        if existed {
            PersistenceLoadError::corrupt(path, format!("{error:#}")).into()
        } else {
            error
        }
    })
}

async fn load_store_component<T, F>(path: &str, existed: bool, future: F) -> eyre::Result<T>
where
    F: Future<Output = eyre::Result<T>>,
{
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(result) => classify_existing_store_error(path, existed, result),
        Err(payload) if existed => {
            let reason = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("persisted-state loader panicked");
            Err(PersistenceLoadError::corrupt(path, reason).into())
        }
        Err(payload) => resume_unwind(payload),
    }
}

#[derive(Debug, Clone)]
pub struct DiskConfig {
    pub config_path: String,
    pub tables_path: String,
    pub version: u32,
}

impl DiskConfig {
    pub fn new<S1: Into<String>, S2: Into<String>>(config_path: S1, table_files_dir: S2, version: u32) -> Self {
        Self {
            config_path: config_path.into(),
            tables_path: table_files_dir.into(),
            version,
        }
    }

    pub fn new_with_table_name<S1: Into<String>, S2: AsRef<str>>(
        config_path: S1,
        table_name_snake_case: S2,
        version: u32,
    ) -> Self {
        let config_path = config_path.into();
        let table_name = table_name_snake_case.as_ref();
        let tables_path = format!("{}/{}", config_path.trim_end_matches('/'), table_name);
        Self {
            config_path,
            tables_path,
            version,
        }
    }
}

impl PersistenceConfig for DiskConfig {
    fn table_path(&self) -> &str {
        &self.tables_path
    }

    fn version(&self) -> u32 {
        self.version
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
> where
    PrimaryKey: TablePrimaryKey,
    <PrimaryKey as TablePrimaryKey>::Generator: PrimaryKeyGeneratorState,
{
    config: DiskConfig,
    pub data: SpaceData,
    pub primary_index: SpacePrimaryIndex,
    pub secondary_indexes: SpaceSecondaryIndexes,
    created_data_file: bool,
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
    SecondaryIndexEvents: Clone + Debug + Default + TableSecondaryIndexEventsOps<AvailableIndexes> + Send,
    PrimaryKeyGenState: Clone + Debug + Send,
    AvailableIndexes: Clone + Copy + Debug + Eq + Hash + Send,
{
    type Config = DiskConfig;

    async fn new(config: Self::Config) -> eyre::Result<Self>
    where
        Self: Sized,
    {
        let table_path = Path::new(&config.tables_path);
        let created_data_file = !table_path.join(WT_DATA_EXTENSION).exists();
        if !table_path.exists() {
            fs::create_dir_all(table_path)?;
        }
        let existed = !created_data_file;

        let data = load_store_component(
            &config.tables_path,
            existed,
            SpaceData::from_table_files_path(config.tables_path.clone(), config.version),
        )
        .await?;
        let primary_index = load_store_component(
            &config.tables_path,
            existed,
            SpacePrimaryIndex::primary_from_table_files_path(config.tables_path.clone(), config.version),
        )
        .await?;
        let secondary_indexes = load_store_component(
            &config.tables_path,
            existed,
            SpaceSecondaryIndexes::from_table_files_path(config.tables_path.clone(), config.version),
        )
        .await?;

        Ok(Self {
            config: config.clone(),
            data,
            primary_index,
            secondary_indexes,
            created_data_file,
            phantom_data: PhantomData,
        })
    }

    async fn apply_operation(
        &mut self,
        op: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents>,
    ) -> eyre::Result<()> {
        match op {
            Operation::Insert(insert) => {
                self.data.save_data(insert.link, insert.bytes.as_ref()).await?;
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
                self.data.save_data(update.link, update.bytes.as_ref()).await?;
                for event in update.primary_key_events {
                    self.primary_index.process_change_event(event).await?;
                }
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
            Operation::Acknowledge(_) => {
                // Acknowledge operations carry orphaned events for sequence continuity.
                Ok(())
            }
        }
    }

    async fn apply_batch_operation(
        &mut self,
        batch_op: BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents, AvailableIndexes>,
    ) -> eyre::Result<()> {
        let batch_data_op = batch_op.get_batch_data_op()?;

        let (pk_evs, secondary_evs) = batch_op.get_indexes_evs()?;
        {
            let data = &mut self.data;
            let primary_index = &mut self.primary_index;
            let secondary_indexes = &mut self.secondary_indexes;
            let mut futs = FuturesUnordered::new();
            futs.push(Either::Left(Either::Right(async move {
                data.save_batch_data(batch_data_op)
                    .await
                    .map_err(|e| e.wrap_err("batch data write"))
            })));
            futs.push(Either::Left(Either::Left(async move {
                primary_index
                    .process_change_event_batch(pk_evs)
                    .await
                    .map_err(|e| e.wrap_err("primary index batch apply"))
            })));
            futs.push(Either::Right(async move {
                secondary_indexes
                    .process_change_event_batch(secondary_evs)
                    .await
                    .map_err(|e| e.wrap_err("secondary index batch apply"))
            }));

            // Drain every future before surfacing errors: `?` on the first
            // failure would drop the FuturesUnordered and cancel the remaining
            // sub-operations at arbitrary await points, leaving e.g. a data
            // page half-written while its index events were abandoned. These
            // futures are not cancellation-safe, so let all started work run
            // to completion. Every failed component is reported (each error is
            // wrapped with its component name above); a mixed outcome means
            // the durable state may already be inconsistent, and knowing WHICH
            // parts failed is what an operator needs.
            let mut errors: Vec<eyre::Report> = Vec::new();
            while let Some(res) = futs.next().await {
                if let Err(e) = res {
                    errors.push(e);
                }
            }
            if errors.len() == 1 {
                return Err(errors.pop().expect("len checked"));
            }
            if !errors.is_empty() {
                let summary = errors.iter().map(|e| format!("{e:#}")).collect::<Vec<_>>().join("; ");
                return Err(eyre::eyre!(
                    "batch apply failed in {} sub-operations: {summary}",
                    errors.len()
                ));
            }
        }

        if let Some(pk_gen_state_update) = batch_op.get_pk_gen_state()? {
            let info = self.data.get_mut_info();
            info.inner.pk_gen_state = pk_gen_state_update;
            self.data.save_info().await?;
        }

        Ok(())
    }

    async fn ensure_schema(
        &mut self,
        row_schema: Vec<(String, String)>,
        primary_key_fields: Vec<String>,
        secondary_index_types: Vec<(String, String)>,
    ) -> eyre::Result<()> {
        let info = self.data.get_mut_info();
        let legacy_empty = info.inner.row_schema.is_empty()
            && info.inner.primary_key_fields.is_empty()
            && info.inner.secondary_index_types.is_empty();

        if legacy_empty {
            info.inner.row_schema = row_schema;
            info.inner.primary_key_fields = primary_key_fields;
            info.inner.secondary_index_types = secondary_index_types;
            return self.data.save_info().await;
        }

        if info.inner.row_schema != row_schema
            || info.inner.primary_key_fields != primary_key_fields
            || info.inner.secondary_index_types != secondary_index_types
        {
            return Err(eyre::eyre!(
                "persisted schema mismatch for {}: stored row schema {:?}, primary key {:?}, indexes {:?}; generated row schema {:?}, primary key {:?}, indexes {:?}",
                info.inner.name,
                info.inner.row_schema,
                info.inner.primary_key_fields,
                info.inner.secondary_index_types,
                row_schema,
                primary_key_fields,
                secondary_index_types,
            ));
        }

        Ok(())
    }

    async fn validate_schema(
        &mut self,
        row_schema: Vec<(String, String)>,
        primary_key_fields: Vec<String>,
        secondary_index_types: Vec<(String, String)>,
    ) -> eyre::Result<()> {
        let info = self.data.get_mut_info();
        let legacy_empty = info.inner.row_schema.is_empty()
            && info.inner.primary_key_fields.is_empty()
            && info.inner.secondary_index_types.is_empty();

        if legacy_empty {
            if self.created_data_file {
                info.inner.row_schema = row_schema;
                info.inner.primary_key_fields = primary_key_fields;
                info.inner.secondary_index_types = secondary_index_types;
                return self.data.save_info().await;
            }
            return Ok(());
        }

        if info.inner.row_schema != row_schema
            || info.inner.primary_key_fields != primary_key_fields
            || info.inner.secondary_index_types != secondary_index_types
        {
            return Err(eyre::eyre!(
                "persisted schema mismatch for {}: stored row schema {:?}, primary key {:?}, indexes {:?}; generated row schema {:?}, primary key {:?}, indexes {:?}",
                info.inner.name,
                info.inner.row_schema,
                info.inner.primary_key_fields,
                info.inner.secondary_index_types,
                row_schema,
                primary_key_fields,
                secondary_index_types,
            ));
        }

        Ok(())
    }

    fn config(&self) -> &DiskConfig {
        &self.config
    }
}
