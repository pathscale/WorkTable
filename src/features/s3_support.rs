//! Typed compatibility adapter; DataBucket owns all physical snapshot storage.
use crate::TableSecondaryIndexEventsOps;
use crate::persistence::operation::{BatchOperation, Operation};
use crate::persistence::{
    DiskConfig, DiskPersistenceEngine, PersistenceConfig, PersistenceEngine, SpaceDataOps, SpaceIndexOps,
    SpaceSecondaryIndexOps,
};
use crate::prelude::{PrimaryKeyGeneratorState, TablePrimaryKey};
use alloc::string::ToString;
use core::{fmt::Debug, hash::Hash, marker::PhantomData};
use data_bucket::storage::legacy_snapshot::HttpAgent as Agent;
pub use data_bucket::storage::legacy_snapshot::{
    ExclusiveFencedLegacyDestination, LegacyPublicationSummary, LegacyS3Config as S3Config, LegacySnapshotConfig,
    LegacySnapshotStore, MigrationCaptureLimits, MigrationCaptureOutcome, MigrationCaptureProvenance,
    RestoredLegacySnapshot,
};
use rkyv::Archive;
#[derive(Debug, Clone)]
pub struct S3DiskConfig {
    pub disk: DiskConfig,
    pub s3: S3Config,
}

impl PersistenceConfig for S3DiskConfig {
    fn table_path(&self) -> &str {
        self.disk.table_path()
    }

    fn version(&self) -> u32 {
        self.disk.version()
    }
}

#[derive(Debug)]
pub struct S3SyncDiskPersistenceEngine<
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
    inner: DiskPersistenceEngine<
        SpaceData,
        SpacePrimaryIndex,
        SpaceSecondaryIndexes,
        PrimaryKey,
        SecondaryIndexEvents,
        AvailableIndexes,
        PrimaryKeyGenState,
    >,
    config: S3DiskConfig,
    snapshot: LegacySnapshotStore,
    phantom: PhantomData<(PrimaryKey, SecondaryIndexEvents, PrimaryKeyGenState, AvailableIndexes)>,
}

impl<
    SpaceData,
    SpacePrimaryIndex,
    SpaceSecondaryIndexes,
    PrimaryKey,
    SecondaryIndexEvents,
    AvailableIndexes,
    PrimaryKeyGenState,
>
    S3SyncDiskPersistenceEngine<
        SpaceData,
        SpacePrimaryIndex,
        SpaceSecondaryIndexes,
        PrimaryKey,
        SecondaryIndexEvents,
        AvailableIndexes,
        PrimaryKeyGenState,
    >
where
    PrimaryKey: Clone + Debug + Ord + TablePrimaryKey + Send + Sync,
    <PrimaryKey as TablePrimaryKey>::Generator: PrimaryKeyGeneratorState,
    SpaceData: SpaceDataOps<PrimaryKeyGenState> + Send + Sync,
    SpacePrimaryIndex: SpaceIndexOps<PrimaryKey> + Send + Sync,
    SpaceSecondaryIndexes: SpaceSecondaryIndexOps<SecondaryIndexEvents> + Send + Sync,
    SecondaryIndexEvents: Clone + Debug + Default + TableSecondaryIndexEventsOps<AvailableIndexes> + Send + Sync,
    PrimaryKeyGenState: Clone + Debug + Send + Sync,
    AvailableIndexes: Clone + Copy + Debug + Eq + Hash + Send + Sync,
{
    /// Initialize from a snapshot that the caller already restored and preflighted.
    ///
    /// The required index files and logical table version are checked before
    /// opening the writable disk engine. Token identity is checked before
    /// reading the local header. Adoption does not perform another remote restore.
    /// The caller must declare the source state layout from known provenance.
    pub async fn from_restored_snapshot<SourcePkGenState>(
        config: S3DiskConfig,
        restored: RestoredLegacySnapshot,
        source_layout: crate::migration::SourceLayout<SourcePkGenState>,
    ) -> eyre::Result<Self>
    where
        SourcePkGenState: Archive,
        SourcePkGenState::Archived:
            for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    {
        let snapshot_config = LegacySnapshotConfig {
            table_path: config.disk.table_path().to_string(),
            s3: config.s3.clone(),
        };
        let primary_files = SpacePrimaryIndex::REQUIRED_PRIMARY_INDEX_FILES
            .ok_or_else(|| eyre::eyre!("primary index implementation does not declare its required files"))?;
        let secondary_files = SpaceSecondaryIndexes::REQUIRED_SECONDARY_INDEX_FILES
            .ok_or_else(|| eyre::eyre!("secondary index implementation does not declare its required files"))?;
        let table_path = std::path::Path::new(config.disk.table_path());
        for &name in primary_files.iter().chain(secondary_files) {
            let path = table_path.join(name);
            eyre::ensure!(
                restored.restored_files().contains(&path)
                    && std::fs::symlink_metadata(&path).is_ok_and(|metadata| metadata.is_file()),
                "required index file {} is missing from restored snapshot",
                path.display()
            );
        }

        let mut snapshot = restored.into_store_for(&snapshot_config)?;
        let stored_version = crate::migration::detect_source_version(config.disk.table_path(), source_layout).await?;
        eyre::ensure!(
            stored_version == config.disk.version,
            "stored table version {stored_version} does not match configured version {}",
            config.disk.version
        );
        for &name in primary_files {
            SpacePrimaryIndex::verify_adoption_file(&table_path.join(name), config.disk.version).await?;
        }
        // Secondary verification is static and read-only; unsupported custom
        // implementations keep the trait default refusal.
        SpaceSecondaryIndexes::verify_adoption(table_path, config.disk.version).await?;

        let inner = DiskPersistenceEngine::new(config.disk.clone()).await?;
        snapshot.capture_baseline()?;
        Ok(Self {
            inner,
            config,
            snapshot,
            phantom: PhantomData,
        })
    }

    /// Open with a caller-owned HTTP agent, including its TLS trust policy.
    pub async fn new_with_agent(config: S3DiskConfig, client: Agent) -> eyre::Result<Self> {
        let mut snapshot = LegacySnapshotStore::open_with_agent(
            LegacySnapshotConfig {
                table_path: config.disk.table_path().to_string(),
                s3: config.s3.clone(),
            },
            client,
        )
        .await?;
        let inner = DiskPersistenceEngine::new(config.disk.clone()).await?;
        snapshot.capture_baseline()?;
        Ok(Self {
            inner,
            config,
            snapshot,
            phantom: PhantomData,
        })
    }
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
    for S3SyncDiskPersistenceEngine<
        SpaceData,
        SpacePrimaryIndex,
        SpaceSecondaryIndexes,
        PrimaryKey,
        SecondaryIndexEvents,
        AvailableIndexes,
        PrimaryKeyGenState,
    >
where
    PrimaryKey: Clone + Debug + Ord + TablePrimaryKey + Send + Sync,
    <PrimaryKey as TablePrimaryKey>::Generator: PrimaryKeyGeneratorState,
    SpaceData: SpaceDataOps<PrimaryKeyGenState> + Send + Sync,
    SpacePrimaryIndex: SpaceIndexOps<PrimaryKey> + Send + Sync,
    SpaceSecondaryIndexes: SpaceSecondaryIndexOps<SecondaryIndexEvents> + Send + Sync,
    SecondaryIndexEvents: Clone + Debug + Default + TableSecondaryIndexEventsOps<AvailableIndexes> + Send + Sync,
    PrimaryKeyGenState: Clone + Debug + Send + Sync,
    AvailableIndexes: Clone + Copy + Debug + Eq + Hash + Send + Sync,
{
    type Config = S3DiskConfig;

    async fn new(config: Self::Config) -> eyre::Result<Self>
    where
        Self: Sized,
    {
        let mut snapshot = LegacySnapshotStore::open(LegacySnapshotConfig {
            table_path: config.disk.table_path().to_string(),
            s3: config.s3.clone(),
        })
        .await?;
        let inner = DiskPersistenceEngine::new(config.disk.clone()).await?;
        snapshot.capture_baseline()?;
        Ok(Self {
            inner,
            config,
            snapshot,
            phantom: PhantomData,
        })
    }

    async fn apply_operation(
        &mut self,
        op: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents>,
    ) -> eyre::Result<()> {
        self.inner.apply_operation(op).await?;
        self.snapshot.sync().await
    }

    async fn initialize_schema_name(&mut self, name: alloc::string::String) -> eyre::Result<()> {
        self.inner.initialize_schema_name(name).await
    }

    async fn ensure_schema(
        &mut self,
        row_schema: alloc::vec::Vec<(alloc::string::String, alloc::string::String)>,
        primary_key_fields: alloc::vec::Vec<alloc::string::String>,
        secondary_index_types: alloc::vec::Vec<(alloc::string::String, alloc::string::String)>,
    ) -> eyre::Result<()> {
        self.inner
            .ensure_schema(row_schema, primary_key_fields, secondary_index_types)
            .await
    }

    async fn validate_schema(
        &mut self,
        row_schema: alloc::vec::Vec<(alloc::string::String, alloc::string::String)>,
        primary_key_fields: alloc::vec::Vec<alloc::string::String>,
        secondary_index_types: alloc::vec::Vec<(alloc::string::String, alloc::string::String)>,
    ) -> eyre::Result<()> {
        self.inner
            .validate_schema(row_schema, primary_key_fields, secondary_index_types)
            .await
    }

    async fn apply_batch_operation(
        &mut self,
        batch_op: BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents, AvailableIndexes>,
    ) -> eyre::Result<()> {
        self.inner.apply_batch_operation(batch_op).await?;
        self.snapshot.sync().await
    }

    fn config(&self) -> &Self::Config {
        &self.config
    }
}
