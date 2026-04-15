use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::path::Path;
use std::time::Duration;

use reqwest::Client;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use url::Url;
use walkdir::WalkDir;

use crate::persistence::operation::{BatchOperation, Operation};
use crate::persistence::{
    DiskConfig, DiskPersistenceEngine, PersistenceConfig, PersistenceEngine, SpaceDataOps,
    SpaceIndexOps, SpaceSecondaryIndexOps,
};
use crate::prelude::{
    PrimaryKeyGeneratorState, TablePrimaryKey, WT_DATA_EXTENSION, WT_INDEX_EXTENSION,
};
use crate::TableSecondaryIndexEventsOps;

#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket_name: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: Option<String>,
    pub prefix: Option<String>,
}

#[derive(Debug, Clone)]
pub struct S3DiskConfig {
    pub disk: DiskConfig,
    pub s3: S3Config,
}

impl PersistenceConfig for S3DiskConfig {
    fn table_path(&self) -> &str {
        self.disk.table_path()
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
>
where
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
    bucket: Bucket,
    credentials: Credentials,
    client: Client,
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
    SecondaryIndexEvents:
        Clone + Debug + Default + TableSecondaryIndexEventsOps<AvailableIndexes> + Send + Sync,
    PrimaryKeyGenState: Clone + Debug + Send + Sync,
    AvailableIndexes: Clone + Copy + Debug + Eq + Hash + Send + Sync,
{
    fn create_bucket(config: &S3Config) -> eyre::Result<(Bucket, Credentials, Client)> {
        let credentials = Credentials::new(&config.access_key, &config.secret_key);
        let endpoint: Url = config.endpoint.parse()?;
        let region = config.region.clone().unwrap_or_else(|| "auto".to_string());
        let bucket = Bucket::new(endpoint, UrlStyle::Path, config.bucket_name.clone(), region)?;

        let client = Client::builder().timeout(Duration::from_secs(30)).build()?;

        Ok((bucket, credentials, client))
    }

    async fn sync_to_s3(&self) -> eyre::Result<()> {
        let table_path = self.config.disk.table_path();
        let table_path = Path::new(table_path);
        let prefix = self.config.s3.prefix.as_deref().unwrap_or("");

        if !table_path.exists() {
            return Ok(());
        }

        for entry in WalkDir::new(table_path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
        {
            let local_path = entry.path();
            let relative = local_path.strip_prefix(table_path).unwrap_or(local_path);
            let table_name = table_path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| eyre::eyre!("Invalid table path"))?;
            let s3_key = Self::full_s3_path(prefix, &relative.to_string_lossy(), table_name);

            tracing::debug!(local_path = %local_path.display(), s3_key = %s3_key, "Uploading file to S3");

            let content = tokio::fs::read(local_path).await?;

            let action = self.bucket.put_object(Some(&self.credentials), &s3_key);
            let url = action.sign(Duration::from_secs(3600));

            self.client
                .put(url)
                .body(content)
                .send()
                .await?
                .error_for_status()?;
        }

        tracing::debug!("S3 sync complete");
        Ok(())
    }

    fn full_s3_path(prefix: &str, s3_path: &str, table_name: &str) -> String {
        let prefix = prefix.trim_end_matches('/');
        let path = s3_path.trim_start_matches('/');
        if prefix.is_empty() {
            format!("{}/{}", table_name, path)
        } else {
            format!("{}/{}/{}", prefix, table_name, path)
        }
    }

    async fn sync_from_s3(
        bucket: &Bucket,
        credentials: &Credentials,
        client: &Client,
        config: &S3DiskConfig,
    ) -> eyre::Result<()> {
        use rusty_s3::actions::ListObjectsV2;

        let table_path = config.disk.table_path();
        let table_path = Path::new(table_path);
        let prefix = config.s3.prefix.as_deref().unwrap_or("");

        let table_name = table_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| eyre::eyre!("Invalid table path"))?;

        let s3_path = Self::full_s3_path(prefix, "", table_name);

        let mut action = bucket.list_objects_v2(Some(credentials));
        action.with_prefix(&s3_path);
        action.with_delimiter("/");
        let url = action.sign(Duration::from_secs(3600));

        let response = client.get(url).send().await?.error_for_status()?;

        let text = response.text().await?;
        let parsed = ListObjectsV2::parse_response(&text)?;

        if parsed.contents.is_empty() {
            tracing::debug!(s3_prefix = %s3_path, "No objects found in S3");
            return Ok(());
        }

        tokio::fs::create_dir_all(table_path).await?;

        for obj in parsed.contents {
            let s3_key = &obj.key;

            let filename = s3_key.rsplit('/').next().unwrap_or(s3_key);

            if !filename.ends_with(WT_DATA_EXTENSION) && !filename.ends_with(WT_INDEX_EXTENSION) {
                tracing::debug!(s3_key = %s3_key, "Skipping non-table file");
                continue;
            }

            let local_path = table_path.join(filename);

            tracing::debug!(s3_key = %s3_key, local_path = %local_path.display(), "Downloading file from S3");

            let action = bucket.get_object(Some(credentials), s3_key);
            let url = action.sign(Duration::from_secs(3600));

            let response = client.get(url).send().await?.error_for_status()?;

            let content = response.bytes().await?;
            tokio::fs::write(&local_path, content).await?;
        }

        tracing::info!(table_name = %table_name, "S3 download sync complete");
        Ok(())
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
    SecondaryIndexEvents:
        Clone + Debug + Default + TableSecondaryIndexEventsOps<AvailableIndexes> + Send + Sync,
    PrimaryKeyGenState: Clone + Debug + Send + Sync,
    AvailableIndexes: Clone + Copy + Debug + Eq + Hash + Send + Sync,
{
    type Config = S3DiskConfig;

    async fn new(config: Self::Config) -> eyre::Result<Self>
    where
        Self: Sized,
    {
        let (bucket, credentials, client) = Self::create_bucket(&config.s3)?;

        if let Err(e) = Self::sync_from_s3(&bucket, &credentials, &client, &config).await {
            tracing::warn!(error = %e, "Failed to sync from S3, continuing with local files");
        }

        let inner = DiskPersistenceEngine::new(config.disk.clone()).await?;

        Ok(Self {
            inner,
            config,
            bucket,
            credentials,
            client,
            phantom: PhantomData,
        })
    }

    async fn apply_operation(
        &mut self,
        op: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents>,
    ) -> eyre::Result<()> {
        self.inner.apply_operation(op).await?;
        self.sync_to_s3().await?;
        Ok(())
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
        self.inner.apply_batch_operation(batch_op).await?;
        self.sync_to_s3().await?;
        Ok(())
    }

    fn config(&self) -> &Self::Config {
        &self.config
    }
}
