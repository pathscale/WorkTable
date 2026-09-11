use alloc::{format, string::String, string::ToString, vec::Vec};
use core::fmt::{Debug, Write as _};
use core::hash::Hash;
use core::marker::PhantomData;
use core::time::Duration;
use std::collections::HashSet;
use std::io::{Read as _, Write as _};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use ureq::Agent;
use url::Url;
use walkdir::WalkDir;

use crate::TableSecondaryIndexEventsOps;
use crate::persistence::operation::{BatchOperation, Operation};
use crate::persistence::{
    DiskConfig, DiskPersistenceEngine, PersistenceConfig, PersistenceEngine, SpaceDataOps, SpaceIndexOps,
    SpaceSecondaryIndexOps,
};
use crate::prelude::{PrimaryKeyGeneratorState, TablePrimaryKey, WT_DATA_EXTENSION, WT_INDEX_EXTENSION};

const MANIFEST_FILE: &str = "manifest.v1";
const MANIFEST_MAGIC: &[u8; 8] = b"WTS3M001";
const CHUNK_SIZE: usize = 4 * 1024 * 1024;
const MAX_MANIFEST_FILES: usize = 16_384;
const MAX_MANIFEST_CHUNKS: usize = 1_048_576;

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

    fn version(&self) -> u32 {
        self.disk.version()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChunkRef {
    length: u32,
    hash: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ManifestFile {
    path: String,
    length: u64,
    chunks: Vec<ChunkRef>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct TableManifest {
    files: Vec<ManifestFile>,
}

impl TableManifest {
    fn encode(&self) -> eyre::Result<Vec<u8>> {
        let file_count = u32::try_from(self.files.len()).map_err(|_| eyre::eyre!("too many S3 manifest files"))?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MANIFEST_MAGIC);
        bytes.extend_from_slice(&file_count.to_le_bytes());

        for file in &self.files {
            validate_relative_path(&file.path)?;
            let path = file.path.as_bytes();
            let path_len = u16::try_from(path.len()).map_err(|_| eyre::eyre!("S3 manifest path is too long"))?;
            let chunk_count =
                u32::try_from(file.chunks.len()).map_err(|_| eyre::eyre!("too many chunks in S3 manifest"))?;
            bytes.extend_from_slice(&path_len.to_le_bytes());
            bytes.extend_from_slice(path);
            bytes.extend_from_slice(&file.length.to_le_bytes());
            bytes.extend_from_slice(&chunk_count.to_le_bytes());
            for chunk in &file.chunks {
                bytes.extend_from_slice(&chunk.length.to_le_bytes());
                bytes.extend_from_slice(&chunk.hash);
            }
        }

        let checksum = blake3::hash(&bytes);
        bytes.extend_from_slice(checksum.as_bytes());
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> eyre::Result<Self> {
        if bytes.len() < MANIFEST_MAGIC.len() + 4 + 32 {
            return Err(eyre::eyre!("S3 manifest is truncated"));
        }
        let (payload, checksum) = bytes.split_at(bytes.len() - 32);
        if blake3::hash(payload).as_bytes() != checksum {
            return Err(eyre::eyre!("S3 manifest checksum mismatch"));
        }

        let mut reader = ManifestReader::new(payload);
        if reader.take(MANIFEST_MAGIC.len())? != MANIFEST_MAGIC {
            return Err(eyre::eyre!("unsupported S3 manifest format"));
        }
        let file_count = reader.u32()? as usize;
        if file_count > MAX_MANIFEST_FILES {
            return Err(eyre::eyre!("S3 manifest contains too many files"));
        }

        let mut files = Vec::with_capacity(file_count);
        let mut previous_path: Option<String> = None;
        let mut total_chunks = 0_usize;
        for _ in 0..file_count {
            let path_len = reader.u16()? as usize;
            if path_len == 0 {
                return Err(eyre::eyre!("S3 manifest contains an empty path"));
            }
            let path = core::str::from_utf8(reader.take(path_len)?)?.to_string();
            validate_relative_path(&path)?;
            if previous_path.as_ref().is_some_and(|previous| previous >= &path) {
                return Err(eyre::eyre!("S3 manifest file paths are not strictly sorted"));
            }
            previous_path = Some(path.clone());

            let length = reader.u64()?;
            let chunk_count = reader.u32()? as usize;
            total_chunks = total_chunks
                .checked_add(chunk_count)
                .ok_or_else(|| eyre::eyre!("S3 manifest chunk count overflow"))?;
            if total_chunks > MAX_MANIFEST_CHUNKS {
                return Err(eyre::eyre!("S3 manifest contains too many chunks"));
            }
            let expected_chunks = if length == 0 {
                0
            } else {
                usize::try_from(length.div_ceil(CHUNK_SIZE as u64))?
            };
            if chunk_count != expected_chunks {
                return Err(eyre::eyre!("S3 manifest chunk count does not match file length"));
            }

            let mut chunks = Vec::with_capacity(chunk_count);
            let mut described_length = 0_u64;
            for index in 0..chunk_count {
                let chunk_length = reader.u32()?;
                if chunk_length == 0 || chunk_length as usize > CHUNK_SIZE {
                    return Err(eyre::eyre!("S3 manifest contains an invalid chunk length"));
                }
                if index + 1 != chunk_count && chunk_length as usize != CHUNK_SIZE {
                    return Err(eyre::eyre!("S3 manifest contains a short interior chunk"));
                }
                let mut hash = [0_u8; 32];
                let hash_length = hash.len();
                hash.copy_from_slice(reader.take(hash_length)?);
                described_length = described_length
                    .checked_add(u64::from(chunk_length))
                    .ok_or_else(|| eyre::eyre!("S3 manifest file length overflow"))?;
                chunks.push(ChunkRef {
                    length: chunk_length,
                    hash,
                });
            }
            if described_length != length {
                return Err(eyre::eyre!("S3 manifest chunks do not cover the file length"));
            }
            files.push(ManifestFile { path, length, chunks });
        }

        if !reader.is_empty() {
            return Err(eyre::eyre!("S3 manifest has trailing data"));
        }
        Ok(Self { files })
    }

    fn committed_chunks(&self) -> HashSet<[u8; 32]> {
        self.files
            .iter()
            .flat_map(|file| file.chunks.iter().map(|chunk| chunk.hash))
            .collect()
    }
}

struct ManifestReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> ManifestReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> eyre::Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| eyre::eyre!("S3 manifest offset overflow"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| eyre::eyre!("S3 manifest is truncated"))?;
        self.position = end;
        Ok(value)
    }

    fn u16(&mut self) -> eyre::Result<u16> {
        let mut bytes = [0_u8; 2];
        let length = bytes.len();
        bytes.copy_from_slice(self.take(length)?);
        Ok(u16::from_le_bytes(bytes))
    }

    fn u32(&mut self) -> eyre::Result<u32> {
        let mut bytes = [0_u8; 4];
        let length = bytes.len();
        bytes.copy_from_slice(self.take(length)?);
        Ok(u32::from_le_bytes(bytes))
    }

    fn u64(&mut self) -> eyre::Result<u64> {
        let mut bytes = [0_u8; 8];
        let length = bytes.len();
        bytes.copy_from_slice(self.take(length)?);
        Ok(u64::from_le_bytes(bytes))
    }

    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
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
    bucket: Bucket,
    credentials: Credentials,
    client: Agent,
    committed_manifest: Option<TableManifest>,
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
    fn create_bucket(config: &S3Config) -> eyre::Result<(Bucket, Credentials, Agent)> {
        let credentials = Credentials::new(&config.access_key, &config.secret_key);
        let endpoint: Url = config.endpoint.parse()?;
        let region = config.region.clone().unwrap_or_else(|| "auto".to_string());
        let bucket = Bucket::new(endpoint, UrlStyle::Path, config.bucket_name.clone(), region)?;

        // Blocking, like every other I/O call in this crate. The persistence
        // engine owns its thread, so a request that blocks it is the right
        // execution shape and does not require a Tokio reactor.
        let client = ureq::AgentBuilder::new().timeout(Duration::from_secs(30)).build();

        Ok((bucket, credentials, client))
    }

    fn table_name(config: &S3DiskConfig) -> eyre::Result<&str> {
        Path::new(config.disk.table_path())
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| eyre::eyre!("invalid table path"))
    }

    fn full_s3_path(prefix: &str, s3_path: &str, table_name: &str) -> String {
        let prefix = prefix.trim_end_matches('/');
        let path = s3_path.trim_start_matches('/');
        if prefix.is_empty() {
            format!("{table_name}/{path}")
        } else {
            format!("{prefix}/{table_name}/{path}")
        }
    }

    fn object_key(&self, path: &str) -> eyre::Result<String> {
        Ok(Self::full_s3_path(
            self.config.s3.prefix.as_deref().unwrap_or(""),
            path,
            Self::table_name(&self.config)?,
        ))
    }

    fn chunk_path(hash: &[u8; 32]) -> String {
        let mut hex = String::with_capacity(64);
        for byte in hash {
            write!(&mut hex, "{byte:02x}").expect("writing to String cannot fail");
        }
        format!("chunks/{hex}")
    }

    fn get_object_optional(
        bucket: &Bucket,
        credentials: &Credentials,
        client: &Agent,
        key: &str,
    ) -> eyre::Result<Option<Vec<u8>>> {
        let action = bucket.get_object(Some(credentials), key);
        let url = action.sign(Duration::from_secs(3600));
        let response = match client.get(url.as_str()).call() {
            Ok(response) => response,
            Err(ureq::Error::Status(404, _)) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let mut bytes = Vec::new();
        response.into_reader().read_to_end(&mut bytes)?;
        Ok(Some(bytes))
    }

    fn put_object_verified(&self, key: &str, bytes: &[u8]) -> eyre::Result<()> {
        let action = self.bucket.put_object(Some(&self.credentials), key);
        let url = action.sign(Duration::from_secs(3600));
        match self.client.put(url.as_str()).send_bytes(bytes) {
            Ok(_) => Ok(()),
            Err(put_error) => {
                // A connection can fail after the object service committed the
                // PUT. Resolve that ambiguity before reporting failure; the
                // caller must never repeat a local database mutation merely to
                // discover that its manifest was already published.
                let stored = Self::get_object_optional(&self.bucket, &self.credentials, &self.client, key)?;
                if stored.as_deref() == Some(bytes) {
                    Ok(())
                } else {
                    Err(put_error.into())
                }
            }
        }
    }

    async fn sync_to_s3(&mut self) -> eyre::Result<()> {
        let table_path = Path::new(self.config.disk.table_path());
        if !table_path.exists() {
            return Ok(());
        }

        let mut local_files = WalkDir::new(table_path)
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|entry| entry.file_type().is_file())
            .filter(|entry| is_table_file(entry.path()))
            .map(|entry| {
                let path = entry.path().to_path_buf();
                let relative = canonical_relative_path(table_path, &path)?;
                Ok((relative, path))
            })
            .collect::<eyre::Result<Vec<_>>>()?;
        local_files.sort_by(|left, right| left.0.cmp(&right.0));

        let committed_chunks = self
            .committed_manifest
            .as_ref()
            .map_or_else(HashSet::new, TableManifest::committed_chunks);
        let mut uploaded_chunks = HashSet::new();
        let mut files = Vec::with_capacity(local_files.len());

        for (relative, local_path) in local_files {
            let mut local_file = std::fs::File::open(&local_path)?;
            let mut buffer = vec![0_u8; CHUNK_SIZE];
            let mut chunks = Vec::new();
            let mut file_length = 0_u64;
            loop {
                let length = read_chunk(&mut local_file, &mut buffer)?;
                if length == 0 {
                    break;
                }
                let bytes = &buffer[..length];
                let chunk = ChunkRef {
                    length: u32::try_from(length)?,
                    hash: *blake3::hash(bytes).as_bytes(),
                };
                if !committed_chunks.contains(&chunk.hash) && uploaded_chunks.insert(chunk.hash) {
                    let key = self.object_key(&Self::chunk_path(&chunk.hash))?;
                    self.put_object_verified(&key, bytes)?;
                }
                file_length = file_length
                    .checked_add(u64::try_from(length)?)
                    .ok_or_else(|| eyre::eyre!("local table file length overflow"))?;
                chunks.push(chunk);
            }
            files.push(ManifestFile {
                path: relative,
                length: file_length,
                chunks,
            });
        }

        let manifest = TableManifest { files };
        let manifest_bytes = manifest.encode()?;
        let manifest_key = self.object_key(MANIFEST_FILE)?;
        self.put_object_verified(&manifest_key, &manifest_bytes)?;
        self.committed_manifest = Some(manifest);

        tracing::debug!(new_chunks = uploaded_chunks.len(), "S3 table manifest committed");
        Ok(())
    }

    async fn sync_from_s3(
        bucket: &Bucket,
        credentials: &Credentials,
        client: &Agent,
        config: &S3DiskConfig,
    ) -> eyre::Result<Option<TableManifest>> {
        let table_name = Self::table_name(config)?;
        let prefix = config.s3.prefix.as_deref().unwrap_or("");
        let manifest_key = Self::full_s3_path(prefix, MANIFEST_FILE, table_name);

        if let Some(bytes) = Self::get_object_optional(bucket, credentials, client, &manifest_key)? {
            let manifest = TableManifest::decode(&bytes)?;
            Self::restore_manifest(bucket, credentials, client, config, &manifest).await?;
            tracing::info!(table_name, "S3 table manifest restored");
            return Ok(Some(manifest));
        }

        if Self::restore_legacy_objects(bucket, credentials, client, config).await? {
            tracing::info!(
                table_name,
                "legacy S3 table objects restored; next write will publish a manifest"
            );
        } else {
            tracing::debug!(table_name, "no committed table objects found in S3");
        }
        Ok(None)
    }

    async fn restore_manifest(
        bucket: &Bucket,
        credentials: &Credentials,
        client: &Agent,
        config: &S3DiskConfig,
        manifest: &TableManifest,
    ) -> eyre::Result<()> {
        let table_path = Path::new(config.disk.table_path());
        let stage = staging_path(table_path, "stage")?;
        remove_path_if_exists(&stage)?;
        std::fs::create_dir_all(&stage)?;

        let prefix = config.s3.prefix.as_deref().unwrap_or("");
        let table_name = Self::table_name(config)?;
        let restore_result = async {
            for file in &manifest.files {
                let local_path = stage.join(&file.path);
                if let Some(parent) = local_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let mut restored_file = std::fs::File::create(&local_path)?;
                let mut restored_length = 0_u64;
                for chunk in &file.chunks {
                    let key = Self::full_s3_path(prefix, &Self::chunk_path(&chunk.hash), table_name);
                    let bytes = Self::get_object_optional(bucket, credentials, client, &key)?
                        .ok_or_else(|| eyre::eyre!("S3 manifest references missing chunk {key}"))?;
                    if bytes.len() != chunk.length as usize || blake3::hash(&bytes).as_bytes() != &chunk.hash {
                        return Err(eyre::eyre!("S3 chunk failed length or hash validation: {key}"));
                    }
                    restored_file.write_all(&bytes)?;
                    restored_length = restored_length
                        .checked_add(u64::try_from(bytes.len())?)
                        .ok_or_else(|| eyre::eyre!("restored S3 file length overflow"))?;
                }
                if restored_length != file.length {
                    return Err(eyre::eyre!("restored S3 file has the wrong length: {}", file.path));
                }
                restored_file.flush()?;
            }
            Ok::<(), eyre::Report>(())
        }
        .await;

        if let Err(error) = restore_result {
            let _ = std::fs::remove_dir_all(&stage);
            return Err(error);
        }
        publish_staged_table(table_path, &stage)
    }

    async fn restore_legacy_objects(
        bucket: &Bucket,
        credentials: &Credentials,
        client: &Agent,
        config: &S3DiskConfig,
    ) -> eyre::Result<bool> {
        use rusty_s3::actions::ListObjectsV2;

        let table_path = Path::new(config.disk.table_path());
        let table_name = Self::table_name(config)?;
        let prefix = config.s3.prefix.as_deref().unwrap_or("");
        let table_root = Self::full_s3_path(prefix, "", table_name);
        let mut continuation = None;
        let mut objects = Vec::new();

        loop {
            let mut action = bucket.list_objects_v2(Some(credentials));
            action.with_prefix(&table_root);
            if let Some(token) = continuation.as_deref() {
                action.with_continuation_token(token);
            }
            let url = action.sign(Duration::from_secs(3600));
            let response = client.get(url.as_str()).call()?;
            let parsed = ListObjectsV2::parse_response(&response.into_string()?)?;
            for object in parsed.contents {
                let Some(relative) = object.key.strip_prefix(&table_root) else {
                    continue;
                };
                if !is_table_name(relative) || validate_relative_path(relative).is_err() {
                    continue;
                }
                let bytes = Self::get_object_optional(bucket, credentials, client, &object.key)?
                    .ok_or_else(|| eyre::eyre!("listed S3 object disappeared: {}", object.key))?;
                objects.push((relative.to_string(), bytes));
            }
            continuation = parsed.next_continuation_token;
            if continuation.is_none() {
                break;
            }
        }

        if objects.is_empty() {
            return Ok(false);
        }
        objects.sort_by(|left, right| left.0.cmp(&right.0));
        let stage = staging_path(table_path, "legacy-stage")?;
        remove_path_if_exists(&stage)?;
        std::fs::create_dir_all(&stage)?;
        for (relative, bytes) in objects {
            crate::fsx::write(stage.join(relative), bytes).await?;
        }
        publish_staged_table(table_path, &stage)?;
        Ok(true)
    }
}

fn is_table_name(name: &str) -> bool {
    name.ends_with(WT_DATA_EXTENSION) || name.ends_with(WT_INDEX_EXTENSION)
}

#[cfg(test)]
fn describe_chunks(content: &[u8]) -> Vec<ChunkRef> {
    content
        .chunks(CHUNK_SIZE)
        .map(|bytes| ChunkRef {
            length: u32::try_from(bytes.len()).expect("a fixed S3 chunk always fits in u32"),
            hash: *blake3::hash(bytes).as_bytes(),
        })
        .collect()
}

fn read_chunk(file: &mut std::fs::File, buffer: &mut [u8]) -> std::io::Result<usize> {
    let mut length = 0;
    while length < buffer.len() {
        let read = file.read(&mut buffer[length..])?;
        if read == 0 {
            break;
        }
        length += read;
    }
    Ok(length)
}

fn is_table_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(is_table_name)
}

fn validate_relative_path(path: &str) -> eyre::Result<()> {
    if path.is_empty() || path.chars().any(|character| matches!(character, '\\' | ':' | '\0')) {
        return Err(eyre::eyre!("invalid S3 manifest path"));
    }
    let parsed = Path::new(path);
    if parsed
        .components()
        .all(|component| matches!(component, Component::Normal(_)))
    {
        Ok(())
    } else {
        Err(eyre::eyre!("unsafe S3 manifest path: {path}"))
    }
}

fn canonical_relative_path(root: &Path, path: &Path) -> eyre::Result<String> {
    let relative = path.strip_prefix(root)?;
    let mut value = String::new();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(eyre::eyre!("unsafe local table path: {}", path.display()));
        };
        let component = component
            .to_str()
            .ok_or_else(|| eyre::eyre!("table path is not valid UTF-8: {}", path.display()))?;
        if !value.is_empty() {
            value.push('/');
        }
        value.push_str(component);
    }
    validate_relative_path(&value)?;
    Ok(value)
}

fn staging_path(table_path: &Path, label: &str) -> eyre::Result<PathBuf> {
    let parent = table_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = table_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| eyre::eyre!("invalid table path"))?;
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(parent.join(format!(".{name}.s3-{label}-{}-{timestamp}", std::process::id())))
}

fn remove_path_if_exists(path: &Path) -> eyre::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(path)?,
        Ok(_) => std::fs::remove_file(path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn publish_staged_table(table_path: &Path, stage: &Path) -> eyre::Result<()> {
    if let Some(parent) = table_path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    if !table_path.exists() {
        std::fs::rename(stage, table_path)?;
        return Ok(());
    }

    let backup = staging_path(table_path, "backup")?;
    std::fs::rename(table_path, &backup)?;
    if let Err(error) = std::fs::rename(stage, table_path) {
        if let Err(rollback_error) = std::fs::rename(&backup, table_path) {
            return Err(eyre::eyre!(
                "failed to install restored S3 table ({error}) and failed to restore local table ({rollback_error})"
            ));
        }
        return Err(error.into());
    }
    if let Err(error) = std::fs::remove_dir_all(&backup) {
        tracing::warn!(path = %backup.display(), error = %error, "restored table but could not remove backup directory");
    }
    Ok(())
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
        let (bucket, credentials, client) = Self::create_bucket(&config.s3)?;
        // If a manifest exists, failure is fatal: continuing with local files
        // could publish stale state over a newer committed remote generation.
        let committed_manifest = Self::sync_from_s3(&bucket, &credentials, &client, &config).await?;
        let inner = DiskPersistenceEngine::new(config.disk.clone()).await?;

        Ok(Self {
            inner,
            config,
            bucket,
            credentials,
            client,
            committed_manifest,
            phantom: PhantomData,
        })
    }

    async fn apply_operation(
        &mut self,
        op: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents>,
    ) -> eyre::Result<()> {
        self.inner.apply_operation(op).await?;
        self.sync_to_s3().await
    }

    async fn apply_batch_operation(
        &mut self,
        batch_op: BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents, AvailableIndexes>,
    ) -> eyre::Result<()> {
        self.inner.apply_batch_operation(batch_op).await?;
        self.sync_to_s3().await
    }

    fn config(&self) -> &Self::Config {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(bytes: &[u8]) -> ChunkRef {
        ChunkRef {
            length: bytes.len() as u32,
            hash: *blake3::hash(bytes).as_bytes(),
        }
    }

    #[test]
    fn manifest_round_trip_is_deterministic() {
        let manifest = TableManifest {
            files: vec![
                ManifestFile {
                    path: ".wt.data".to_string(),
                    length: 3,
                    chunks: vec![chunk(b"abc")],
                },
                ManifestFile {
                    path: "primary.wt.idx".to_string(),
                    length: 0,
                    chunks: Vec::new(),
                },
            ],
        };
        let encoded = manifest.encode().unwrap();
        assert_eq!(TableManifest::decode(&encoded).unwrap(), manifest);
        assert_eq!(manifest.encode().unwrap(), encoded);
    }

    #[test]
    fn manifest_rejects_corruption_and_unsafe_paths() {
        let manifest = TableManifest {
            files: vec![ManifestFile {
                path: ".wt.data".to_string(),
                length: 3,
                chunks: vec![chunk(b"abc")],
            }],
        };
        let mut encoded = manifest.encode().unwrap();
        encoded[12] ^= 1;
        assert!(TableManifest::decode(&encoded).is_err());

        let unsafe_manifest = TableManifest {
            files: vec![ManifestFile {
                path: "../outside.wt.data".to_string(),
                length: 0,
                chunks: Vec::new(),
            }],
        };
        assert!(unsafe_manifest.encode().is_err());
    }

    #[test]
    fn manifest_requires_exact_chunk_coverage() {
        let manifest = TableManifest {
            files: vec![ManifestFile {
                path: ".wt.data".to_string(),
                length: 4,
                chunks: vec![chunk(b"abc")],
            }],
        };
        let encoded = manifest.encode().unwrap();
        assert!(TableManifest::decode(&encoded).is_err());
    }

    #[test]
    fn a_small_change_to_a_large_file_reuses_unchanged_chunks() {
        let original = vec![7_u8; 10 * 1024 * 1024];
        let mut changed = original.clone();
        changed[5 * 1024 * 1024] = 9;

        let original_chunks = describe_chunks(&original);
        let changed_chunks = describe_chunks(&changed);
        assert_eq!(original_chunks.len(), 3);
        assert_eq!(changed_chunks.len(), 3);
        assert_eq!(
            original_chunks
                .iter()
                .zip(&changed_chunks)
                .filter(|(left, right)| left != right)
                .count(),
            1
        );
        assert_eq!(changed_chunks[1].length as usize, CHUNK_SIZE);
    }
}
