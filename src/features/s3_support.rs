use alloc::{format, string::String, string::ToString, vec::Vec};
use core::fmt::{Debug, Write as _};
use core::hash::Hash;
use core::marker::PhantomData;
use core::time::Duration;
use std::collections::{HashMap, HashSet};
use std::io::{BufReader, Read as _, Seek as _, SeekFrom, Write as _};
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
const MANIFEST_MAGIC_V1: &[u8; 8] = b"WTS3M001";
const MANIFEST_MAGIC_V2: &[u8; 8] = b"WTS3M002";
/// The throughput target for a full upload or a run of adjacent dirty pages.
/// It is deliberately a target rather than a minimum: one changed DataBucket
/// page is published as one page-sized immutable segment.
const SEGMENT_TARGET: usize = 4 * 1024 * 1024;
const CHANGE_BLOCK_SIZE: usize = data_bucket::PAGE_SIZE;
const MAX_MANIFEST_FILES: usize = 16_384;
const MAX_MANIFEST_EXTENTS: usize = 4_194_304;

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
struct SegmentExtent {
    file_offset: u64,
    length: u32,
    segment_offset: u32,
    segment_length: u32,
    hash: [u8; 32],
}

impl SegmentExtent {
    fn file_end(&self) -> eyre::Result<u64> {
        self.file_offset
            .checked_add(u64::from(self.length))
            .ok_or_else(|| eyre::eyre!("S3 manifest extent offset overflow"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ManifestFile {
    path: String,
    length: u64,
    extents: Vec<SegmentExtent>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct TableManifest {
    files: Vec<ManifestFile>,
}

impl TableManifest {
    fn encode(&self) -> eyre::Result<Vec<u8>> {
        let file_count = u32::try_from(self.files.len()).map_err(|_| eyre::eyre!("too many S3 manifest files"))?;
        if self.files.len() > MAX_MANIFEST_FILES {
            return Err(eyre::eyre!("too many S3 manifest files"));
        }
        let total_extents = self.files.iter().try_fold(0_usize, |total, file| {
            total
                .checked_add(file.extents.len())
                .ok_or_else(|| eyre::eyre!("S3 manifest extent count overflow"))
        })?;
        if total_extents > MAX_MANIFEST_EXTENTS {
            return Err(eyre::eyre!("too many extents in S3 manifest"));
        }
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MANIFEST_MAGIC_V2);
        bytes.extend_from_slice(&file_count.to_le_bytes());

        for file in &self.files {
            validate_manifest_file(file)?;
            validate_relative_path(&file.path)?;
            let path = file.path.as_bytes();
            let path_len = u16::try_from(path.len()).map_err(|_| eyre::eyre!("S3 manifest path is too long"))?;
            let extent_count =
                u32::try_from(file.extents.len()).map_err(|_| eyre::eyre!("too many extents in S3 manifest"))?;
            bytes.extend_from_slice(&path_len.to_le_bytes());
            bytes.extend_from_slice(path);
            bytes.extend_from_slice(&file.length.to_le_bytes());
            bytes.extend_from_slice(&extent_count.to_le_bytes());
            for extent in &file.extents {
                bytes.extend_from_slice(&extent.file_offset.to_le_bytes());
                bytes.extend_from_slice(&extent.length.to_le_bytes());
                bytes.extend_from_slice(&extent.segment_offset.to_le_bytes());
                bytes.extend_from_slice(&extent.segment_length.to_le_bytes());
                bytes.extend_from_slice(&extent.hash);
            }
        }

        let checksum = blake3::hash(&bytes);
        bytes.extend_from_slice(checksum.as_bytes());
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> eyre::Result<Self> {
        if bytes.len() < MANIFEST_MAGIC_V2.len() + 4 + 32 {
            return Err(eyre::eyre!("S3 manifest is truncated"));
        }
        let (payload, checksum) = bytes.split_at(bytes.len() - 32);
        if blake3::hash(payload).as_bytes() != checksum {
            return Err(eyre::eyre!("S3 manifest checksum mismatch"));
        }

        let magic = payload
            .get(..MANIFEST_MAGIC_V2.len())
            .ok_or_else(|| eyre::eyre!("S3 manifest is truncated"))?;
        if magic == MANIFEST_MAGIC_V1 {
            return Self::decode_v1(payload);
        }
        if magic != MANIFEST_MAGIC_V2 {
            return Err(eyre::eyre!("unsupported S3 manifest format"));
        }

        let mut reader = ManifestReader::new(payload);
        reader.take(MANIFEST_MAGIC_V2.len())?;
        let file_count = reader.u32()? as usize;
        if file_count > MAX_MANIFEST_FILES {
            return Err(eyre::eyre!("S3 manifest contains too many files"));
        }

        let mut files = Vec::with_capacity(file_count);
        let mut previous_path: Option<String> = None;
        let mut total_extents = 0_usize;
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
            let extent_count = reader.u32()? as usize;
            total_extents = total_extents
                .checked_add(extent_count)
                .ok_or_else(|| eyre::eyre!("S3 manifest extent count overflow"))?;
            if total_extents > MAX_MANIFEST_EXTENTS {
                return Err(eyre::eyre!("S3 manifest contains too many extents"));
            }

            let mut extents = Vec::with_capacity(extent_count);
            for _ in 0..extent_count {
                let file_offset = reader.u64()?;
                let extent_length = reader.u32()?;
                let segment_offset = reader.u32()?;
                let segment_length = reader.u32()?;
                let mut hash = [0_u8; 32];
                let hash_length = hash.len();
                hash.copy_from_slice(reader.take(hash_length)?);
                extents.push(SegmentExtent {
                    file_offset,
                    length: extent_length,
                    segment_offset,
                    segment_length,
                    hash,
                });
            }
            let file = ManifestFile { path, length, extents };
            validate_manifest_file(&file)?;
            files.push(file);
        }

        if !reader.is_empty() {
            return Err(eyre::eyre!("S3 manifest has trailing data"));
        }
        Ok(Self { files })
    }

    fn decode_v1(payload: &[u8]) -> eyre::Result<Self> {
        let mut reader = ManifestReader::new(payload);
        if reader.take(MANIFEST_MAGIC_V1.len())? != MANIFEST_MAGIC_V1 {
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
            if total_chunks > MAX_MANIFEST_EXTENTS {
                return Err(eyre::eyre!("S3 manifest contains too many chunks"));
            }
            let expected_chunks = if length == 0 {
                0
            } else {
                usize::try_from(length.div_ceil(SEGMENT_TARGET as u64))?
            };
            if chunk_count != expected_chunks {
                return Err(eyre::eyre!("S3 manifest chunk count does not match file length"));
            }

            let mut extents = Vec::with_capacity(chunk_count);
            let mut file_offset = 0_u64;
            for index in 0..chunk_count {
                let chunk_length = reader.u32()?;
                if chunk_length == 0 || chunk_length as usize > SEGMENT_TARGET {
                    return Err(eyre::eyre!("S3 manifest contains an invalid chunk length"));
                }
                if index + 1 != chunk_count && chunk_length as usize != SEGMENT_TARGET {
                    return Err(eyre::eyre!("S3 manifest contains a short interior chunk"));
                }
                let mut hash = [0_u8; 32];
                let hash_length = hash.len();
                hash.copy_from_slice(reader.take(hash_length)?);
                extents.push(SegmentExtent {
                    file_offset,
                    length: chunk_length,
                    segment_offset: 0,
                    segment_length: chunk_length,
                    hash,
                });
                file_offset = file_offset
                    .checked_add(u64::from(chunk_length))
                    .ok_or_else(|| eyre::eyre!("S3 manifest file length overflow"))?;
            }
            let file = ManifestFile { path, length, extents };
            validate_manifest_file(&file)?;
            files.push(file);
        }
        if !reader.is_empty() {
            return Err(eyre::eyre!("S3 manifest has trailing data"));
        }
        Ok(Self { files })
    }

    fn committed_segments(&self) -> HashSet<[u8; 32]> {
        self.files
            .iter()
            .flat_map(|file| file.extents.iter().map(|extent| extent.hash))
            .collect()
    }

    fn file(&self, path: &str) -> Option<&ManifestFile> {
        self.files
            .binary_search_by(|file| file.path.as_str().cmp(path))
            .ok()
            .map(|index| &self.files[index])
    }
}

fn validate_manifest_file(file: &ManifestFile) -> eyre::Result<()> {
    let mut described_length = 0_u64;
    for extent in &file.extents {
        if extent.file_offset != described_length {
            return Err(eyre::eyre!("S3 manifest extents do not cover the file contiguously"));
        }
        if extent.length == 0 || extent.segment_length == 0 || extent.segment_length as usize > SEGMENT_TARGET {
            return Err(eyre::eyre!("S3 manifest contains an invalid extent length"));
        }
        let segment_end = extent
            .segment_offset
            .checked_add(extent.length)
            .ok_or_else(|| eyre::eyre!("S3 manifest segment offset overflow"))?;
        if segment_end > extent.segment_length {
            return Err(eyre::eyre!("S3 manifest extent exceeds its segment"));
        }
        described_length = extent.file_end()?;
    }
    if described_length != file.length {
        return Err(eyre::eyre!("S3 manifest extents do not cover the file length"));
    }
    Ok(())
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
    committed_blocks: HashMap<String, Vec<[u8; 32]>>,
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

        let committed_segments = self
            .committed_manifest
            .as_ref()
            .map_or_else(HashSet::new, TableManifest::committed_segments);
        let mut uploaded_segments = HashSet::new();
        let mut files = Vec::with_capacity(local_files.len());
        let mut next_blocks = HashMap::with_capacity(local_files.len());

        for (relative, local_path) in local_files {
            let blocks = describe_file_blocks(&local_path)?;
            let file_length = blocks.last().map_or(0, LocalBlock::end);
            let previous_file = self
                .committed_manifest
                .as_ref()
                .and_then(|manifest| manifest.file(&relative));
            let previous_hashes = self.committed_blocks.get(&relative);
            let mut extents = match previous_file {
                Some(file) => trim_extents(&file.extents, file_length)?,
                None => Vec::new(),
            };

            let dirty = blocks
                .iter()
                .enumerate()
                .map(|(index, block)| {
                    previous_file.is_none()
                        || previous_hashes
                            .and_then(|hashes| hashes.get(index))
                            .is_none_or(|hash| hash != &block.hash)
                })
                .collect::<Vec<_>>();

            let mut index = 0;
            while index < blocks.len() {
                if !dirty[index] {
                    index += 1;
                    continue;
                }
                let first = index;
                let mut segment_length = blocks[index].length as usize;
                index += 1;
                while index < blocks.len()
                    && dirty[index]
                    && segment_length + blocks[index].length as usize <= SEGMENT_TARGET
                {
                    segment_length += blocks[index].length as usize;
                    index += 1;
                }

                let file_offset = blocks[first].offset;
                let bytes = read_file_range(&local_path, file_offset, segment_length)?;
                let hash = *blake3::hash(&bytes).as_bytes();
                if !committed_segments.contains(&hash) && uploaded_segments.insert(hash) {
                    let key = self.object_key(&Self::chunk_path(&hash))?;
                    self.put_object_verified(&key, &bytes)?;
                }
                let extent = SegmentExtent {
                    file_offset,
                    length: u32::try_from(segment_length)?,
                    segment_offset: 0,
                    segment_length: u32::try_from(segment_length)?,
                    hash,
                };
                extents = overlay_extent(&extents, extent)?;
            }

            let file = ManifestFile {
                path: relative,
                length: file_length,
                extents,
            };
            validate_manifest_file(&file)?;
            next_blocks.insert(file.path.clone(), blocks.into_iter().map(|block| block.hash).collect());
            files.push(file);
        }

        let manifest = TableManifest { files };
        let manifest_bytes = manifest.encode()?;
        let manifest_key = self.object_key(MANIFEST_FILE)?;
        self.put_object_verified(&manifest_key, &manifest_bytes)?;
        self.committed_manifest = Some(manifest);
        self.committed_blocks = next_blocks;

        tracing::debug!(new_segments = uploaded_segments.len(), "S3 table manifest committed");
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
                restored_file.set_len(file.length)?;
                let mut by_segment: HashMap<[u8; 32], (u32, Vec<&SegmentExtent>)> = HashMap::new();
                for extent in &file.extents {
                    let entry = by_segment
                        .entry(extent.hash)
                        .or_insert_with(|| (extent.segment_length, Vec::new()));
                    if entry.0 != extent.segment_length {
                        return Err(eyre::eyre!("S3 manifest gives one segment conflicting lengths"));
                    }
                    entry.1.push(extent);
                }
                for (hash, (segment_length, extents)) in by_segment {
                    let key = Self::full_s3_path(prefix, &Self::chunk_path(&hash), table_name);
                    let bytes = Self::get_object_optional(bucket, credentials, client, &key)?
                        .ok_or_else(|| eyre::eyre!("S3 manifest references missing segment {key}"))?;
                    if bytes.len() != segment_length as usize || blake3::hash(&bytes).as_bytes() != &hash {
                        return Err(eyre::eyre!("S3 segment failed length or hash validation: {key}"));
                    }
                    for extent in extents {
                        let from = extent.segment_offset as usize;
                        let to = from
                            .checked_add(extent.length as usize)
                            .ok_or_else(|| eyre::eyre!("S3 manifest segment slice overflow"))?;
                        restored_file.seek(SeekFrom::Start(extent.file_offset))?;
                        restored_file.write_all(&bytes[from..to])?;
                    }
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

#[derive(Clone, Debug)]
struct LocalBlock {
    offset: u64,
    length: u32,
    hash: [u8; 32],
}

impl LocalBlock {
    fn end(&self) -> u64 {
        self.offset + u64::from(self.length)
    }
}

fn describe_file_blocks(path: &Path) -> eyre::Result<Vec<LocalBlock>> {
    let mut file = BufReader::with_capacity(SEGMENT_TARGET, std::fs::File::open(path)?);
    let mut buffer = vec![0_u8; CHANGE_BLOCK_SIZE];
    let mut blocks = Vec::new();
    let mut offset = 0_u64;
    loop {
        let length = read_buffer(&mut file, &mut buffer)?;
        if length == 0 {
            break;
        }
        blocks.push(LocalBlock {
            offset,
            length: u32::try_from(length)?,
            hash: *blake3::hash(&buffer[..length]).as_bytes(),
        });
        offset = offset
            .checked_add(u64::try_from(length)?)
            .ok_or_else(|| eyre::eyre!("local table file length overflow"))?;
    }
    Ok(blocks)
}

fn describe_table_blocks(table_path: &Path) -> eyre::Result<HashMap<String, Vec<[u8; 32]>>> {
    if !table_path.exists() {
        return Ok(HashMap::new());
    }
    let mut result = HashMap::new();
    for entry in WalkDir::new(table_path) {
        let entry = entry?;
        if !entry.file_type().is_file() || !is_table_file(entry.path()) {
            continue;
        }
        let relative = canonical_relative_path(table_path, entry.path())?;
        let hashes = describe_file_blocks(entry.path())?
            .into_iter()
            .map(|block| block.hash)
            .collect();
        result.insert(relative, hashes);
    }
    Ok(result)
}

fn read_file_range(path: &Path, offset: u64, length: usize) -> eyre::Result<Vec<u8>> {
    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0_u8; length];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn read_buffer(file: &mut impl std::io::Read, buffer: &mut [u8]) -> std::io::Result<usize> {
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

fn trim_extents(extents: &[SegmentExtent], length: u64) -> eyre::Result<Vec<SegmentExtent>> {
    let mut trimmed = Vec::new();
    for extent in extents {
        if extent.file_offset >= length {
            break;
        }
        let keep = extent.file_end()?.min(length) - extent.file_offset;
        let mut extent = extent.clone();
        extent.length = u32::try_from(keep)?;
        trimmed.push(extent);
    }
    Ok(trimmed)
}

fn overlay_extent(extents: &[SegmentExtent], replacement: SegmentExtent) -> eyre::Result<Vec<SegmentExtent>> {
    let start = replacement.file_offset;
    let end = replacement.file_end()?;
    let mut result = Vec::with_capacity(extents.len() + 2);
    let mut inserted = false;

    for extent in extents {
        let extent_end = extent.file_end()?;
        if extent_end <= start {
            result.push(extent.clone());
            continue;
        }
        if extent.file_offset >= end {
            if !inserted {
                result.push(replacement.clone());
                inserted = true;
            }
            result.push(extent.clone());
            continue;
        }

        if extent.file_offset < start {
            let mut left = extent.clone();
            left.length = u32::try_from(start - extent.file_offset)?;
            result.push(left);
        }
        if !inserted {
            result.push(replacement.clone());
            inserted = true;
        }
        if extent_end > end {
            let skipped = u32::try_from(end - extent.file_offset)?;
            let mut right = extent.clone();
            right.file_offset = end;
            right.length = u32::try_from(extent_end - end)?;
            right.segment_offset = right
                .segment_offset
                .checked_add(skipped)
                .ok_or_else(|| eyre::eyre!("S3 manifest segment offset overflow"))?;
            result.push(right);
        }
    }
    if !inserted {
        result.push(replacement);
    }
    merge_adjacent_extents(result)
}

fn merge_adjacent_extents(extents: Vec<SegmentExtent>) -> eyre::Result<Vec<SegmentExtent>> {
    let mut merged: Vec<SegmentExtent> = Vec::with_capacity(extents.len());
    for extent in extents {
        if let Some(previous) = merged.last_mut() {
            let contiguous_file = previous.file_end()? == extent.file_offset;
            let contiguous_segment = previous
                .segment_offset
                .checked_add(previous.length)
                .is_some_and(|offset| offset == extent.segment_offset);
            if contiguous_file
                && contiguous_segment
                && previous.segment_length == extent.segment_length
                && previous.hash == extent.hash
            {
                previous.length = previous
                    .length
                    .checked_add(extent.length)
                    .ok_or_else(|| eyre::eyre!("S3 manifest extent length overflow"))?;
                continue;
            }
        }
        merged.push(extent);
    }
    Ok(merged)
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
        let committed_blocks = describe_table_blocks(Path::new(config.disk.table_path()))?;

        Ok(Self {
            inner,
            config,
            bucket,
            credentials,
            client,
            committed_manifest,
            committed_blocks,
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

    fn extent(file_offset: u64, bytes: &[u8]) -> SegmentExtent {
        SegmentExtent {
            file_offset,
            length: bytes.len() as u32,
            segment_offset: 0,
            segment_length: bytes.len() as u32,
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
                    extents: vec![extent(0, b"abc")],
                },
                ManifestFile {
                    path: "primary.wt.idx".to_string(),
                    length: 0,
                    extents: Vec::new(),
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
                extents: vec![extent(0, b"abc")],
            }],
        };
        let mut encoded = manifest.encode().unwrap();
        encoded[12] ^= 1;
        assert!(TableManifest::decode(&encoded).is_err());

        let unsafe_manifest = TableManifest {
            files: vec![ManifestFile {
                path: "../outside.wt.data".to_string(),
                length: 0,
                extents: Vec::new(),
            }],
        };
        assert!(unsafe_manifest.encode().is_err());
    }

    #[test]
    fn manifest_requires_exact_extent_coverage() {
        let manifest = TableManifest {
            files: vec![ManifestFile {
                path: ".wt.data".to_string(),
                length: 4,
                extents: vec![extent(0, b"abc")],
            }],
        };
        assert!(manifest.encode().is_err());
    }

    #[test]
    fn manifest_requires_strictly_sorted_unique_paths() {
        let file = |path: &str| ManifestFile {
            path: path.to_string(),
            length: 0,
            extents: Vec::new(),
        };
        let unsorted = TableManifest {
            files: vec![file("primary.wt.idx"), file(".wt.data")],
        }
        .encode()
        .unwrap();
        assert!(TableManifest::decode(&unsorted).is_err());

        let duplicate = TableManifest {
            files: vec![file(".wt.data"), file(".wt.data")],
        }
        .encode()
        .unwrap();
        assert!(TableManifest::decode(&duplicate).is_err());
    }

    #[test]
    fn a_page_change_splits_a_large_segment_without_reuploading_it() {
        let original = vec![7_u8; SEGMENT_TARGET];
        let original_extent = extent(0, &original);
        let changed_page = vec![9_u8; CHANGE_BLOCK_SIZE];
        let replacement = extent(CHANGE_BLOCK_SIZE as u64, &changed_page);

        let extents = overlay_extent(core::slice::from_ref(&original_extent), replacement.clone()).unwrap();
        assert_eq!(extents.len(), 3);
        assert_eq!(extents[0].length as usize, CHANGE_BLOCK_SIZE);
        assert_eq!(extents[1], replacement);
        assert_eq!(extents[2].file_offset, (2 * CHANGE_BLOCK_SIZE) as u64);
        assert_eq!(extents[2].segment_offset, (2 * CHANGE_BLOCK_SIZE) as u32);
        assert_eq!(extents[2].file_end().unwrap(), SEGMENT_TARGET as u64);
        assert_eq!(extents[0].hash, original_extent.hash);
        assert_eq!(extents[2].hash, original_extent.hash);
    }

    #[test]
    fn legacy_manifest_decodes_as_segment_extents() {
        let contents = [vec![1_u8; SEGMENT_TARGET], vec![2_u8; 19]];
        let mut payload = Vec::new();
        payload.extend_from_slice(MANIFEST_MAGIC_V1);
        payload.extend_from_slice(&1_u32.to_le_bytes());
        let path = b".wt.data";
        payload.extend_from_slice(&(path.len() as u16).to_le_bytes());
        payload.extend_from_slice(path);
        payload.extend_from_slice(&((SEGMENT_TARGET + 19) as u64).to_le_bytes());
        payload.extend_from_slice(&2_u32.to_le_bytes());
        for bytes in &contents {
            payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            payload.extend_from_slice(blake3::hash(bytes).as_bytes());
        }
        let checksum = blake3::hash(&payload);
        payload.extend_from_slice(checksum.as_bytes());

        let manifest = TableManifest::decode(&payload).unwrap();
        let file = &manifest.files[0];
        assert_eq!(file.extents.len(), 2);
        assert_eq!(file.extents[0].length as usize, SEGMENT_TARGET);
        assert_eq!(file.extents[1].file_offset, SEGMENT_TARGET as u64);
        assert!(manifest.encode().unwrap().starts_with(MANIFEST_MAGIC_V2));
    }
}
