//! Ongoing WorkTable persistence through immutable S3 generations.
//!
//! This engine is opt-in. The legacy S3 persistence engine and its default
//! behavior are unchanged. Each completed disk batch is published by comparing
//! the commit pointer against the ETag observed for the prior generation.

use alloc::{format, string::String, vec::Vec};
use core::fmt::Debug;
use core::hash::Hash;
use core::marker::PhantomData;
use core::time::Duration;
use std::fs;
use std::io::{BufReader, Read as _};
use std::path::{Component, Path, PathBuf};

use ureq::Agent;
use uuid::Uuid;
use walkdir::WalkDir;

use super::s3_generation::{
    S3_GENERATION_SEGMENT_MAX_BYTES, S3GenerationFile, S3GenerationPublishOutcome,
    S3GenerationPublishReceipt, S3GenerationPublisher, S3GenerationSegment, S3ObjectSnapshot,
    S3ObjectVersion,
};
use super::s3_generation_reader::{S3GenerationReadReceipt, S3GenerationReader};
use super::s3_support::{S3DiskConfig, S3TransportOptions, rename_directory_no_replace};
use crate::TableSecondaryIndexEventsOps;
use crate::persistence::operation::{BatchOperation, Operation};
use crate::persistence::{
    DiskConfig, DiskPersistenceEngine, PersistenceConfig, PersistenceEngine, SpaceDataOps,
    SpaceIndexOps, SpaceSecondaryIndexOps,
};
use crate::prelude::{
    PrimaryKeyGeneratorState, TablePrimaryKey, WT_DATA_EXTENSION, WT_INDEX_EXTENSION,
};

/// Persistence candidate that durably publishes every successful WorkTable
/// batch as a new immutable S3 generation.
///
/// A failed conditional write, a local persistence error, or a readback that
/// cannot prove the current remote table matches local files fences this
/// instance. Reopen it to restore and validate the committed generation before
/// accepting more writes.
pub struct S3GenerationPersistenceEngine<
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
    publisher: S3GenerationPublisher,
    reader: S3GenerationReader,
    _local_writer_lock: fs::File,
    committed_version: Option<S3ObjectVersion>,
    committed_table_hash: Option<[u8; 32]>,
    fenced: bool,
    marker: PhantomData<(PrimaryKey, SecondaryIndexEvents, AvailableIndexes, PrimaryKeyGenState)>,
}

struct OwnedTableFile {
    path: String,
    bytes: Vec<u8>,
    length: u64,
}

struct OwnedTableSnapshot {
    files: Vec<OwnedTableFile>,
    hash: [u8; 32],
}

struct TableFilePath {
    path: String,
    local_path: PathBuf,
    length: u64,
}

struct TableFingerprint {
    hash: [u8; 32],
    file_count: u32,
    total_bytes: u64,
}

impl OwnedTableSnapshot {
    fn read(table_path: &Path) -> eyre::Result<Self> {
        let paths = collect_table_file_paths(table_path)?;
        let mut files = Vec::with_capacity(paths.len());
        for path in paths {
            let bytes = fs::read(&path.local_path)?;
            if u64::try_from(bytes.len())? != path.length {
                eyre::bail!("WorkTable file changed while creating its snapshot");
            }
            files.push(OwnedTableFile {
                path: path.path,
                bytes,
                length: path.length,
            });
        }

        let mut hash = blake3::Hasher::new();
        hash.update(&u64::try_from(files.len())?.to_le_bytes());
        for file in &files {
            update_table_hash_header(&mut hash, &file.path, file.length)?;
            hash.update(&file.bytes);
        }

        Ok(Self {
            files,
            hash: *hash.finalize().as_bytes(),
        })
    }

    fn generation_segments(&self) -> eyre::Result<Vec<Vec<S3GenerationSegment<'_>>>> {
        let mut files = Vec::with_capacity(self.files.len());
        for file in &self.files {
            let mut segments = Vec::new();
            let mut file_offset = 0_u64;
            for bytes in file.bytes.chunks(S3_GENERATION_SEGMENT_MAX_BYTES) {
                segments.push(S3GenerationSegment { file_offset, bytes });
                file_offset = file_offset
                    .checked_add(u64::try_from(bytes.len())?)
                    .ok_or_else(|| eyre::eyre!("WorkTable segment offset overflow"))?;
            }
            files.push(segments);
        }
        Ok(files)
    }

    fn generation_files<'a>(
        &'a self,
        segments: &'a [Vec<S3GenerationSegment<'a>>],
    ) -> Vec<S3GenerationFile<'a>> {
        self.files
            .iter()
            .zip(segments)
            .map(|(file, segments)| S3GenerationFile {
                path: &file.path,
                length: file.length,
                segments,
            })
            .collect()
    }
}

fn collect_table_file_paths(table_path: &Path) -> eyre::Result<Vec<TableFilePath>> {
    if !table_path.is_dir() {
        eyre::bail!("WorkTable persistence path is not a directory");
    }

    let mut files = Vec::new();
    for entry in WalkDir::new(table_path).follow_links(false) {
        let entry = entry?;
        if entry.depth() == 0 {
            continue;
        }
        let file_type = entry.file_type();
        if file_type.is_symlink() {
            eyre::bail!("WorkTable snapshot contains a symbolic link");
        }
        if !file_type.is_file() || !is_worktable_file(entry.path()) {
            continue;
        }

        let relative_path = table_file_relative_path(table_path, entry.path())?;
        if entry
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(WT_DATA_EXTENSION))
            && relative_path != WT_DATA_EXTENSION
        {
            eyre::bail!("WorkTable data file must be at the table root");
        }
        files.push(TableFilePath {
            path: relative_path,
            local_path: entry.path().to_path_buf(),
            length: entry.metadata()?.len(),
        });
    }

    files.sort_by(|left, right| left.path.cmp(&right.path));
    if !files
        .iter()
        .any(|file| file.path == WT_DATA_EXTENSION && file.length != 0)
    {
        eyre::bail!("WorkTable snapshot has no non-empty data file");
    }
    let primary_index_path = format!("primary{WT_INDEX_EXTENSION}");
    if !files
        .iter()
        .any(|file| file.path == primary_index_path && file.length != 0)
    {
        eyre::bail!("WorkTable snapshot has no non-empty primary index");
    }
    Ok(files)
}

fn update_table_hash_header(
    hash: &mut blake3::Hasher,
    path: &str,
    length: u64,
) -> eyre::Result<()> {
    hash.update(&u64::try_from(path.len())?.to_le_bytes());
    hash.update(path.as_bytes());
    hash.update(&length.to_le_bytes());
    Ok(())
}

fn fingerprint_table(table_path: &Path) -> eyre::Result<TableFingerprint> {
    let files = collect_table_file_paths(table_path)?;
    let file_count = u32::try_from(files.len())?;
    let mut hash = blake3::Hasher::new();
    hash.update(&u64::try_from(files.len())?.to_le_bytes());
    let mut total_bytes = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];

    for file in files {
        update_table_hash_header(&mut hash, &file.path, file.length)?;
        let mut reader = BufReader::with_capacity(buffer.len(), fs::File::open(&file.local_path)?);
        let mut bytes_read = 0_u64;
        loop {
            let length = reader.read(&mut buffer)?;
            if length == 0 {
                break;
            }
            hash.update(&buffer[..length]);
            bytes_read = bytes_read
                .checked_add(u64::try_from(length)?)
                .ok_or_else(|| eyre::eyre!("WorkTable snapshot length overflow"))?;
        }
        if bytes_read != file.length {
            eyre::bail!("WorkTable file changed while validating its snapshot");
        }
        total_bytes = total_bytes
            .checked_add(file.length)
            .ok_or_else(|| eyre::eyre!("WorkTable snapshot length overflow"))?;
    }

    Ok(TableFingerprint {
        hash: *hash.finalize().as_bytes(),
        file_count,
        total_bytes,
    })
}

struct RemoteGenerationState {
    version: S3ObjectVersion,
    pointer_hash: [u8; 32],
    generation_id: Uuid,
    file_count: u32,
    table_hash: [u8; 32],
}

fn restore_pointer_to(
    reader: &S3GenerationReader,
    pointer: S3ObjectSnapshot,
    destination: &Path,
) -> eyre::Result<RemoteGenerationState> {
    let pointer_hash = *blake3::hash(&pointer.bytes).as_bytes();
    let receipt = reader.restore_to(destination)?;
    if receipt.pointer_hash != pointer_hash {
        eyre::bail!("S3 generation pointer changed during strict restore");
    }

    let snapshot = fingerprint_table(destination)?;
    if receipt.file_count != snapshot.file_count || receipt.total_bytes != snapshot.total_bytes {
        eyre::bail!("S3 generation restore does not match its committed file inventory");
    }

    Ok(remote_generation_state(pointer, receipt, snapshot.hash))
}

fn remote_generation_state(
    pointer: S3ObjectSnapshot,
    receipt: S3GenerationReadReceipt,
    table_hash: [u8; 32],
) -> RemoteGenerationState {
    RemoteGenerationState {
        version: pointer.version,
        pointer_hash: receipt.pointer_hash,
        generation_id: receipt.generation_id,
        file_count: receipt.file_count,
        table_hash,
    }
}

fn table_file_relative_path(table_path: &Path, file_path: &Path) -> eyre::Result<String> {
    let relative = file_path.strip_prefix(table_path)?;
    let mut components = Vec::new();
    for component in relative.components() {
        let Component::Normal(value) = component else {
            eyre::bail!("WorkTable snapshot contains an unsafe file path");
        };
        let value = value
            .to_str()
            .ok_or_else(|| eyre::eyre!("WorkTable snapshot path is not UTF-8"))?;
        if value.contains(['/', '\\', ':']) || value.bytes().any(|byte| byte.is_ascii_control()) {
            eyre::bail!("WorkTable snapshot contains an unsafe file path");
        }
        components.push(value);
    }
    if components.is_empty() {
        eyre::bail!("WorkTable snapshot contains an empty file path");
    }
    Ok(components.join("/"))
}

fn is_worktable_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(WT_DATA_EXTENSION) || name.ends_with(WT_INDEX_EXTENSION))
}

fn sibling_path(table_path: &Path, label: &str) -> eyre::Result<PathBuf> {
    let parent = table_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = table_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| eyre::eyre!("invalid WorkTable path"))?;
    Ok(parent.join(format!(".{name}.s3-generation-{label}-{}", Uuid::new_v4())))
}

fn ensure_table_parent(table_path: &Path) -> eyre::Result<()> {
    if let Some(parent) = table_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn acquire_local_writer_lock(table_path: &Path) -> eyre::Result<fs::File> {
    ensure_table_parent(table_path)?;
    let parent = table_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent)?;
    let name = table_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| eyre::eyre!("invalid WorkTable path"))?;
    let lock_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(parent.join(format!(".{name}.s3-generation-writer.lock")))?;
    match lock_file.try_lock() {
        Ok(()) => Ok(lock_file),
        Err(fs::TryLockError::WouldBlock) => {
            eyre::bail!("WorkTable table path already has an active local writer")
        }
        Err(error) => Err(eyre::eyre!("failed to acquire WorkTable local writer lock: {error:?}")),
    }
}

fn ensure_directory_or_absent(path: &Path) -> eyre::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => eyre::bail!("WorkTable persistence path is not a real directory"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn remove_path_if_exists(path: &Path) -> eyre::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path)?,
        Ok(_) => fs::remove_file(path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn install_restored_table(table_path: &Path, stage: &Path) -> eyre::Result<()> {
    ensure_table_parent(table_path)?;
    match fs::symlink_metadata(table_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            rename_directory_no_replace(stage, table_path)?;
            Ok(())
        }
        Err(error) => Err(error.into()),
        Ok(metadata) if !metadata.is_dir() => {
            eyre::bail!("WorkTable persistence path is not a real directory")
        }
        Ok(_) => {
            let backup = sibling_path(table_path, "backup")?;
            fs::rename(table_path, &backup)?;
            if let Err(error) = rename_directory_no_replace(stage, table_path) {
                if let Err(rollback_error) = fs::rename(&backup, table_path) {
                    eyre::bail!(
                        "failed to install validated S3 generation ({error}) and restore local table ({rollback_error})"
                    );
                }
                return Err(error.into());
            }
            if let Err(error) = fs::remove_dir_all(&backup) {
                tracing::warn!(path = %backup.display(), error = %error, "installed validated S3 generation but could not remove prior local table");
            }
            Ok(())
        }
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
>
    S3GenerationPersistenceEngine<
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
    /// Open with a caller-owned HTTP agent, including its TLS policy.
    pub async fn new_with_agent(config: S3DiskConfig, client: Agent) -> eyre::Result<Self> {
        Self::new_with_agent_and_transport(config, client, S3TransportOptions::default()).await
    }

    /// Open with explicit temporary credentials and URL style using the
    /// default HTTP agent configuration.
    pub async fn new_with_transport(
        config: S3DiskConfig,
        transport: S3TransportOptions,
    ) -> eyre::Result<Self> {
        let client = ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(30))
            .build();
        Self::new_with_agent_and_transport(config, client, transport).await
    }

    /// Open with a caller-owned HTTP agent and explicit per-table S3
    /// transport settings.
    pub async fn new_with_agent_and_transport(
        config: S3DiskConfig,
        client: Agent,
        transport: S3TransportOptions,
    ) -> eyre::Result<Self> {
        let table_path = PathBuf::from(config.disk.table_path());
        let local_writer_lock = acquire_local_writer_lock(&table_path)?;
        ensure_directory_or_absent(&table_path)?;

        let publisher = S3GenerationPublisher::new_with_agent_and_transport(
            &config,
            client.clone(),
            transport.clone(),
        )?;
        let reader = S3GenerationReader::new_with_agent_and_transport(&config, client, transport)?;
        let mut committed_version = None;
        let mut committed_table_hash = None;

        if let Some(pointer) = publisher.get_commit_pointer()? {
            ensure_table_parent(&table_path)?;
            let stage = sibling_path(&table_path, "restore")?;
            let restored = match restore_pointer_to(&reader, pointer, &stage) {
                Ok(restored) => restored,
                Err(error) => {
                    if let Err(cleanup_error) = remove_path_if_exists(&stage) {
                        eyre::bail!(
                            "S3 generation restore failed ({error}) and staging cleanup failed ({cleanup_error})"
                        );
                    }
                    return Err(error);
                }
            };
            let stage_path = match stage.to_str() {
                Some(path) => path.to_owned(),
                None => {
                    if let Err(cleanup_error) = remove_path_if_exists(&stage) {
                        eyre::bail!(
                            "S3 generation staging path is not UTF-8 and cleanup failed ({cleanup_error})"
                        );
                    }
                    eyre::bail!("S3 generation staging path is not UTF-8");
                }
            };
            let validation_disk = DiskConfig {
                config_path: config.disk.config_path.clone(),
                tables_path: stage_path,
                version: config.disk.version,
            };
            let validation_result: eyre::Result<()> = async {
                let validation_inner: DiskPersistenceEngine<
                    SpaceData,
                    SpacePrimaryIndex,
                    SpaceSecondaryIndexes,
                    PrimaryKey,
                    SecondaryIndexEvents,
                    AvailableIndexes,
                    PrimaryKeyGenState,
                > = DiskPersistenceEngine::new(validation_disk).await?;
                drop(validation_inner);
                let validated = fingerprint_table(&stage)?;
                if validated.hash != restored.table_hash {
                    eyre::bail!("disk initialization changed the validated S3 generation");
                }
                Ok(())
            }
            .await;
            if let Err(error) = validation_result {
                if let Err(cleanup_error) = remove_path_if_exists(&stage) {
                    eyre::bail!(
                        "S3 generation disk validation failed ({error}) and staging cleanup failed ({cleanup_error})"
                    );
                }
                return Err(error);
            }
            if let Err(error) = install_restored_table(&table_path, &stage) {
                if let Err(cleanup_error) = remove_path_if_exists(&stage) {
                    eyre::bail!(
                        "S3 generation install failed ({error}) and staging cleanup failed ({cleanup_error})"
                    );
                }
                return Err(error);
            }
            committed_version = Some(restored.version);
            committed_table_hash = Some(restored.table_hash);
        }

        let inner = DiskPersistenceEngine::new(config.disk.clone()).await?;
        let mut engine = Self {
            inner,
            config,
            publisher,
            reader,
            _local_writer_lock: local_writer_lock,
            committed_version,
            committed_table_hash,
            fenced: false,
            marker: PhantomData,
        };

        if let Some(expected_hash) = engine.committed_table_hash {
            let local = fingerprint_table(&table_path)?;
            if local.hash != expected_hash {
                eyre::bail!("local WorkTable changed while opening a validated S3 generation");
            }
            let current = engine.read_current_generation()?;
            if current.table_hash != expected_hash {
                eyre::bail!("S3 generation changed while opening the committed WorkTable");
            }
            engine.committed_version = Some(current.version);
        } else {
            engine.publish_local_snapshot()?;
        }

        Ok(engine)
    }

    fn ensure_not_fenced(&self) -> eyre::Result<()> {
        if self.fenced {
            eyre::bail!(
                "S3 generation persistence is fenced; reopen to restore the committed generation"
            );
        }
        Ok(())
    }

    fn read_current_generation(&self) -> eyre::Result<RemoteGenerationState> {
        let table_path = Path::new(self.config.disk.table_path());
        let restore_path = sibling_path(table_path, "readback")?;
        let result = (|| {
            let pointer = self.publisher.get_commit_pointer()?.ok_or_else(|| {
                eyre::eyre!("S3 generation commit pointer is missing during readback")
            })?;
            restore_pointer_to(&self.reader, pointer, &restore_path)
        })();
        let cleanup = remove_path_if_exists(&restore_path);
        match (result, cleanup) {
            (Ok(state), Ok(())) => Ok(state),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(cleanup_error)) => Err(cleanup_error),
            (Err(error), Err(cleanup_error)) => eyre::bail!(
                "S3 generation readback failed ({error}) and staging cleanup failed ({cleanup_error})"
            ),
        }
    }

    fn publish_local_snapshot(&mut self) -> eyre::Result<()> {
        self.ensure_not_fenced()?;
        let snapshot = match OwnedTableSnapshot::read(Path::new(self.config.disk.table_path())) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.fenced = true;
                return Err(error);
            }
        };
        if self.committed_table_hash == Some(snapshot.hash) {
            return Ok(());
        }

        let segments = match snapshot.generation_segments() {
            Ok(segments) => segments,
            Err(error) => {
                self.fenced = true;
                return Err(error);
            }
        };
        let files = snapshot.generation_files(&segments);
        let generation_id = Uuid::new_v4();
        let publish_result = self.publisher.publish_generation(
            generation_id,
            &files,
            self.committed_version.as_ref(),
        );

        match publish_result {
            Ok(S3GenerationPublishOutcome::Published(receipt))
            | Ok(S3GenerationPublishOutcome::AlreadyPublished(receipt)) => {
                self.validate_and_adopt(&snapshot, Some(&receipt), None)
            }
            Ok(S3GenerationPublishOutcome::Conflict) => self.validate_and_adopt(
                &snapshot,
                None,
                Some(eyre::eyre!("S3 generation compare-and-swap conflict")),
            ),
            Err(error) => self.validate_and_adopt(&snapshot, None, Some(error)),
        }
    }

    fn validate_and_adopt(
        &mut self,
        local: &OwnedTableSnapshot,
        receipt: Option<&S3GenerationPublishReceipt>,
        publish_error: Option<eyre::Report>,
    ) -> eyre::Result<()> {
        let remote = match self.read_current_generation() {
            Ok(remote) => remote,
            Err(readback_error) => {
                self.fenced = true;
                return match publish_error {
                    Some(publish_error) => Err(eyre::eyre!(
                        "generation publish could not be validated by readback ({readback_error}); publish result: {publish_error}"
                    )),
                    None => Err(eyre::eyre!(
                        "generation publish could not be validated by readback ({readback_error})"
                    )),
                };
            }
        };

        if remote.table_hash != local.hash {
            self.fenced = true;
            return match publish_error {
                Some(publish_error) => Err(eyre::eyre!(
                    "S3 generation was not confirmed and remote files differ from local state; engine fenced: {publish_error}"
                )),
                None => Err(eyre::eyre!(
                    "S3 generation readback differs from local state; engine fenced"
                )),
            };
        }

        let current_local = match fingerprint_table(Path::new(self.config.disk.table_path())) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.fenced = true;
                return Err(error);
            }
        };
        if current_local.hash != local.hash {
            self.fenced = true;
            eyre::bail!("local WorkTable changed during S3 generation publication; engine fenced");
        }

        if let Some(receipt) = receipt {
            if remote.pointer_hash == receipt.pointer_hash
                && (remote.generation_id != receipt.generation_id
                    || remote.file_count != receipt.file_count)
            {
                self.fenced = true;
                eyre::bail!(
                    "S3 generation receipt does not match validated pointer; engine fenced"
                );
            }
        }

        self.committed_version = Some(remote.version);
        self.committed_table_hash = Some(local.hash);
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
    for S3GenerationPersistenceEngine<
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

    async fn new(config: Self::Config) -> eyre::Result<Self> {
        let client = ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(30))
            .build();
        Self::new_with_agent(config, client).await
    }

    async fn apply_operation(
        &mut self,
        operation: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents>,
    ) -> eyre::Result<()> {
        self.ensure_not_fenced()?;
        if let Err(error) = self.inner.apply_operation(operation).await {
            self.fenced = true;
            return Err(error);
        }
        self.publish_local_snapshot()
    }

    async fn apply_batch_operation(
        &mut self,
        operation: BatchOperation<
            PrimaryKeyGenState,
            PrimaryKey,
            SecondaryIndexEvents,
            AvailableIndexes,
        >,
    ) -> eyre::Result<()> {
        self.ensure_not_fenced()?;
        if let Err(error) = self.inner.apply_batch_operation(operation).await {
            self.fenced = true;
            return Err(error);
        }
        self.publish_local_snapshot()
    }

    async fn reclaim_data_pages(&mut self, page_ids: Vec<data_bucket::PageId>) -> eyre::Result<()> {
        self.ensure_not_fenced()?;
        if let Err(error) = self.inner.reclaim_data_pages(page_ids).await {
            self.fenced = true;
            return Err(error);
        }
        self.publish_local_snapshot()
    }

    async fn ensure_schema(
        &mut self,
        row_schema: Vec<(String, String)>,
        primary_key_fields: Vec<String>,
        secondary_index_types: Vec<(String, String)>,
    ) -> eyre::Result<()> {
        self.ensure_not_fenced()?;
        if let Err(error) = self
            .inner
            .ensure_schema(row_schema, primary_key_fields, secondary_index_types)
            .await
        {
            self.fenced = true;
            return Err(error);
        }
        self.publish_local_snapshot()
    }

    async fn validate_schema(
        &mut self,
        row_schema: Vec<(String, String)>,
        primary_key_fields: Vec<String>,
        secondary_index_types: Vec<(String, String)>,
    ) -> eyre::Result<()> {
        self.ensure_not_fenced()?;
        if let Err(error) = self
            .inner
            .validate_schema(row_schema, primary_key_fields, secondary_index_types)
            .await
        {
            self.fenced = true;
            return Err(error);
        }
        self.publish_local_snapshot()
    }

    fn config(&self) -> &Self::Config {
        &self.config
    }
}
