//! Strict restoration of the immutable WTS3G001 S3 generation format.
//!
//! This reader has its own read-only S3 transport and independently parses and
//! validates the commit pointer. It requires a fresh destination and never
//! falls back to legacy objects or an empty table. Restored files are staged in
//! a sibling directory and atomically installed without replacing a path that
//! appeared after the initial freshness check.

use core::time::Duration;
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{Read as _, Write as _};
use std::path::{Component, Path};

use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use ureq::Agent;
use url::Url;
use uuid::Uuid;

use super::s3_support::S3DiskConfig;
use super::s3_support::rename_directory_no_replace;
use crate::persistence::PersistenceConfig;
use crate::prelude::{WT_DATA_EXTENSION, WT_INDEX_EXTENSION};

const COMMIT_MAGIC: &[u8; 8] = b"WTS3G001";
const COMMIT_POINTER_KEY: &str = "generation-commit.v1";
const CHECKSUM_LENGTH: usize = 32;
const MAX_FILES: usize = 16_384;
const MAX_SEGMENTS: usize = 4_194_304;
const SEGMENT_TARGET: usize = 4 * 1024 * 1024;
const MAX_POINTER_BYTES: usize = 256 * 1024 * 1024;
const MAX_OBJECT_BYTES: usize = 256 * 1024 * 1024;
const REQUEST_TTL: Duration = Duration::from_secs(3600);

/// Summary of a generation restored from its committed pointer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3GenerationReadReceipt {
    /// Identity encoded in the WTS3G001 pointer and used in segment keys.
    pub generation_id: Uuid,
    /// BLAKE3 of the complete pointer, including its checksum.
    pub pointer_hash: [u8; 32],
    /// Number of table files restored.
    pub file_count: u32,
    /// Number of segment references validated and restored.
    pub segment_count: u32,
    /// Sum of all restored file lengths.
    pub total_bytes: u64,
}

/// Strict reader for an M320-published WTS3G001 generation.
///
/// This owns a read-only S3 client. It does not depend on or modify the
/// generation publisher or WorkTable's legacy S3 engine. A missing commit
/// pointer is an error; it is never interpreted as an empty table or a request
/// to list legacy objects.
pub struct S3GenerationReader {
    bucket: Bucket,
    credentials: Credentials,
    client: Agent,
    table_root: String,
}

impl S3GenerationReader {
    /// Create a reader using the caller's configured HTTP agent and TLS policy.
    pub fn new_with_agent(config: &S3DiskConfig, client: Agent) -> eyre::Result<Self> {
        let credentials = Credentials::new(&config.s3.access_key, &config.s3.secret_key);
        let endpoint: Url = config.s3.endpoint.parse()?;
        let region = config.s3.region.clone().unwrap_or_else(|| "auto".to_string());
        let bucket = Bucket::new(endpoint, UrlStyle::Path, config.s3.bucket_name.clone(), region)?;

        let table_name = Path::new(config.disk.table_path())
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| eyre::eyre!("invalid table path"))?;
        validate_relative_key(table_name)?;

        let prefix = config.s3.prefix.as_deref().unwrap_or("").trim_end_matches('/');
        if !prefix.is_empty() {
            validate_relative_key(prefix)?;
        }
        let table_root = if prefix.is_empty() {
            table_name.to_owned()
        } else {
            format!("{prefix}/{table_name}")
        };

        Ok(Self {
            bucket,
            credentials,
            client,
            table_root,
        })
    }

    /// Restore the committed generation into a path that does not already
    /// exist. The method verifies every referenced segment while writing a
    /// sibling staging directory, then atomically installs that complete
    /// directory with a no-replace operation. Existing destinations are never
    /// replaced or modified, including one created during the S3 read.
    pub fn restore_to(&self, destination: impl AsRef<Path>) -> eyre::Result<S3GenerationReadReceipt> {
        let destination = destination.as_ref();
        ensure_fresh_destination(destination)?;

        let pointer = self
            .get_commit_pointer()?
            .ok_or_else(|| eyre::eyre!("S3 generation commit pointer is missing; refusing an empty restore"))?;
        let pointer_hash = *blake3::hash(&pointer).as_bytes();
        let generation = GenerationCommit::decode(&pointer)?;
        drop(pointer);
        let receipt = S3GenerationReadReceipt {
            generation_id: generation.generation_id,
            pointer_hash,
            file_count: generation.file_count,
            segment_count: generation.segment_count,
            total_bytes: generation.total_bytes,
        };

        let stage = staging_path(destination)?;
        if let Some(parent) = destination.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        // Reserve a unique sibling staging directory. The complete snapshot is
        // written and synced there before an atomic no-replace install exposes
        // it at the requested destination.
        fs::create_dir(&stage)?;
        if let Err(error) = restore_files(self, &stage, &generation) {
            return match fs::remove_dir_all(&stage) {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(eyre::eyre!(
                    "S3 generation restore failed ({error}) and incomplete staging cleanup failed ({cleanup_error})"
                )),
            };
        }
        if let Err(error) = rename_directory_no_replace(&stage, destination) {
            return match fs::remove_dir_all(&stage) {
                Ok(()) => Err(error.into()),
                Err(cleanup_error) => Err(eyre::eyre!(
                    "S3 generation install failed ({error}) and staging cleanup failed ({cleanup_error})"
                )),
            };
        }

        Ok(receipt)
    }

    fn get_commit_pointer(&self) -> eyre::Result<Option<Vec<u8>>> {
        self.get_object(COMMIT_POINTER_KEY)
    }

    fn get_object(&self, relative_key: &str) -> eyre::Result<Option<Vec<u8>>> {
        let key = self.object_key(relative_key)?;
        let action = self.bucket.get_object(Some(&self.credentials), &key);
        let url = action.sign(REQUEST_TTL);
        let response = match self.client.get(url.as_str()).call() {
            Ok(response) => response,
            Err(ureq::Error::Status(404, _)) => return Ok(None),
            Err(error) => return Err(sanitized_ureq_error(error)),
        };

        let mut bytes = Vec::new();
        response
            .into_reader()
            .take((MAX_OBJECT_BYTES as u64) + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_OBJECT_BYTES {
            eyre::bail!("S3 generation object exceeds the size limit");
        }
        Ok(Some(bytes))
    }

    fn object_key(&self, relative_key: &str) -> eyre::Result<String> {
        validate_relative_key(relative_key)?;
        Ok(format!("{}/{relative_key}", self.table_root))
    }
}

#[derive(Debug)]
struct GenerationCommit {
    generation_id: Uuid,
    file_count: u32,
    segment_count: u32,
    total_bytes: u64,
    files: Vec<GenerationFile>,
}

#[derive(Debug)]
struct GenerationFile {
    path: String,
    length: u64,
    segments: Vec<GenerationSegment>,
}

#[derive(Clone, Copy, Debug)]
struct GenerationSegment {
    file_offset: u64,
    length: u32,
    hash: [u8; 32],
}

impl GenerationCommit {
    fn decode(bytes: &[u8]) -> eyre::Result<Self> {
        let minimum_length = COMMIT_MAGIC.len() + 16 + 4 + CHECKSUM_LENGTH;
        if bytes.len() < minimum_length {
            eyre::bail!("S3 generation commit pointer is truncated");
        }
        if bytes.len() > MAX_POINTER_BYTES {
            eyre::bail!("S3 generation commit pointer exceeds the size limit");
        }

        let payload_length = bytes.len() - CHECKSUM_LENGTH;
        let (payload, checksum) = bytes.split_at(payload_length);
        if blake3::hash(payload).as_bytes() != checksum {
            eyre::bail!("S3 generation commit pointer checksum mismatch");
        }

        let mut reader = PointerReader::new(payload);
        if reader.take(COMMIT_MAGIC.len())? != COMMIT_MAGIC {
            eyre::bail!("unsupported S3 generation commit pointer format");
        }
        let generation_id = Uuid::from_bytes(reader.array::<16>()?);
        if generation_id.is_nil() {
            eyre::bail!("S3 generation identity must not be nil");
        }

        let file_count = reader.u32()?;
        let file_count_usize = usize::try_from(file_count)?;
        if file_count_usize == 0 || file_count_usize > MAX_FILES {
            eyre::bail!("S3 generation contains an invalid file count");
        }

        let mut files = Vec::with_capacity(file_count_usize);
        let mut previous_path: Option<String> = None;
        let mut seen_paths = HashSet::with_capacity(file_count_usize);
        let mut total_segments = 0_usize;
        let mut total_bytes = 0_u64;
        let mut has_data_file = false;
        let mut has_primary_index = false;
        let primary_index_path = format!("primary{WT_INDEX_EXTENSION}");

        for _ in 0..file_count_usize {
            let path_length = usize::from(reader.u16()?);
            if path_length == 0 {
                eyre::bail!("S3 generation contains an empty table path");
            }
            let path = core::str::from_utf8(reader.take(path_length)?)?.to_owned();
            validate_table_path(&path)?;
            if previous_path
                .as_ref()
                .is_some_and(|previous| previous.as_str() >= path.as_str())
            {
                eyre::bail!("S3 generation table paths are not strictly sorted");
            }
            reject_file_parent_conflict(&path, &seen_paths)?;
            seen_paths.insert(path.clone());
            previous_path = Some(path.clone());

            if path != WT_DATA_EXTENSION
                && !Path::new(&path)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(WT_INDEX_EXTENSION))
            {
                eyre::bail!("S3 generation contains a non-table file");
            }

            let file_length = reader.u64()?;
            if path == WT_DATA_EXTENSION {
                if file_length == 0 {
                    eyre::bail!("S3 generation data file must not be empty");
                }
                has_data_file = true;
            }
            if path.as_str() == primary_index_path.as_str() {
                if file_length == 0 {
                    eyre::bail!("S3 generation primary index must not be empty");
                }
                has_primary_index = true;
            }
            total_bytes = total_bytes
                .checked_add(file_length)
                .ok_or_else(|| eyre::eyre!("S3 generation total file length overflow"))?;
            let file_segment_count = usize::try_from(reader.u32()?)?;
            total_segments = total_segments
                .checked_add(file_segment_count)
                .ok_or_else(|| eyre::eyre!("S3 generation segment count overflow"))?;
            if total_segments > MAX_SEGMENTS {
                eyre::bail!("S3 generation contains too many segments");
            }

            let mut segments = Vec::new();
            let mut expected_offset = 0_u64;
            for _ in 0..file_segment_count {
                let file_offset = reader.u64()?;
                let length = reader.u32()?;
                let hash = reader.array::<32>()?;
                if file_offset != expected_offset {
                    eyre::bail!("S3 generation segments do not cover a file contiguously");
                }
                if length == 0 || usize::try_from(length)? > SEGMENT_TARGET {
                    eyre::bail!("S3 generation contains an invalid segment length");
                }
                expected_offset = expected_offset
                    .checked_add(u64::from(length))
                    .ok_or_else(|| eyre::eyre!("S3 generation file length overflow"))?;
                if expected_offset > file_length {
                    eyre::bail!("S3 generation segments exceed the exact file length");
                }
                segments.push(GenerationSegment {
                    file_offset,
                    length,
                    hash,
                });
            }
            if expected_offset != file_length {
                eyre::bail!("S3 generation segments do not cover the exact file length");
            }
            files.push(GenerationFile {
                path,
                length: file_length,
                segments,
            });
        }

        if !has_data_file {
            eyre::bail!("S3 generation does not contain its WorkTable data file");
        }
        if !has_primary_index {
            eyre::bail!("S3 generation does not contain its non-empty primary index");
        }
        if !reader.is_empty() {
            eyre::bail!("S3 generation commit pointer has trailing data");
        }

        Ok(Self {
            generation_id,
            file_count,
            segment_count: u32::try_from(total_segments)?,
            total_bytes,
            files,
        })
    }
}

fn restore_files(reader: &S3GenerationReader, destination: &Path, generation: &GenerationCommit) -> eyre::Result<()> {
    for file in &generation.files {
        let local_path = destination.join(&file.path);
        if let Some(parent) = local_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut restored_file = OpenOptions::new().write(true).create_new(true).open(&local_path)?;
        let mut written = 0_u64;

        for segment in &file.segments {
            if segment.file_offset != written {
                eyre::bail!("S3 generation segments do not cover a file contiguously");
            }
            let key = segment_key(generation.generation_id, &segment.hash);
            let bytes = reader
                .get_object(&key)?
                .ok_or_else(|| eyre::eyre!("S3 generation references a missing segment"))?;
            if bytes.len() != usize::try_from(segment.length)? || blake3::hash(&bytes).as_bytes() != &segment.hash {
                eyre::bail!("S3 generation segment failed length or hash verification");
            }
            restored_file.write_all(&bytes)?;
            written = written
                .checked_add(u64::from(segment.length))
                .ok_or_else(|| eyre::eyre!("S3 generation restored length overflow"))?;
        }

        restored_file.flush()?;
        restored_file.sync_all()?;
        if written != file.length || restored_file.metadata()?.len() != file.length {
            eyre::bail!("S3 generation restored file has an unexpected length");
        }
    }
    Ok(())
}

fn segment_key(generation_id: Uuid, hash: &[u8; 32]) -> String {
    format!("generations/{generation_id}/segments/{}", hex_hash(hash))
}

fn hex_hash(hash: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(hash.len() * 2);
    for &byte in hash {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn validate_table_path(path: &str) -> eyre::Result<()> {
    if path.is_empty()
        || path.starts_with('/')
        || path.chars().any(|character| matches!(character, '\\' | ':' | '\0'))
        || path.bytes().any(|byte| byte.is_ascii_control())
        || path
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..")
        || !Path::new(path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        eyre::bail!("S3 generation contains an unsafe table path");
    }
    if path != WT_DATA_EXTENSION
        && !Path::new(path)
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(WT_INDEX_EXTENSION))
    {
        eyre::bail!("S3 generation contains a path that is not a WorkTable file");
    }
    Ok(())
}

fn validate_relative_key(value: &str) -> eyre::Result<()> {
    if value.is_empty()
        || value.starts_with('/')
        || value.contains('\\')
        || value.bytes().any(|byte| byte == 0 || byte.is_ascii_control())
        || value
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..")
    {
        eyre::bail!("S3 object path must be a safe relative key");
    }
    Ok(())
}

fn sanitized_ureq_error(error: ureq::Error) -> eyre::Report {
    match error {
        ureq::Error::Status(status, _) => {
            eyre::eyre!("S3 request failed with HTTP status {status}")
        }
        ureq::Error::Transport(_) => eyre::eyre!("S3 request failed at the transport layer"),
    }
}

fn reject_file_parent_conflict(path: &str, seen_paths: &HashSet<String>) -> eyre::Result<()> {
    let mut prefix = String::new();
    let mut components = path.split('/').peekable();
    while let Some(component) = components.next() {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(component);
        if components.peek().is_some() && seen_paths.contains(prefix.as_str()) {
            eyre::bail!("S3 generation file paths conflict as file and directory");
        }
    }
    Ok(())
}

fn ensure_fresh_destination(path: &Path) -> eyre::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => eyre::bail!("S3 generation restore destination already exists"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn staging_path(destination: &Path) -> eyre::Result<std::path::PathBuf> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| eyre::eyre!("invalid S3 generation restore destination"))?;
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(parent.join(format!(".{name}.wts3g-stage-{}-{timestamp}", std::process::id())))
}

struct PointerReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> PointerReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> eyre::Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| eyre::eyre!("S3 generation pointer offset overflow"))?;
        let result = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| eyre::eyre!("S3 generation commit pointer is truncated"))?;
        self.position = end;
        Ok(result)
    }

    fn array<const N: usize>(&mut self) -> eyre::Result<[u8; N]> {
        let mut value = [0_u8; N];
        value.copy_from_slice(self.take(N)?);
        Ok(value)
    }

    fn u16(&mut self) -> eyre::Result<u16> {
        Ok(u16::from_le_bytes(self.array::<2>()?))
    }

    fn u32(&mut self) -> eyre::Result<u32> {
        Ok(u32::from_le_bytes(self.array::<4>()?))
    }

    fn u64(&mut self) -> eyre::Result<u64> {
        Ok(u64::from_le_bytes(self.array::<8>()?))
    }

    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }
}
