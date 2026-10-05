//! Conditional publication of immutable per-table S3 generations.
//!
//! This module only writes the new generation namespace. It does not change
//! the existing manifest reader, legacy listing fallback, or automatic S3
//! persistence writer.

use core::time::Duration;
use std::collections::HashSet;
use std::fmt::Write as _;
use std::io::Read as _;
use std::path::{Component, Path};

use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use ureq::Agent;
use url::Url;
use uuid::Uuid;

use super::s3_support::S3DiskConfig;
use crate::persistence::PersistenceConfig;
use crate::prelude::{WT_DATA_EXTENSION, WT_INDEX_EXTENSION};

const COMMIT_MAGIC: &[u8; 8] = b"WTS3G001";
const COMMIT_POINTER_KEY: &str = "generation-commit.v1";
const SEGMENT_TARGET: usize = 4 * 1024 * 1024;
const MAX_FILES: usize = 16_384;
const MAX_SEGMENTS: usize = 4_194_304;
const MAX_OBJECT_BYTES: usize = 256 * 1024 * 1024;
const REQUEST_TTL: Duration = Duration::from_secs(3600);

/// One immutable byte range in a file of the generation.
pub struct S3GenerationSegment<'a> {
    /// Offset of these bytes in the reconstructed file.
    pub file_offset: u64,
    /// Complete segment bytes. Each segment is stored as its own object.
    pub bytes: &'a [u8],
}

/// One complete file in the generation. Files and their segments must be
/// sorted by path and file offset respectively, with no gaps or overlaps.
pub struct S3GenerationFile<'a> {
    /// Relative table-file path, such as .wt.data.
    pub path: &'a str,
    /// Exact reconstructed file length.
    pub length: u64,
    /// Extents that cover the file from byte zero to its exact length.
    pub segments: &'a [S3GenerationSegment<'a>],
}

/// An opaque S3 entity tag captured from a successful response or GET.
///
/// Construct versions by reading the current object. The ETag is kept exactly
/// as returned by S3 so it can be used as an If-Match value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3ObjectVersion {
    etag: String,
}

impl S3ObjectVersion {
    /// Return the opaque, quoted ETag returned by S3.
    pub fn etag(&self) -> &str {
        &self.etag
    }

    fn from_etag(etag: &str) -> eyre::Result<Self> {
        let opaque = etag
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .ok_or_else(|| eyre::eyre!("S3 response did not contain a strong quoted ETag"))?;
        if opaque.is_empty() || opaque.bytes().any(|byte| byte == b'"' || byte <= 0x20 || byte == 0x7f) {
            eyre::bail!("S3 response did not contain a usable ETag");
        }
        Ok(Self { etag: etag.to_owned() })
    }
}

/// An object body and its version, read from the configured table namespace.
pub struct S3ObjectSnapshot {
    /// Complete object bytes returned by the same GET as version.
    pub bytes: Vec<u8>,
    /// Strong ETag for these bytes.
    pub version: S3ObjectVersion,
}

/// Result of an If-None-Match or If-Match write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConditionalWriteOutcome {
    /// The conditional PUT succeeded and returned this new object version.
    Applied(S3ObjectVersion),
    /// The response was ambiguous, but a read confirmed the requested bytes
    /// are already current.
    AlreadyApplied(S3ObjectVersion),
    /// S3 rejected the condition. The caller can read the current value and
    /// choose whether to retry against its new version.
    Conflict,
}

/// Receipt for a generation whose commit pointer has been published.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3GenerationPublishReceipt {
    /// UUID used in the immutable generation object namespace.
    pub generation_id: Uuid,
    /// ETag returned for the committed pointer.
    pub pointer_version: S3ObjectVersion,
    /// BLAKE3 of the full commit-pointer object, including its checksum.
    pub pointer_hash: [u8; 32],
    /// Number of complete files described by the pointer.
    pub file_count: u32,
    /// Number of immutable segments referenced by the pointer.
    pub segment_count: u32,
}

/// Result of publishing the mutable commit pointer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum S3GenerationPublishOutcome {
    /// The pointer was atomically created or replaced.
    Published(S3GenerationPublishReceipt),
    /// A read confirmed this exact pointer was already current.
    AlreadyPublished(S3GenerationPublishReceipt),
    /// S3 rejected the pointer create or compare-and-swap condition.
    Conflict,
}

/// S3 client scoped to one table's new immutable generation namespace.
///
/// Conditional requests are signed by rusty-s3 and carry the same header on
/// the ureq request. The table root is derived from S3DiskConfig, while object
/// methods accept only safe relative keys.
pub struct S3GenerationPublisher {
    bucket: Bucket,
    credentials: Credentials,
    client: Agent,
    table_root: String,
}

impl S3GenerationPublisher {
    /// Create a generation publisher with the caller's configured HTTP agent.
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

    /// Read a table-relative object and capture the strong ETag needed for CAS.
    pub fn get_object(&self, relative_key: &str) -> eyre::Result<Option<S3ObjectSnapshot>> {
        let key = self.object_key(relative_key)?;
        self.get_object_at(&key)
    }

    /// Read the new-format commit pointer. This does not read or fall back to
    /// the legacy manifest or loose-object namespace.
    pub fn get_commit_pointer(&self) -> eyre::Result<Option<S3ObjectSnapshot>> {
        self.get_object(COMMIT_POINTER_KEY)
    }

    /// Create an object only if no current object exists at this table-relative
    /// key. A precondition failure is returned as Conflict.
    pub fn put_if_absent(&self, relative_key: &str, bytes: &[u8]) -> eyre::Result<ConditionalWriteOutcome> {
        let key = self.object_key(relative_key)?;
        self.put_conditionally(&key, bytes, None)
    }

    /// Replace an object only while its ETag still equals expected_version.
    pub fn compare_and_swap(
        &self,
        relative_key: &str,
        expected_version: &S3ObjectVersion,
        bytes: &[u8],
    ) -> eyre::Result<ConditionalWriteOutcome> {
        let key = self.object_key(relative_key)?;
        self.put_conditionally(&key, bytes, Some(expected_version))
    }

    /// Upload every immutable segment under a UUID-specific namespace, read
    /// each distinct segment back and verify its exact bytes, then conditionally
    /// publish one checksummed commit pointer.
    ///
    /// Pass None for create-if-absent, or the current commit pointer's ETag for
    /// compare-and-swap. Persist and reuse generation_id across retries; never
    /// reuse it for a different snapshot. A failed pointer condition leaves
    /// only unreferenced generation objects.
    pub fn publish_generation(
        &self,
        generation_id: Uuid,
        files: &[S3GenerationFile<'_>],
        expected_commit: Option<&S3ObjectVersion>,
    ) -> eyre::Result<S3GenerationPublishOutcome> {
        if files.is_empty() {
            eyre::bail!("S3 generation must include WorkTable files");
        }
        if generation_id.is_nil() {
            eyre::bail!("S3 generation ID must not be nil");
        }
        let file_count = u32::try_from(files.len())?;
        if files.len() > MAX_FILES {
            eyre::bail!("S3 generation contains too many files");
        }

        let generation = generation_id.to_string();
        let mut pointer = Vec::new();
        pointer.extend_from_slice(COMMIT_MAGIC);
        pointer.extend_from_slice(generation_id.as_bytes());
        pointer.extend_from_slice(&file_count.to_le_bytes());

        let mut previous_path: Option<&str> = None;
        let mut seen_paths = HashSet::with_capacity(files.len());
        let mut upload_segments = Vec::new();
        let mut segment_count = 0_usize;
        let primary_index_path = format!("primary{WT_INDEX_EXTENSION}");
        let mut has_data_file = false;
        let mut has_primary_index = false;
        for file in files {
            validate_table_path(file.path)?;
            if previous_path.is_some_and(|previous| previous >= file.path) {
                eyre::bail!("S3 generation file paths must be strictly sorted");
            }
            reject_file_parent_conflict(file.path, &seen_paths)?;
            seen_paths.insert(file.path.to_owned());
            previous_path = Some(file.path);

            if file.path == WT_DATA_EXTENSION {
                if file.length == 0 {
                    eyre::bail!("S3 generation data file must not be empty");
                }
                has_data_file = true;
            }
            if file.path == primary_index_path.as_str() {
                if file.length == 0 {
                    eyre::bail!("S3 generation primary index must not be empty");
                }
                has_primary_index = true;
            }

            let path = file.path.as_bytes();
            let path_length =
                u16::try_from(path.len()).map_err(|_| eyre::eyre!("S3 generation file path is too long"))?;
            pointer.extend_from_slice(&path_length.to_le_bytes());
            pointer.extend_from_slice(path);
            pointer.extend_from_slice(&file.length.to_le_bytes());

            let extent_count = u32::try_from(file.segments.len())?;
            pointer.extend_from_slice(&extent_count.to_le_bytes());
            let mut expected_offset = 0_u64;
            for segment in file.segments {
                if segment.file_offset != expected_offset {
                    eyre::bail!("S3 generation extents must cover each file contiguously");
                }
                if segment.bytes.is_empty() || segment.bytes.len() > SEGMENT_TARGET {
                    eyre::bail!("S3 generation segment has an invalid length");
                }
                let length = u32::try_from(segment.bytes.len())?;
                let hash = *blake3::hash(segment.bytes).as_bytes();
                let relative_key = format!("generations/{generation}/segments/{}", hex_hash(&hash));

                pointer.extend_from_slice(&segment.file_offset.to_le_bytes());
                pointer.extend_from_slice(&length.to_le_bytes());
                pointer.extend_from_slice(&hash);
                upload_segments.push((relative_key, segment.bytes, hash));
                expected_offset = expected_offset
                    .checked_add(u64::from(length))
                    .ok_or_else(|| eyre::eyre!("S3 generation file length overflow"))?;
                segment_count = segment_count
                    .checked_add(1)
                    .ok_or_else(|| eyre::eyre!("S3 generation segment count overflow"))?;
                if segment_count > MAX_SEGMENTS {
                    eyre::bail!("S3 generation contains too many segments");
                }
            }
            if expected_offset != file.length {
                eyre::bail!("S3 generation extents do not cover the exact file length");
            }
            if pointer.len() > MAX_OBJECT_BYTES {
                eyre::bail!("S3 generation commit pointer is too large");
            }
        }

        if !has_data_file {
            eyre::bail!("S3 generation must include its WorkTable data file");
        }
        if !has_primary_index {
            eyre::bail!("S3 generation must include its non-empty primary index");
        }

        let checksum = blake3::hash(&pointer);
        pointer.extend_from_slice(checksum.as_bytes());
        if pointer.len() > MAX_OBJECT_BYTES {
            eyre::bail!("S3 generation commit pointer is too large");
        }
        let pointer_hash = *blake3::hash(&pointer).as_bytes();

        let mut verified = HashSet::new();
        for (relative_key, bytes, hash) in upload_segments {
            if !verified.insert(hash) {
                continue;
            }
            match self.put_if_absent(&relative_key, bytes)? {
                ConditionalWriteOutcome::Applied(_)
                | ConditionalWriteOutcome::AlreadyApplied(_)
                | ConditionalWriteOutcome::Conflict => {}
            }
            let stored = self
                .get_object(&relative_key)?
                .ok_or_else(|| eyre::eyre!("S3 generation segment disappeared after upload"))?;
            if stored.bytes.as_slice() != bytes
                || stored.bytes.len() != bytes.len()
                || blake3::hash(&stored.bytes).as_bytes() != &hash
            {
                eyre::bail!("S3 generation segment failed read-back verification");
            }
        }

        let pointer_result = match expected_commit {
            Some(version) => self.compare_and_swap(COMMIT_POINTER_KEY, version, &pointer)?,
            None => self.put_if_absent(COMMIT_POINTER_KEY, &pointer)?,
        };
        let segment_count = u32::try_from(segment_count)?;
        let receipt = |pointer_version| S3GenerationPublishReceipt {
            generation_id,
            pointer_version,
            pointer_hash,
            file_count,
            segment_count,
        };
        Ok(match pointer_result {
            ConditionalWriteOutcome::Applied(version) => S3GenerationPublishOutcome::Published(receipt(version)),
            ConditionalWriteOutcome::AlreadyApplied(version) => {
                S3GenerationPublishOutcome::AlreadyPublished(receipt(version))
            }
            ConditionalWriteOutcome::Conflict => S3GenerationPublishOutcome::Conflict,
        })
    }

    fn object_key(&self, relative_key: &str) -> eyre::Result<String> {
        validate_relative_key(relative_key)?;
        Ok(format!("{}/{relative_key}", self.table_root))
    }

    fn get_object_at(&self, key: &str) -> eyre::Result<Option<S3ObjectSnapshot>> {
        let action = self.bucket.get_object(Some(&self.credentials), key);
        let url = action.sign(REQUEST_TTL);
        let response = match self.client.get(url.as_str()).call() {
            Ok(response) => response,
            Err(ureq::Error::Status(404, _)) => return Ok(None),
            Err(error) => return Err(sanitized_ureq_error(error)),
        };

        let etag = response
            .header("etag")
            .ok_or_else(|| eyre::eyre!("S3 object response did not include an ETag"))?
            .to_owned();
        let version = S3ObjectVersion::from_etag(&etag)?;
        let mut bytes = Vec::new();
        response
            .into_reader()
            .take((MAX_OBJECT_BYTES as u64) + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_OBJECT_BYTES {
            eyre::bail!("S3 object exceeds the generation API size limit");
        }
        Ok(Some(S3ObjectSnapshot { bytes, version }))
    }

    fn put_conditionally(
        &self,
        key: &str,
        bytes: &[u8],
        expected_version: Option<&S3ObjectVersion>,
    ) -> eyre::Result<ConditionalWriteOutcome> {
        let (header_name, header_value) = match expected_version {
            Some(version) => ("if-match", version.etag()),
            None => ("if-none-match", "*"),
        };
        let mut action = self.bucket.put_object(Some(&self.credentials), key);
        action.headers_mut().insert(header_name, header_value);
        let url = action.sign(REQUEST_TTL);
        let request = self.client.put(url.as_str()).set(header_name, header_value);

        match request.send_bytes(bytes) {
            Ok(response) => {
                let etag = response.header("etag").map(str::to_owned);
                let mut reader = response.into_reader();
                std::io::copy(&mut reader, &mut std::io::sink())?;
                if let Some(etag) = etag {
                    return Ok(ConditionalWriteOutcome::Applied(S3ObjectVersion::from_etag(&etag)?));
                }
                let current = self
                    .get_object_at(key)?
                    .ok_or_else(|| eyre::eyre!("S3 object disappeared after conditional PUT"))?;
                if current.bytes.as_slice() != bytes {
                    eyre::bail!("S3 object changed before its ETag could be read");
                }
                Ok(ConditionalWriteOutcome::AlreadyApplied(current.version))
            }
            Err(error) => {
                let condition_failed = matches!(&error, ureq::Error::Status(409 | 412, _));
                let write_error = sanitized_ureq_error(error);
                if condition_failed {
                    return match self.get_object_at(key) {
                        Ok(Some(current)) if current.bytes.as_slice() == bytes => {
                            Ok(ConditionalWriteOutcome::AlreadyApplied(current.version))
                        }
                        _ => Ok(ConditionalWriteOutcome::Conflict),
                    };
                }
                match self.get_object_at(key) {
                    Ok(Some(current)) if current.bytes.as_slice() == bytes => {
                        Ok(ConditionalWriteOutcome::AlreadyApplied(current.version))
                    }
                    _ => Err(write_error),
                }
            }
        }
    }
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

fn hex_hash(hash: &[u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in hash {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn sanitized_ureq_error(error: ureq::Error) -> eyre::Report {
    match error {
        ureq::Error::Status(status, _) => {
            eyre::eyre!("S3 request failed with HTTP status {status}")
        }
        ureq::Error::Transport(_) => eyre::eyre!("S3 request failed at the transport layer"),
    }
}
