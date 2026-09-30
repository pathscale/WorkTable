//! Conditional S3 leases for single-writer persisted tables.

use std::env;
use std::fmt::{self, Debug, Formatter};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusty_s3::credentials::Ec2SecurityCredentialsMetadataResponse;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use ureq::Agent;
use url::Url;

const LEASE_OBJECT: &str = "lease.v1";
const LEASE_MAGIC: &[u8; 8] = b"WTLEASE1";
const IMDS_TOKEN_URL: &str = "http://169.254.169.254/latest/api/token";
const IMDS_ROLE_URL: &str = "http://169.254.169.254/latest/meta-data/iam/security-credentials/";
const IMDS_TOKEN_TTL: &str = "21600";
const CREDENTIAL_REFRESH_MARGIN: Duration = Duration::from_secs(300);

/// Configuration for one host's S3 lease object.
///
/// Use a host-specific `prefix` in the same bucket as the table manifests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3LeaseConfig {
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub owner: String,
    pub ttl_secs: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LeaseRecord {
    owner: String,
    expires_at_unix: u64,
    generation: u64,
}

impl LeaseRecord {
    fn encode(&self) -> eyre::Result<Vec<u8>> {
        let owner = self.owner.as_bytes();
        let owner_length = u32::try_from(owner.len())?;
        let mut bytes = Vec::with_capacity(28 + owner.len());
        bytes.extend_from_slice(LEASE_MAGIC);
        bytes.extend_from_slice(&self.generation.to_le_bytes());
        bytes.extend_from_slice(&self.expires_at_unix.to_le_bytes());
        bytes.extend_from_slice(&owner_length.to_le_bytes());
        bytes.extend_from_slice(owner);
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> eyre::Result<Self> {
        if bytes.len() < 28 || bytes.get(..8) != Some(&LEASE_MAGIC[..]) {
            return Err(eyre::eyre!("S3 lease record is truncated or has an unknown format"));
        }
        let generation = u64::from_le_bytes(bytes[8..16].try_into()?);
        let expires_at_unix = u64::from_le_bytes(bytes[16..24].try_into()?);
        let owner_length = usize::try_from(u32::from_le_bytes(bytes[24..28].try_into()?))?;
        if owner_length > 4096 || bytes.len() != 28 + owner_length {
            return Err(eyre::eyre!("S3 lease record has an invalid owner length"));
        }
        let owner = core::str::from_utf8(&bytes[28..])?.to_owned();
        if owner.is_empty() || generation == 0 {
            return Err(eyre::eyre!("S3 lease record has an empty owner or zero generation"));
        }
        Ok(Self {
            owner,
            expires_at_unix,
            generation,
        })
    }
}

struct LeaseState {
    record: LeaseRecord,
    etag: String,
    held: bool,
}

/// A held lease. Clones of its `Arc` observe the same fencing state.
pub struct S3Lease {
    config: S3LeaseConfig,
    bucket: Bucket,
    key: String,
    credentials: S3CredentialsProvider,
    client: Agent,
    state: Arc<Mutex<LeaseState>>,
    control: Arc<Mutex<()>>,
}

impl Clone for S3Lease {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            credentials: self.credentials.clone(),
            client: self.client.clone(),
            state: self.state.clone(),
            control: self.control.clone(),
        }
    }
}

impl Debug for S3Lease {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let state = self.lock_state();
        formatter
            .debug_struct("S3Lease")
            .field("config", &self.config)
            .field("generation", &state.record.generation)
            .field("expires_at_unix", &state.record.expires_at_unix)
            .field("etag", &state.etag)
            .field("held", &state.held)
            .finish()
    }
}

impl S3Lease {
    /// Acquires the lease by conditionally creating its S3 object, or taking
    /// over an expired record with its current ETag.
    pub async fn acquire(config: S3LeaseConfig) -> eyre::Result<Self> {
        validate_config(&config)?;
        let endpoint = s3_endpoint(&config.region)?;
        let bucket = Bucket::new(endpoint, UrlStyle::Path, config.bucket.clone(), config.region.clone())?;
        let client = http_client(Duration::from_secs(30));
        let credentials = S3CredentialsProvider::default_chain(config.region.clone());
        Self::acquire_with_parts(config, bucket, credentials, client).await
    }

    /// Creates a lease against a supplied S3-compatible endpoint and static
    /// credentials. This is useful for object-store adapters and local tests.
    #[doc(hidden)]
    pub async fn acquire_with_endpoint(
        config: S3LeaseConfig,
        endpoint: String,
        access_key: String,
        secret_key: String,
    ) -> eyre::Result<Self> {
        validate_config(&config)?;
        let endpoint: Url = endpoint.parse()?;
        let bucket = Bucket::new(endpoint, UrlStyle::Path, config.bucket.clone(), config.region.clone())?;
        let client = http_client(Duration::from_secs(30));
        let credentials = S3CredentialsProvider::static_or_default(access_key, secret_key, config.region.clone())?;
        Self::acquire_with_parts(config, bucket, credentials, client).await
    }

    async fn acquire_with_parts(
        config: S3LeaseConfig,
        bucket: Bucket,
        credentials: S3CredentialsProvider,
        client: Agent,
    ) -> eyre::Result<Self> {
        let key = lease_key(&config.prefix);
        let current = get_object_with_etag(&bucket, &credentials, &client, &key)?;
        let now = unix_time()?;
        let (record, condition) = match current {
            None => (
                LeaseRecord {
                    owner: config.owner.clone(),
                    expires_at_unix: expiration(now, config.ttl_secs)?,
                    generation: 1,
                },
                PutCondition::IfNoneMatch,
            ),
            Some((bytes, etag)) => {
                let previous = LeaseRecord::decode(&bytes)?;
                if previous.expires_at_unix > now {
                    return Err(eyre::eyre!(
                        "S3 lease is held by {} through Unix time {}",
                        previous.owner,
                        previous.expires_at_unix
                    ));
                }
                let generation = previous
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| eyre::eyre!("S3 lease generation overflow"))?;
                (
                    LeaseRecord {
                        owner: config.owner.clone(),
                        expires_at_unix: expiration(now, config.ttl_secs)?,
                        generation,
                    },
                    PutCondition::IfMatch(etag),
                )
            }
        };
        let bytes = record.encode()?;
        let etag = put_object_conditional(&bucket, &credentials, &client, &key, &bytes, &condition)?;

        Ok(Self {
            config,
            bucket,
            key,
            credentials,
            client,
            state: Arc::new(Mutex::new(LeaseState {
                record,
                etag,
                held: true,
            })),
            control: Arc::new(Mutex::new(())),
        })
    }

    /// Returns this lease's fencing generation.
    pub fn generation(&self) -> u64 {
        self.lock_state().record.generation
    }

    /// Returns whether the locally observed lease has not expired or been
    /// fenced by a failed renewal or liveness check.
    pub fn is_held(&self) -> bool {
        let state = self.lock_state();
        state.held && state.record.expires_at_unix > unix_time().unwrap_or(u64::MAX)
    }

    /// Renews the lease with `If-Match` on its last acknowledged ETag.
    /// Any uncertain or failed renewal immediately fences this handle.
    pub async fn renew(&self) -> eyre::Result<()> {
        let _control = self.lock_control();
        let previous = {
            let state = self.lock_state();
            if !state.held || state.record.expires_at_unix <= unix_time()? {
                drop(state);
                self.mark_lost();
                return Err(eyre::eyre!("cannot renew an expired or fenced S3 lease"));
            }
            (state.record.clone(), state.etag.clone())
        };
        let renewed = LeaseRecord {
            owner: self.config.owner.clone(),
            expires_at_unix: expiration(unix_time()?, self.config.ttl_secs)?,
            generation: previous.0.generation,
        };
        let result = put_object_conditional(
            &self.bucket,
            &self.credentials,
            &self.client,
            &self.key,
            &renewed.encode()?,
            &PutCondition::IfMatch(previous.1.clone()),
        );
        match result {
            Ok(etag) => {
                let mut state = self.lock_state();
                if state.held && state.etag == previous.1 && state.record.generation == renewed.generation {
                    state.record = renewed;
                    state.etag = etag;
                    Ok(())
                } else {
                    state.held = false;
                    Err(eyre::eyre!("S3 lease changed while renewal was in flight"))
                }
            }
            Err(error) => {
                self.mark_lost();
                Err(error.wrap_err("S3 lease renewal failed; this writer is fenced"))
            }
        }
    }

    /// Releases the lease by conditionally marking its record expired.
    /// Keeping the generation in S3 makes every later takeover monotonic.
    pub async fn release(&self) -> eyre::Result<()> {
        let _control = self.lock_control();
        let (mut record, etag) = {
            let state = self.lock_state();
            if !state.held {
                return Err(eyre::eyre!("cannot release a fenced S3 lease"));
            }
            (state.record.clone(), state.etag.clone())
        };
        record.expires_at_unix = 0;
        let result = put_object_conditional(
            &self.bucket,
            &self.credentials,
            &self.client,
            &self.key,
            &record.encode()?,
            &PutCondition::IfMatch(etag),
        );
        self.mark_lost();
        result.map(|_| ())
    }

    pub(crate) fn check_live_generation(&self, generation: u64) -> eyre::Result<()> {
        let _control = self.lock_control();
        let (record, etag, held) = {
            let state = self.lock_state();
            (state.record.clone(), state.etag.clone(), state.held)
        };
        if !held || record.generation != generation || record.expires_at_unix <= unix_time()? {
            self.mark_lost();
            return Err(eyre::eyre!("S3 lease generation is expired or fenced"));
        }

        let live = get_object_with_etag(&self.bucket, &self.credentials, &self.client, &self.key);
        match live {
            Ok(Some((bytes, current_etag))) => {
                let current = match LeaseRecord::decode(&bytes) {
                    Ok(current) => current,
                    Err(error) => {
                        self.mark_lost();
                        return Err(error.wrap_err("S3 lease record is invalid; this writer is fenced"));
                    }
                };
                if current_etag == etag
                    && current.owner == self.config.owner
                    && current.generation == generation
                    && current.expires_at_unix > unix_time()?
                {
                    Ok(())
                } else {
                    self.mark_lost();
                    Err(eyre::eyre!("S3 lease owner, ETag, generation, or expiry changed"))
                }
            }
            Ok(None) => {
                self.mark_lost();
                Err(eyre::eyre!("S3 lease object disappeared"))
            }
            Err(error) => {
                self.mark_lost();
                Err(error.wrap_err("unable to confirm live S3 lease; this writer is fenced"))
            }
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, LeaseState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_control(&self) -> MutexGuard<'_, ()> {
        self.control.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn mark_lost(&self) {
        self.lock_state().held = false;
    }
}

#[derive(Clone)]
pub(crate) struct S3CredentialsProvider {
    source: Arc<CredentialSource>,
}

enum CredentialSource {
    Static(Credentials),
    Default(DefaultCredentialChain),
}

struct DefaultCredentialChain {
    region: String,
    metadata_client: Agent,
    cache: Mutex<Option<CachedCredentials>>,
}

struct CachedCredentials {
    credentials: Credentials,
    refresh_at: SystemTime,
}

impl Debug for S3CredentialsProvider {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let source = match self.source.as_ref() {
            CredentialSource::Static(_) => "static",
            CredentialSource::Default(_) => "default-chain",
        };
        formatter.debug_tuple("S3CredentialsProvider").field(&source).finish()
    }
}

impl S3CredentialsProvider {
    pub(crate) fn static_or_default(access_key: String, secret_key: String, region: String) -> eyre::Result<Self> {
        match (access_key.is_empty(), secret_key.is_empty()) {
            (false, false) => Ok(Self {
                source: Arc::new(CredentialSource::Static(Credentials::new(access_key, secret_key))),
            }),
            (true, true) => Ok(Self::default_chain(region)),
            _ => Err(eyre::eyre!("both S3 access key and secret key must be configured")),
        }
    }

    fn default_chain(region: String) -> Self {
        Self {
            source: Arc::new(CredentialSource::Default(DefaultCredentialChain {
                region,
                metadata_client: ureq::AgentBuilder::new()
                    .try_proxy_from_env(false)
                    .timeout(Duration::from_secs(2))
                    .build(),
                cache: Mutex::new(None),
            })),
        }
    }

    pub(crate) fn resolve(&self) -> eyre::Result<Credentials> {
        match self.source.as_ref() {
            CredentialSource::Static(credentials) => Ok(credentials.clone()),
            CredentialSource::Default(chain) => chain.resolve(),
        }
    }
}

impl DefaultCredentialChain {
    fn resolve(&self) -> eyre::Result<Credentials> {
        let now = SystemTime::now();
        {
            let cache = self
                .cache
                .lock()
                .map_err(|_| eyre::eyre!("credential cache lock poisoned"))?;
            if let Some(cached) = cache.as_ref()
                && cached.refresh_at > now
            {
                return Ok(cached.credentials.clone());
            }
        }

        let (credentials, expires_at) = resolve_default_credentials(&self.region, &self.metadata_client)?;
        let refresh_at = match expires_at {
            Some(expiry) => expiry
                .checked_sub(CREDENTIAL_REFRESH_MARGIN)
                .filter(|refresh| *refresh > now)
                .unwrap_or(now),
            None => now + Duration::from_secs(60),
        };
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| eyre::eyre!("credential cache lock poisoned"))?;
        *cache = Some(CachedCredentials {
            credentials: credentials.clone(),
            refresh_at,
        });
        Ok(credentials)
    }
}

fn resolve_default_credentials(region: &str, client: &Agent) -> eyre::Result<(Credentials, Option<SystemTime>)> {
    if let Some(credentials) = credentials_from_environment()? {
        return Ok((credentials, None));
    }
    if let Some(credentials) = credentials_from_profile()? {
        return Ok((credentials, None));
    }
    if let Some(credentials) = credentials_from_container(client)? {
        return Ok(credentials);
    }
    if env::var("AWS_EC2_METADATA_DISABLED").is_ok_and(|value| value.eq_ignore_ascii_case("true")) {
        return Err(eyre::eyre!("no S3 credentials found and instance metadata is disabled"));
    }
    credentials_from_instance_role(region, client)
}

fn credentials_from_environment() -> eyre::Result<Option<Credentials>> {
    let key = env::var("AWS_ACCESS_KEY_ID").ok();
    let secret = env::var("AWS_SECRET_ACCESS_KEY").ok();
    match (key, secret) {
        (None, None) => Ok(None),
        (Some(key), Some(secret)) if !key.is_empty() && !secret.is_empty() => {
            let token = env::var("AWS_SESSION_TOKEN")
                .ok()
                .or_else(|| env::var("AWS_SECURITY_TOKEN").ok());
            let credentials = match token {
                Some(token) => Credentials::new_with_token(key, secret, token),
                None => Credentials::new(key, secret),
            };
            Ok(Some(credentials))
        }
        _ => Err(eyre::eyre!("AWS access key environment variables are incomplete")),
    }
}

fn credentials_from_profile() -> eyre::Result<Option<Credentials>> {
    let profile = env::var("AWS_PROFILE")
        .ok()
        .or_else(|| env::var("AWS_DEFAULT_PROFILE").ok())
        .unwrap_or_else(|| "default".to_owned());
    let path = env::var_os("AWS_SHARED_CREDENTIALS_FILE")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".aws/credentials")));
    let Some(path) = path else {
        return Ok(None);
    };
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    let mut current_profile = String::new();
    let mut key = None;
    let mut secret = None;
    let mut token = None;
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(section) = line.strip_prefix('[').and_then(|value| value.strip_suffix(']')) {
            if current_profile == profile {
                break;
            }
            current_profile = section.trim().to_owned();
            continue;
        }
        if current_profile != profile {
            continue;
        }
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        match name.trim() {
            "aws_access_key_id" => key = Some(value.trim().to_owned()),
            "aws_secret_access_key" => secret = Some(value.trim().to_owned()),
            "aws_session_token" | "aws_security_token" => token = Some(value.trim().to_owned()),
            _ => {}
        }
    }
    match (key, secret) {
        (Some(key), Some(secret)) if !key.is_empty() && !secret.is_empty() => {
            let credentials = match token {
                Some(token) => Credentials::new_with_token(key, secret, token),
                None => Credentials::new(key, secret),
            };
            Ok(Some(credentials))
        }
        (None, None) => Ok(None),
        _ => Err(eyre::eyre!("AWS shared credentials profile is incomplete")),
    }
}

fn credentials_from_container(client: &Agent) -> eyre::Result<Option<(Credentials, Option<SystemTime>)>> {
    let full_uri = env::var("AWS_CONTAINER_CREDENTIALS_FULL_URI").ok();
    let relative_uri = env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI").ok();
    let Some(uri) = full_uri.or_else(|| relative_uri.map(|value| format!("http://169.254.170.2{value}"))) else {
        return Ok(None);
    };
    let mut request = client.get(&uri);
    if let Ok(token) = env::var("AWS_CONTAINER_AUTHORIZATION_TOKEN") {
        request = request.set("Authorization", &token);
    } else if let Ok(token_file) = env::var("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE") {
        let token = fs::read_to_string(token_file)?;
        request = request.set("Authorization", token.trim());
    }
    let response = request.call()?;
    let body = response.into_string()?;
    parse_metadata_credentials(&body).map(Some)
}

fn credentials_from_instance_role(region: &str, client: &Agent) -> eyre::Result<(Credentials, Option<SystemTime>)> {
    if region.is_empty() {
        return Err(eyre::eyre!(
            "an explicit AWS region is required for instance-role credentials"
        ));
    }
    let token = client
        .put(IMDS_TOKEN_URL)
        .set("X-aws-ec2-metadata-token-ttl-seconds", IMDS_TOKEN_TTL)
        .call()?
        .into_string()?;
    let roles = client
        .get(IMDS_ROLE_URL)
        .set("X-aws-ec2-metadata-token", token.trim())
        .call()?
        .into_string()?;
    let role = roles
        .lines()
        .map(str::trim)
        .find(|role| !role.is_empty())
        .ok_or_else(|| eyre::eyre!("instance metadata returned no IAM role"))?;
    let mut credentials_url: Url = IMDS_ROLE_URL.parse()?;
    credentials_url
        .path_segments_mut()
        .map_err(|()| eyre::eyre!("invalid instance metadata URL"))?
        .push(role);
    let body = client
        .get(credentials_url.as_str())
        .set("X-aws-ec2-metadata-token", token.trim())
        .call()?
        .into_string()?;
    parse_metadata_credentials(&body)
}

fn parse_metadata_credentials(body: &str) -> eyre::Result<(Credentials, Option<SystemTime>)> {
    let response = Ec2SecurityCredentialsMetadataResponse::deserialize(body)?;
    let expiration_seconds = u64::try_from(response.expiration().as_second())?;
    let expires_at = UNIX_EPOCH
        .checked_add(Duration::from_secs(expiration_seconds))
        .ok_or_else(|| eyre::eyre!("credential expiration is outside the system clock range"))?;
    if expires_at <= SystemTime::now() + Duration::from_secs(30) {
        return Err(eyre::eyre!("credential provider returned expired credentials"));
    }
    Ok((response.into_credentials(), Some(expires_at)))
}

#[derive(Debug)]
enum PutCondition {
    IfNoneMatch,
    IfMatch(String),
}

fn put_object_conditional(
    bucket: &Bucket,
    credentials: &S3CredentialsProvider,
    client: &Agent,
    key: &str,
    bytes: &[u8],
    condition: &PutCondition,
) -> eyre::Result<String> {
    let credentials = credentials.resolve()?;
    let mut action = bucket.put_object(Some(&credentials), key);
    let (header, value) = match condition {
        PutCondition::IfNoneMatch => ("if-none-match", "*"),
        PutCondition::IfMatch(etag) => ("if-match", etag.as_str()),
    };
    action.headers_mut().insert(header, value);
    let url = action.sign(Duration::from_secs(3600));
    let response = client.put(url.as_str()).set(header, value).send_bytes(bytes)?;
    response
        .header("ETag")
        .map(ToOwned::to_owned)
        .ok_or_else(|| eyre::eyre!("conditional S3 write did not return an ETag"))
}

fn get_object_with_etag(
    bucket: &Bucket,
    credentials: &S3CredentialsProvider,
    client: &Agent,
    key: &str,
) -> eyre::Result<Option<(Vec<u8>, String)>> {
    let credentials = credentials.resolve()?;
    let action = bucket.get_object(Some(&credentials), key);
    let url = action.sign(Duration::from_secs(3600));
    let response = match client.get(url.as_str()).call() {
        Ok(response) => response,
        Err(ureq::Error::Status(404, _)) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let etag = response
        .header("ETag")
        .map(ToOwned::to_owned)
        .ok_or_else(|| eyre::eyre!("S3 lease read did not return an ETag"))?;
    let mut bytes = Vec::new();
    response.into_reader().read_to_end(&mut bytes)?;
    Ok(Some((bytes, etag)))
}

fn validate_config(config: &S3LeaseConfig) -> eyre::Result<()> {
    if config.bucket.is_empty() || config.region.is_empty() || config.owner.is_empty() {
        return Err(eyre::eyre!("S3 lease bucket, region, and owner must be non-empty"));
    }
    if config.owner.len() > 4096 {
        return Err(eyre::eyre!("S3 lease owner exceeds 4096 bytes"));
    }
    if config.ttl_secs == 0 {
        return Err(eyre::eyre!("S3 lease TTL must be greater than zero"));
    }
    if config
        .prefix
        .split('/')
        .any(|part| matches!(part, "." | "..") || part.contains('\\') || part.contains('\0'))
    {
        return Err(eyre::eyre!("S3 lease prefix contains an unsafe path component"));
    }
    Ok(())
}

fn lease_key(prefix: &str) -> String {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        LEASE_OBJECT.to_owned()
    } else {
        format!("{prefix}/{LEASE_OBJECT}")
    }
}

fn expiration(now: u64, ttl_secs: u64) -> eyre::Result<u64> {
    now.checked_add(ttl_secs)
        .ok_or_else(|| eyre::eyre!("S3 lease expiry overflow"))
}

fn unix_time() -> eyre::Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

fn s3_endpoint(region: &str) -> eyre::Result<Url> {
    if region == "us-east-1" {
        Ok(Url::parse("https://s3.amazonaws.com")?)
    } else {
        Ok(Url::parse(&format!("https://s3.{region}.amazonaws.com"))?)
    }
}

fn http_client(timeout: Duration) -> Agent {
    ureq::AgentBuilder::new().timeout(timeout).build()
}
