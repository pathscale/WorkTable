//! Real S3-compatible acceptance for WorkTable's per-table and database-wide persistence.
//!
//! The fixture is deliberately opt-in. It writes only below a caller-approved
//! base prefix, appends a unique run prefix, and never deletes remote objects.

use std::collections::BTreeMap;
use std::env;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use worktable::data_bucket::storage::s3::{S3Config as DataBucketS3Config, S3StoreError};
use worktable::data_bucket::storage::{
    CatalogMutation, CatalogRecord, DomainError, GenerationPlan, StorageDomainId, TableId,
};
use worktable::features::s3_generation::{
    S3GenerationFile, S3GenerationPublishOutcome, S3GenerationPublisher, S3GenerationSegment,
};
use worktable::features::s3_generation_reader::S3GenerationReader;
use worktable::features::s3_support::{S3Config as TableS3Config, S3DiskConfig, S3TransportOptions};
use worktable::prelude::eyre;
use worktable::prelude::*;
use worktable::{DatabaseS3DiskConfig, S3Database, database_s3_persistence, worktable};

const SEGMENT_TARGET_BYTES: u64 = 4 * 1024 * 1024;

worktable!(
    name: M331Acceptance,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        marker: u64,
        payload: String,
    },
);

database_s3_persistence!(M331AcceptanceWorkTable);

fn main() -> eyre::Result<()> {
    let approved = env::var("WORKTABLE_S3_ACCEPTANCE_APPROVED").unwrap_or_default();
    if approved != "YES" {
        return Err(eyre::eyre!(
            "set WORKTABLE_S3_ACCEPTANCE_APPROVED=YES only after confirming the base prefix is isolated"
        ));
    }

    let base_prefix = required_env("WORKTABLE_S3_ACCEPTANCE_PREFIX")?;
    let base_prefix = base_prefix.trim_matches('/');
    if base_prefix.is_empty()
        || base_prefix
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(eyre::eyre!("the approved S3 prefix must be a safe, non-empty path"));
    }

    let run_nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let run_id = format!("m331-{}-{run_nanos:x}", std::process::id());
    let write_prefix = format!("{base_prefix}/worktable-s3-acceptance/{run_id}");
    let config = s3_config(write_prefix.clone())?;
    let local_root = env::temp_dir().join(format!("worktable-s3-acceptance-{run_id}"));
    std::fs::create_dir_all(&local_root)
        .map_err(|_| eyre::eyre!("could not create the local acceptance scratch directory"))?;

    println!("S3 acceptance write prefix: {write_prefix}");
    block_on(run_acceptance(config, run_nanos, local_root))
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct ParkThread(thread::Thread);

    impl Wake for ParkThread {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ParkThread(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

fn required_env(name: &str) -> eyre::Result<String> {
    let value = env::var(name).map_err(|_| eyre::eyre!("required environment variable {name} is missing"))?;
    if value.is_empty() {
        return Err(eyre::eyre!("required environment variable {name} is empty"));
    }
    Ok(value)
}

fn s3_config(prefix: String) -> eyre::Result<DataBucketS3Config> {
    let virtual_host_style = env::var("WORKTABLE_S3_VIRTUAL_HOST_STYLE")
        .ok()
        .map(|value| value.parse::<bool>())
        .transpose()
        .map_err(|_| eyre::eyre!("WORKTABLE_S3_VIRTUAL_HOST_STYLE must be true or false"))?
        .unwrap_or(false);

    Ok(DataBucketS3Config {
        bucket_name: required_env("WORKTABLE_S3_BUCKET")?,
        endpoint: required_env("WORKTABLE_S3_ENDPOINT")?,
        access_key: required_env("WORKTABLE_S3_ACCESS_KEY")?,
        secret_key: required_env("WORKTABLE_S3_SECRET_KEY")?,
        session_token: env::var("WORKTABLE_S3_SESSION_TOKEN")
            .ok()
            .filter(|token| !token.is_empty()),
        region: required_env("WORKTABLE_S3_REGION")?,
        prefix: Some(prefix),
        virtual_host_style,
    })
}

async fn run_acceptance(config: DataBucketS3Config, run_nanos: u128, local_root: PathBuf) -> eyre::Result<()> {
    run_real_table_generation_roundtrip(config.clone(), &local_root).await?;
    println!("PASS real persisted WorkTable WTS3G001 publish, cold restore, and reopen");

    run_worktable_segment_and_cold_reopen(config.clone(), domain_id(run_nanos, 1), &local_root).await?;
    println!("PASS database-wide S3 under-filled segments and cold WorkTable reader reopen");

    run_concurrent_cas_conflict(config.clone(), domain_id(run_nanos, 2))?;
    println!("PASS concurrent database-wide S3 generation CAS conflict");

    run_after_write_lost_ack(config, domain_id(run_nanos, 3))?;
    println!("PASS database-wide after-write lost acknowledgement and cold catalog reopen");
    Ok(())
}

async fn run_real_table_generation_roundtrip(config: DataBucketS3Config, local_root: &Path) -> eyre::Result<()> {
    let table_name = M331AcceptanceWorkTable::name_snake_case();
    let source_disk = DiskConfig::new_with_table_name(
        local_root.join("per-table-source").display().to_string(),
        table_name,
        M331AcceptanceWorkTable::version(),
    );
    let source_engine = M331AcceptancePersistenceEngine::new(source_disk.clone()).await?;
    let source = M331AcceptanceWorkTable::load(source_engine).await?;
    let expected_payload = "actual persisted WorkTable files round-trip through WTS3G001".repeat(8);
    source
        .insert(M331AcceptanceRow {
            id: source.get_next_pk().into(),
            marker: 332,
            payload: expected_payload.clone(),
        })
        .await?;
    source.wait_for_ops().await?;
    drop(source);

    let source_table_path = Path::new(source_disk.table_path());
    let persisted_files = read_actual_table_files(source_table_path)?;
    require_nonempty_primary_snapshot(&persisted_files)?;

    let prefix = config
        .prefix
        .as_deref()
        .ok_or_else(|| eyre::eyre!("the approved S3 write prefix is missing"))?;
    let s3 = TableS3Config {
        bucket_name: config.bucket_name.clone(),
        endpoint: config.endpoint.clone(),
        access_key: config.access_key.clone(),
        secret_key: config.secret_key.clone(),
        region: Some(config.region.clone()),
        prefix: Some(format!("{prefix}/per-table-generation")),
    };
    let publisher_config = S3DiskConfig {
        disk: source_disk,
        s3: s3.clone(),
    };
    let agent = ureq::AgentBuilder::new()
        .timeout(std::time::Duration::from_secs(30))
        .build();
    let transport = S3TransportOptions {
        session_token: config.session_token.clone(),
        virtual_host_style: config.virtual_host_style,
    };
    let publisher =
        S3GenerationPublisher::new_with_agent_and_transport(&publisher_config, agent.clone(), transport.clone())?;
    let segment_storage = build_generation_segments(&persisted_files)?;
    let files = persisted_files
        .iter()
        .zip(&segment_storage)
        .map(|((path, bytes), segments)| {
            Ok(S3GenerationFile {
                path,
                length: u64::try_from(bytes.len())?,
                segments,
            })
        })
        .collect::<eyre::Result<Vec<_>>>()?;
    let generation_id = worktable::prelude::uuid::Uuid::new_v4();
    let published_generation = match publisher.publish_generation(generation_id, &files, None)? {
        S3GenerationPublishOutcome::Published(receipt) | S3GenerationPublishOutcome::AlreadyPublished(receipt) => {
            receipt.generation_id
        }
        S3GenerationPublishOutcome::Conflict => {
            return Err(eyre::eyre!(
                "the unique per-table generation prefix unexpectedly already had a commit pointer"
            ));
        }
    };

    let restore_base = local_root.join("per-table-cold");
    let restore_disk = DiskConfig::new_with_table_name(
        restore_base.display().to_string(),
        table_name,
        M331AcceptanceWorkTable::version(),
    );
    let reader_config = S3DiskConfig {
        disk: restore_disk.clone(),
        s3,
    };
    let reader = S3GenerationReader::new_with_agent_and_transport(&reader_config, agent, transport)?;
    let restored_generation = reader.restore_to(restore_disk.table_path())?;
    if restored_generation.generation_id != published_generation
        || usize::try_from(restored_generation.file_count)? != files.len()
    {
        return Err(eyre::eyre!(
            "the cold reader restored a different WTS3G001 generation than the one just published"
        ));
    }
    require_nonempty_primary_snapshot(&read_actual_table_files(Path::new(restore_disk.table_path()))?)?;

    let cold_engine = M331AcceptancePersistenceEngine::new(restore_disk).await?;
    let cold = M331AcceptanceWorkTable::load(cold_engine).await?;
    let row = cold
        .select(0)
        .ok_or_else(|| eyre::eyre!("the cold WTS3G001 WorkTable open did not restore the inserted row"))?;
    if row.marker != 332 || row.payload.as_str() != expected_payload.as_str() {
        return Err(eyre::eyre!(
            "the cold WTS3G001 WorkTable open restored unexpected row contents"
        ));
    }
    cold.wait_for_ops().await?;
    Ok(())
}

fn read_actual_table_files(table_path: &Path) -> eyre::Result<Vec<(String, Vec<u8>)>> {
    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(table_path).follow_links(false) {
        let entry = entry.map_err(|_| eyre::eyre!("could not enumerate the persisted WorkTable files"))?;
        if entry.file_type().is_symlink() {
            return Err(eyre::eyre!("persisted WorkTable snapshot contains a symlink"));
        }
        if !entry.file_type().is_file() {
            continue;
        }
        let relative = entry
            .path()
            .strip_prefix(table_path)?
            .to_str()
            .ok_or_else(|| eyre::eyre!("persisted WorkTable path is not UTF-8"))?
            .replace(std::path::MAIN_SEPARATOR, "/");
        if relative != worktable::prelude::WT_DATA_EXTENSION
            && !relative.ends_with(worktable::prelude::WT_INDEX_EXTENSION)
        {
            return Err(eyre::eyre!("persisted table directory contains a non-WorkTable file"));
        }
        let bytes = std::fs::read(entry.path())?;
        files.push((relative, bytes));
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    if files.is_empty() {
        return Err(eyre::eyre!("the disk engine created no persisted WorkTable files"));
    }
    Ok(files)
}

fn require_nonempty_primary_snapshot(files: &[(String, Vec<u8>)]) -> eyre::Result<()> {
    let data_path = worktable::prelude::WT_DATA_EXTENSION;
    let primary_index_path = format!("primary{}", worktable::prelude::WT_INDEX_EXTENSION);
    for required in [data_path, primary_index_path.as_str()] {
        let bytes = files
            .iter()
            .find(|(path, _)| path.as_str() == required)
            .map(|(_, bytes)| bytes)
            .ok_or_else(|| eyre::eyre!("persisted snapshot is missing required file {required}"))?;
        if bytes.is_empty() {
            return Err(eyre::eyre!("persisted snapshot required file {required} is empty"));
        }
    }
    Ok(())
}

fn build_generation_segments<'a>(files: &'a [(String, Vec<u8>)]) -> eyre::Result<Vec<Vec<S3GenerationSegment<'a>>>> {
    let segments = files
        .iter()
        .map(|(_, bytes)| {
            let mut offset = 0_u64;
            let mut segments = Vec::new();
            for chunk in bytes.chunks(SEGMENT_TARGET_BYTES as usize) {
                segments.push(S3GenerationSegment {
                    file_offset: offset,
                    bytes: chunk,
                });
                offset = offset
                    .checked_add(u64::try_from(chunk.len())?)
                    .ok_or_else(|| eyre::eyre!("persisted WorkTable file length overflow"))?;
            }
            if offset != u64::try_from(bytes.len())? {
                return Err(eyre::eyre!("persisted WorkTable segments do not cover the file"));
            }
            Ok(segments)
        })
        .collect::<eyre::Result<Vec<_>>>()?;
    Ok(segments)
}

async fn run_worktable_segment_and_cold_reopen(
    config: DataBucketS3Config,
    domain: StorageDomainId,
    local_root: &Path,
) -> eyre::Result<()> {
    let database = open_database(domain, 1, config.clone())?;
    let writer_disk = disk_config(&local_root.join("writer"));
    let writer_engine = M331AcceptanceDatabaseS3PersistenceEngine::new(DatabaseS3DiskConfig {
        disk: writer_disk,
        database: database.clone(),
    })
    .await?;
    let table = M331AcceptanceWorkTable::load(writer_engine).await?;
    let expected_payload = "one persisted row in an under-filled S3 segment".repeat(32);

    table
        .insert(M331AcceptanceRow {
            id: table.get_next_pk().into(),
            marker: 331,
            payload: expected_payload.clone(),
        })
        .await?;
    table.wait_for_ops().await?;

    let table_id = database
        .catalog()
        .system_tables()
        .into_iter()
        .find(|record| record.name.as_str() == M331AcceptanceWorkTable::name_snake_case())
        .map(|record| record.table_id)
        .ok_or_else(|| eyre::eyre!("the persisted table is missing from the remote WorkTable catalog"))?;
    let mut segment_ends = BTreeMap::<[u8; 32], u64>::new();
    for page in database
        .catalog()
        .system_pages()
        .into_iter()
        .filter(|page| page.table_id == table_id)
    {
        let end = page
            .object_offset
            .checked_add(u64::from(page.encoded_length))
            .ok_or_else(|| eyre::eyre!("remote segment extent overflow"))?;
        segment_ends
            .entry(page.object)
            .and_modify(|last| *last = (*last).max(end))
            .or_insert(end);
    }
    if segment_ends.is_empty()
        || segment_ends
            .values()
            .any(|length| *length == 0 || *length > SEGMENT_TARGET_BYTES)
        || !segment_ends.values().any(|length| *length < SEGMENT_TARGET_BYTES)
    {
        return Err(eyre::eyre!(
            "the real S3 catalog did not describe valid under-filled segments"
        ));
    }

    let row = table
        .select(0)
        .ok_or_else(|| eyre::eyre!("the writer could not read its inserted row"))?;
    if row.marker != 331 || row.payload.as_str() != expected_payload.as_str() {
        return Err(eyre::eyre!("the writer read back unexpected row contents"));
    }
    drop(table);
    drop(database);

    let reader_database = open_database(domain, 2, config)?;
    let reader_engine = M331AcceptanceDatabaseS3PersistenceEngine::new(DatabaseS3DiskConfig {
        disk: disk_config(&local_root.join("cold-reader")),
        database: reader_database,
    })
    .await?;
    let reader = M331AcceptanceWorkTable::load(reader_engine).await?;
    let restored = reader
        .select(0)
        .ok_or_else(|| eyre::eyre!("a cold S3 reader did not restore the committed row"))?;
    if restored.marker != 331 || restored.payload.as_str() != expected_payload.as_str() {
        return Err(eyre::eyre!("the cold S3 reader restored unexpected row contents"));
    }
    reader.wait_for_ops().await?;
    Ok(())
}

fn run_concurrent_cas_conflict(config: DataBucketS3Config, domain: StorageDomainId) -> eyre::Result<()> {
    let seed = open_database(domain, 10, config.clone())?;
    seed.register_table("m331_cas_seed", 1)
        .map_err(|_| eyre::eyre!("could not create the initial CAS generation"))?;
    drop(seed);

    // Both independent handles load the same parent generation before either
    // writer is released. Their distinct catalog mutations race through the
    // real signed S3 generation commit path.
    let left = open_database(domain, 11, config.clone())?;
    let right = open_database(domain, 11, config.clone())?;
    let barrier = Arc::new(Barrier::new(3));
    let (left_result, right_result) = std::thread::scope(|scope| {
        let left_barrier = barrier.clone();
        let left_task = scope.spawn(move || {
            left_barrier.wait();
            left.register_table("m331_cas_left", 1)
        });
        let right_barrier = barrier.clone();
        let right_task = scope.spawn(move || {
            right_barrier.wait();
            right.register_table("m331_cas_right", 1)
        });
        barrier.wait();
        let left_result = left_task
            .join()
            .unwrap_or_else(|_| panic!("left S3 CAS writer panicked"));
        let right_result = right_task
            .join()
            .unwrap_or_else(|_| panic!("right S3 CAS writer panicked"));
        (left_result, right_result)
    });

    let left_outcome = classify_cas(left_result);
    let right_outcome = classify_cas(right_result);
    if !matches!(
        (left_outcome, right_outcome),
        (CasOutcome::Committed, CasOutcome::Conflict) | (CasOutcome::Conflict, CasOutcome::Committed)
    ) {
        return Err(eyre::eyre!(
            "the concurrent S3 generation writes did not produce one commit and one CAS conflict"
        ));
    }

    let reopened = open_database(domain, 12, config)?;
    let tables = reopened.catalog().system_tables();
    let left_visible = tables.iter().any(|table| table.name.as_str() == "m331_cas_left");
    let right_visible = tables.iter().any(|table| table.name.as_str() == "m331_cas_right");
    if left_visible == right_visible
        || left_visible != (left_outcome == CasOutcome::Committed)
        || right_visible != (right_outcome == CasOutcome::Committed)
    {
        return Err(eyre::eyre!(
            "the cold CAS reader did not see exactly the winning catalog mutation"
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CasOutcome {
    Committed,
    Conflict,
    OtherFailure,
}

fn classify_cas(result: Result<TableId, DomainError<S3StoreError>>) -> CasOutcome {
    match result {
        Ok(_) => CasOutcome::Committed,
        Err(DomainError::Store(S3StoreError::Conflict)) => CasOutcome::Conflict,
        Err(_) => CasOutcome::OtherFailure,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommitFailure {
    Store,
    InjectedAfterWrite,
}

fn after_write_failure_hook() -> Result<(), CommitFailure> {
    Err(CommitFailure::InjectedAfterWrite)
}

fn commit_with_lost_acknowledgement(database: &S3Database, plan: GenerationPlan) -> Result<(), CommitFailure> {
    database.commit_generation(plan).map_err(|_| CommitFailure::Store)?;
    after_write_failure_hook()
}

fn run_after_write_lost_ack(config: DataBucketS3Config, domain: StorageDomainId) -> eyre::Result<()> {
    let database = open_database(domain, 20, config.clone())?;
    let table_id = database
        .register_table("m331_after_write", 1)
        .map_err(|_| eyre::eyre!("could not create the after-write fixture table"))?;
    let generation_before = database.generation();
    let mut table = database
        .catalog()
        .system_tables()
        .into_iter()
        .find(|table| table.table_id == table_id)
        .ok_or_else(|| eyre::eyre!("the after-write fixture table is absent from the local catalog"))?;
    table.applied_generation = generation_before + 1;
    table.durable_generation = generation_before + 1;
    let mut generation = database.begin_generation()?;
    generation.update_catalog(CatalogMutation::Upsert(CatalogRecord::Table(table)));

    match commit_with_lost_acknowledgement(&database, generation.finish()) {
        Err(CommitFailure::InjectedAfterWrite) => {}
        Err(CommitFailure::Store) => {
            return Err(eyre::eyre!("the S3 write failed before the after-write hook"));
        }
        Ok(()) => return Err(eyre::eyre!("the after-write failure hook did not fire")),
    }
    drop(database);

    let cold = open_database(domain, 21, config)?;
    let committed = cold
        .catalog()
        .system_tables()
        .into_iter()
        .find(|table| table.table_id == table_id)
        .ok_or_else(|| eyre::eyre!("the cold reader could not find the remotely committed table"))?;
    if cold.generation() != generation_before + 1
        || committed.applied_generation != generation_before + 1
        || committed.durable_generation != generation_before + 1
    {
        return Err(eyre::eyre!(
            "the cold reader did not observe the generation whose acknowledgement was lost"
        ));
    }
    Ok(())
}

fn open_database(domain: StorageDomainId, writer_epoch: u64, config: DataBucketS3Config) -> eyre::Result<S3Database> {
    S3Database::open_s3(domain, writer_epoch, config)
        .map_err(|_| eyre::eyre!("could not open the S3 database; verify endpoint, credentials, and prefix scope"))
}

fn disk_config(root: &Path) -> DiskConfig {
    DiskConfig::new_with_table_name(
        root.to_string_lossy().into_owned(),
        M331AcceptanceWorkTable::name_snake_case(),
        M331AcceptanceWorkTable::version(),
    )
}

fn domain_id(run_nanos: u128, scenario: u8) -> StorageDomainId {
    let mut id = run_nanos.to_le_bytes();
    id[..4].copy_from_slice(&std::process::id().to_le_bytes());
    id[15] ^= scenario;
    StorageDomainId(id)
}
