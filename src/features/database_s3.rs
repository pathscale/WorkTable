//! Database-wide S3 persistence through DataBucket generations.

use alloc::{format, string::String, vec::Vec};
use core::fmt::{Debug, Formatter};
use core::hash::Hash;
use core::marker::PhantomData;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use data_bucket::storage::{
    CatalogMutation, CatalogName, CatalogRecord, PageAddress, PageKind, SystemIndexRecord, TableId,
};

use crate::persistence::operation::{BatchOperation, Operation};
use crate::persistence::{
    DiskConfig, DiskPersistenceEngine, PersistenceConfig, PersistenceEngine, SpaceDataOps, SpaceIndexOps,
    SpaceSecondaryIndexOps,
};
use crate::prelude::{PrimaryKeyGeneratorState, TablePrimaryKey, WT_DATA_EXTENSION, WT_INDEX_EXTENSION};
use crate::{S3Database, TableSecondaryIndexEventsOps};

const INDEX_CHUNK_BYTES: usize = data_bucket::PAGE_SIZE;

#[derive(Clone)]
pub struct DatabaseS3DiskConfig {
    pub disk: DiskConfig,
    pub database: S3Database,
}

impl Debug for DatabaseS3DiskConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("DatabaseS3DiskConfig")
            .field("disk", &self.disk)
            .field("domain", &self.database.id())
            .finish()
    }
}

impl PersistenceConfig for DatabaseS3DiskConfig {
    fn table_path(&self) -> &str {
        self.disk.table_path()
    }

    fn version(&self) -> u32 {
        self.disk.version()
    }
}

pub struct DatabaseS3PersistenceEngine<
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
    config: DatabaseS3DiskConfig,
    table_id: TableId,
    marker: PhantomData<(PrimaryKey, SecondaryIndexEvents, AvailableIndexes, PrimaryKeyGenState)>,
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
    DatabaseS3PersistenceEngine<
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
    fn table_name(config: &DatabaseS3DiskConfig) -> eyre::Result<&str> {
        Path::new(config.disk.table_path())
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| eyre::eyre!("invalid table path"))
    }

    fn restore_from_database(config: &DatabaseS3DiskConfig, table_id: TableId) -> eyre::Result<()> {
        let catalog = config.database.catalog();
        let pages = catalog
            .system_pages()
            .into_iter()
            .filter(|page| page.table_id == table_id)
            .collect::<Vec<_>>();
        if pages.is_empty() {
            return Ok(());
        }
        let table = catalog
            .system_tables()
            .into_iter()
            .find(|table| table.table_id == table_id)
            .ok_or_else(|| eyre::eyre!("system catalog has pages for an unknown table"))?;
        let indexes = catalog
            .system_indexes()
            .into_iter()
            .filter(|index| index.table_id == table_id)
            .map(|index| (index.space_id, index))
            .collect::<BTreeMap<_, _>>();
        let table_path = Path::new(config.disk.table_path());
        let stage = staging_path(table_path, "domain-stage")?;
        remove_path_if_exists(&stage)?;
        std::fs::create_dir_all(&stage)?;

        let restore = (|| {
            let mut lengths = BTreeMap::<PathBuf, u64>::new();
            for page in pages {
                let relative = if page.space_id == table.data_space_id {
                    PathBuf::from(WT_DATA_EXTENSION)
                } else {
                    let index = indexes
                        .get(&page.space_id)
                        .ok_or_else(|| eyre::eyre!("system catalog page has no owning index"))?;
                    if index.primary {
                        PathBuf::from(format!("primary{WT_INDEX_EXTENSION}"))
                    } else {
                        safe_index_file_name(&index.name)?
                    }
                };
                let address = PageAddress {
                    domain: config.database.id(),
                    table_id,
                    space_id: page.space_id,
                    page_id: page.page_id,
                    page_kind: page.page_kind,
                };
                let image = config
                    .database
                    .read_page(address)?
                    .ok_or_else(|| eyre::eyre!("system catalog page object is missing"))?;
                let offset = if page.space_id == table.data_space_id {
                    if image.len() != table.page_stride as usize {
                        return Err(eyre::eyre!("remote data page length does not match the table stride"));
                    }
                    let header = data_bucket::inspect_page_image_header(&image)?;
                    if header.page_id != page.page_id || header.space_id != page.space_id {
                        return Err(eyre::eyre!(
                            "remote data page identity does not match the system catalog"
                        ));
                    }
                    u64::from(table.page_stride).checked_mul(usize::from(page.page_id) as u64)
                } else {
                    if image.is_empty() || image.len() > INDEX_CHUNK_BYTES {
                        return Err(eyre::eyre!("remote index chunk has an invalid length"));
                    }
                    (INDEX_CHUNK_BYTES as u64).checked_mul(usize::from(page.page_id) as u64)
                }
                .ok_or_else(|| eyre::eyre!("remote page offset overflow"))?;
                let path = stage.join(relative);
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .open(&path)?;
                file.seek(SeekFrom::Start(offset))?;
                file.write_all(&image)?;
                lengths
                    .entry(path)
                    .and_modify(|length| *length = (*length).max(offset + image.len() as u64))
                    .or_insert(offset + image.len() as u64);
            }
            for (path, length) in lengths {
                let file = std::fs::OpenOptions::new().write(true).open(path)?;
                file.set_len(length)?;
                file.sync_all()?;
            }
            Ok::<(), eyre::Report>(())
        })();
        if let Err(error) = restore {
            let _ = std::fs::remove_dir_all(&stage);
            return Err(error);
        }
        publish_stage(table_path, &stage)
    }

    fn sync_to_database(&self, dirty_data_pages: &[data_bucket::PageId]) -> eyre::Result<()> {
        let mut dirty_data_pages = dirty_data_pages.iter().copied().collect::<BTreeSet<_>>();
        // Page zero carries the space metadata and generator state updated by
        // ordinary mutations.
        dirty_data_pages.insert(data_bucket::PageId::from(0));
        let scan = scan_table(
            Path::new(self.config.disk.table_path()),
            SpaceData::PAGE_STRIDE,
            self.config.database.id(),
            self.table_id,
            &dirty_data_pages,
        )?;
        // A generation is optimistic in DataBucket, so the catalog snapshot,
        // builder and commit must be one serialized interval for all table
        // workers sharing this database handle.
        let _generation_guard = self.config.database.generation_commit_guard();
        let catalog = self.config.database.catalog();
        let current_pages = catalog
            .system_pages()
            .into_iter()
            .filter(|page| page.table_id == self.table_id)
            .map(|page| (CatalogRecord::Page(page.clone()).key(), page))
            .collect::<BTreeMap<_, _>>();
        let scanned_keys = scan.pages.keys().copied().collect::<BTreeSet<_>>();
        let mut generation = self.config.database.begin_generation()?;
        let mut changed = false;
        for (key, page) in &scan.pages {
            if current_pages
                .get(key)
                .is_some_and(|current| current.checksum == page.hash)
            {
                continue;
            }
            generation.put_page(page.address, page.image.clone(), 0, 0);
            changed = true;
        }
        for (key, page) in &current_pages {
            let observed = page.page_kind != PageKind::Data || dirty_data_pages.contains(&page.page_id);
            if observed && !scanned_keys.contains(key) {
                generation.delete_page(PageAddress {
                    domain: self.config.database.id(),
                    table_id: self.table_id,
                    space_id: page.space_id,
                    page_id: page.page_id,
                    page_kind: page.page_kind,
                });
                changed = true;
            }
        }

        let mut table = catalog
            .system_tables()
            .into_iter()
            .find(|table| table.table_id == self.table_id)
            .ok_or_else(|| eyre::eyre!("registered table is missing from the system catalog"))?;
        if table.data_space_id != scan.data_space_id || table.page_stride != SpaceData::PAGE_STRIDE {
            table.data_space_id = scan.data_space_id;
            table.page_stride = SpaceData::PAGE_STRIDE;
            generation.update_catalog(CatalogMutation::Upsert(CatalogRecord::Table(table)));
            changed = true;
        }

        let current_indexes = catalog
            .system_indexes()
            .into_iter()
            .filter(|index| index.table_id == self.table_id)
            .collect::<Vec<_>>();
        let mut next_index_id = current_indexes
            .iter()
            .map(|index| index.index_id)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        for scanned in &scan.indexes {
            let existing = current_indexes
                .iter()
                .find(|index| index.name == scanned.name || index.space_id == scanned.space_id);
            let mut index = SystemIndexRecord {
                table_id: self.table_id,
                index_id: existing.map_or_else(
                    || {
                        let id = next_index_id;
                        next_index_id = next_index_id.saturating_add(1);
                        id
                    },
                    |index| index.index_id,
                ),
                space_id: scanned.space_id,
                primary: scanned.primary,
                name: scanned.name.clone(),
                entries: existing.map_or(0, |index| index.entries),
                generation: existing.map_or(self.config.database.generation() + 1, |index| index.generation),
            };
            if existing != Some(&index) {
                index.generation = self.config.database.generation() + 1;
                generation.update_catalog(CatalogMutation::Upsert(CatalogRecord::Index(index)));
                changed = true;
            }
        }
        for index in current_indexes {
            if !scan.indexes.iter().any(|scanned| scanned.space_id == index.space_id) {
                generation.update_catalog(CatalogMutation::Delete(CatalogRecord::Index(index).key()));
                changed = true;
            }
        }
        if changed {
            self.config.database.commit_generation(generation.finish())?;
        }
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
    for DatabaseS3PersistenceEngine<
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
    type Config = DatabaseS3DiskConfig;

    async fn new(config: Self::Config) -> eyre::Result<Self> {
        let table_name = Self::table_name(&config)?;
        let existing = config
            .database
            .catalog()
            .system_tables()
            .into_iter()
            .find(|table| table.name.as_str() == table_name);
        let table_id = if let Some(table) = existing {
            if table.schema_version != config.disk.version() || table.page_stride != SpaceData::PAGE_STRIDE {
                return Err(eyre::eyre!(
                    "remote table metadata mismatch for {table_name}: stored schema version {} and page stride {}, requested {} and {}",
                    table.schema_version,
                    table.page_stride,
                    config.disk.version(),
                    SpaceData::PAGE_STRIDE
                ));
            }
            table.table_id
        } else {
            config
                .database
                .register_table_with_stride(table_name, config.disk.version(), SpaceData::PAGE_STRIDE)?
        };
        Self::restore_from_database(&config, table_id)?;
        let inner = DiskPersistenceEngine::new(config.disk.clone()).await?;
        Ok(Self {
            inner,
            config,
            table_id,
            marker: PhantomData,
        })
    }

    async fn apply_operation(
        &mut self,
        operation: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents>,
    ) -> eyre::Result<()> {
        let dirty_data_pages = operation
            .row_mutation_refs()
            .map(|(link, _)| link.page_id)
            .collect::<Vec<_>>();
        self.inner.apply_operation(operation).await?;
        self.sync_to_database(&dirty_data_pages)
    }

    async fn apply_batch_operation(
        &mut self,
        operation: BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents, AvailableIndexes>,
    ) -> eyre::Result<()> {
        let dirty_data_pages = operation.row_page_ids();
        self.inner.apply_batch_operation(operation).await?;
        self.sync_to_database(&dirty_data_pages)
    }

    async fn reclaim_data_pages(&mut self, page_ids: Vec<data_bucket::PageId>) -> eyre::Result<()> {
        let dirty_data_pages = page_ids.clone();
        self.inner.reclaim_data_pages(page_ids).await?;
        self.sync_to_database(&dirty_data_pages)
    }

    async fn ensure_schema(
        &mut self,
        row_schema: Vec<(String, String)>,
        primary_key_fields: Vec<String>,
        secondary_index_types: Vec<(String, String)>,
    ) -> eyre::Result<()> {
        self.inner
            .ensure_schema(row_schema, primary_key_fields, secondary_index_types)
            .await
    }

    async fn validate_schema(
        &mut self,
        row_schema: Vec<(String, String)>,
        primary_key_fields: Vec<String>,
        secondary_index_types: Vec<(String, String)>,
    ) -> eyre::Result<()> {
        self.inner
            .validate_schema(row_schema, primary_key_fields, secondary_index_types)
            .await
    }

    fn config(&self) -> &Self::Config {
        &self.config
    }
}

fn safe_index_file_name(name: &CatalogName) -> eyre::Result<PathBuf> {
    let name = name.as_str();
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\']) {
        return Err(eyre::eyre!("remote index name is not a single file-name component"));
    }
    Ok(PathBuf::from(format!("{name}{WT_INDEX_EXTENSION}")))
}

struct ScannedPage {
    address: PageAddress,
    image: Vec<u8>,
    hash: [u8; 32],
}

struct ScannedIndex {
    name: CatalogName,
    space_id: data_bucket::SpaceId,
    primary: bool,
}

struct TableScan {
    data_space_id: data_bucket::SpaceId,
    pages: BTreeMap<[u8; 32], ScannedPage>,
    indexes: Vec<ScannedIndex>,
}

fn scan_table(
    root: &Path,
    stride: u32,
    domain: data_bucket::storage::StorageDomainId,
    table_id: TableId,
    dirty_data_pages: &BTreeSet<data_bucket::PageId>,
) -> eyre::Result<TableScan> {
    let mut pages = BTreeMap::new();
    let mut indexes = Vec::new();
    let mut data_space_id = data_bucket::SpaceId(0);
    if !root.exists() {
        return Ok(TableScan {
            data_space_id,
            pages,
            indexes,
        });
    }
    let mut paths = std::fs::read_dir(root)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    for path in paths {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let is_data = name == WT_DATA_EXTENSION;
        let is_index = name.ends_with(WT_INDEX_EXTENSION);
        if !is_data && !is_index {
            continue;
        }
        if is_data {
            let length = std::fs::metadata(&path)?.len();
            if length % u64::from(stride) != 0 {
                return Err(eyre::eyre!("data file length is not a whole number of pages"));
            }
            let mut file = std::fs::File::open(&path)?;
            let page_count = length / u64::from(stride);
            for page_id in dirty_data_pages {
                let page_number = usize::from(*page_id) as u64;
                if page_number >= page_count {
                    continue;
                }
                file.seek(SeekFrom::Start(page_number * u64::from(stride)))?;
                let mut image = vec![0; stride as usize];
                file.read_exact(&mut image)?;
                let header = data_bucket::inspect_page_image_header(&image)?;
                if header.page_id != *page_id {
                    return Err(eyre::eyre!("data page identity does not match its file position"));
                }
                if data_space_id.0 == 0 {
                    data_space_id = header.space_id;
                } else if data_space_id != header.space_id {
                    return Err(eyre::eyre!("one data file contains multiple space identifiers"));
                }
                let page_kind = if header.page_type == data_bucket::PageType::Data {
                    PageKind::Data
                } else {
                    PageKind::Metadata
                };
                insert_scanned_page(
                    &mut pages,
                    PageAddress {
                        domain,
                        table_id,
                        space_id: header.space_id,
                        page_id: header.page_id,
                        page_kind,
                    },
                    image,
                )?;
            }
        } else {
            let primary = name == format!("primary{WT_INDEX_EXTENSION}");
            let logical_name = if primary {
                "primary"
            } else {
                name.strip_suffix(WT_INDEX_EXTENSION)
                    .ok_or_else(|| eyre::eyre!("invalid index file name"))?
            };
            let space_id = index_space_id(name);
            if space_id == data_space_id || indexes.iter().any(|index| index.space_id == space_id) {
                return Err(eyre::eyre!("generated index storage identifier collision"));
            }
            indexes.push(ScannedIndex {
                name: CatalogName::new(logical_name)?,
                space_id,
                primary,
            });
            let page_kind = if primary {
                PageKind::PrimaryIndex
            } else {
                PageKind::SecondaryIndex
            };
            let mut file = std::fs::File::open(&path)?;
            let mut page_id = 0_u32;
            loop {
                let mut image = vec![0; INDEX_CHUNK_BYTES];
                let read = file.read(&mut image)?;
                if read == 0 {
                    break;
                }
                image.truncate(read);
                insert_scanned_page(
                    &mut pages,
                    PageAddress {
                        domain,
                        table_id,
                        space_id,
                        page_id: page_id.into(),
                        page_kind,
                    },
                    image,
                )?;
                page_id = page_id
                    .checked_add(1)
                    .ok_or_else(|| eyre::eyre!("index file has too many chunks"))?;
            }
        }
    }
    Ok(TableScan {
        data_space_id,
        pages,
        indexes,
    })
}

fn index_space_id(name: &str) -> data_bucket::SpaceId {
    let hash = blake3::hash(name.as_bytes());
    let bytes = hash.as_bytes();
    let mut id = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    if id == 0 {
        id = 1;
    }
    data_bucket::SpaceId(id)
}

fn insert_scanned_page(
    pages: &mut BTreeMap<[u8; 32], ScannedPage>,
    address: PageAddress,
    image: Vec<u8>,
) -> eyre::Result<()> {
    let key = CatalogRecord::page_key(address);
    let hash = *blake3::hash(&image).as_bytes();
    if pages.insert(key, ScannedPage { address, image, hash }).is_some() {
        return Err(eyre::eyre!("duplicate logical page in table files"));
    }
    Ok(())
}

fn staging_path(table_path: &Path, label: &str) -> eyre::Result<PathBuf> {
    let parent = table_path.parent().unwrap_or_else(|| Path::new("."));
    let name = table_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| eyre::eyre!("invalid table path"))?;
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(parent.join(format!(".{name}.{label}-{}-{nonce}", std::process::id())))
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

fn publish_stage(table_path: &Path, stage: &Path) -> eyre::Result<()> {
    if let Some(parent) = table_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if !table_path.exists() {
        std::fs::rename(stage, table_path)?;
        return Ok(());
    }
    let backup = staging_path(table_path, "domain-backup")?;
    std::fs::rename(table_path, &backup)?;
    if let Err(error) = std::fs::rename(stage, table_path) {
        std::fs::rename(&backup, table_path)?;
        return Err(error.into());
    }
    std::fs::remove_dir_all(backup)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::safe_index_file_name;
    use data_bucket::storage::CatalogName;

    #[test]
    fn remote_index_names_cannot_escape_the_restore_directory() {
        for name in ["../victim", "/absolute", r"..\victim", r"C:\victim"] {
            let name = CatalogName::new(name).unwrap();
            assert!(safe_index_file_name(&name).is_err(), "accepted {name:?}");
        }
        let name = CatalogName::new("orders_by_date").unwrap();
        assert_eq!(
            safe_index_file_name(&name).unwrap(),
            std::path::PathBuf::from(format!("orders_by_date{}", crate::prelude::WT_INDEX_EXTENSION))
        );
    }
}
