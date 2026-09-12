//! The database-wide system catalog backed by a generated WorkTable.

use data_bucket::storage::{
    CatalogError, CatalogKey, CatalogMutation, CatalogName, CatalogRecord, CatalogRecordKind, CatalogWritePermit,
    CommittedGeneration, DomainError, GenerationBuilder, GenerationPlan, PageAddress, PageStore, PreparedSystemCatalog,
    ReplicaState, ReplicationErrorCode, StorageDomain, StorageDomainId, SystemCatalog, SystemIndexRecord,
    SystemPageRecord, SystemReplicationRecord, SystemTableRecord, TableId, WriterEpoch,
};
use data_bucket::{PageId, SpaceId};
use parking_lot::RwLock;

use crate::prelude::*;
use crate::worktable;

type CatalogKeyBytes = [u8; 32];
type CatalogNameBytes = [u8; 96];
type ObjectBytes = [u8; 32];

worktable!(
    name: StorageCatalog,
    vec: true,
    columns: {
        key: CatalogKeyBytes primary_key using fxhash,
        kind: u8,
        table_id: u32,
        space_id: u32,
        page_id: u32,
        page_kind: u8,
        generation: u64,
        index_id: u32,
        name_length: u8,
        name_bytes: CatalogNameBytes,
        schema_version: u32,
        data_space_id: u32,
        page_stride: u32,
        row_count: u64,
        live_row_bytes: u64,
        allocated_data_pages: u64,
        live_data_pages: u64,
        primary_index_entries: u64,
        secondary_index_entries: u64,
        tombstones: u64,
        applied_generation: u64,
        durable_generation: u64,
        object: ObjectBytes,
        object_offset: u64,
        encoded_length: u32,
        decoded_length: u32,
        checksum: ObjectBytes,
        live_rows: u32,
        live_bytes: u32,
        entries: u64,
        index_space_id: u32,
        index_primary: bool,
        upstash: u8,
        tigris: u8,
        last_error: u16,
    },
);

/// DataBucket owns writes through its private permit; applications receive a
/// query-only view over the same generated table.
pub struct GeneratedSystemCatalog {
    table: RwLock<StorageCatalogWorkTable>,
}

impl Default for GeneratedSystemCatalog {
    fn default() -> Self {
        Self {
            table: RwLock::new(StorageCatalogWorkTable::new()),
        }
    }
}

impl GeneratedSystemCatalog {
    #[must_use]
    pub fn view(&self) -> SystemCatalogView<'_> {
        SystemCatalogView { catalog: self }
    }
}

/// Read access to catalog rows. Mutations remain part of DataBucket's commit.
#[derive(Clone, Copy)]
pub struct SystemCatalogView<'a> {
    catalog: &'a GeneratedSystemCatalog,
}

impl SystemCatalogView<'_> {
    #[must_use]
    pub fn record(&self, key: &CatalogKey) -> Option<CatalogRecord> {
        self.catalog.record(key)
    }

    #[must_use]
    pub fn tables(&self) -> Vec<SystemTableRecord> {
        self.catalog
            .records(CatalogRecordKind::Table)
            .into_iter()
            .filter_map(|record| match record {
                CatalogRecord::Table(row) => Some(row),
                _ => None,
            })
            .collect()
    }

    #[must_use]
    pub fn system_tables(&self) -> Vec<SystemTableRecord> {
        self.tables()
    }

    #[must_use]
    pub fn pages(&self) -> Vec<SystemPageRecord> {
        self.catalog
            .records(CatalogRecordKind::Page)
            .into_iter()
            .filter_map(|record| match record {
                CatalogRecord::Page(row) => Some(row),
                _ => None,
            })
            .collect()
    }

    #[must_use]
    pub fn system_pages(&self) -> Vec<SystemPageRecord> {
        self.pages()
    }

    #[must_use]
    pub fn indexes(&self) -> Vec<SystemIndexRecord> {
        self.catalog
            .records(CatalogRecordKind::Index)
            .into_iter()
            .filter_map(|record| match record {
                CatalogRecord::Index(row) => Some(row),
                _ => None,
            })
            .collect()
    }

    #[must_use]
    pub fn system_indexes(&self) -> Vec<SystemIndexRecord> {
        self.indexes()
    }

    #[must_use]
    pub fn replication(&self) -> Vec<SystemReplicationRecord> {
        self.catalog
            .records(CatalogRecordKind::Replication)
            .into_iter()
            .filter_map(|record| match record {
                CatalogRecord::Replication(row) => Some(row),
                _ => None,
            })
            .collect()
    }

    #[must_use]
    pub fn system_replication(&self) -> Vec<SystemReplicationRecord> {
        self.replication()
    }
}

/// One database-wide DataBucket domain with one generated system catalog.
pub struct Database<S: PageStore> {
    domain: Arc<RwLock<StorageDomain<GeneratedSystemCatalog, S>>>,
}

impl<S: PageStore> Clone for Database<S> {
    fn clone(&self) -> Self {
        Self {
            domain: self.domain.clone(),
        }
    }
}

impl<S: PageStore> Database<S> {
    #[must_use]
    pub fn new(id: StorageDomainId, writer_epoch: WriterEpoch, store: S) -> Self {
        Self {
            domain: Arc::new(RwLock::new(StorageDomain::new(
                id,
                writer_epoch,
                GeneratedSystemCatalog::default(),
                store,
            ))),
        }
    }

    pub fn open(id: StorageDomainId, writer_epoch: WriterEpoch, store: S) -> Result<Self, DomainError<S::Error>> {
        let domain = StorageDomain::open(id, writer_epoch, GeneratedSystemCatalog::default(), store)?;
        Ok(Self {
            domain: Arc::new(RwLock::new(domain)),
        })
    }

    pub fn begin_generation(&self) -> Result<GenerationBuilder, DomainError<S::Error>> {
        self.domain.read().begin_generation()
    }

    #[must_use]
    pub fn id(&self) -> StorageDomainId {
        self.domain.read().id()
    }

    #[must_use]
    pub fn generation(&self) -> u64 {
        self.domain.read().generation()
    }

    pub fn register_table(&self, name: &str, schema_version: u32) -> Result<TableId, DomainError<S::Error>> {
        self.register_table_with_stride(name, schema_version, data_bucket::PAGE_SIZE as u32)
    }

    pub fn register_table_with_stride(
        &self,
        name: &str,
        schema_version: u32,
        page_stride: u32,
    ) -> Result<TableId, DomainError<S::Error>> {
        let name = CatalogName::new(name).map_err(DomainError::Catalog)?;
        let mut domain = self.domain.write();
        let tables = domain.catalog().records(CatalogRecordKind::Table);
        let existing = tables.iter().find_map(|record| match record {
            CatalogRecord::Table(table) if table.name == name => Some(table.clone()),
            _ => None,
        });
        let mut table = if let Some(table) = existing {
            if table.schema_version == schema_version && table.page_stride == page_stride {
                return Ok(table.table_id);
            }
            table
        } else {
            let next = tables
                .iter()
                .filter_map(|record| match record {
                    CatalogRecord::Table(table) => Some(table.table_id.0),
                    _ => None,
                })
                .max()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or(DomainError::Catalog(CatalogError::InvalidMutation))?;
            SystemTableRecord {
                table_id: TableId(next),
                name,
                schema_version,
                data_space_id: SpaceId(0),
                page_stride,
                row_count: 0,
                live_row_bytes: 0,
                allocated_data_pages: 0,
                live_data_pages: 0,
                primary_index_entries: 0,
                secondary_index_entries: 0,
                tombstones: 0,
                applied_generation: domain.generation(),
                durable_generation: domain.generation(),
            }
        };
        table.schema_version = schema_version;
        table.page_stride = page_stride;
        let table_id = table.table_id;
        let mut generation = domain.begin_generation()?;
        generation.update_catalog(CatalogMutation::Upsert(CatalogRecord::Table(table)));
        domain.commit_generation(generation.finish())?;
        Ok(table_id)
    }

    pub fn commit_generation(&self, plan: GenerationPlan) -> Result<CommittedGeneration, DomainError<S::Error>> {
        self.domain.write().commit_generation(plan)
    }

    pub fn read_page(&self, address: PageAddress) -> Result<Option<Vec<u8>>, DomainError<S::Error>> {
        self.domain.read().read_page(address)
    }

    #[must_use]
    pub fn catalog(&self) -> DatabaseCatalog<S> {
        DatabaseCatalog {
            domain: self.domain.clone(),
        }
    }
}

#[cfg(feature = "s3-support")]
impl Database<data_bucket::storage::s3::S3PageStore> {
    pub fn open_s3(
        id: StorageDomainId,
        writer_epoch: WriterEpoch,
        config: data_bucket::storage::s3::S3Config,
    ) -> Result<Self, DomainError<data_bucket::storage::s3::S3StoreError>> {
        let store = data_bucket::storage::s3::S3PageStore::new(config).map_err(DomainError::Store)?;
        Self::open(id, writer_epoch, store)
    }
}

/// Cloneable read-only access to the database's generated catalog.
pub struct DatabaseCatalog<S: PageStore> {
    domain: Arc<RwLock<StorageDomain<GeneratedSystemCatalog, S>>>,
}

impl<S: PageStore> Clone for DatabaseCatalog<S> {
    fn clone(&self) -> Self {
        Self {
            domain: self.domain.clone(),
        }
    }
}

impl<S: PageStore> DatabaseCatalog<S> {
    #[must_use]
    pub fn record(&self, key: &CatalogKey) -> Option<CatalogRecord> {
        self.domain.read().catalog().record(key)
    }

    #[must_use]
    pub fn system_tables(&self) -> Vec<SystemTableRecord> {
        records_of(&self.domain, CatalogRecordKind::Table)
            .into_iter()
            .filter_map(|record| match record {
                CatalogRecord::Table(row) => Some(row),
                _ => None,
            })
            .collect()
    }

    #[must_use]
    pub fn system_pages(&self) -> Vec<SystemPageRecord> {
        records_of(&self.domain, CatalogRecordKind::Page)
            .into_iter()
            .filter_map(|record| match record {
                CatalogRecord::Page(row) => Some(row),
                _ => None,
            })
            .collect()
    }

    #[must_use]
    pub fn system_indexes(&self) -> Vec<SystemIndexRecord> {
        records_of(&self.domain, CatalogRecordKind::Index)
            .into_iter()
            .filter_map(|record| match record {
                CatalogRecord::Index(row) => Some(row),
                _ => None,
            })
            .collect()
    }

    #[must_use]
    pub fn system_replication(&self) -> Vec<SystemReplicationRecord> {
        records_of(&self.domain, CatalogRecordKind::Replication)
            .into_iter()
            .filter_map(|record| match record {
                CatalogRecord::Replication(row) => Some(row),
                _ => None,
            })
            .collect()
    }
}

fn records_of<S: PageStore>(
    domain: &RwLock<StorageDomain<GeneratedSystemCatalog, S>>,
    kind: CatalogRecordKind,
) -> Vec<CatalogRecord> {
    domain.read().catalog().records(kind)
}

pub struct PreparedGeneratedCatalog {
    table: StorageCatalogWorkTable,
    checkpoint: Vec<u8>,
}

impl PreparedSystemCatalog for PreparedGeneratedCatalog {
    fn checkpoint(&self) -> &[u8] {
        &self.checkpoint
    }
}

impl SystemCatalog for GeneratedSystemCatalog {
    type Prepared = PreparedGeneratedCatalog;

    fn prepare(
        &self,
        _permit: &CatalogWritePermit,
        mutations: &[CatalogMutation],
    ) -> Result<Self::Prepared, CatalogError> {
        let bytes = self.table.read().unload().map_err(|_| CatalogError::Codec)?;
        let mut table = StorageCatalogWorkTable::load(&bytes).map_err(|_| CatalogError::Codec)?;
        for mutation in mutations {
            match mutation {
                CatalogMutation::Upsert(record) => table.upsert(record_to_row(record)),
                CatalogMutation::Delete(key) => {
                    if let Some(existing) = table.select(key) {
                        let mut tombstone = existing.clone();
                        tombstone.kind = 0;
                        table.upsert(tombstone);
                    }
                }
            }
        }
        let checkpoint = table.unload().map_err(|_| CatalogError::Codec)?;
        Ok(PreparedGeneratedCatalog { table, checkpoint })
    }

    fn prepare_restore(&self, _permit: &CatalogWritePermit, checkpoint: &[u8]) -> Result<Self::Prepared, CatalogError> {
        let table = StorageCatalogWorkTable::load(checkpoint).map_err(|_| CatalogError::Codec)?;
        Ok(PreparedGeneratedCatalog {
            table,
            checkpoint: checkpoint.to_vec(),
        })
    }

    fn publish(&self, _permit: &CatalogWritePermit, prepared: Self::Prepared) {
        *self.table.write() = prepared.table;
    }

    fn record(&self, key: &CatalogKey) -> Option<CatalogRecord> {
        self.table.read().select(key).and_then(row_to_record)
    }

    fn records(&self, kind: CatalogRecordKind) -> Vec<CatalogRecord> {
        self.table
            .read()
            .select_all()
            .filter(|row| row.kind == kind as u8)
            .filter_map(row_to_record)
            .collect()
    }
}

fn record_to_row(record: &CatalogRecord) -> StorageCatalogRow {
    let mut row = empty_row(record.key(), record.kind());
    match record {
        CatalogRecord::Table(value) => {
            row.table_id = value.table_id.0;
            row.name_length = value.name.length();
            row.name_bytes = *value.name.bytes();
            row.schema_version = value.schema_version;
            row.data_space_id = value.data_space_id.0;
            row.page_stride = value.page_stride;
            row.row_count = value.row_count;
            row.live_row_bytes = value.live_row_bytes;
            row.allocated_data_pages = value.allocated_data_pages;
            row.live_data_pages = value.live_data_pages;
            row.primary_index_entries = value.primary_index_entries;
            row.secondary_index_entries = value.secondary_index_entries;
            row.tombstones = value.tombstones;
            row.applied_generation = value.applied_generation;
            row.durable_generation = value.durable_generation;
        }
        CatalogRecord::Page(value) => {
            row.table_id = value.table_id.0;
            row.space_id = value.space_id.0;
            row.page_id = usize::from(value.page_id) as u32;
            row.page_kind = value.page_kind as u8;
            row.generation = value.generation;
            row.object = value.object;
            row.object_offset = value.object_offset;
            row.encoded_length = value.encoded_length;
            row.decoded_length = value.decoded_length;
            row.checksum = value.checksum;
            row.live_rows = value.live_rows;
            row.live_bytes = value.live_bytes;
        }
        CatalogRecord::Index(value) => {
            row.table_id = value.table_id.0;
            row.index_id = value.index_id;
            row.index_space_id = value.space_id.0;
            row.index_primary = value.primary;
            row.name_length = value.name.length();
            row.name_bytes = *value.name.bytes();
            row.entries = value.entries;
            row.generation = value.generation;
        }
        CatalogRecord::Replication(value) => {
            row.generation = value.generation;
            row.upstash = value.upstash as u8;
            row.tigris = value.tigris as u8;
            row.last_error = value.last_error.map_or(0, |error| error as u16);
        }
    }
    row
}

fn empty_row(key: CatalogKey, kind: CatalogRecordKind) -> StorageCatalogRow {
    StorageCatalogRow {
        key,
        kind: kind as u8,
        table_id: 0,
        space_id: 0,
        page_id: 0,
        page_kind: 0,
        generation: 0,
        index_id: 0,
        name_length: 0,
        name_bytes: [0; 96],
        schema_version: 0,
        data_space_id: 0,
        page_stride: 0,
        row_count: 0,
        live_row_bytes: 0,
        allocated_data_pages: 0,
        live_data_pages: 0,
        primary_index_entries: 0,
        secondary_index_entries: 0,
        tombstones: 0,
        applied_generation: 0,
        durable_generation: 0,
        object: [0; 32],
        object_offset: 0,
        encoded_length: 0,
        decoded_length: 0,
        checksum: [0; 32],
        live_rows: 0,
        live_bytes: 0,
        entries: 0,
        index_space_id: 0,
        index_primary: false,
        upstash: 0,
        tigris: 0,
        last_error: 0,
    }
}

fn row_to_record(row: &StorageCatalogRow) -> Option<CatalogRecord> {
    match row.kind {
        value if value == CatalogRecordKind::Table as u8 => Some(CatalogRecord::Table(SystemTableRecord {
            table_id: data_bucket::storage::TableId(row.table_id),
            name: catalog_name(row)?,
            schema_version: row.schema_version,
            data_space_id: SpaceId(row.data_space_id),
            page_stride: row.page_stride,
            row_count: row.row_count,
            live_row_bytes: row.live_row_bytes,
            allocated_data_pages: row.allocated_data_pages,
            live_data_pages: row.live_data_pages,
            primary_index_entries: row.primary_index_entries,
            secondary_index_entries: row.secondary_index_entries,
            tombstones: row.tombstones,
            applied_generation: row.applied_generation,
            durable_generation: row.durable_generation,
        })),
        value if value == CatalogRecordKind::Page as u8 => Some(CatalogRecord::Page(SystemPageRecord {
            table_id: data_bucket::storage::TableId(row.table_id),
            space_id: SpaceId(row.space_id),
            page_id: PageId::from(row.page_id),
            page_kind: page_kind(row.page_kind)?,
            generation: row.generation,
            object: row.object,
            object_offset: row.object_offset,
            encoded_length: row.encoded_length,
            decoded_length: row.decoded_length,
            checksum: row.checksum,
            live_rows: row.live_rows,
            live_bytes: row.live_bytes,
        })),
        value if value == CatalogRecordKind::Index as u8 => Some(CatalogRecord::Index(SystemIndexRecord {
            table_id: data_bucket::storage::TableId(row.table_id),
            index_id: row.index_id,
            space_id: SpaceId(row.index_space_id),
            primary: row.index_primary,
            name: catalog_name(row)?,
            entries: row.entries,
            generation: row.generation,
        })),
        value if value == CatalogRecordKind::Replication as u8 => {
            Some(CatalogRecord::Replication(SystemReplicationRecord {
                generation: row.generation,
                upstash: replica_state(row.upstash)?,
                tigris: replica_state(row.tigris)?,
                last_error: replication_error(row.last_error)?,
            }))
        }
        _ => None,
    }
}

fn catalog_name(row: &StorageCatalogRow) -> Option<data_bucket::storage::CatalogName> {
    let length = usize::from(row.name_length);
    let name = core::str::from_utf8(row.name_bytes.get(..length)?).ok()?;
    data_bucket::storage::CatalogName::new(name).ok()
}

fn page_kind(value: u8) -> Option<data_bucket::storage::PageKind> {
    match value {
        1 => Some(data_bucket::storage::PageKind::Data),
        2 => Some(data_bucket::storage::PageKind::PrimaryIndex),
        3 => Some(data_bucket::storage::PageKind::SecondaryIndex),
        4 => Some(data_bucket::storage::PageKind::Metadata),
        _ => None,
    }
}

fn replica_state(value: u8) -> Option<ReplicaState> {
    match value {
        0 => Some(ReplicaState::Absent),
        1 => Some(ReplicaState::Staged),
        2 => Some(ReplicaState::Durable),
        3 => Some(ReplicaState::Failed),
        _ => None,
    }
}

fn replication_error(value: u16) -> Option<Option<ReplicationErrorCode>> {
    match value {
        0 => Some(None),
        1 => Some(Some(ReplicationErrorCode::Transport)),
        2 => Some(Some(ReplicationErrorCode::Conflict)),
        3 => Some(Some(ReplicationErrorCode::Corrupt)),
        4 => Some(Some(ReplicationErrorCode::Unauthorized)),
        _ => None,
    }
}
