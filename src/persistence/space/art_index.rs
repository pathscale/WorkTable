//! Backend-native ART checkpoints with a logical Set/Remove write-ahead log.
//!
//! Checkpoints preserve the selected backend's pointer-free physical topology.
//! The WAL is logical because raw pointers, locks, SMR state, and allocator
//! addresses are process-local. Compaction reconstructs a temporary native ART,
//! applies the WAL, writes a new native checkpoint atomically, and drops the
//! temporary tree; no duplicate ART is retained during normal operation.

use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use data_bucket::{Link, page::PageId};
use eyre::{Context, bail, eyre};
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::index::{
    ArcticIndex, ArcticKey, ArcticMultiIndex, CongeeIndex, CongeeKey, PersistentArcticIndex,
    PersistentArcticMultiIndex, PersistentArtIndex, PersistentCongeeIndex, UniqueIndex,
};
use crate::persistence::SpaceIndexOps;
use crate::persistence::space::BatchChangeEvent;
use crate::prelude::WT_INDEX_EXTENSION;
use crate::util::OffsetEqLink;

const FILE_MAGIC: &[u8; 8] = b"WTART001";
const WAL_MAGIC: &[u8; 4] = b"WAL1";
const FORMAT_VERSION: u16 = 1;
const HEADER_LEN: usize = 32;
const WAL_HEADER_LEN: usize = 12;
const COMPACT_WAL_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Backend {
    Arctic = 1,
    Congee = 2,
    /// Non-unique Arctic: one record per `(key, link)` pair.
    ArcticMulti = 3,
}

impl Backend {
    fn from_byte(byte: u8) -> eyre::Result<Self> {
        match byte {
            1 => Ok(Self::Arctic),
            2 => Ok(Self::Congee),
            3 => Ok(Self::ArcticMulti),
            _ => bail!("unknown ART backend tag {byte}"),
        }
    }
}

/// Stable fixed-width codec used by logical ART WAL records.
///
/// Generated single-column primary-key newtypes delegate this contract to
/// their supported unsigned integer field.
pub trait ArtPersistenceKey: Clone + Debug + Eq + Hash + Ord + Send + Sync + 'static {
    /// Number of key bytes written to the WAL.
    const WIDTH: u8;

    /// Appends exactly [`Self::WIDTH`] bytes in big-endian order.
    fn encode_art_key(&self, output: &mut Vec<u8>);

    /// Decodes exactly [`Self::WIDTH`] bytes.
    fn decode_art_key(bytes: &[u8]) -> eyre::Result<Self>;
}

macro_rules! impl_art_persistence_key {
    ($($type:ty),+ $(,)?) => {
        $(
            impl ArtPersistenceKey for $type {
                const WIDTH: u8 = std::mem::size_of::<Self>() as u8;

                fn encode_art_key(&self, output: &mut Vec<u8>) {
                    output.extend_from_slice(&self.to_be_bytes());
                }

                fn decode_art_key(bytes: &[u8]) -> eyre::Result<Self> {
                    let bytes: [u8; std::mem::size_of::<Self>()] = bytes
                        .try_into()
                        .map_err(|_| eyre!("invalid {}-byte ART key", Self::WIDTH))?;
                    Ok(Self::from_be_bytes(bytes))
                }
            }
        )+
    };
}

impl_art_persistence_key!(u8, u16, u32, u64, u128, usize);

/// Signed keys, encoded through the same sign-bit flip the in-memory ART uses.
///
/// Signed types have `to_be_bytes` of their own, so a plain round trip would
/// work and the flip is **not load-bearing today**: every use of this trait is
/// a WAL record encoded and decoded whole, and nothing sorts or ranges over the
/// encoded bytes.
///
/// It is here for consistency with why the unsigned encoding is big-endian at
/// all. Endianness is irrelevant to a pure round trip; big-endian was chosen so
/// byte order is numeric order, and two's complement breaks that for signed
/// keys because a negative has its high bit set. Flipping the sign bit keeps
/// the stated property true for signed keys too, so a future reader who relies
/// on it is not caught out by a gap that only exists for half the key types.
macro_rules! impl_art_persistence_key_signed {
    ($($type:ty => $raw:ty),+ $(,)?) => {
        $(
            impl ArtPersistenceKey for $type {
                const WIDTH: u8 = std::mem::size_of::<Self>() as u8;

                fn encode_art_key(&self, output: &mut Vec<u8>) {
                    let raw = (*self as $raw) ^ ((1 as $raw) << (<$raw>::BITS - 1));
                    output.extend_from_slice(&raw.to_be_bytes());
                }

                fn decode_art_key(bytes: &[u8]) -> eyre::Result<Self> {
                    let bytes: [u8; std::mem::size_of::<Self>()] = bytes
                        .try_into()
                        .map_err(|_| eyre!("invalid {}-byte ART key", Self::WIDTH))?;
                    let raw = <$raw>::from_be_bytes(bytes) ^ ((1 as $raw) << (<$raw>::BITS - 1));
                    Ok(raw as Self)
                }
            }
        )+
    };
}

impl_art_persistence_key_signed!(i8 => u8, i16 => u16, i32 => u32, i64 => u64, i128 => u128, isize => usize);

#[derive(Clone, Debug, Eq, PartialEq)]
enum WalOp {
    /// Unique files: associate the key with this link. Multi files: add one
    /// `(key, link)` pair.
    Set(Link),
    /// Unique files only: drop the key.
    Remove,
    /// Multi files only: drop one `(key, link)` pair.
    RemovePair(Link),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WalRecord<K> {
    event_id: u64,
    key: K,
    op: WalOp,
}

#[derive(Debug)]
struct Image<K> {
    snapshot: Vec<u8>,
    wal: Vec<WalRecord<K>>,
    wal_bytes: u64,
    durable_len: u64,
}

#[derive(Debug)]
struct ArtFile<K> {
    path: PathBuf,
    file: File,
    backend: Backend,
    table_version: u32,
    wal_bytes: u64,
    marker: PhantomData<K>,
}

/// Builds the sibling temporary file used for atomic rewrites by appending
/// `.tmp` to the complete file name. `Path::with_extension` must not be used
/// here: it replaces the final extension, so `primary.wt.idx` would become
/// `primary.wt.<new>` and two different index files could collide on one
/// temporary name.
fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().map(ToOwned::to_owned).unwrap_or_default();
    name.push(".tmp");
    path.with_file_name(name)
}

impl<K: ArtPersistenceKey> ArtFile<K> {
    async fn open(path: PathBuf, backend: Backend, table_version: u32, empty_snapshot: Vec<u8>) -> eyre::Result<Self> {
        // A crash between writing a temporary checkpoint and renaming it over
        // the live file leaves the `.tmp` sibling behind. It was never made
        // visible, so it is dead weight; remove it before it can be confused
        // with live state or block a future rename.
        let stale_temporary = temporary_path(&path);
        if stale_temporary.exists() {
            tokio::fs::remove_file(&stale_temporary).await?;
        }
        if !path.exists() {
            Self::write_new_file(&path, backend, table_version, &empty_snapshot).await?;
        }
        let image = Self::read_image(&path, backend, table_version).await?;
        let mut file = OpenOptions::new().read(true).write(true).open(&path).await?;
        // Remove an incomplete final frame before appending. Leaving it in
        // place would make every later valid frame unreachable on recovery.
        file.set_len(image.durable_len).await?;
        file.seek(std::io::SeekFrom::End(0)).await?;
        Ok(Self {
            path,
            file,
            backend,
            table_version,
            wal_bytes: image.wal_bytes,
            marker: PhantomData,
        })
    }

    async fn read_image(path: &Path, backend: Backend, table_version: u32) -> eyre::Result<Image<K>> {
        let mut file = File::open(path)
            .await
            .wrap_err_with(|| format!("open ART index {}", path.display()))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        if bytes.len() < HEADER_LEN {
            bail!("ART index {} has a truncated header", path.display());
        }
        if &bytes[..8] != FILE_MAGIC {
            bail!("ART index {} has an invalid magic", path.display());
        }
        let format = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
        if format != FORMAT_VERSION {
            bail!("unsupported ART index format {format}");
        }
        let found_backend = Backend::from_byte(bytes[10])?;
        if found_backend != backend {
            bail!("ART index backend mismatch: expected {backend:?}, found {found_backend:?}");
        }
        if bytes[11] != K::WIDTH {
            bail!("ART key width mismatch: expected {}, found {}", K::WIDTH, bytes[11]);
        }
        let found_table_version = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
        if found_table_version != table_version {
            bail!("ART index table version mismatch: expected {table_version}, found {found_table_version}");
        }
        let snapshot_len = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let snapshot_len = usize::try_from(snapshot_len).map_err(|_| eyre!("ART snapshot is too large"))?;
        let snapshot_crc = u32::from_le_bytes(bytes[24..28].try_into().unwrap());
        let snapshot_end = HEADER_LEN
            .checked_add(snapshot_len)
            .ok_or_else(|| eyre!("ART snapshot length overflow"))?;
        if snapshot_end > bytes.len() {
            bail!("ART index {} has a truncated snapshot", path.display());
        }
        let snapshot = bytes[HEADER_LEN..snapshot_end].to_vec();
        if crc32fast::hash(&snapshot) != snapshot_crc {
            bail!("ART index {} snapshot checksum mismatch", path.display());
        }

        let mut wal = Vec::new();
        let mut position = snapshot_end;
        let mut durable_end = snapshot_end;
        while position < bytes.len() {
            // A crash can leave the final frame header or payload incomplete.
            // Only an incomplete final frame is ignored; complete corruption
            // remains a hard error.
            if bytes.len() - position < WAL_HEADER_LEN {
                break;
            }
            if &bytes[position..position + 4] != WAL_MAGIC {
                bail!("ART WAL frame at byte {position} has an invalid magic");
            }
            let payload_len = u32::from_le_bytes(bytes[position + 4..position + 8].try_into().unwrap()) as usize;
            let expected_len = 9usize + K::WIDTH as usize + 12;
            if payload_len != expected_len {
                bail!("ART WAL frame at byte {position} has invalid payload length {payload_len}");
            }
            let payload_crc = u32::from_le_bytes(bytes[position + 8..position + 12].try_into().unwrap());
            let payload_start = position + WAL_HEADER_LEN;
            let payload_end = payload_start
                .checked_add(payload_len)
                .ok_or_else(|| eyre!("ART WAL frame length overflow"))?;
            if payload_end > bytes.len() {
                break;
            }
            let payload = &bytes[payload_start..payload_end];
            if crc32fast::hash(payload) != payload_crc {
                bail!("ART WAL frame at byte {position} checksum mismatch");
            }
            wal.push(decode_wal_record::<K>(payload)?);
            position = payload_end;
            durable_end = payload_end;
        }

        Ok(Image {
            snapshot,
            wal,
            wal_bytes: (durable_end - snapshot_end) as u64,
            durable_len: durable_end as u64,
        })
    }

    async fn append(&mut self, records: &[WalRecord<K>]) -> eyre::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut bytes = Vec::with_capacity(records.len() * (WAL_HEADER_LEN + 32));
        for record in records {
            let payload = encode_wal_record(record);
            bytes.extend_from_slice(WAL_MAGIC);
            bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
            bytes.extend_from_slice(&payload);
        }
        self.file.write_all(&bytes).await?;
        self.file.flush().await?;
        self.wal_bytes += bytes.len() as u64;
        Ok(())
    }

    fn should_compact(&self) -> bool {
        self.wal_bytes >= COMPACT_WAL_BYTES
    }

    async fn rewrite(&mut self, snapshot: &[u8]) -> eyre::Result<()> {
        Self::write_file_atomically(&self.path, self.backend, self.table_version, snapshot).await?;
        self.file = OpenOptions::new().read(true).write(true).open(&self.path).await?;
        self.file.seek(std::io::SeekFrom::End(0)).await?;
        self.wal_bytes = 0;
        Ok(())
    }

    /// Writes a complete checkpoint next to `path` and renames it into place,
    /// so a crash at any point leaves either the previous file or the new one,
    /// never a truncated in-between state.
    async fn write_file_atomically(
        path: &Path,
        backend: Backend,
        table_version: u32,
        snapshot: &[u8],
    ) -> eyre::Result<()> {
        let temporary = temporary_path(path);
        Self::write_new_file(&temporary, backend, table_version, snapshot).await?;
        tokio::fs::rename(&temporary, path).await?;
        Ok(())
    }

    async fn write_new_file(path: &Path, backend: Backend, table_version: u32, snapshot: &[u8]) -> eyre::Result<()> {
        let mut header = Vec::with_capacity(HEADER_LEN);
        header.extend_from_slice(FILE_MAGIC);
        header.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        header.push(backend as u8);
        header.push(K::WIDTH);
        header.extend_from_slice(&table_version.to_le_bytes());
        header.extend_from_slice(&(snapshot.len() as u64).to_le_bytes());
        header.extend_from_slice(&crc32fast::hash(snapshot).to_le_bytes());
        header.extend_from_slice(&0u32.to_le_bytes());
        debug_assert_eq!(header.len(), HEADER_LEN);

        let mut file = File::create(path).await?;
        file.write_all(&header).await?;
        file.write_all(snapshot).await?;
        file.flush().await?;
        file.sync_data().await?;
        Ok(())
    }
}

fn encode_wal_record<K: ArtPersistenceKey>(record: &WalRecord<K>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(9 + K::WIDTH as usize + 12);
    bytes.extend_from_slice(&record.event_id.to_le_bytes());
    match record.op {
        WalOp::Set(_) => bytes.push(1),
        WalOp::Remove => bytes.push(2),
        WalOp::RemovePair(_) => bytes.push(3),
    }
    record.key.encode_art_key(&mut bytes);
    let link = match record.op {
        WalOp::Set(link) | WalOp::RemovePair(link) => link,
        WalOp::Remove => Link::default(),
    };
    let page_id: usize = link.page_id.into();
    bytes.extend_from_slice(&(page_id as u32).to_le_bytes());
    bytes.extend_from_slice(&link.offset.to_le_bytes());
    bytes.extend_from_slice(&link.length.to_le_bytes());
    bytes
}

fn decode_wal_record<K: ArtPersistenceKey>(bytes: &[u8]) -> eyre::Result<WalRecord<K>> {
    let expected_len = 9 + K::WIDTH as usize + 12;
    if bytes.len() != expected_len {
        bail!("invalid ART WAL payload length {}", bytes.len());
    }
    let event_id = u64::from_le_bytes(bytes[..8].try_into().unwrap());
    let operation = bytes[8];
    let key_end = 9 + K::WIDTH as usize;
    let key = K::decode_art_key(&bytes[9..key_end])?;
    let page_id = u32::from_le_bytes(bytes[key_end..key_end + 4].try_into().unwrap());
    let offset = u32::from_le_bytes(bytes[key_end + 4..key_end + 8].try_into().unwrap());
    let length = u32::from_le_bytes(bytes[key_end + 8..key_end + 12].try_into().unwrap());
    let link = Link {
        page_id: PageId::from(page_id),
        offset,
        length,
    };
    let op = match operation {
        1 => WalOp::Set(link),
        2 => WalOp::Remove,
        3 => WalOp::RemovePair(link),
        _ => bail!("invalid ART WAL operation {operation}"),
    };
    Ok(WalRecord { event_id, key, op })
}

fn logical_record<K: ArtPersistenceKey>(event: ChangeEvent<Pair<K, Link>>) -> eyre::Result<WalRecord<K>> {
    match event {
        ChangeEvent::InsertAt {
            event_id,
            max_value,
            value,
            index,
        } if index == 0 && max_value == value => Ok(WalRecord {
            event_id: event_id.inner(),
            key: value.key,
            op: WalOp::Set(value.value),
        }),
        ChangeEvent::RemoveAt {
            event_id,
            max_value,
            value,
            index,
        } if index == 0 && max_value == value => Ok(WalRecord {
            event_id: event_id.inner(),
            key: value.key,
            op: WalOp::Remove,
        }),
        _ => bail!("native ART persistence received a structural WorkTablesIndex event"),
    }
}

fn apply_wal<K, V, I>(index: &I, wal: &[WalRecord<K>], wrap: impl Fn(Link) -> V) -> eyre::Result<()>
where
    K: ArtPersistenceKey,
    V: Clone + Send + 'static,
    I: UniqueIndex<K, V>,
{
    for record in wal {
        match record.op {
            WalOp::Set(link) => {
                index.insert_value(record.key.clone(), wrap(link));
            }
            WalOp::Remove => {
                index.remove_value(&record.key);
            }
            WalOp::RemovePair(_) => bail!("unique ART WAL contains a non-unique pair record"),
        }
    }
    Ok(())
}

/// The multi variant of [`logical_record`]: `InsertAt` adds one `(key, link)`
/// pair, `RemoveAt` drops the one pair it names.
fn logical_multi_record<K: ArtPersistenceKey>(event: ChangeEvent<Pair<K, Link>>) -> eyre::Result<WalRecord<K>> {
    match event {
        ChangeEvent::InsertAt {
            event_id,
            max_value,
            value,
            index,
        } if index == 0 && max_value == value => Ok(WalRecord {
            event_id: event_id.inner(),
            key: value.key,
            op: WalOp::Set(value.value),
        }),
        ChangeEvent::RemoveAt {
            event_id,
            max_value,
            value,
            index,
        } if index == 0 && max_value == value => Ok(WalRecord {
            event_id: event_id.inner(),
            key: value.key,
            op: WalOp::RemovePair(value.value),
        }),
        _ => bail!("native ART persistence received a structural WorkTablesIndex event"),
    }
}

fn apply_multi_wal<K, V>(
    index: &ArcticMultiIndex<K, V>,
    wal: &[WalRecord<K>],
    wrap: impl Fn(Link) -> V,
) -> eyre::Result<()>
where
    K: ArtPersistenceKey + ArcticKey,
    V: Clone + Debug + PartialEq + Send + Sync + 'static,
{
    for record in wal {
        match record.op {
            WalOp::Set(link) => index.insert_pair(record.key.clone(), wrap(link)),
            WalOp::RemovePair(link) => {
                index.remove_pair(&record.key, &wrap(link));
            }
            WalOp::Remove => bail!("non-unique ART WAL contains a unique whole-key removal"),
        }
    }
    Ok(())
}

/// Multi checkpoints are a flat pair list, not a preserved topology: replay
/// rebuilds the tree by insertion, exactly like WAL recovery, so the snapshot
/// format stays independent of the in-memory slot layout.
fn encode_multi_pairs<K: ArtPersistenceKey>(pairs: impl Iterator<Item = (K, Link)>) -> Vec<u8> {
    let mut body = Vec::new();
    let mut count: u64 = 0;
    for (key, link) in pairs {
        key.encode_art_key(&mut body);
        encode_link(link, &mut body);
        count += 1;
    }
    let mut output = Vec::with_capacity(8 + body.len());
    output.extend_from_slice(&count.to_le_bytes());
    output.extend(body);
    output
}

fn decode_multi_pairs<K: ArtPersistenceKey>(bytes: &[u8]) -> eyre::Result<Vec<(K, Link)>> {
    let mut decoder = Decoder::new(bytes);
    let count = decoder.u64()?;
    let mut pairs = Vec::with_capacity(usize::try_from(count).map_err(|_| eyre!("ART pair count overflow"))?);
    for _ in 0..count {
        let key = K::decode_art_key(decoder.take(K::WIDTH as usize)?)?;
        let link = decoder.link()?;
        pairs.push((key, link));
    }
    decoder.finish()?;
    Ok(pairs)
}

/// Disk-side Arctic checkpoint and WAL state.
#[derive(Debug)]
pub struct SpaceArcticIndex<K: ArtPersistenceKey, const INNER_PAGE_SIZE: u32> {
    file: ArtFile<K>,
}

impl<K, const INNER_PAGE_SIZE: u32> SpaceArcticIndex<K, INNER_PAGE_SIZE>
where
    K: ArtPersistenceKey + ArcticKey,
    K::Raw: arctic::topology::Key,
{
    async fn new(path: PathBuf, table_version: u32) -> eyre::Result<Self> {
        let mut empty = ArcticIndex::<K, Link>::default();
        let snapshot = encode_arctic_topology(&empty.export_topology(|link| *link)?)?;
        Ok(Self {
            file: ArtFile::open(path, Backend::Arctic, table_version, snapshot).await?,
        })
    }

    /// Reconstructs a persisted Arctic index from its native checkpoint and WAL.
    pub async fn load_index<const N: usize>(
        path: impl AsRef<Path>,
        table_version: u32,
    ) -> eyre::Result<PersistentArcticIndex<K, OffsetEqLink<N>>> {
        let image = ArtFile::<K>::read_image(path.as_ref(), Backend::Arctic, table_version).await?;
        let topology = decode_arctic_topology(&image.snapshot)?;
        let index = ArcticIndex::from_topology(topology, OffsetEqLink)?;
        apply_wal(&index, &image.wal, OffsetEqLink)?;
        Ok(PersistentArtIndex::from_inner(index))
    }

    /// Writes a complete native Arctic checkpoint with an empty WAL.
    pub async fn write_checkpoint<const N: usize>(
        path: impl AsRef<Path>,
        table_version: u32,
        index: &mut PersistentArcticIndex<K, OffsetEqLink<N>>,
    ) -> eyre::Result<()> {
        let topology = index.inner_mut().export_topology(|link| link.0)?;
        let snapshot = encode_arctic_topology(&topology)?;
        // Temp-file-plus-rename: creating the live file in place truncated the
        // previous checkpoint and the WAL, so a crash mid-checkpoint destroyed
        // both the old and the new state.
        ArtFile::<K>::write_file_atomically(path.as_ref(), Backend::Arctic, table_version, &snapshot).await
    }

    async fn compact(&mut self) -> eyre::Result<()> {
        let image = ArtFile::<K>::read_image(&self.file.path, Backend::Arctic, self.file.table_version).await?;
        let topology = decode_arctic_topology(&image.snapshot)?;
        let mut index = ArcticIndex::from_topology(topology, |link| link)?;
        apply_wal(&index, &image.wal, |link| link)?;
        let snapshot = encode_arctic_topology(&index.export_topology(|link| *link)?)?;
        self.file.rewrite(&snapshot).await
    }
}

impl<K, const INNER_PAGE_SIZE: u32> SpaceIndexOps<K> for SpaceArcticIndex<K, INNER_PAGE_SIZE>
where
    K: ArtPersistenceKey + ArcticKey,
    K::Raw: arctic::topology::Key,
{
    async fn primary_from_table_files_path<S: AsRef<str> + Send>(path: S, version: u32) -> eyre::Result<Self> {
        Self::new(
            PathBuf::from(format!("{}/primary{}", path.as_ref(), WT_INDEX_EXTENSION)),
            version,
        )
        .await
    }

    async fn secondary_from_table_files_path<S1: AsRef<str> + Send, S2: AsRef<str> + Send>(
        path: S1,
        name: S2,
        version: u32,
    ) -> eyre::Result<Self> {
        Self::new(
            PathBuf::from(format!("{}/{}{}", path.as_ref(), name.as_ref(), WT_INDEX_EXTENSION)),
            version,
        )
        .await
    }

    async fn bootstrap(_: &mut File, _: String, _: u32) -> eyre::Result<()> {
        Ok(())
    }

    async fn process_change_event(&mut self, event: ChangeEvent<Pair<K, Link>>) -> eyre::Result<()> {
        self.file.append(&[logical_record(event)?]).await?;
        if self.file.should_compact() {
            self.compact().await?;
        }
        Ok(())
    }

    async fn process_change_event_batch(&mut self, events: BatchChangeEvent<K>) -> eyre::Result<()> {
        let records = events
            .into_iter()
            .map(logical_record)
            .collect::<eyre::Result<Vec<_>>>()?;
        self.file.append(&records).await?;
        if self.file.should_compact() {
            self.compact().await?;
        }
        Ok(())
    }
}

/// Disk-side Congee checkpoint and WAL state.
#[derive(Debug)]
pub struct SpaceCongeeIndex<K: ArtPersistenceKey, const INNER_PAGE_SIZE: u32> {
    file: ArtFile<K>,
}

impl<K, const INNER_PAGE_SIZE: u32> SpaceCongeeIndex<K, INNER_PAGE_SIZE>
where
    K: ArtPersistenceKey + CongeeKey,
{
    async fn new(path: PathBuf, table_version: u32) -> eyre::Result<Self> {
        let mut empty = CongeeIndex::<K, Link>::default();
        let snapshot = encode_congee_topology(&empty.export_topology(|link| *link)?)?;
        Ok(Self {
            file: ArtFile::open(path, Backend::Congee, table_version, snapshot).await?,
        })
    }

    /// Reconstructs a persisted Congee index from its native checkpoint and WAL.
    pub async fn load_index<const N: usize>(
        path: impl AsRef<Path>,
        table_version: u32,
    ) -> eyre::Result<PersistentCongeeIndex<K, OffsetEqLink<N>>> {
        let image = ArtFile::<K>::read_image(path.as_ref(), Backend::Congee, table_version).await?;
        let topology = decode_congee_topology(&image.snapshot)?;
        let index = CongeeIndex::from_topology(topology, OffsetEqLink)?;
        apply_wal(&index, &image.wal, OffsetEqLink)?;
        Ok(PersistentArtIndex::from_inner(index))
    }

    /// Writes a complete native Congee checkpoint with an empty WAL.
    pub async fn write_checkpoint<const N: usize>(
        path: impl AsRef<Path>,
        table_version: u32,
        index: &mut PersistentCongeeIndex<K, OffsetEqLink<N>>,
    ) -> eyre::Result<()> {
        let topology = index.inner_mut().export_topology(|link| link.0)?;
        let snapshot = encode_congee_topology(&topology)?;
        // Temp-file-plus-rename, mirroring the Arctic checkpoint above.
        ArtFile::<K>::write_file_atomically(path.as_ref(), Backend::Congee, table_version, &snapshot).await
    }

    async fn compact(&mut self) -> eyre::Result<()> {
        let image = ArtFile::<K>::read_image(&self.file.path, Backend::Congee, self.file.table_version).await?;
        let topology = decode_congee_topology(&image.snapshot)?;
        let mut index = CongeeIndex::from_topology(topology, |link| link)?;
        apply_wal(&index, &image.wal, |link| link)?;
        let snapshot = encode_congee_topology(&index.export_topology(|link| *link)?)?;
        self.file.rewrite(&snapshot).await
    }
}

impl<K, const INNER_PAGE_SIZE: u32> SpaceIndexOps<K> for SpaceCongeeIndex<K, INNER_PAGE_SIZE>
where
    K: ArtPersistenceKey + CongeeKey,
{
    async fn primary_from_table_files_path<S: AsRef<str> + Send>(path: S, version: u32) -> eyre::Result<Self> {
        Self::new(
            PathBuf::from(format!("{}/primary{}", path.as_ref(), WT_INDEX_EXTENSION)),
            version,
        )
        .await
    }

    async fn secondary_from_table_files_path<S1: AsRef<str> + Send, S2: AsRef<str> + Send>(
        path: S1,
        name: S2,
        version: u32,
    ) -> eyre::Result<Self> {
        Self::new(
            PathBuf::from(format!("{}/{}{}", path.as_ref(), name.as_ref(), WT_INDEX_EXTENSION)),
            version,
        )
        .await
    }

    async fn bootstrap(_: &mut File, _: String, _: u32) -> eyre::Result<()> {
        Ok(())
    }

    async fn process_change_event(&mut self, event: ChangeEvent<Pair<K, Link>>) -> eyre::Result<()> {
        self.file.append(&[logical_record(event)?]).await?;
        if self.file.should_compact() {
            self.compact().await?;
        }
        Ok(())
    }

    async fn process_change_event_batch(&mut self, events: BatchChangeEvent<K>) -> eyre::Result<()> {
        let records = events
            .into_iter()
            .map(logical_record)
            .collect::<eyre::Result<Vec<_>>>()?;
        self.file.append(&records).await?;
        if self.file.should_compact() {
            self.compact().await?;
        }
        Ok(())
    }
}

/// Disk-side checkpoint and WAL state for a non-unique Arctic index.
#[derive(Debug)]
pub struct SpaceArcticMultiIndex<K: ArtPersistenceKey, const INNER_PAGE_SIZE: u32> {
    file: ArtFile<K>,
}

impl<K, const INNER_PAGE_SIZE: u32> SpaceArcticMultiIndex<K, INNER_PAGE_SIZE>
where
    K: ArtPersistenceKey + ArcticKey,
{
    async fn new(path: PathBuf, table_version: u32) -> eyre::Result<Self> {
        let snapshot = encode_multi_pairs(std::iter::empty::<(K, Link)>());
        Ok(Self {
            file: ArtFile::open(path, Backend::ArcticMulti, table_version, snapshot).await?,
        })
    }

    /// Reconstructs a persisted non-unique Arctic index from its pair-list
    /// checkpoint and WAL.
    pub async fn load_index<const N: usize>(
        path: impl AsRef<Path>,
        table_version: u32,
    ) -> eyre::Result<PersistentArcticMultiIndex<K, OffsetEqLink<N>>> {
        let image = ArtFile::<K>::read_image(path.as_ref(), Backend::ArcticMulti, table_version).await?;
        let index = ArcticMultiIndex::<K, OffsetEqLink<N>>::default();
        for (key, link) in decode_multi_pairs::<K>(&image.snapshot)? {
            index.insert_pair(key, OffsetEqLink(link));
        }
        apply_multi_wal(&index, &image.wal, OffsetEqLink)?;
        Ok(PersistentArtIndex::from_inner(index))
    }

    /// Writes a complete pair-list checkpoint with an empty WAL.
    pub async fn write_checkpoint<const N: usize>(
        path: impl AsRef<Path>,
        table_version: u32,
        index: &mut PersistentArcticMultiIndex<K, OffsetEqLink<N>>,
    ) -> eyre::Result<()> {
        let snapshot = encode_multi_pairs(index.inner().iter().map(|(key, link)| (key, link.0)));
        // Temp-file-plus-rename, mirroring the unique Arctic checkpoint.
        ArtFile::<K>::write_file_atomically(path.as_ref(), Backend::ArcticMulti, table_version, &snapshot).await
    }

    async fn compact(&mut self) -> eyre::Result<()> {
        let image = ArtFile::<K>::read_image(&self.file.path, Backend::ArcticMulti, self.file.table_version).await?;
        let index = ArcticMultiIndex::<K, Link>::default();
        for (key, link) in decode_multi_pairs::<K>(&image.snapshot)? {
            index.insert_pair(key, link);
        }
        apply_multi_wal(&index, &image.wal, |link| link)?;
        let snapshot = encode_multi_pairs(index.iter());
        self.file.rewrite(&snapshot).await
    }
}

impl<K, const INNER_PAGE_SIZE: u32> SpaceIndexOps<K> for SpaceArcticMultiIndex<K, INNER_PAGE_SIZE>
where
    K: ArtPersistenceKey + ArcticKey,
{
    async fn primary_from_table_files_path<S: AsRef<str> + Send>(path: S, version: u32) -> eyre::Result<Self> {
        Self::new(
            PathBuf::from(format!("{}/primary{}", path.as_ref(), WT_INDEX_EXTENSION)),
            version,
        )
        .await
    }

    async fn secondary_from_table_files_path<S1: AsRef<str> + Send, S2: AsRef<str> + Send>(
        path: S1,
        name: S2,
        version: u32,
    ) -> eyre::Result<Self> {
        Self::new(
            PathBuf::from(format!("{}/{}{}", path.as_ref(), name.as_ref(), WT_INDEX_EXTENSION)),
            version,
        )
        .await
    }

    async fn bootstrap(_: &mut File, _: String, _: u32) -> eyre::Result<()> {
        Ok(())
    }

    async fn process_change_event(&mut self, event: ChangeEvent<Pair<K, Link>>) -> eyre::Result<()> {
        self.file.append(&[logical_multi_record(event)?]).await?;
        if self.file.should_compact() {
            self.compact().await?;
        }
        Ok(())
    }

    async fn process_change_event_batch(&mut self, events: BatchChangeEvent<K>) -> eyre::Result<()> {
        let records = events
            .into_iter()
            .map(logical_multi_record)
            .collect::<eyre::Result<Vec<_>>>()?;
        self.file.append(&records).await?;
        if self.file.should_compact() {
            self.compact().await?;
        }
        Ok(())
    }
}

fn encode_link(link: Link, output: &mut Vec<u8>) {
    let page_id: usize = link.page_id.into();
    output.extend_from_slice(&(page_id as u32).to_le_bytes());
    output.extend_from_slice(&link.offset.to_le_bytes());
    output.extend_from_slice(&link.length.to_le_bytes());
}

struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, count: usize) -> eyre::Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(count)
            .ok_or_else(|| eyre!("ART topology length overflow"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| eyre!("truncated ART topology at byte {}", self.position))?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> eyre::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> eyre::Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> eyre::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn link(&mut self) -> eyre::Result<Link> {
        let page_id = u32::from_le_bytes(self.take(4)?.try_into().unwrap());
        let offset = u32::from_le_bytes(self.take(4)?.try_into().unwrap());
        let length = u32::from_le_bytes(self.take(4)?.try_into().unwrap());
        Ok(Link {
            page_id: PageId::from(page_id),
            offset,
            length,
        })
    }

    fn finish(self) -> eyre::Result<()> {
        if self.position != self.bytes.len() {
            bail!(
                "ART topology contains {} trailing bytes",
                self.bytes.len() - self.position
            );
        }
        Ok(())
    }
}

fn encode_arctic_topology(topology: &arctic::topology::Topology<Link>) -> eyre::Result<Vec<u8>> {
    let mut output = Vec::new();
    output.extend_from_slice(&topology.version.to_le_bytes());
    match &topology.root {
        Some(root) => {
            output.push(1);
            encode_arctic_edge(root, &mut output)?;
        }
        None => output.push(0),
    }
    Ok(output)
}

fn encode_arctic_edge(edge: &arctic::topology::Edge<Link>, output: &mut Vec<u8>) -> eyre::Result<()> {
    output.extend_from_slice(&edge.metadata.to_le_bytes());
    match &edge.child {
        arctic::topology::Child::Value(link) => {
            output.push(0);
            encode_link(*link, output);
        }
        arctic::topology::Child::Node(node) => {
            output.push(1);
            encode_arctic_node(node, output)?;
        }
    }
    Ok(())
}

fn encode_arctic_node(node: &arctic::topology::Node<Link>, output: &mut Vec<u8>) -> eyre::Result<()> {
    let kind = match node.kind {
        arctic::topology::NodeKind::Node3 => 3,
        arctic::topology::NodeKind::Node15 => 15,
        arctic::topology::NodeKind::Node47 => 47,
        arctic::topology::NodeKind::Node256 => 255,
    };
    output.push(kind);
    output.extend_from_slice(&node.slot_count.to_le_bytes());
    let branch_count = u16::try_from(node.branches.len()).map_err(|_| eyre!("too many Arctic branches"))?;
    output.extend_from_slice(&branch_count.to_le_bytes());
    for branch in &node.branches {
        output.push(branch.key);
        output.extend_from_slice(&branch.slot.to_le_bytes());
        encode_arctic_edge(&branch.edge, output)?;
    }
    Ok(())
}

fn decode_arctic_topology(bytes: &[u8]) -> eyre::Result<arctic::topology::Topology<Link>> {
    let mut decoder = Decoder::new(bytes);
    let version = decoder.u16()?;
    let root = match decoder.u8()? {
        0 => None,
        1 => Some(decode_arctic_edge(&mut decoder, 0)?),
        tag => bail!("invalid Arctic root tag {tag}"),
    };
    decoder.finish()?;
    Ok(arctic::topology::Topology { version, root })
}

fn decode_arctic_edge(decoder: &mut Decoder<'_>, depth: usize) -> eyre::Result<arctic::topology::Edge<Link>> {
    if depth > 16 {
        bail!("Arctic topology exceeds maximum key depth");
    }
    let metadata = decoder.u64()?;
    let child = match decoder.u8()? {
        0 => arctic::topology::Child::Value(decoder.link()?),
        1 => arctic::topology::Child::Node(decode_arctic_node(decoder, depth + 1)?),
        tag => bail!("invalid Arctic child tag {tag}"),
    };
    Ok(arctic::topology::Edge { metadata, child })
}

fn decode_arctic_node(decoder: &mut Decoder<'_>, depth: usize) -> eyre::Result<arctic::topology::Node<Link>> {
    let (kind, capacity) = match decoder.u8()? {
        3 => (arctic::topology::NodeKind::Node3, 3),
        15 => (arctic::topology::NodeKind::Node15, 15),
        47 => (arctic::topology::NodeKind::Node47, 47),
        255 => (arctic::topology::NodeKind::Node256, 256),
        tag => bail!("invalid Arctic node kind {tag}"),
    };
    let slot_count = decoder.u16()?;
    let count = decoder.u16()? as usize;
    if count > capacity {
        bail!("Arctic node has {count} branches but capacity is {capacity}");
    }
    let mut branches = Vec::with_capacity(count);
    for _ in 0..count {
        branches.push(arctic::topology::Branch {
            key: decoder.u8()?,
            slot: decoder.u16()?,
            edge: decode_arctic_edge(decoder, depth)?,
        });
    }
    Ok(arctic::topology::Node {
        kind,
        slot_count,
        branches,
    })
}

fn encode_congee_topology(topology: &congee::topology::Topology<Link>) -> eyre::Result<Vec<u8>> {
    let mut output = Vec::new();
    output.extend_from_slice(&topology.version.to_le_bytes());
    encode_congee_node(&topology.root, &mut output)?;
    Ok(output)
}

fn encode_congee_node(node: &congee::topology::Node<Link>, output: &mut Vec<u8>) -> eyre::Result<()> {
    let kind = match node.kind {
        congee::topology::NodeKind::N4 => 4,
        congee::topology::NodeKind::N16 => 16,
        congee::topology::NodeKind::N48 => 48,
        congee::topology::NodeKind::N256 => 255,
    };
    output.push(kind);
    let prefix_len = u8::try_from(node.prefix.len()).map_err(|_| eyre!("Congee prefix is too long"))?;
    output.push(prefix_len);
    output.extend_from_slice(&node.prefix);
    let branch_count = u16::try_from(node.branches.len()).map_err(|_| eyre!("too many Congee branches"))?;
    output.extend_from_slice(&branch_count.to_le_bytes());
    let free_count = u16::try_from(node.free_slots.len()).map_err(|_| eyre!("too many Congee free slots"))?;
    output.extend_from_slice(&free_count.to_le_bytes());
    output.extend_from_slice(&node.free_slots);
    for branch in &node.branches {
        output.push(branch.key);
        output.extend_from_slice(&branch.slot.to_le_bytes());
        match &branch.child {
            congee::topology::Child::Value(link) => {
                output.push(0);
                encode_link(*link, output);
            }
            congee::topology::Child::Node(child) => {
                output.push(1);
                encode_congee_node(child, output)?;
            }
        }
    }
    Ok(())
}

fn decode_congee_topology(bytes: &[u8]) -> eyre::Result<congee::topology::Topology<Link>> {
    let mut decoder = Decoder::new(bytes);
    let version = decoder.u16()?;
    let root = decode_congee_node(&mut decoder, 0)?;
    decoder.finish()?;
    Ok(congee::topology::Topology { version, root })
}

fn decode_congee_node(decoder: &mut Decoder<'_>, depth: usize) -> eyre::Result<congee::topology::Node<Link>> {
    if depth > 8 {
        bail!("Congee topology exceeds its eight-byte key depth");
    }
    let (kind, capacity) = match decoder.u8()? {
        4 => (congee::topology::NodeKind::N4, 4),
        16 => (congee::topology::NodeKind::N16, 16),
        48 => (congee::topology::NodeKind::N48, 48),
        255 => (congee::topology::NodeKind::N256, 256),
        tag => bail!("invalid Congee node kind {tag}"),
    };
    let prefix_len = decoder.u8()? as usize;
    if prefix_len > 8 {
        bail!("Congee prefix length {prefix_len} exceeds eight bytes");
    }
    let prefix = decoder.take(prefix_len)?.to_vec();
    let branch_count = decoder.u16()? as usize;
    if branch_count > capacity {
        bail!("Congee node has {branch_count} branches but capacity is {capacity}");
    }
    let free_count = decoder.u16()? as usize;
    if free_count > 48 {
        bail!("Congee node has {free_count} free slots");
    }
    let free_slots = decoder.take(free_count)?.to_vec();
    let mut branches = Vec::with_capacity(branch_count);
    for _ in 0..branch_count {
        let key = decoder.u8()?;
        let slot = decoder.u16()?;
        let child = match decoder.u8()? {
            0 => congee::topology::Child::Value(decoder.link()?),
            1 => congee::topology::Child::Node(decode_congee_node(decoder, depth + 1)?),
            tag => bail!("invalid Congee child tag {tag}"),
        };
        branches.push(congee::topology::Branch { key, slot, child });
    }
    Ok(congee::topology::Node {
        kind,
        prefix,
        branches,
        free_slots,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(value: u32) -> Link {
        Link {
            page_id: value.into(),
            offset: value * 2,
            length: value * 3,
        }
    }

    fn set_event(id: u64, key: u64, value: Link) -> ChangeEvent<Pair<u64, Link>> {
        let pair = Pair { key, value };
        ChangeEvent::InsertAt {
            event_id: id.into(),
            max_value: pair.clone(),
            value: pair,
            index: 0,
        }
    }

    fn congee_contains_kind(node: &congee::topology::Node<Link>, expected: congee::topology::NodeKind) -> bool {
        node.kind == expected
            || node.branches.iter().any(|branch| match &branch.child {
                congee::topology::Child::Value(_) => false,
                congee::topology::Child::Node(child) => congee_contains_kind(child, expected),
            })
    }

    #[test]
    fn wal_round_trip() {
        for record in [
            WalRecord {
                event_id: 17,
                key: 42u64,
                op: WalOp::Set(link(3)),
            },
            WalRecord {
                event_id: 18,
                key: 42u64,
                op: WalOp::Remove,
            },
        ] {
            assert_eq!(decode_wal_record(&encode_wal_record(&record)).unwrap(), record);
        }
    }

    #[test]
    fn arctic_topology_codec_preserves_physical_shape() {
        let mut index = ArcticIndex::<u64, Link>::default();
        for key in 0..80 {
            index.insert_value(key, link(key as u32 + 1));
        }
        let topology = index.export_topology(|link| *link).unwrap();
        let bytes = encode_arctic_topology(&topology).unwrap();
        let decoded = decode_arctic_topology(&bytes).unwrap();
        assert_eq!(decoded, topology);
    }

    #[test]
    fn congee_topology_codec_preserves_physical_shape() {
        let mut index = CongeeIndex::<u64, Link>::default();
        for key in 0..80 {
            index.insert_value(key, link(key as u32 + 1));
        }
        let topology = index.export_topology(|link| *link).unwrap();
        let bytes = encode_congee_topology(&topology).unwrap();
        let decoded = decode_congee_topology(&bytes).unwrap();
        assert_eq!(decoded, topology);
    }

    #[tokio::test]
    async fn torn_wal_tail_is_truncated_before_new_appends() {
        let path = std::env::temp_dir().join(format!("worktable-art-torn-{}.wt.idx", uuid::Uuid::new_v4()));
        let mut space = SpaceArcticIndex::<u64, 4096>::new(path.clone(), 1).await.unwrap();
        space.process_change_event(set_event(0, 7, link(7))).await.unwrap();
        drop(space);

        let durable_len = tokio::fs::metadata(&path).await.unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&path).await.unwrap();
        file.write_all(&WAL_MAGIC[..2]).await.unwrap();
        file.flush().await.unwrap();
        drop(file);
        assert_eq!(tokio::fs::metadata(&path).await.unwrap().len(), durable_len + 2);

        let mut space = SpaceArcticIndex::<u64, 4096>::new(path.clone(), 1).await.unwrap();
        assert_eq!(tokio::fs::metadata(&path).await.unwrap().len(), durable_len);
        space.process_change_event(set_event(1, 8, link(8))).await.unwrap();
        drop(space);

        let index = SpaceArcticIndex::<u64, 4096>::load_index::<4096>(&path, 1)
            .await
            .unwrap();
        assert_eq!(index.get_value(&7).unwrap().0, link(7));
        assert_eq!(index.get_value(&8).unwrap().0, link(8));
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn compaction_replaces_wal_with_native_checkpoint() {
        let path = std::env::temp_dir().join(format!("worktable-art-compact-{}.wt.idx", uuid::Uuid::new_v4()));
        let mut space = SpaceCongeeIndex::<u64, 4096>::new(path.clone(), 3).await.unwrap();
        let events = (0..128).map(|key| set_event(key, key, link(key as u32 + 1))).collect();
        space.process_change_event_batch(events).await.unwrap();
        assert!(
            !ArtFile::<u64>::read_image(&path, Backend::Congee, 3)
                .await
                .unwrap()
                .wal
                .is_empty()
        );
        space.compact().await.unwrap();
        let image = ArtFile::<u64>::read_image(&path, Backend::Congee, 3).await.unwrap();
        assert!(image.wal.is_empty());
        let topology = decode_congee_topology(&image.snapshot).unwrap();
        assert!(congee_contains_kind(&topology.root, congee::topology::NodeKind::N256));
        drop(space);

        let index = SpaceCongeeIndex::<u64, 4096>::load_index::<4096>(&path, 3)
            .await
            .unwrap();
        assert_eq!(index.len(), 128);
        assert_eq!(index.get_value(&91).unwrap().0, link(92));
        tokio::fs::remove_file(path).await.unwrap();
    }

    fn remove_event(id: u64, key: u64, value: Link) -> ChangeEvent<Pair<u64, Link>> {
        let pair = Pair { key, value };
        ChangeEvent::RemoveAt {
            event_id: id.into(),
            max_value: pair.clone(),
            value: pair,
            index: 0,
        }
    }

    #[test]
    fn multi_wal_records_round_trip() {
        for record in [
            WalRecord {
                event_id: 21,
                key: 42u64,
                op: WalOp::Set(link(3)),
            },
            WalRecord {
                event_id: 22,
                key: 42u64,
                op: WalOp::RemovePair(link(3)),
            },
        ] {
            assert_eq!(decode_wal_record(&encode_wal_record(&record)).unwrap(), record);
        }
    }

    #[test]
    fn multi_pair_snapshot_round_trips() {
        let pairs = (0..100u64).map(|n| (n / 4, link(n as u32 + 1))).collect::<Vec<_>>();
        let bytes = encode_multi_pairs(pairs.iter().cloned());
        assert_eq!(decode_multi_pairs::<u64>(&bytes).unwrap(), pairs);
        assert!(decode_multi_pairs::<u64>(&bytes[..bytes.len() - 1]).is_err());
        assert_eq!(
            decode_multi_pairs::<u64>(&encode_multi_pairs(std::iter::empty::<(u64, Link)>())).unwrap(),
            vec![]
        );
    }

    #[test]
    fn unique_replay_rejects_pair_records_and_vice_versa() {
        let unique = ArcticIndex::<u64, Link>::default();
        let pair_record = WalRecord {
            event_id: 0,
            key: 5u64,
            op: WalOp::RemovePair(link(1)),
        };
        assert!(apply_wal(&unique, &[pair_record], |link| link).is_err());

        let multi = ArcticMultiIndex::<u64, Link>::default();
        let whole_key_record = WalRecord {
            event_id: 0,
            key: 5u64,
            op: WalOp::Remove,
        };
        assert!(apply_multi_wal(&multi, &[whole_key_record], |link| link).is_err());
    }

    #[tokio::test]
    async fn multi_space_replays_wal_and_compacts() {
        let path = std::env::temp_dir().join(format!("worktable-art-multi-{}.wt.idx", uuid::Uuid::new_v4()));
        let mut space = SpaceArcticMultiIndex::<u64, 4096>::new(path.clone(), 5).await.unwrap();
        // Three links under key 7, one later removed; one link under key 9.
        let events = vec![
            set_event(0, 7, link(1)),
            set_event(1, 7, link(2)),
            set_event(2, 7, link(3)),
            set_event(3, 9, link(4)),
            remove_event(4, 7, link(2)),
        ];
        space.process_change_event_batch(events).await.unwrap();

        let index = SpaceArcticMultiIndex::<u64, 4096>::load_index::<4096>(&path, 5)
            .await
            .unwrap();
        let links = index.get(&7).map(|(_, link)| link.0).collect::<Vec<_>>();
        assert_eq!(links, vec![link(1), link(3)]);
        assert_eq!(index.get(&9).map(|(_, link)| link.0).collect::<Vec<_>>(), vec![link(4)]);

        space.compact().await.unwrap();
        let image = ArtFile::<u64>::read_image(&path, Backend::ArcticMulti, 5)
            .await
            .unwrap();
        assert!(image.wal.is_empty());
        drop(space);

        let index = SpaceArcticMultiIndex::<u64, 4096>::load_index::<4096>(&path, 5)
            .await
            .unwrap();
        assert_eq!(index.len(), 3);
        let links = index.get(&7).map(|(_, link)| link.0).collect::<Vec<_>>();
        assert_eq!(links, vec![link(1), link(3)]);

        // A unique reader must refuse the multi file outright.
        assert!(ArtFile::<u64>::read_image(&path, Backend::Arctic, 5).await.is_err());
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn multi_checkpoint_round_trips_through_write_and_load() {
        let path = std::env::temp_dir().join(format!("worktable-art-multi-ckpt-{}.wt.idx", uuid::Uuid::new_v4()));
        let mut index = SpaceArcticMultiIndex::<u128, 4096>::new(path.clone(), 1)
            .await
            .map(|_| PersistentArcticMultiIndex::<u128, OffsetEqLink<4096>>::default())
            .unwrap();
        for n in 0..50u32 {
            index.insert_pair(u128::MAX - (n as u128 % 5), OffsetEqLink(link(n + 1)));
        }
        SpaceArcticMultiIndex::<u128, 4096>::write_checkpoint::<4096>(&path, 1, &mut index)
            .await
            .unwrap();

        let reloaded = SpaceArcticMultiIndex::<u128, 4096>::load_index::<4096>(&path, 1)
            .await
            .unwrap();
        assert_eq!(reloaded.len(), 50);
        assert_eq!(reloaded.get(&(u128::MAX - 3)).len(), 10);
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[test]
    fn temporary_path_appends_to_the_full_file_name() {
        assert_eq!(
            temporary_path(Path::new("/tables/user/primary.wt.idx")),
            PathBuf::from("/tables/user/primary.wt.idx.tmp")
        );
    }

    #[tokio::test]
    async fn checkpoints_go_through_a_temporary_file_and_stale_temporaries_are_removed() {
        let path = std::env::temp_dir().join(format!("worktable-art-atomic-{}.wt.idx", uuid::Uuid::new_v4()));
        let temporary = temporary_path(&path);

        // A leftover temporary from a crashed checkpoint must be cleaned up
        // when the index opens.
        tokio::fs::write(&temporary, b"crashed checkpoint leftovers")
            .await
            .unwrap();
        let mut space = SpaceArcticIndex::<u64, 4096>::new(path.clone(), 1).await.unwrap();
        assert!(!temporary.exists(), "stale temporary should be removed on open");

        space.process_change_event(set_event(0, 7, link(7))).await.unwrap();
        space.compact().await.unwrap();
        assert!(!temporary.exists(), "compaction must not leave its temporary behind");
        drop(space);

        // `write_checkpoint` used to truncate the live file in place; it now
        // stages the checkpoint in the sibling temporary and renames it over,
        // so the final file is complete and no temporary survives.
        let mut index = SpaceArcticIndex::<u64, 4096>::load_index::<4096>(&path, 1)
            .await
            .unwrap();
        index.insert_value(9, crate::util::OffsetEqLink(link(9)));
        SpaceArcticIndex::<u64, 4096>::write_checkpoint::<4096>(&path, 1, &mut index)
            .await
            .unwrap();
        assert!(!temporary.exists(), "checkpoint must not leave its temporary behind");

        let reloaded = SpaceArcticIndex::<u64, 4096>::load_index::<4096>(&path, 1)
            .await
            .unwrap();
        assert_eq!(reloaded.get_value(&7).unwrap().0, link(7));
        assert_eq!(reloaded.get_value(&9).unwrap().0, link(9));
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn complete_corruption_and_header_mismatches_are_rejected() {
        let path = std::env::temp_dir().join(format!("worktable-art-corrupt-{}.wt.idx", uuid::Uuid::new_v4()));
        let mut space = SpaceArcticIndex::<u64, 4096>::new(path.clone(), 7).await.unwrap();
        space.process_change_event(set_event(0, 9, link(9))).await.unwrap();
        drop(space);

        assert!(ArtFile::<u64>::read_image(&path, Backend::Arctic, 8).await.is_err());
        assert!(ArtFile::<u64>::read_image(&path, Backend::Congee, 7).await.is_err());
        assert!(ArtFile::<u32>::read_image(&path, Backend::Arctic, 7).await.is_err());

        let mut file = OpenOptions::new().read(true).write(true).open(&path).await.unwrap();
        file.seek(std::io::SeekFrom::End(-1)).await.unwrap();
        let mut last = [0u8; 1];
        file.read_exact(&mut last).await.unwrap();
        file.seek(std::io::SeekFrom::End(-1)).await.unwrap();
        file.write_all(&[last[0] ^ 0x80]).await.unwrap();
        file.flush().await.unwrap();
        drop(file);

        assert!(ArtFile::<u64>::read_image(&path, Backend::Arctic, 7).await.is_err());
        tokio::fs::remove_file(path).await.unwrap();
    }
}
