use data_bucket::page::PageId;
use derive_more::{Display, Error, From};
use parking_lot::Mutex;
use parking_lot::RwLock;
#[cfg(feature = "perf_measurements")]
use performance_measurement_codegen::performance_measurement;
use rkyv::{
    Archive, Deserialize, Portable, Serialize,
    api::high::HighDeserializer,
    rancor::Strategy,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{BuildHasherDefault, Hasher};
use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;
use std::{
    fmt::Debug,
    sync::Arc,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
};

use crate::in_memory::empty_link_registry::EmptyLinkRegistry;
use crate::in_memory::publication::{DELETED, GHOSTED, PublishedRow, VACUUMED};
use crate::prelude::ArchivedRowWrapper;
use crate::util::OffsetEqLink;
use crate::{
    in_memory::{
        DATA_INNER_LENGTH, Data, DataExecutionError,
        row::{RowWrapper, StorableRow},
    },
    prelude::Link,
};

fn page_id_mapper(page_id: usize) -> usize {
    page_id - 1usize
}

const PUBLICATION_SHARD_COUNT: usize = 64;
const RETIREMENT_BACKLOG_WARN_AT: usize = 1_024;

fn mix_publication_offset(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// `OffsetEqLink` already reduces publication keys to a trusted internal u64
/// storage offset. Avalanche that offset so both hash-table bucket bits and
/// SIMD control bits remain distributed for aligned, monotonically allocated
/// row positions.
struct PublicationHasher(u64);

impl Default for PublicationHasher {
    fn default() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
}

impl Hasher for PublicationHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = self.0;
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        self.0 = mix_publication_offset(hash);
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = mix_publication_offset(self.0 ^ value);
    }
}

type PublicationMap<Row, const DATA_LENGTH: usize> =
    HashMap<OffsetEqLink<DATA_LENGTH>, Arc<PublishedRow<Row>>, BuildHasherDefault<PublicationHasher>>;

type PublicationShards<Row, const DATA_LENGTH: usize> =
    [RwLock<PublicationMap<Row, DATA_LENGTH>>; PUBLICATION_SHARD_COUNT];

fn publication_shard<const DATA_LENGTH: usize>(key: &OffsetEqLink<DATA_LENGTH>) -> usize {
    mix_publication_offset(key.absolute_index()) as usize & (PUBLICATION_SHARD_COUNT - 1)
}

fn queue_retirement<T>(queue: &Mutex<Vec<T>>, pending_retirements: &AtomicUsize, queue_name: &'static str, value: T) {
    let mut queue = queue.lock();
    queue.push(value);
    let len = queue.len();
    pending_retirements.fetch_add(1, Ordering::Release);
    if len >= RETIREMENT_BACKLOG_WARN_AT && len.is_power_of_two() {
        tracing::warn!(
            queue = queue_name,
            len,
            "versioned publication retirement backlog is growing"
        );
    }
}

pub struct ReadGuard<'a> {
    active_readers: &'a AtomicU64,
    marker: PhantomData<&'a ()>,
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        self.active_readers.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Page storage with immutable row publication.
///
/// # Versioned-publication synchronization
///
/// Generated readers enter the grace period before resolving an index link.
/// Writers must remove or replace every index reference before queueing the old
/// link for retirement. That unlink-before-retire invariant is what makes a
/// reader entering after the reclaimer observes zero unable to acquire the old
/// link.
///
/// Locks are acquired in this order when more than one is needed:
/// `page_access` -> `pages` -> one `published_rows` shard. Reclamation holds the
/// retirement queues, then briefly acquires individual publication shards and
/// the empty-link/page registries. Callers must not invoke reclamation while
/// retaining a retirement-queue guard.
#[derive(Debug)]
pub struct DataPages<Row, const DATA_LENGTH: usize = DATA_INNER_LENGTH>
where
    Row: StorableRow,
{
    /// Immutable application-visible row versions. Published readers never
    /// borrow the mutable archived page image.
    published_rows: PublicationShards<Row, DATA_LENGTH>,

    /// Protects the mutable page image used by writers, vacuum, and
    /// persistence. Application reads use `published_rows` after hydration.
    page_access: RwLock<()>,

    /// Read-side grace period protecting the interval from index lookup until
    /// an immutable row version has been acquired.
    active_readers: AtomicU64,

    retired_links: Mutex<Vec<Link>>,

    retired_pages: Mutex<Vec<PageId>>,

    retired_publications: Mutex<Vec<OffsetEqLink<DATA_LENGTH>>>,

    /// Avoids taking all retirement-queue mutexes on mutations when there is
    /// no reclamation work pending.
    pending_retirements: AtomicUsize,

    /// Pages vector. Currently, not lock free.
    pages: RwLock<Vec<Arc<Data<<Row as StorableRow>::WrappedRow, DATA_LENGTH>>>>,

    empty_links: EmptyLinkRegistry<DATA_LENGTH>,

    empty_pages: Arc<RwLock<VecDeque<PageId>>>,

    /// Count of saved rows.
    row_count: AtomicU64,

    last_page_id: AtomicU32,

    current_page_id: AtomicU32,
}

impl<Row, const DATA_LENGTH: usize> Default for DataPages<Row, DATA_LENGTH>
where
    Row: StorableRow,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Row, const DATA_LENGTH: usize> DataPages<Row, DATA_LENGTH>
where
    Row: StorableRow,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
{
    fn publication_flags(row: &<Row as StorableRow>::WrappedRow) -> u8 {
        let mut flags = 0;
        if row.is_ghosted() {
            flags |= GHOSTED;
        }
        if row.is_deleted() {
            flags |= DELETED;
        }
        if row.is_vacuumed() {
            flags |= VACUUMED;
        }
        flags
    }

    fn publish_wrapped_row(&self, link: Link, wrapped: <Row as StorableRow>::WrappedRow) {
        let flags = Self::publication_flags(&wrapped);
        let row = wrapped.get_inner();
        let key = OffsetEqLink(link);

        let mut published_rows = self.published_rows[publication_shard(&key)].write();
        if let Some(slot) = published_rows.get(&key).cloned() {
            drop(published_rows);
            slot.replace(row, flags);
        } else {
            published_rows.insert(key, Arc::new(PublishedRow::new(row, flags)));
        }
    }

    fn stage_published_row(&self, link: Link, row: Row) {
        let wrapped = <Row as StorableRow>::WrappedRow::from_inner(row);
        self.publish_wrapped_row(link, wrapped);
    }

    fn published_slot(&self, link: Link) -> Option<Arc<PublishedRow<Row>>> {
        let key = OffsetEqLink(link);
        self.published_rows[publication_shard(&key)].read().get(&key).cloned()
    }

    fn published_slot_or_hydrate(&self, link: Link) -> Result<Arc<PublishedRow<Row>>, ExecutionError>
    where
        <<Row as StorableRow>::WrappedRow as Archive>::Archived:
            Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        if let Some(slot) = self.published_slot(link) {
            return Ok(slot);
        }

        let _page_access = self.page_access.read();
        if let Some(slot) = self.published_slot(link) {
            return Ok(slot);
        }

        let pages = self.pages.read();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let wrapped = page.get_row(link).map_err(ExecutionError::DataPageError)?;
        let flags = Self::publication_flags(&wrapped);
        let slot = Arc::new(PublishedRow::new(wrapped.get_inner(), flags));
        let key = OffsetEqLink(link);
        let mut published_rows = self.published_rows[publication_shard(&key)].write();
        Ok(published_rows.entry(key).or_insert(slot).clone())
    }

    pub fn read_guard(&self) -> ReadGuard<'_> {
        self.active_readers.fetch_add(1, Ordering::SeqCst);

        ReadGuard {
            active_readers: &self.active_readers,
            marker: PhantomData,
        }
    }

    fn reclaim_retired(&self) {
        if self.pending_retirements.load(Ordering::Acquire) == 0 {
            return;
        }
        if self.active_readers.load(Ordering::SeqCst) != 0 {
            return;
        }

        let mut retired_links = self.retired_links.lock();
        let mut retired_pages = self.retired_pages.lock();
        let mut retired_publications = self.retired_publications.lock();
        if self.active_readers.load(Ordering::SeqCst) != 0 {
            return;
        }

        // A whole-page retirement subsumes every free link within that page.
        // Publishing both would let one allocator reset/reuse the page while
        // another writes through an overlapping link from the same page.
        let whole_pages: HashSet<_> = retired_pages.iter().copied().collect();
        for link in retired_links.drain(..) {
            let key = OffsetEqLink(link);
            self.published_rows[publication_shard(&key)].write().remove(&key);
            if !whole_pages.contains(&link.page_id) {
                self.empty_links.push(link);
            }
        }
        for key in retired_publications.drain(..) {
            self.published_rows[publication_shard(&key)].write().remove(&key);
        }
        if !retired_pages.is_empty() {
            let mut empty_pages = self.empty_pages.write();
            empty_pages.extend(retired_pages.drain(..));
        }
        self.pending_retirements.store(0, Ordering::Release);
    }

    pub fn new() -> Self {
        Self {
            published_rows: std::array::from_fn(|_| RwLock::new(PublicationMap::default())),
            page_access: RwLock::new(()),
            active_readers: AtomicU64::new(0),
            retired_links: Mutex::new(Vec::new()),
            retired_pages: Mutex::new(Vec::new()),
            retired_publications: Mutex::new(Vec::new()),
            pending_retirements: AtomicUsize::new(0),
            // We are starting ID's from `1` because `0`'s page in file is info page.
            pages: RwLock::new(vec![Arc::new(Data::new(1.into()))]),
            empty_links: EmptyLinkRegistry::<DATA_LENGTH>::default(),
            empty_pages: Default::default(),
            row_count: AtomicU64::new(0),
            last_page_id: AtomicU32::new(1),
            current_page_id: AtomicU32::new(1),
        }
    }

    pub fn from_data(vec: Vec<Arc<Data<<Row as StorableRow>::WrappedRow, DATA_LENGTH>>>) -> Self {
        // TODO: Add row_count persistence.
        if vec.is_empty() {
            Self::new()
        } else {
            let last_page_id = vec.len();
            Self {
                published_rows: std::array::from_fn(|_| RwLock::new(PublicationMap::default())),
                page_access: RwLock::new(()),
                active_readers: AtomicU64::new(0),
                retired_links: Mutex::new(Vec::new()),
                retired_pages: Mutex::new(Vec::new()),
                retired_publications: Mutex::new(Vec::new()),
                pending_retirements: AtomicUsize::new(0),
                pages: RwLock::new(vec),
                empty_links: EmptyLinkRegistry::default(),
                empty_pages: Default::default(),
                row_count: AtomicU64::new(0),
                last_page_id: AtomicU32::new(last_page_id as u32),
                current_page_id: AtomicU32::new(last_page_id as u32),
            }
        }
    }

    pub fn insert(&self, row: Row) -> Result<Link, ExecutionError>
    where
        Row: Archive
            + Clone
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <Row as StorableRow>::WrappedRow:
            Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
    {
        let general_row = <Row as StorableRow>::WrappedRow::from_inner(row.clone());

        self.reclaim_retired();

        if let Some((link, vacuum_guard)) = self.empty_links.pop_max() {
            // `vacuum_guard` keeps vacuum from reclaiming the link's page
            // until the write through the link below has completed. Hold it
            // for the whole block.
            let _vacuum_guard = vacuum_guard;
            let _page_access = self.page_access.write();
            let pages = self.pages.read();
            let current_page: usize = page_id_mapper(link.page_id.into());
            let page = &pages[current_page];

            match unsafe { page.try_save_row_by_link(&general_row, link) } {
                Ok((link, left_link)) => {
                    if let Some(l) = left_link {
                        self.empty_links.push(l);
                    }
                    self.stage_published_row(link, row);
                    return Ok(link);
                }
                Err(e) => match e {
                    DataExecutionError::InvalidLink => {
                        self.empty_links.push(link);
                    }
                    DataExecutionError::PageIsFull { .. }
                    | DataExecutionError::PageTooSmall { .. }
                    | DataExecutionError::SerializeError
                    | DataExecutionError::DeserializeError => return Err(e.into()),
                },
            }
        }

        loop {
            let (link, tried_page) = {
                let _page_access = self.page_access.write();
                let pages = self.pages.read();
                let current_page = page_id_mapper(self.current_page_id.load(Ordering::Acquire) as usize);
                let page = &pages[current_page];

                (page.save_row(&general_row), current_page)
            };
            match link {
                Ok(link) => {
                    self.stage_published_row(link, row);
                    self.row_count.fetch_add(1, Ordering::Relaxed);
                    return Ok(link);
                }
                Err(e) => match e {
                    DataExecutionError::PageIsFull { .. } => {
                        if tried_page == page_id_mapper(self.current_page_id.load(Ordering::Relaxed) as usize) {
                            let empty_page = self.empty_pages.write().pop_front();
                            if let Some(page_id) = empty_page {
                                // Retired pages retain their old bytes until
                                // the read-side grace period completes. Reset
                                // only after reclamation made the page
                                // available for reuse.
                                let _page_access = self.page_access.write();
                                let pages = self.pages.read();
                                pages[page_id_mapper(page_id.into())].reset();
                                self.current_page_id.store(page_id.into(), Ordering::Release);
                            } else {
                                self.add_next_page(tried_page);
                            }
                        }
                    }
                    DataExecutionError::PageTooSmall { .. }
                    | DataExecutionError::SerializeError
                    | DataExecutionError::DeserializeError
                    | DataExecutionError::InvalidLink => return Err(e.into()),
                },
            };
        }
    }

    pub fn insert_cdc(&self, row: Row) -> Result<(Link, Vec<u8>), ExecutionError>
    where
        Row: Archive
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>
            + Clone,
        <Row as StorableRow>::WrappedRow:
            Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
    {
        let link = self.insert(row.clone())?;
        let general_row = <Row as StorableRow>::WrappedRow::from_inner(row);
        let bytes = rkyv::to_bytes(&general_row)
            .expect("should be ok as insert not failed")
            .into_vec();
        Ok((link, bytes))
    }

    fn add_next_page(&self, tried_page: usize) {
        let mut pages = self.pages.write();
        if tried_page == page_id_mapper(self.current_page_id.load(Ordering::Acquire) as usize) {
            let index = self.last_page_id.fetch_add(1, Ordering::AcqRel) + 1;

            pages.push(Arc::new(Data::new(index.into())));
            self.current_page_id.store(index, Ordering::Release);
        }
    }

    /// Allocates a new page or reuses a free page from `empty_pages`.
    /// Does **NOT** set the page as `current`.
    pub fn allocate_new_or_pop_free(&self) -> Arc<Data<<Row as StorableRow>::WrappedRow, DATA_LENGTH>> {
        self.reclaim_retired();

        let page_id = {
            let mut empty_pages = self.empty_pages.write();
            empty_pages.pop_front()
        };

        if let Some(page_id) = page_id {
            let _page_access = self.page_access.write();
            let pages = self.pages.read();
            let index = page_id_mapper(page_id.into());
            let page = pages[index].clone();
            page.reset();

            return page;
        }

        let mut pages = self.pages.write();
        let index = self.last_page_id.fetch_add(1, Ordering::AcqRel) + 1;
        let page = Arc::new(Data::new(index.into()));
        pages.push(page.clone());

        page
    }

    #[cfg_attr(feature = "perf_measurements", performance_measurement(prefix_name = "DataPages"))]
    pub fn select<L: Into<Link>>(&self, link: L) -> Result<Row, ExecutionError>
    where
        Row: Archive
            + Clone
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived:
            Portable + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        let link = link.into();
        let slot = self.published_slot_or_hydrate(link)?;
        Ok(slot.snapshot().as_ref().clone())
    }

    pub fn select_non_ghosted(&self, link: Link) -> Result<Row, ExecutionError>
    where
        Row: Archive
            + Clone
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived:
            Portable + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        let slot = self.published_slot_or_hydrate(link)?;
        let (row, flags) = slot.load();
        if flags & GHOSTED != 0 {
            return Err(ExecutionError::Ghosted);
        }
        if flags & DELETED != 0 {
            return Err(ExecutionError::Deleted);
        }
        Ok(row.as_ref().clone())
    }

    /// Loads one persisted row through rkyv validation without publishing it.
    ///
    /// This is used by the table load audit before the persistence worker is
    /// started. It is intentionally separate from the steady-state read path.
    pub fn select_non_ghosted_checked(&self, link: Link) -> Result<Row, ExecutionError>
    where
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>
            + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    {
        let pages = self.pages.read();
        let page_id: usize = link.page_id.into();
        let page_index = page_id
            .checked_sub(1)
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let page = pages
            .get(page_index)
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let wrapped = page.get_row_checked(link).map_err(ExecutionError::DataPageError)?;
        if wrapped.is_ghosted() {
            return Err(ExecutionError::Ghosted);
        }
        if wrapped.is_deleted() {
            return Err(ExecutionError::Deleted);
        }
        Ok(wrapped.get_inner())
    }

    pub fn select_non_vacuumed(&self, link: Link) -> Result<Row, ExecutionError>
    where
        Row: Archive
            + Clone
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived:
            Portable + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        let slot = self.published_slot_or_hydrate(link)?;
        let (row, flags) = slot.load();
        if flags & GHOSTED != 0 {
            return Err(ExecutionError::Ghosted);
        }
        if flags & VACUUMED != 0 {
            return Err(ExecutionError::Vacuumed);
        }
        if flags & DELETED != 0 {
            return Err(ExecutionError::Deleted);
        }
        Ok(row.as_ref().clone())
    }

    #[cfg_attr(feature = "perf_measurements", performance_measurement(prefix_name = "DataPages"))]
    pub fn with_ref<Op, Res>(&self, link: Link, op: Op) -> Result<Res, ExecutionError>
    where
        Row: Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        Op: Fn(&<<Row as StorableRow>::WrappedRow as Archive>::Archived) -> Res,
    {
        let _page_access = self.page_access.read();
        let pages = self.pages.read();
        let page = pages
            .get::<usize>(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let gen_row = page.get_row_ref(link).map_err(ExecutionError::DataPageError)?;
        let res = op(gen_row);
        Ok(res)
    }

    #[allow(clippy::missing_safety_doc)]
    #[cfg_attr(feature = "perf_measurements", performance_measurement(prefix_name = "DataPages"))]
    pub unsafe fn with_mut_ref<Op, Res>(&self, link: Link, mut op: Op) -> Result<Res, ExecutionError>
    where
        Row: Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: Portable,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived:
            Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
        Op: FnMut(&mut <<Row as StorableRow>::WrappedRow as Archive>::Archived) -> Res,
    {
        let _page_access = self.page_access.write();
        let pages = self.pages.read();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let res = {
            let gen_row = unsafe {
                page.get_mut_row_ref(link)
                    .map_err(ExecutionError::DataPageError)?
                    .unseal_unchecked()
            };
            op(gen_row)
        };

        {
            let wrapped = page.get_row(link).map_err(ExecutionError::DataPageError)?;
            self.publish_wrapped_row(link, wrapped);
        }

        Ok(res)
    }

    /// # Safety
    /// This function is `unsafe` because it modifies archived memory directly.
    /// The caller must ensure that:
    /// - The `link` is valid and points to a properly initialized row.
    /// - No other references to the same row exist during modification.
    /// - The operation does not cause data races or memory corruption.
    pub unsafe fn update<const N: usize>(&self, row: Row, link: Link) -> Result<Link, ExecutionError>
    where
        Row: Archive + Clone,
        <Row as StorableRow>::WrappedRow:
            Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
    {
        let _page_access = self.page_access.write();
        let pages = self.pages.read();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let gen_row = <Row as StorableRow>::WrappedRow::from_inner(row.clone());
        let result = unsafe {
            page.save_row_by_link(&gen_row, link)
                .map_err(ExecutionError::DataPageError)
        }?;
        self.stage_published_row(link, row);
        Ok(result)
    }

    /// In-place update of an already-live row at `link`: re-serialize the full
    /// row into the SAME slot and republish it as LIVE (unghosted). Unlike
    /// [`Self::update`], this does not stage the row as a new (ghosted)
    /// publication — a live row that is edited must stay visible to readers.
    /// The caller must guarantee the new row serializes to the same length as
    /// the current slot (so it fits exactly).
    ///
    /// # Persistence
    /// This path emits **no** persistence CDC. It is only sound for tables that
    /// are not persisted (or on a persistence sink that reconstructs state from
    /// the page image on reload). Do NOT route a persisted-table update through
    /// this method: the row would change in memory and republish but no change
    /// event would reach disk, silently losing durability until reload. The
    /// generated persisted update path deliberately keeps the reinsert path for
    /// this reason.
    ///
    /// Serialization and the exact-length check finish before any page byte is
    /// changed. `page_access` excludes low-level archived-page readers during
    /// the copy, while generated reads continue from the old immutable
    /// publication until [`Self::publish_wrapped_row`] replaces the complete
    /// owned row and flags together.
    ///
    /// # Safety
    /// Same contract as [`Self::update`]: `link` must be valid and no other
    /// mutable references to the row may exist during modification.
    pub unsafe fn update_in_place<const N: usize>(&self, row: Row, link: Link) -> Result<(), ExecutionError>
    where
        Row: Archive + Clone,
        <Row as StorableRow>::WrappedRow:
            Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: Portable + ArchivedRowWrapper,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived:
            Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        let _page_access = self.page_access.write();
        let pages = self.pages.read();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        // Write the new bytes into the slot. `save_row_by_link` requires the
        // serialized wrapped row to be EXACTLY the slot length; the caller only
        // guaranteed equal *field* sizes, which need not imply equal total
        // serialized length (alignment/padding). If it does not fit, report it
        // so the caller can fall back to a reinsert instead of corrupting.
        // `row` is consumed by the wrapper here (no clone): it is not used again.
        let gen_row = <Row as StorableRow>::WrappedRow::from_inner(row);
        unsafe {
            page.save_row_by_link(&gen_row, link)
                .map_err(ExecutionError::DataPageError)?;
        }
        // Clear the ghost bit on the stored row and republish the LIVE image
        // from the page, exactly like `with_mut_ref` — so the publication cache
        // is not left ghosted (a fresh `from_inner` wrapper is ghosted).
        unsafe {
            page.get_mut_row_ref(link)
                .map_err(ExecutionError::DataPageError)?
                .unseal_unchecked()
                .unghost();
        }
        let wrapped = page.get_row(link).map_err(ExecutionError::DataPageError)?;
        self.publish_wrapped_row(link, wrapped);
        Ok(())
    }

    pub fn delete(&self, link: Link) -> Result<(), ExecutionError>
    where
        Row: Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <Row as StorableRow>::WrappedRow:
            Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: ArchivedRowWrapper
            + Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        unsafe { self.with_mut_ref(link, |r| r.delete())? }

        queue_retirement(&self.retired_links, &self.pending_retirements, "links", link);
        self.reclaim_retired();
        Ok(())
    }

    pub fn select_raw(&self, link: Link) -> Result<Vec<u8>, ExecutionError> {
        let _page_access = self.page_access.read();
        let pages = self.pages.read();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        page.get_raw_row(link).map_err(ExecutionError::DataPageError)
    }

    pub fn mark_page_empty(&self, page_id: PageId) {
        if u32::from(page_id) != self.current_page_id.load(Ordering::Acquire) {
            queue_retirement(&self.retired_pages, &self.pending_retirements, "pages", page_id);
            self.reclaim_retired();
        }
    }

    /// Marks [`Page`] as full if it's not current [`Page`], which means put
    /// [`Link`] from it's current offset to the end of the page in
    /// [`EmptyLinkRegistry`] and set `free_offset` to max value.
    ///
    /// [`Page`]: Data
    pub fn mark_page_full(&self, page_id: PageId) {
        if u32::from(page_id) == self.current_page_id.load(Ordering::Acquire) {
            return;
        }

        let pages = self.pages.read();
        let index = page_id_mapper(page_id.into());

        if let Some(page) = pages.get(index) {
            let free_offset = page.free_offset.load(Ordering::Acquire);
            let remaining = DATA_LENGTH.saturating_sub(free_offset as usize);

            if remaining > 0 {
                let link = Link {
                    page_id,
                    offset: free_offset,
                    length: remaining as u32,
                };
                self.empty_links.push(link);
            }

            page.free_offset.store(DATA_LENGTH as u32, Ordering::Release);
        }
    }

    pub fn get_empty_pages(&self) -> Vec<PageId> {
        let g = self.empty_pages.read();
        g.iter().copied().collect()
    }

    pub fn get_page(&self, page_id: PageId) -> Option<Arc<Data<<Row as StorableRow>::WrappedRow, DATA_LENGTH>>> {
        let pages = self.pages.read();
        let page = pages.get(page_id_mapper(page_id.into()))?;
        Some(page.clone())
    }

    /// Bytes actually occupied across every page.
    ///
    /// The sum of each page's `free_offset`, which is what `get_bytes` was
    /// being used to compute. That copied every page image onto the heap by
    /// value in order to read one `u32` alongside each, so a periodic metrics
    /// poll memcpy'd the whole table and streamed it through cache. This reads
    /// the counters and nothing else.
    pub fn used_bytes(&self) -> u64 {
        let _page_access = self.page_access.read();
        let pages = self.pages.read();
        pages
            .iter()
            .map(|p| u64::from(p.free_offset.load(Ordering::Relaxed)))
            .sum()
    }

    pub fn get_bytes(&self) -> Vec<([u8; DATA_LENGTH], u32)> {
        let _page_access = self.page_access.read();
        let pages = self.pages.read();
        pages
            .iter()
            .map(|p| (p.get_bytes(), p.free_offset.load(Ordering::Relaxed)))
            .collect()
    }

    /// Copies a row to another page without exposing either mutable byte
    /// image to application readers.
    ///
    /// # Safety
    ///
    /// The caller must hold the row's exclusive logical lock, ensure
    /// `from_link` still identifies the indexed source row, and verify that
    /// `to_page_id` has enough capacity for the complete serialized row. No
    /// concurrent low-level mutation may access either physical row while the
    /// move is in progress. After success, the caller must swing every index
    /// reference to the returned link before retiring `from_link`.
    pub(crate) unsafe fn move_row_for_vacuum(
        &self,
        from_link: Link,
        to_page_id: PageId,
    ) -> Result<(Vec<u8>, Link), ExecutionError>
    where
        Row: Clone,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: ArchivedRowWrapper
            + Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        let _page_access = self.page_access.write();
        let pages = self.pages.read();
        let from_page = pages
            .get(page_id_mapper(from_link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(from_link.page_id))?;
        let to_page = pages
            .get(page_id_mapper(to_page_id.into()))
            .ok_or(ExecutionError::PageNotFound(to_page_id))?;

        let raw_data = from_page
            .get_raw_row(from_link)
            .map_err(ExecutionError::DataPageError)?;
        let archived = unsafe {
            from_page
                .get_mut_row_ref(from_link)
                .map_err(ExecutionError::DataPageError)?
                .unseal_unchecked()
        };
        archived.set_in_vacuum_process();
        let new_link = to_page.save_raw_row(&raw_data).map_err(ExecutionError::DataPageError)?;

        {
            let old_wrapped = from_page.get_row(from_link).map_err(ExecutionError::DataPageError)?;
            self.publish_wrapped_row(from_link, old_wrapped);
            let new_wrapped = to_page.get_row(new_link).map_err(ExecutionError::DataPageError)?;
            self.publish_wrapped_row(new_link, new_wrapped);
        }

        Ok((raw_data, new_link))
    }

    pub(crate) fn retire_published_link(&self, link: Link) {
        queue_retirement(
            &self.retired_publications,
            &self.pending_retirements,
            "publications",
            OffsetEqLink(link),
        );
        self.reclaim_retired();
    }

    pub fn get_page_count(&self) -> usize {
        self.pages.read().len()
    }

    pub fn get_empty_links(&self) -> Vec<Link> {
        self.empty_links.iter().collect()
    }

    /// How many empty links the registry holds.
    ///
    /// `get_empty_links().len()` allocated a `Vec` of every link to read its
    /// length, which `system_info` did on every call.
    pub fn empty_links_count(&self) -> usize {
        self.empty_links.iter().count()
    }

    pub fn empty_links_registry(&self) -> &EmptyLinkRegistry<DATA_LENGTH> {
        &self.empty_links
    }

    pub fn with_empty_links(mut self, links: Vec<Link>) -> Self {
        let registry = EmptyLinkRegistry::default();
        for l in links {
            registry.push(l)
        }
        self.empty_links = registry;

        self
    }

    pub fn current_page_id(&self) -> PageId {
        self.current_page_id.load(Ordering::Acquire).into()
    }
}

#[derive(Debug, Display, Error, From, PartialEq)]
pub enum ExecutionError {
    DataPageError(DataExecutionError),

    PageNotFound(#[error(not(source))] PageId),

    Locked,

    Ghosted,

    Vacuumed,

    Deleted,
}

impl ExecutionError {
    pub fn is_vacuumed(&self) -> bool {
        matches!(self, Self::Vacuumed)
    }

    /// True when the error means "no row lives at this link (any more)" —
    /// the row was deleted, ghosted, vacuumed away, or its page is gone.
    /// Snapshot-building code skips such candidates; every other variant is
    /// a real storage error and must propagate.
    pub fn is_row_absent(&self) -> bool {
        matches!(
            self,
            Self::Ghosted | Self::Deleted | Self::Vacuumed | Self::PageNotFound(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;
    use std::time::Instant;

    use parking_lot::RwLock;
    use rkyv::with::{AtomicLoad, Relaxed};
    use rkyv::{Archive, Deserialize, Serialize};

    use crate::in_memory::data::Data;
    use crate::in_memory::pages::{DataPages, ExecutionError};
    use crate::in_memory::{DATA_INNER_LENGTH, PagesExecutionError, RowWrapper, StorableRow};
    use crate::prelude::ArchivedRowWrapper;
    use data_bucket::Link;

    #[derive(Archive, Copy, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
    struct TestRow {
        a: u64,
        b: u64,
    }

    /// General `Row` wrapper that is used to append general data for every `Inner`
    /// `Row`.
    #[derive(Archive, Deserialize, Debug, Serialize)]
    pub struct GeneralRow<Inner> {
        /// Inner generic `Row`.
        pub inner: Inner,

        /// Indicator for ghosted rows.
        #[rkyv(with = AtomicLoad<Relaxed>)]
        pub is_ghosted: AtomicBool,

        /// Indicator for vacuumed rows.
        #[rkyv(with = AtomicLoad<Relaxed>)]
        pub is_vacuumed: AtomicBool,

        /// Indicator for deleted rows.
        #[rkyv(with = AtomicLoad<Relaxed>)]
        pub deleted: AtomicBool,
    }

    impl<Inner> RowWrapper<Inner> for GeneralRow<Inner> {
        fn get_inner(self) -> Inner {
            self.inner
        }

        fn is_ghosted(&self) -> bool {
            self.is_ghosted.load(Ordering::Relaxed)
        }

        fn is_vacuumed(&self) -> bool {
            self.is_vacuumed.load(Ordering::Relaxed)
        }

        fn is_deleted(&self) -> bool {
            self.deleted.load(Ordering::Relaxed)
        }

        /// Creates new [`GeneralRow`] from `Inner`.
        fn from_inner(inner: Inner) -> Self {
            Self {
                inner,
                is_ghosted: AtomicBool::new(true),
                is_vacuumed: AtomicBool::new(false),
                deleted: AtomicBool::new(false),
            }
        }
    }

    impl StorableRow for TestRow {
        type WrappedRow = GeneralRow<TestRow>;
    }

    impl<T> ArchivedRowWrapper for ArchivedGeneralRow<T>
    where
        T: Archive,
    {
        fn unghost(&mut self) {
            self.is_ghosted = false
        }
        fn set_in_vacuum_process(&mut self) {
            self.is_vacuumed = true
        }
        fn delete(&mut self) {
            self.deleted = true
        }
        fn is_deleted(&self) -> bool {
            self.deleted
        }
    }

    #[test]
    fn insert() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert(row).unwrap();

        assert_eq!(link.page_id, 1.into());
        assert_eq!(link.length, 24);
        assert_eq!(link.offset, 0);

        assert_eq!(pages.row_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn insert_many() {
        let pages = DataPages::<TestRow>::new();

        for _ in 0..10_000 {
            let row = TestRow { a: 10, b: 20 };
            pages.insert(row).unwrap();
        }

        assert_eq!(pages.row_count.load(Ordering::Relaxed), 10_000);
        assert!(pages.current_page_id.load(Ordering::Relaxed) > 2);
    }

    #[test]
    fn select() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert(row).unwrap();
        let res = pages.select(link).unwrap();

        assert_eq!(res, row)
    }

    #[test]
    fn select_non_ghosted() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert(row).unwrap();
        let res = pages.select_non_ghosted(link);
        assert!(res.is_err());
        assert_eq!(res.err(), Some(PagesExecutionError::Ghosted))
    }

    #[test]
    fn versioned_insert_stays_hidden_until_unghost() {
        let pages = DataPages::<TestRow>::new();
        let row = TestRow { a: 7, b: 9 };
        let link = pages.insert(row).unwrap();

        assert_eq!(pages.select_non_ghosted(link), Err(ExecutionError::Ghosted));
        unsafe {
            pages.with_mut_ref(link, |archived| archived.unghost()).unwrap();
        }
        assert_eq!(pages.select_non_ghosted(link), Ok(row));
    }

    #[test]
    fn versioned_reader_observes_old_row_while_page_update_is_incomplete() {
        let pages = Arc::new(DataPages::<TestRow>::new());
        let link = pages.insert(TestRow { a: 0, b: 0 }).unwrap();
        unsafe {
            pages.with_mut_ref(link, |row| row.unghost()).unwrap();
        }

        let (first_field_written_tx, first_field_written_rx) = mpsc::channel();
        let (finish_update_tx, finish_update_rx) = mpsc::channel();
        let writer_pages = pages.clone();
        let writer = thread::spawn(move || unsafe {
            writer_pages
                .with_mut_ref(link, |archived| {
                    archived.inner.a = 1.into();
                    first_field_written_tx.send(()).unwrap();
                    finish_update_rx.recv().unwrap();
                    archived.inner.b = 1.into();
                })
                .unwrap();
        });

        first_field_written_rx.recv().unwrap();
        let (read_tx, read_rx) = mpsc::channel();
        let reader_pages = pages.clone();
        let reader = thread::spawn(move || {
            read_tx.send(reader_pages.select_non_ghosted(link)).unwrap();
        });

        assert_eq!(
            read_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Ok(TestRow { a: 0, b: 0 }),
            "reader must use the old immutable version instead of page bytes"
        );

        finish_update_tx.send(()).unwrap();
        writer.join().unwrap();
        reader.join().unwrap();
        assert_eq!(pages.select_non_ghosted(link), Ok(TestRow { a: 1, b: 1 }));
    }

    #[test]
    fn failed_exact_length_update_preserves_page_bytes_and_publication() {
        let pages = DataPages::<TestRow>::new();
        let old_row = TestRow { a: 10, b: 20 };
        let link = pages.insert(old_row).unwrap();
        unsafe {
            pages.with_mut_ref(link, |row| row.unghost()).unwrap();
        }
        let old_bytes = pages.select_raw(link).unwrap();
        let wrong_length = Link {
            length: link.length - 1,
            ..link
        };

        let result = unsafe { pages.update_in_place::<DATA_INNER_LENGTH>(TestRow { a: 30, b: 40 }, wrong_length) };

        assert!(matches!(
            result,
            Err(ExecutionError::DataPageError(
                crate::in_memory::DataExecutionError::InvalidLink
            ))
        ));
        assert_eq!(pages.select_raw(link).unwrap(), old_bytes);
        assert_eq!(pages.select_non_ghosted(link), Ok(old_row));
    }

    #[test]
    fn retired_version_survives_link_reuse_for_in_flight_reader() {
        let pages = DataPages::<TestRow>::new();
        let link = pages.insert(TestRow { a: 1, b: 1 }).unwrap();
        unsafe {
            pages.with_mut_ref(link, |row| row.unghost()).unwrap();
        }
        let old_slot = pages.published_slot(link).unwrap();
        let old_version = old_slot.snapshot();

        pages.delete(link).unwrap();
        let reused_link = pages.insert(TestRow { a: 2, b: 2 }).unwrap();
        assert_eq!(reused_link, link);
        assert_eq!(pages.select_non_ghosted(reused_link), Err(ExecutionError::Ghosted));
        unsafe {
            pages.with_mut_ref(reused_link, |row| row.unghost()).unwrap();
        }

        assert_eq!(old_version.as_ref(), &TestRow { a: 1, b: 1 });
        assert_eq!(pages.select_non_ghosted(reused_link), Ok(TestRow { a: 2, b: 2 }));
    }

    #[test]
    fn read_grace_period_prevents_link_aba() {
        let pages = DataPages::<TestRow>::new();
        let old_link = pages.insert(TestRow { a: 1, b: 1 }).unwrap();
        unsafe {
            pages.with_mut_ref(old_link, |row| row.unghost()).unwrap();
        }

        let read_guard = pages.read_guard();
        pages.delete(old_link).unwrap();
        let new_link = pages.insert(TestRow { a: 2, b: 2 }).unwrap();
        assert_ne!(new_link, old_link, "retired link was reused by an active reader");

        drop(read_guard);
        pages.reclaim_retired();
        assert!(pages.get_empty_links().contains(&old_link));
    }

    #[test]
    fn read_grace_period_prevents_vacuumed_page_reuse() {
        let pages = DataPages::<TestRow>::from_data(vec![
            Arc::new(Data::new(1.into())),
            Arc::new(Data::new(2.into())),
            Arc::new(Data::new(3.into())),
        ]);
        pages.current_page_id.store(2, Ordering::Release);
        let old_link = pages.insert(TestRow { a: 1, b: 1 }).unwrap();
        unsafe {
            pages.with_mut_ref(old_link, |row| row.unghost()).unwrap();
        }
        pages.current_page_id.store(3, Ordering::Release);

        let read_guard = pages.read_guard();
        pages.retire_published_link(old_link);
        pages.mark_page_empty(old_link.page_id);

        let temporary_page = pages.allocate_new_or_pop_free();
        assert_ne!(
            temporary_page.id, old_link.page_id,
            "vacuumed source page was reused while an old index reader was active"
        );
        assert_eq!(
            pages.select_non_ghosted(old_link),
            Ok(TestRow { a: 1, b: 1 }),
            "the old publication must survive until the reader leaves"
        );

        drop(read_guard);
        pages.reclaim_retired();
        assert!(pages.get_empty_pages().contains(&old_link.page_id));

        let reused_page = pages.allocate_new_or_pop_free();
        assert_eq!(reused_page.id, old_link.page_id);
        assert_eq!(reused_page.free_offset.load(Ordering::Acquire), 0);
        assert!(pages.published_slot(old_link).is_none());
    }

    #[test]
    fn whole_page_reclamation_does_not_publish_overlapping_empty_links() {
        let pages = DataPages::<TestRow>::from_data(vec![
            Arc::new(Data::new(1.into())),
            Arc::new(Data::new(2.into())),
            Arc::new(Data::new(3.into())),
        ]);
        pages.current_page_id.store(2, Ordering::Release);
        let old_link = pages.insert(TestRow { a: 1, b: 1 }).unwrap();
        unsafe {
            pages.with_mut_ref(old_link, |row| row.unghost()).unwrap();
        }
        pages.current_page_id.store(3, Ordering::Release);

        let read_guard = pages.read_guard();
        pages.delete(old_link).unwrap();
        pages.mark_page_empty(old_link.page_id);
        drop(read_guard);
        pages.reclaim_retired();

        assert!(pages.get_empty_pages().contains(&old_link.page_id));
        assert!(
            pages
                .get_empty_links()
                .iter()
                .all(|link| link.page_id != old_link.page_id),
            "whole-page and inner-link allocators must not receive overlapping storage"
        );
    }

    #[test]
    fn select_non_vacuumed_returns_row_when_valid() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert(row).unwrap();

        unsafe {
            pages
                .with_mut_ref(link, |archived| {
                    archived.unghost();
                })
                .unwrap();
        }

        let res = pages.select_non_vacuumed(link);
        assert!(
            res.is_ok(),
            "select_non_vacuumed should return Ok for unghosted, non-vacuumed row"
        );
        assert_eq!(res.unwrap(), TestRow { a: 10, b: 20 });
    }

    #[test]
    fn select_non_vacuumed_returns_ghosted_error_for_ghosted_row() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert(row).unwrap();

        let res = pages.select_non_vacuumed(link);
        assert!(res.is_err());
        assert_eq!(res.err(), Some(ExecutionError::Ghosted));
    }

    #[test]
    fn select_non_vacuumed_returns_vacuumed_error_for_vacuumed_row() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert(row).unwrap();

        unsafe {
            pages
                .with_mut_ref(link, |archived| {
                    archived.unghost();
                })
                .unwrap();
        }

        unsafe {
            pages
                .with_mut_ref(link, |archived| archived.set_in_vacuum_process())
                .unwrap();
        }

        let res = pages.select_non_vacuumed(link);
        assert!(res.is_err());
        assert_eq!(res.err(), Some(ExecutionError::Vacuumed));
    }

    #[test]
    fn select_non_vacuumed_errors_on_vacuumed_even_if_unghosted() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow { a: 42, b: 99 };
        let link = pages.insert(row).unwrap();

        unsafe {
            pages
                .with_mut_ref(link, |archived| {
                    archived.set_in_vacuum_process();
                })
                .unwrap();
        }

        let res = pages.select_non_vacuumed(link);
        assert!(res.is_err());
        assert_eq!(
            res.err(),
            Some(ExecutionError::Ghosted),
            "Should check ghosted before vacuumed"
        );
    }

    #[test]
    fn update() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert(row).unwrap();
        let res = pages.select(link).unwrap();

        assert_eq!(res, row)
    }

    #[test]
    fn delete() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert(row).unwrap();
        pages.delete(link).unwrap();

        assert_eq!(pages.empty_links.pop_max().map(|(l, _)| l), Some(link));
        pages.empty_links.push(link);

        let row = TestRow { a: 20, b: 20 };
        let new_link = pages.insert(row).unwrap();
        assert_eq!(new_link, link)
    }

    #[test]
    fn insert_on_empty() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert(row).unwrap();
        let _ = pages.delete(link);
        let link_new = pages.insert(row).unwrap();

        assert_eq!(link, link_new);
        assert_eq!(pages.select(link).unwrap(), TestRow { a: 10, b: 20 })
    }

    //#[test]
    fn _bench() {
        let pages = Arc::new(DataPages::<TestRow>::new());

        let mut v = Vec::new();

        let now = Instant::now();

        for j in 0..10 {
            let pages_shared = pages.clone();
            let h = thread::spawn(move || {
                for i in 0..1000 {
                    let row = TestRow { a: i, b: j * i + 1 };

                    pages_shared.insert(row).unwrap();
                }
            });

            v.push(h)
        }

        for h in v {
            h.join().unwrap()
        }

        let elapsed = now.elapsed();

        println!("wt2 {elapsed:?}")
    }

    #[test]
    fn bench_set() {
        let pages = Arc::new(RwLock::new(HashSet::new()));

        let mut v = Vec::new();

        let now = Instant::now();

        for j in 0..10 {
            let pages_shared = pages.clone();
            let h = thread::spawn(move || {
                for i in 0..1000 {
                    let row = TestRow { a: i, b: j * i + 1 };

                    let mut pages = pages_shared.write();
                    pages.insert(row);
                }
            });

            v.push(h)
        }

        for h in v {
            h.join().unwrap()
        }

        let elapsed = now.elapsed();

        println!("set {elapsed:?}")
    }

    #[test]
    fn bench_vec() {
        let pages = Arc::new(RwLock::new(Vec::new()));

        let mut v = Vec::new();

        let now = Instant::now();

        for j in 0..10 {
            let pages_shared = pages.clone();
            let h = thread::spawn(move || {
                for i in 0..1000 {
                    let row = TestRow { a: i, b: j * i + 1 };

                    let mut pages = pages_shared.write();
                    pages.push(row);
                }
            });

            v.push(h)
        }

        for h in v {
            h.join().unwrap()
        }

        let elapsed = now.elapsed();

        println!("vec {elapsed:?}")
    }

    #[test]
    fn allocate_new_or_pop_free_creates_page_correctly() {
        let pages = DataPages::<TestRow>::new();

        let initial_last_id = pages.last_page_id.load(Ordering::Relaxed);
        let initial_current = pages.current_page_id.load(Ordering::Relaxed);
        let initial_count = pages.get_page_count();

        let _allocated_page = pages.allocate_new_or_pop_free();

        assert_eq!(pages.last_page_id.load(Ordering::Relaxed), initial_last_id + 1);

        assert_eq!(
            pages.current_page_id.load(Ordering::Relaxed),
            initial_current,
            "current_page_id should NOT change after allocate_new_or_pop_free"
        );

        assert_eq!(pages.get_page_count(), initial_count + 1);

        let retrieved_page = pages.get_page((initial_last_id + 1).into());
        assert!(retrieved_page.is_some());
    }

    #[test]
    fn allocate_multiple_new_pages() {
        let pages = DataPages::<TestRow>::new();

        let initial_last_id = pages.last_page_id.load(Ordering::Relaxed);
        let initial_current = pages.current_page_id.load(Ordering::Relaxed);

        let _page2 = pages.allocate_new_or_pop_free();
        let _page3 = pages.allocate_new_or_pop_free();
        let _page4 = pages.allocate_new_or_pop_free();

        assert_eq!(pages.last_page_id.load(Ordering::Relaxed), initial_last_id + 3);
        assert_eq!(pages.current_page_id.load(Ordering::Relaxed), initial_current);
        assert_eq!(pages.get_page_count(), 4);
    }

    #[test]
    fn insert_continues_on_current_page_after_allocation() {
        let pages = DataPages::<TestRow>::new();

        pages.allocate_new_or_pop_free();

        let row = TestRow { a: 42, b: 99 };
        let link = pages.insert(row).unwrap();

        assert_eq!(link.page_id, 1.into());
    }

    #[test]
    fn allocate_new_or_pop_free_concurrent() {
        let pages = Arc::new(DataPages::<TestRow>::new());
        let mut handles = Vec::new();

        for _ in 0..10 {
            let pages_clone = pages.clone();
            let handle = thread::spawn(move || {
                for _ in 0..10 {
                    pages_clone.allocate_new_or_pop_free();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(pages.get_page_count(), 101);
        assert_eq!(pages.last_page_id.load(Ordering::Relaxed), 101);
    }

    #[test]
    fn allocated_page_has_correct_initial_state() {
        let pages = DataPages::<TestRow>::new();

        let allocated = pages.allocate_new_or_pop_free();

        assert_eq!(allocated.free_offset.load(Ordering::Relaxed), 0);
        assert_eq!(allocated.free_space(), DATA_INNER_LENGTH);
    }

    #[test]
    fn skips_explicitly_allocated_page() {
        let pages = DataPages::<TestRow>::new();

        // Allocate page explicitly
        pages.allocate_new_or_pop_free();
        assert_eq!(pages.last_page_id.load(Ordering::Relaxed), 2);
        assert_eq!(pages.current_page_id.load(Ordering::Relaxed), 1);

        loop {
            let row = TestRow {
                a: 42,
                b: pages.row_count.load(Ordering::Relaxed),
            };
            let link = pages.insert(row).unwrap();
            if link.page_id != 1.into() {
                break;
            }
        }

        let row = TestRow { a: 999, b: 888 };
        let new_link = pages.insert(row).unwrap();

        assert_eq!(new_link.page_id, 3.into(), "New insert should go to page 3, not page 2");
        assert_eq!(pages.current_page_id.load(Ordering::Relaxed), 3);
        assert_eq!(pages.get_page_count(), 3);
    }

    #[test]
    fn allocate_new_or_pop_free_reuses_empty_page() {
        let pages = DataPages::<TestRow>::from_data(vec![
            Arc::new(Data::new(1.into())),
            Arc::new(Data::new(2.into())),
            Arc::new(Data::new(3.into())),
        ]);

        pages.mark_page_empty(2.into());

        let initial_last_id = pages.last_page_id.load(Ordering::Relaxed);
        let initial_page_count = pages.get_page_count();

        let reused_page = pages.allocate_new_or_pop_free();

        assert_eq!(reused_page.id, 2.into(), "Should reuse page 2");
        assert_eq!(
            pages.last_page_id.load(Ordering::Relaxed),
            initial_last_id,
            "last_page_id should NOT increment when reusing"
        );
        assert_eq!(
            pages.get_page_count(),
            initial_page_count,
            "Page count should NOT increase when reusing"
        );
        assert_eq!(
            reused_page.free_offset.load(Ordering::Relaxed),
            0,
            "Reused page should be reset (free_offset = 0)"
        );
        assert_eq!(
            reused_page.free_space(),
            DATA_INNER_LENGTH,
            "Reused page should have full free space"
        );

        let row = TestRow { a: 111, b: 222 };
        let link = pages.insert(row).unwrap();
        assert_eq!(link.page_id, 3.into());

        pages.current_page_id.store(2, Ordering::Release);
        let row2 = TestRow { a: 333, b: 444 };
        let link2 = pages.insert(row2).unwrap();
        assert_eq!(link2.page_id, 2.into(), "Should write to reused page 2");

        let retrieved = pages.select(link2).unwrap();
        assert_eq!(retrieved, row2);
    }

    #[test]
    fn mark_page_full_adds_empty_link_and_sets_free_offset() {
        let pages = DataPages::<TestRow>::from_data(vec![Arc::new(Data::new(1.into())), Arc::new(Data::new(2.into()))]);

        // to manually insert on page 1
        pages.current_page_id.store(1, Ordering::Release);

        let row = TestRow { a: 10, b: 20 };
        let _link = pages.insert(row).unwrap();

        pages.current_page_id.store(2, Ordering::Release);
        pages.mark_page_full(1.into());

        let empty_links = pages.get_empty_links();
        assert!(!empty_links.is_empty(), "Should have empty links");

        let link = empty_links.first().unwrap();
        assert_eq!(link.page_id, 1.into());
        assert_eq!(
            link.length,
            DATA_INNER_LENGTH as u32 - 24,
            "Should have remaining space = DATA_INNER_LENGTH - 24"
        );

        let page = pages.get_page(1.into()).unwrap();
        assert_eq!(
            page.free_offset.load(Ordering::Relaxed),
            DATA_INNER_LENGTH as u32,
            "free_offset should be set to DATA_LENGTH"
        );
    }

    #[test]
    fn mark_page_full_does_nothing_for_current_or_nonexistent_page() {
        let pages = DataPages::<TestRow>::new();

        let initial_empty_links = pages.get_empty_links().len();
        pages.mark_page_full(1.into());

        assert_eq!(
            pages.get_empty_links().len(),
            initial_empty_links,
            "Should not add empty links for current page"
        );

        let page = pages.get_page(1.into()).unwrap();
        assert_ne!(
            page.free_offset.load(Ordering::Relaxed),
            DATA_INNER_LENGTH as u32,
            "free_offset should NOT be modified for current page"
        );

        pages.mark_page_full(999.into());

        assert!(pages.get_empty_links().is_empty());
    }

    #[test]
    fn mark_page_full_with_partial_page() {
        let pages = DataPages::<TestRow>::from_data(vec![Arc::new(Data::new(1.into())), Arc::new(Data::new(2.into()))]);

        for _ in 0..10 {
            let row = TestRow { a: 42, b: 99 };
            pages.insert(row).unwrap();
        }

        let page = pages.get_page(1.into()).unwrap();
        let free_offset_before = page.free_offset.load(Ordering::Relaxed);
        let expected_remaining = DATA_INNER_LENGTH as u32 - free_offset_before;

        pages.current_page_id.store(2, Ordering::Release);
        pages.mark_page_full(1.into());

        let empty_links = pages.get_empty_links();
        let link = empty_links.first().unwrap();
        assert_eq!(link.offset, free_offset_before);
        assert_eq!(link.length, expected_remaining);

        assert_eq!(page.free_offset.load(Ordering::Relaxed), DATA_INNER_LENGTH as u32);
    }

    #[test]
    fn mark_page_full_with_no_remaining_space() {
        let pages = DataPages::<TestRow>::from_data(vec![Arc::new(Data::new(1.into())), Arc::new(Data::new(2.into()))]);

        let page = pages.get_page(1.into()).unwrap();
        page.free_offset.store(DATA_INNER_LENGTH as u32, Ordering::Release);

        pages.current_page_id.store(2, Ordering::Release);
        pages.mark_page_full(1.into());

        assert!(pages.get_empty_links().is_empty());
    }
}
