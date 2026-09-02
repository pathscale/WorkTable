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
use crate::util::epoch::EpochDomain;
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

/// Most retired items one reclaim call may recycle inline. Bounds the latency
/// a mutating call can absorb from reclamation; the remainder stays claimed
/// for the next caller.
const RECLAIM_BATCH_LIMIT: usize = 256;

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

/// A read-side grace-period guard: an epoch pin in the table's own
/// [`EpochDomain`].
///
/// Acquiring one is a thread-local operation (no shared read-modify-write, no
/// cache line shared with other readers). While it is held, no item retired
/// after the pin can be recycled; items retired entirely before the pin are
/// not protected, which is sound because writers unlink every index reference
/// before retiring (see the type-level docs on [`DataPages`]).
///
/// Not `Send`: the pin belongs to the acquiring thread. Hold it across the
/// synchronous read window (index lookup through row-version acquisition),
/// not across `.await` points.
pub struct ReadGuard<'a> {
    _guard: crate::util::epoch::Guard<'a>,
    marker: PhantomData<&'a ()>,
}

/// One unit of retired state waiting out its grace period. Reclamation is
/// *recycling*, not just freeing: links return to `empty_links`, pages return
/// to `empty_pages`, and publication slots leave their shard map.
#[derive(Debug, Clone, Copy)]
enum Retired<const DATA_LENGTH: usize> {
    /// A freed row slot: remove its publication, then hand the slot back to
    /// the empty-link allocator (unless a whole-page retirement supersedes
    /// it).
    Link(Link),
    /// A wholly emptied page: purge any of its stale empty links, then hand
    /// the page back to the empty-page allocator.
    Page(PageId),
    /// A publication whose row bytes moved elsewhere (vacuum): remove the
    /// shard entry only, the physical slot stays owned by its page.
    Publication(OffsetEqLink<DATA_LENGTH>),
}

/// Page storage with immutable row publication.
///
/// # Versioned-publication synchronization
///
/// Generated readers enter the grace period before resolving an index link by
/// pinning the table's epoch domain. Writers must remove or replace every
/// index reference before queueing the old link for retirement. That
/// unlink-before-retire invariant is what makes a reader pinning after the
/// retirement unable to acquire the old link, and it is why such late readers
/// do not need to block recycling.
///
/// # Retirement and reclamation
///
/// Retired items enter one FIFO queue in retirement order, and each
/// retirement defers an epoch marker. A marker executes only once every
/// reader pinned at retirement time has unpinned; the number of executed
/// markers therefore bounds a safe-to-recycle *prefix* of the queue (a later
/// item's grace expiring implies every earlier item's grace expired, because
/// the readers blocking an earlier item were still pinned when the later item
/// was retired). Reclaimers drain a bounded batch of that prefix and run the
/// recycle logic; they never wait for a global zero-reader instant, so
/// reclamation progresses under continuously overlapping readers and no
/// unbounded backlog can form.
///
/// FIFO order also preserves the whole-page subsumption invariant: a link
/// retirement for page P precedes P's whole-page retirement in the queue
/// (guaranteed by row-lock ordering: once vacuum has moved or observed every
/// row of P, no later delete can resolve a link on P), and processing P
/// purges any of P's links from the empty-link registry, so the whole-page
/// and inner-link allocators never hold overlapping storage.
///
/// # Lock order
///
/// Each page carries its own access barrier (`Data::access`); there is no
/// table-wide page lock. Locks are acquired in this order when more than one
/// is needed:
///
/// 1. generated row/lock-manager locks (outside this type, always first);
/// 2. `empty_pages` (only the insert page-switch path holds it into 3/4);
/// 3. `pages` (the vector lock; its write side is only taken for growth,
///    with no page lock held);
/// 4. one or two per-page `Data::access` locks — two only in the vacuum row
///    move, always in ascending page-id order;
/// 5. one `published_rows` shard, or the empty-link registry's `op_lock`.
///
/// Reclamation holds the retirement queue, then briefly acquires individual
/// publication shards and the empty-link/page registries; nothing acquires
/// the retirement queue while holding any of those (or any page lock), so
/// the order is acyclic. Callers must not invoke reclamation while retaining
/// the retirement-queue guard.
#[derive(Debug)]
pub struct DataPages<Row, const DATA_LENGTH: usize = DATA_INNER_LENGTH>
where
    Row: StorableRow,
{
    /// Immutable application-visible row versions. Published readers never
    /// borrow the mutable archived page image.
    published_rows: PublicationShards<Row, DATA_LENGTH>,

    /// Read-side grace periods protecting the interval from index lookup
    /// until an immutable row version has been acquired. Owned by this table:
    /// a reader of another table never delays reclamation here.
    epoch: EpochDomain,

    /// Retired items in retirement order, awaiting grace expiry.
    retired: Mutex<VecDeque<Retired<DATA_LENGTH>>>,

    /// How many queued retirements' grace periods have expired. Incremented
    /// by deferred epoch markers; consumed (front-of-queue) by reclaimers.
    /// Shared with the markers through an `Arc` so a marker outliving the
    /// table stays sound.
    reclaimable: Arc<AtomicUsize>,

    /// Queue length mirror, so mutations skip reclamation without locking
    /// when there is no work pending.
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

        let pages = self.pages.read();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let _page_guard = page.access.read();
        // Re-check under the page barrier: a writer publishing this row holds
        // the exclusive side, so once we hold the shared side the publication
        // map is current for this page's rows.
        if let Some(slot) = self.published_slot(link) {
            return Ok(slot);
        }
        let wrapped = page.get_row(link).map_err(ExecutionError::DataPageError)?;
        let flags = Self::publication_flags(&wrapped);
        let slot = Arc::new(PublishedRow::new(wrapped.get_inner(), flags));
        let key = OffsetEqLink(link);
        let mut published_rows = self.published_rows[publication_shard(&key)].write();
        Ok(published_rows.entry(key).or_insert(slot).clone())
    }

    pub fn read_guard(&self) -> ReadGuard<'_> {
        ReadGuard {
            _guard: self.epoch.pin(),
            marker: PhantomData,
        }
    }

    /// Queue one retired item and defer its grace marker.
    ///
    /// The marker is flushed to the domain's global queue immediately so any
    /// thread can later collect it; it executes only after every reader
    /// pinned right now has unpinned.
    fn retire(&self, item: Retired<DATA_LENGTH>) {
        self.retire_many(std::iter::once(item));
    }

    /// Queue several retired items behind one grace marker.
    ///
    /// Retiring `n` items one at a time takes `n` domain advances, and an
    /// advance is the expensive half: it is the only operation here that has to
    /// decide what every reader can still reach. Retiring a batch behind a
    /// single marker is the same guarantee, because the marker is stamped after
    /// the whole batch is queued and therefore covers all of it.
    ///
    /// The lock is taken once for the batch rather than once per item, which
    /// also stops a bulk delete interleaving its queue pushes with concurrent
    /// mutations for no reason.
    fn retire_many(&self, items: impl IntoIterator<Item = Retired<DATA_LENGTH>>) {
        let mut queued = 0usize;
        let len = {
            let mut retired = self.retired.lock();
            for item in items {
                retired.push_back(item);
                queued += 1;
            }
            retired.len()
        };
        if queued == 0 {
            return;
        }
        self.pending_retirements.fetch_add(queued, Ordering::Release);
        if len >= RETIREMENT_BACKLOG_WARN_AT && len.is_power_of_two() {
            tracing::warn!(len, "versioned publication retirement backlog is growing");
        }
        let reclaimable = Arc::clone(&self.reclaimable);
        let guard = self.epoch.pin();
        // One marker for the whole batch. It is stamped now, so it expires only
        // after every reader pinned now has unpinned, which is exactly the
        // condition each item would have waited for individually.
        self.epoch.retire(move || {
            reclaimable.fetch_add(queued, Ordering::Release);
        });
        drop(guard);
        self.epoch.advance();
    }

    /// Incrementally recycle retired items whose grace period has expired.
    ///
    /// Never waits for readers: if any reader pinned before a retirement is
    /// still active, that item (and everything after it) simply stays queued.
    /// Each call drains at most [`RECLAIM_BATCH_LIMIT`] items, so a mutation
    /// never absorbs an unbounded backlog inline.
    fn reclaim_retired(&self) {
        if self.pending_retirements.load(Ordering::Acquire) == 0 {
            return;
        }
        // Help the epoch forward. Each step is bounded and thread-local-ish;
        // three advances are what a marker needs from defer to execution in
        // the quiet case, and under active readers these are cheap no-ops.
        for _ in 0..4 {
            if self.reclaimable.load(Ordering::Acquire) != 0 {
                break;
            }
            self.epoch.advance();
        }

        let claimed = self.reclaimable.swap(0, Ordering::AcqRel);
        if claimed == 0 {
            return;
        }

        let mut retired = self.retired.lock();
        let take = claimed.min(RECLAIM_BATCH_LIMIT).min(retired.len());
        if claimed > take {
            // Batch limit hit: return the unused claims for the next caller.
            self.reclaimable.fetch_add(claimed - take, Ordering::Release);
        }

        // A whole-page retirement subsumes every free link within that page.
        // Publishing both would let one allocator reset/reuse the page while
        // another writes through an overlapping link from the same page. A
        // link is skipped when its page's retirement is queued anywhere
        // behind it, and a page purges its stale links on processing, so the
        // invariant holds across batch boundaries in both directions.
        let queued_pages: HashSet<PageId> = retired
            .iter()
            .filter_map(|item| match item {
                Retired::Page(page_id) => Some(*page_id),
                _ => None,
            })
            .collect();

        for _ in 0..take {
            let Some(item) = retired.pop_front() else {
                break;
            };
            match item {
                Retired::Link(link) => {
                    let key = OffsetEqLink(link);
                    self.published_rows[publication_shard(&key)].write().remove(&key);
                    if !queued_pages.contains(&link.page_id) {
                        self.empty_links.push(link);
                    }
                }
                Retired::Publication(key) => {
                    self.published_rows[publication_shard(&key)].write().remove(&key);
                }
                Retired::Page(page_id) => {
                    // Purge stale fragments of this page from the link
                    // allocator before exposing the whole page for reuse.
                    self.empty_links.remove_link_for_page(page_id);
                    self.empty_pages.write().push_back(page_id);
                }
            }
            self.pending_retirements.fetch_sub(1, Ordering::Release);
        }
    }

    pub fn new() -> Self {
        Self {
            published_rows: std::array::from_fn(|_| RwLock::new(PublicationMap::default())),
            epoch: EpochDomain::new(),
            retired: Mutex::new(VecDeque::new()),
            reclaimable: Arc::new(AtomicUsize::new(0)),
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
                epoch: EpochDomain::new(),
                retired: Mutex::new(VecDeque::new()),
                reclaimable: Arc::new(AtomicUsize::new(0)),
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
            let pages = self.pages.read();
            let current_page: usize = page_id_mapper(link.page_id.into());
            let page = &pages[current_page];
            let _page_guard = page.access.write();

            match unsafe { page.try_save_row_by_link(&general_row, link) } {
                Ok((link, left_link)) => {
                    if let Some(l) = left_link {
                        self.empty_links.push(l);
                    }
                    self.stage_published_row(link, row);
                    self.row_count.fetch_add(1, Ordering::Relaxed);
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
                let pages = self.pages.read();
                let current_page = page_id_mapper(self.current_page_id.load(Ordering::Acquire) as usize);
                let page = &pages[current_page];
                let _page_guard = page.access.write();
                // Re-check under the page barrier. A switch may have completed
                // between the load above and the lock; in the worst case the
                // stale page has since been vacuumed, queued as empty, and
                // reset, and a save into it would be silently destroyed by the
                // next reuse. A page never becomes current again once switched
                // away (switch targets come from empty_pages or fresh
                // allocation, and vacuum never empties the current page), so
                // current still naming this page here proves it cannot be in
                // (or headed for) the empty-page pool while the barrier is
                // held.
                if current_page != page_id_mapper(self.current_page_id.load(Ordering::Acquire) as usize) {
                    continue;
                }

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
                        // Re-check `current_page_id` under the empty_pages
                        // write lock and hold that lock through the switch,
                        // mirroring `add_next_page`. Two threads that both
                        // failed on the same page would otherwise each pop an
                        // empty page; the loser's store gets overwritten and
                        // its popped page is orphaned (permanent capacity
                        // leak). No other path acquires empty_pages while
                        // holding a page barrier, so the order here is safe.
                        let mut empty_pages = self.empty_pages.write();
                        if tried_page == page_id_mapper(self.current_page_id.load(Ordering::Acquire) as usize) {
                            if let Some(page_id) = empty_pages.pop_front() {
                                // Retired pages retain their old bytes until
                                // the read-side grace period completes. Reset
                                // only after reclamation made the page
                                // available for reuse.
                                let pages = self.pages.read();
                                let page = &pages[page_id_mapper(page_id.into())];
                                let _page_guard = page.access.write();
                                page.reset();
                                self.current_page_id.store(page_id.into(), Ordering::Release);
                            } else {
                                drop(empty_pages);
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
        // The first serialization succeeding inside insert() does not make
        // this one infallible: the serializer arena can still fail here.
        // Propagate instead of panicking the caller.
        let bytes = rkyv::to_bytes(&general_row)
            .map_err(|_| DataExecutionError::SerializeError)?
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
            let pages = self.pages.read();
            let index = page_id_mapper(page_id.into());
            let page = pages[index].clone();
            {
                let _page_guard = page.access.write();
                page.reset();
            }

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
        let pages = self.pages.read();
        let page = pages
            .get::<usize>(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let _page_guard = page.access.read();
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
        let pages = self.pages.read();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let _page_guard = page.access.write();
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
        let pages = self.pages.read();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let _page_guard = page.access.write();
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
    /// changed. The page's write barrier excludes low-level archived-page
    /// readers during the copy, while generated reads continue from the old immutable
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
        let pages = self.pages.read();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let _page_guard = page.access.write();
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

        self.row_count.fetch_sub(1, Ordering::Relaxed);
        self.retire(Retired::Link(link));
        self.reclaim_retired();
        Ok(())
    }

    /// Ghost every link in `links`, behind one grace marker.
    ///
    /// Same per-row effect as calling [`Self::delete`] in a loop: each row is
    /// marked deleted in place and its link is queued for reuse once no reader
    /// can still reach it. The difference is that the batch takes one domain
    /// advance and one reclaim pass instead of one of each per row, and an
    /// advance is the expensive half of a retirement.
    ///
    /// Ghosting is done first, for all links, and the batch is retired only
    /// after. A link must not become reusable while a later row in the same
    /// batch is still being marked, or a concurrent insert could claim it and
    /// be ghosted by this call.
    ///
    /// On error the links ghosted so far are still retired: they are genuinely
    /// deleted, and dropping them from the queue would leak their storage for
    /// the life of the table. The caller learns which link failed and how many
    /// preceded it.
    pub fn delete_many(&self, links: &[Link]) -> Result<(), ExecutionError>
    where
        Row: Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <Row as StorableRow>::WrappedRow:
            Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: ArchivedRowWrapper
            + Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        if links.is_empty() {
            return Ok(());
        }

        let mut ghosted = 0usize;
        let mut failure = None;
        for link in links {
            match unsafe { self.with_mut_ref(*link, |r| r.delete()) } {
                Ok(()) => ghosted += 1,
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }

        if ghosted > 0 {
            self.row_count.fetch_sub(ghosted as u64, Ordering::Relaxed);
            self.retire_many(links[..ghosted].iter().map(|link| Retired::Link(*link)));
            self.reclaim_retired();
        }

        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub fn select_raw(&self, link: Link) -> Result<Vec<u8>, ExecutionError> {
        let pages = self.pages.read();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let _page_guard = page.access.read();
        page.get_raw_row(link).map_err(ExecutionError::DataPageError)
    }

    pub fn mark_page_empty(&self, page_id: PageId) {
        if u32::from(page_id) != self.current_page_id.load(Ordering::Acquire) {
            self.retire(Retired::Page(page_id));
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
    /// Approximate under concurrency: a failing `save_row`'s transient
    /// reservation may be counted before its rollback. Metrics only.
    pub fn used_bytes(&self) -> u64 {
        let pages = self.pages.read();
        pages
            .iter()
            .map(|p| u64::from(p.free_offset.load(Ordering::Relaxed)))
            .sum()
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
        let pages = self.pages.read();
        let from_page = pages
            .get(page_id_mapper(from_link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(from_link.page_id))?;
        let to_page = pages
            .get(page_id_mapper(to_page_id.into()))
            .ok_or(ExecutionError::PageNotFound(to_page_id))?;
        // The one genuinely multi-page mutation: both barriers are needed,
        // taken in ascending page-id order so no lock cycle can form with a
        // concurrent pair.
        let _page_guards = if from_link.page_id == to_page_id {
            (from_page.access.write(), None)
        } else if u32::from(from_link.page_id) < u32::from(to_page_id) {
            let first = from_page.access.write();
            (first, Some(to_page.access.write()))
        } else {
            let first = to_page.access.write();
            (first, Some(from_page.access.write()))
        };

        let raw_data = from_page
            .get_raw_row(from_link)
            .map_err(ExecutionError::DataPageError)?;
        // Copy to the destination BEFORE flagging the source. The vacuumed
        // flag used to be set first, so a failing destination save returned
        // with the flag durably set in the source page image: after a restart
        // the row would load as vacuumed with no copy anywhere (row loss).
        // The whole method holds both pages' write barriers, so the order is
        // invisible to concurrent readers.
        let new_link = to_page.save_raw_row(&raw_data).map_err(ExecutionError::DataPageError)?;
        let archived = unsafe {
            from_page
                .get_mut_row_ref(from_link)
                .map_err(ExecutionError::DataPageError)?
                .unseal_unchecked()
        };
        archived.set_in_vacuum_process();

        {
            let old_wrapped = from_page.get_row(from_link).map_err(ExecutionError::DataPageError)?;
            self.publish_wrapped_row(from_link, old_wrapped);
            let new_wrapped = to_page.get_row(new_link).map_err(ExecutionError::DataPageError)?;
            self.publish_wrapped_row(new_link, new_wrapped);
        }

        Ok((raw_data, new_link))
    }

    pub(crate) fn retire_published_link(&self, link: Link) {
        self.retire(Retired::Publication(OffsetEqLink(link)));
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
        self.empty_links.len()
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

    /// A helper thread that holds (or releases) one `ReadGuard` on command,
    /// so a test can interleave reader intervals across threads. One thread
    /// cannot model overlapping readers: nested epoch pins keep the thread's
    /// participant at its first epoch.
    struct RemoteReader {
        commands: mpsc::Sender<bool>,
        done: mpsc::Receiver<()>,
        thread: Option<thread::JoinHandle<()>>,
    }

    impl RemoteReader {
        fn spawn(pages: Arc<DataPages<TestRow>>) -> Self {
            let (commands, command_rx) = mpsc::channel::<bool>();
            let (done_tx, done) = mpsc::channel();
            let thread = thread::spawn(move || {
                let mut guard = None;
                while let Ok(pin) = command_rx.recv() {
                    drop(guard.take());
                    if pin {
                        guard = Some(pages.read_guard());
                    }
                    done_tx.send(()).unwrap();
                }
                drop(guard);
            });
            Self {
                commands,
                done,
                thread: Some(thread),
            }
        }

        fn pin(&self) {
            self.commands.send(true).unwrap();
            self.done.recv().unwrap();
        }

        fn unpin(&self) {
            self.commands.send(false).unwrap();
            self.done.recv().unwrap();
        }
    }

    impl Drop for RemoteReader {
        fn drop(&mut self) {
            let (disconnected, _rx) = mpsc::channel();
            let _ = std::mem::replace(&mut self.commands, disconnected);
            if let Some(thread) = self.thread.take() {
                thread.join().unwrap();
            }
        }
    }

    /// The scenario the retired old scheme could never reclaim in: readers
    /// keep overlapping hand-over-hand, so there is no instant with zero
    /// active readers, yet every individual reader is short-lived. The old
    /// global-counter scheme required a zero-reader instant and would leave
    /// the retired link queued forever (this assertion fails against it);
    /// epochs only need each reader that predates the retirement to finish.
    #[test]
    fn reclamation_progresses_under_continuous_reader_overlap() {
        let pages = Arc::new(DataPages::<TestRow>::new());
        let link = pages.insert(TestRow { a: 1, b: 1 }).unwrap();
        unsafe {
            pages.with_mut_ref(link, |row| row.unghost()).unwrap();
        }

        let first = RemoteReader::spawn(pages.clone());
        let second = RemoteReader::spawn(pages.clone());

        // Retire the link while `first` is mid-read.
        first.pin();
        pages.delete(link).unwrap();
        assert!(
            !pages.get_empty_links().contains(&link),
            "the retired link must stay unrecyclable while a reader from before \
             the retirement is active"
        );

        // Hand over twice; at every instant at least one reader is active.
        second.pin();
        first.unpin();
        pages.reclaim_retired();
        first.pin();
        second.unpin();

        for _ in 0..8 {
            pages.reclaim_retired();
            if pages.get_empty_links().contains(&link) {
                break;
            }
        }
        assert!(
            pages.get_empty_links().contains(&link),
            "reclamation must progress although readers never stopped overlapping"
        );

        // The recycled capacity is really usable mid-overlap: the next insert
        // reuses the slot.
        let reused = pages.insert(TestRow { a: 2, b: 2 }).unwrap();
        assert_eq!(reused, link);
        first.unpin();
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
    fn page_is_full_switch_does_not_orphan_empty_pages() {
        use data_bucket::page::PageId;

        // Two 24-byte rows per page, so concurrent inserters hit the
        // PageIsFull switch path constantly.
        const PAGE: usize = 64;
        let pages = Arc::new(DataPages::<TestRow, PAGE>::from_data(
            (1..=64u32).map(|i| Arc::new(Data::new(i.into()))).collect(),
        ));
        for i in 1..=63u32 {
            pages.mark_page_empty(i.into());
        }
        assert_eq!(pages.get_empty_pages().len(), 63);

        let mut handles = Vec::new();
        for t in 0..8u64 {
            let p = pages.clone();
            handles.push(thread::spawn(move || {
                for i in 0..40u64 {
                    p.insert(TestRow { a: t, b: i }).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // Every page must remain reachable by an allocator: current, queued
        // as empty, covered by an empty link, or too full for another row.
        // A page with room that is none of these was popped by a racing
        // page switch and orphaned.
        let row_size = 24usize;
        let current = pages.current_page_id();
        let empty: HashSet<PageId> = pages.get_empty_pages().into_iter().collect();
        let linked: HashSet<PageId> = pages.get_empty_links().iter().map(|l| l.page_id).collect();
        for id in 1..=pages.get_page_count() as u32 {
            let page_id = PageId::from(id);
            let page = pages.get_page(page_id).unwrap();
            let reachable = page_id == current
                || empty.contains(&page_id)
                || linked.contains(&page_id)
                || page.free_space() < row_size;
            assert!(
                reachable,
                "page {id} was orphaned with {} free bytes",
                page.free_space()
            );
        }
    }

    #[test]
    fn failed_vacuum_move_leaves_source_row_unflagged() {
        let pages = DataPages::<TestRow>::from_data(vec![Arc::new(Data::new(1.into())), Arc::new(Data::new(2.into()))]);
        pages.current_page_id.store(1, Ordering::Release);

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert(row).unwrap();
        unsafe {
            pages.with_mut_ref(link, |r| r.unghost()).unwrap();
        }

        // Fill the destination so save_raw_row must fail there.
        let to_page = pages.get_page(2.into()).unwrap();
        to_page.free_offset.store(DATA_INNER_LENGTH as u32, Ordering::Release);

        let result = unsafe { pages.move_row_for_vacuum(link, 2.into()) };
        assert!(matches!(
            result,
            Err(ExecutionError::DataPageError(
                crate::in_memory::DataExecutionError::PageIsFull { .. }
            ))
        ));

        // The source row must NOT be left flagged as in-vacuum-process: that
        // flag is written into the persisted page image, and with no copy on
        // the destination it would mean durable row loss after a restart.
        let vacuumed = pages.with_ref(link, |r| r.is_vacuumed).unwrap();
        assert!(!vacuumed, "failed move must not leave the source marked vacuumed");
        assert_eq!(pages.select_non_vacuumed(link), Ok(row));
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
        assert_eq!(pages.row_count.load(Ordering::Relaxed), 1);
        pages.delete(link).unwrap();
        assert_eq!(
            pages.row_count.load(Ordering::Relaxed),
            0,
            "delete must decrement row_count"
        );

        assert_eq!(pages.empty_links.pop_max().map(|(l, _)| l), Some(link));
        pages.empty_links.push(link);

        let row = TestRow { a: 20, b: 20 };
        let new_link = pages.insert(row).unwrap();
        assert_eq!(new_link, link);
        assert_eq!(
            pages.row_count.load(Ordering::Relaxed),
            1,
            "an empty-link-reuse insert must increment row_count"
        );
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
