use arc_swap::ArcSwap;
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
use std::collections::{HashSet, VecDeque};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicUsize};
use std::{
    fmt::Debug,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::in_memory::empty_link_registry::EmptyLinkRegistry;
use crate::prelude::ArchivedRowWrapper;
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

const PAGE_DIRECTORY_CHUNK_SIZE: usize = 64;
const PAGE_DIRECTORY_ROOTS: usize = 64;
const GHOSTED: u8 = 1 << 0;
const DELETED: u8 = 1 << 1;
const VACUUMED: u8 = 1 << 2;
const RETIREMENT_BACKLOG_WARN_AT: usize = 1_024;

/// Most retired items one reclaim call may recycle inline. Bounds the latency
/// a mutating call can absorb from reclamation; the remainder stays claimed
/// for the next caller.
const RECLAIM_BATCH_LIMIT: usize = 256;

/// How many retirements may wait before a *producer* of them absorbs a sweep.
///
/// Reclamation exists to hand storage back for reuse, and the consumer of that
/// storage is `insert`, which already sweeps unconditionally before it looks
/// for a free link. A delete that also sweeps is doing the consumer's work on
/// the producer's thread, and doing it one item at a time, which is the
/// expensive way: freeing links in a batch lets the registry merge a
/// contiguous run into a single insertion instead of coalescing once per link.
///
/// So the delete paths sweep only to keep the queue bounded, not to make space
/// available. This cap is what a delete-only workload accumulates before it
/// pays, and a sweep drains up to [`RECLAIM_BATCH_LIMIT`] at once, so matching
/// them means a triggered sweep clears the backlog it was triggered by.
///
/// Deliberately not applied to `mark_page_empty`. It is not a hot path, so
/// deferring it would change when pages become allocatable for no measurable
/// gain.
const RECLAIM_BACKLOG_TRIGGER: usize = RECLAIM_BATCH_LIMIT;

#[derive(Debug)]
struct PageDirectoryChunk<T> {
    pages: [AtomicPtr<T>; PAGE_DIRECTORY_CHUNK_SIZE],
}

impl<T> PageDirectoryChunk<T> {
    fn new() -> Self {
        Self {
            pages: std::array::from_fn(|_| AtomicPtr::new(std::ptr::null_mut())),
        }
    }
}

/// Non-owning, stable page pointers for the first 4,096 pages (64 MiB at the
/// default page size). `DataPages::pages` owns every allocation; this directory
/// exists solely to avoid shared ArcSwap snapshot accounting on point access.
#[derive(Debug)]
struct PageDirectory<T> {
    roots: [AtomicPtr<PageDirectoryChunk<T>>; PAGE_DIRECTORY_ROOTS],
    chunks: Mutex<Vec<Box<PageDirectoryChunk<T>>>>,
}

impl<T> PageDirectory<T> {
    fn new(pages: &[Arc<T>]) -> Self {
        let directory = Self {
            roots: std::array::from_fn(|_| AtomicPtr::new(std::ptr::null_mut())),
            chunks: Mutex::new(Vec::new()),
        };
        for (index, page) in pages.iter().enumerate() {
            directory.publish(index, page);
        }
        directory
    }

    fn publish(&self, index: usize, page: &Arc<T>) {
        let root_index = index / PAGE_DIRECTORY_CHUNK_SIZE;
        let Some(root) = self.roots.get(root_index) else {
            return;
        };
        let mut chunk = root.load(Ordering::Acquire);
        if chunk.is_null() {
            let mut chunks = self.chunks.lock();
            chunk = root.load(Ordering::Acquire);
            if chunk.is_null() {
                chunks.push(Box::new(PageDirectoryChunk::new()));
                chunk = std::ptr::from_ref::<PageDirectoryChunk<T>>(
                    chunks.last().expect("the chunk was just appended").as_ref(),
                )
                .cast_mut();
                root.store(chunk, Ordering::Release);
            }
        }

        // SAFETY: `chunk` points into one of the Boxes retained in `chunks`.
        // Boxes are never removed, so the allocation remains stable.
        unsafe { &*chunk }.pages[index % PAGE_DIRECTORY_CHUNK_SIZE]
            .store(Arc::as_ptr(page).cast_mut(), Ordering::Release);
    }

    fn get(&self, index: usize) -> Option<&T> {
        let root = self.roots.get(index / PAGE_DIRECTORY_CHUNK_SIZE)?;
        let chunk = root.load(Ordering::Acquire);
        if chunk.is_null() {
            return None;
        }
        // SAFETY: `publish` retains this chunk's Box for this directory's
        // lifetime and stores page pointers only after their owning Arc is in
        // the immutable-snapshot directory.
        let page = unsafe { &*chunk }.pages[index % PAGE_DIRECTORY_CHUNK_SIZE].load(Ordering::Acquire);
        if page.is_null() {
            None
        } else {
            // SAFETY: page entries are appended but never removed or replaced,
            // and every future `pages` snapshot retains the same Arc.
            Some(unsafe { &*page })
        }
    }
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
/// *recycling*, not just freeing: links return to `empty_links` and pages
/// return to `empty_pages`.
#[derive(Debug, Clone, Copy)]
enum Retired {
    /// A freed row slot: hand the slot back to the empty-link allocator unless
    /// a whole-page retirement supersedes it.
    Link(Link),
    /// A wholly emptied page: purge any of its stale empty links, then hand
    /// the page back to the empty-page allocator.
    Page(PageId),
}

/// Page storage with row-granular read/write exclusion.
///
/// # Read synchronization
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
/// 3. `pages_write` (only for appending a page, with no page lock held);
/// 4. one or two per-page `Data::access` locks — two only in the vacuum row
///    move, always in ascending page-id order;
/// 5. one exact cell lock, or the empty-link registry's `op_lock`.
///
/// Reclamation holds the retirement queue, then briefly acquires the
/// empty-link/page registries; nothing acquires the retirement queue while
/// holding either registry (or any page/row lock), so the order is acyclic.
/// Callers must not invoke reclamation while retaining the retirement-queue
/// guard.
#[derive(Debug)]
pub struct DataPages<Row, const DATA_LENGTH: usize = DATA_INNER_LENGTH>
where
    Row: StorableRow,
{
    /// Read-side grace periods protecting the interval from index lookup
    /// until an immutable row version has been acquired. Owned by this table:
    /// a reader of another table never delays reclamation here.
    epoch: EpochDomain,

    /// Retired items in retirement order, awaiting grace expiry.
    retired: Mutex<VecDeque<Retired>>,

    /// How many queued retirements' grace periods have expired. Incremented
    /// by deferred epoch markers; consumed (front-of-queue) by reclaimers.
    /// Shared with the markers through an `Arc` so a marker outliving the
    /// table stays sound.
    reclaimable: Arc<AtomicUsize>,

    /// Queue length mirror, so mutations skip reclamation without locking
    /// when there is no work pending.
    pending_retirements: AtomicUsize,

    /// How many queued retirements are whole pages. A reclaim sweep has to
    /// know whether any page retirement is queued behind the links it is
    /// about to free, and the only way to answer that from the queue itself
    /// is to scan all of it, on every sweep, which is quadratic in the
    /// backlog. Pages are retired rarely, so counting them makes the answer
    /// one relaxed load in the case that matters.
    queued_page_retirements: AtomicUsize,

    /// Immutable page-directory snapshots. Reads load one snapshot without a
    /// shared read-modify-write; rare growth copies and swaps the short vector.
    pages: ArcSwap<Vec<Arc<Data<<Row as StorableRow>::WrappedRow, DATA_LENGTH>>>>,
    /// Stable pointers for point access without ArcSwap's shared snapshot
    /// accounting. The corresponding `Arc`s remain owned by `pages`.
    page_directory: PageDirectory<Data<<Row as StorableRow>::WrappedRow, DATA_LENGTH>>,
    pages_write: Mutex<()>,

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
    fn page_ref(
        &self,
        page_id: PageId,
    ) -> Result<&Data<<Row as StorableRow>::WrappedRow, DATA_LENGTH>, ExecutionError> {
        let index = page_id_mapper(page_id.into());
        if let Some(page) = self.page_directory.get(index) {
            return Ok(page);
        }

        let page = {
            let pages = self.pages.load();
            pages.get(index).map(Arc::as_ptr)
        }
        .ok_or(ExecutionError::PageNotFound(page_id))?;

        // SAFETY: as above, the current directory retains this allocation and
        // all future directory snapshots clone its Arc.
        Ok(unsafe { &*page })
    }

    fn publish_page(&self, page: &Arc<Data<<Row as StorableRow>::WrappedRow, DATA_LENGTH>>) {
        let index = page_id_mapper(page.id.into());
        self.page_directory.publish(index, page);
    }

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

    fn page_row(&self, link: Link) -> Result<(Row, u8), ExecutionError>
    where
        <<Row as StorableRow>::WrappedRow as Archive>::Archived:
            Portable + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        let page = self.page_ref(link.page_id)?;
        let _cell_guard = page.read_cell(link).map_err(ExecutionError::DataPageError)?;
        let wrapped = page.get_row(link).map_err(ExecutionError::DataPageError)?;
        let flags = Self::publication_flags(&wrapped);
        Ok((wrapped.get_inner(), flags))
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
    fn retire(&self, item: Retired) {
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
    fn retire_many(&self, items: impl IntoIterator<Item = Retired>) {
        let mut queued = 0usize;
        let mut queued_pages = 0usize;
        let len = {
            let mut retired = self.retired.lock();
            for item in items {
                if matches!(item, Retired::Page(_)) {
                    queued_pages += 1;
                }
                retired.push_back(item);
                queued += 1;
            }
            // Both counters are published while the queue lock is still held,
            // because they are not statistics: `reclaim_retired` reads
            // `queued_page_retirements` to decide whether it must look for
            // page retirements queued behind the links it is about to free.
            //
            // Incrementing after the lock was released opened exactly the race
            // the counter exists to close. A reclaimer with already-expired
            // claims could take the queue in the gap, see the page retirement
            // in it, read the counter as zero, skip building `queued_pages`,
            // and hand an older link from that same page back to the
            // allocator. A row written through that link is then destroyed
            // when the page retirement matures and the page is reset.
            //
            // The queue and its two mirrors are one state transition. Anything
            // that observes the queue must observe the counters that describe
            // it.
            if queued_pages > 0 {
                self.queued_page_retirements.fetch_add(queued_pages, Ordering::Release);
            }
            self.pending_retirements.fetch_add(queued, Ordering::Release);
            retired.len()
        };
        if queued == 0 {
            return;
        }
        if len >= RETIREMENT_BACKLOG_WARN_AT && len.is_power_of_two() {
            tracing::warn!(len, "row retirement backlog is growing");
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
    /// Drains whatever retirements have expired, now.
    ///
    /// For consumers of freed storage that plan from the empty-link registry
    /// rather than allocating through it. Vacuum is the one that matters:
    /// it chooses which pages to compact from `get_per_page_info`, so with
    /// reclamation deferred it would plan against a stale picture and skip
    /// pages whose rows were deleted but not yet reclaimed.
    ///
    /// `insert` and `allocate_new_or_pop_free` do not need this. They already
    /// sweep on their own path, because they are about to ask for storage
    /// rather than to reason about it.
    pub fn reclaim_pending(&self) {
        // Drain everything currently reclaimable, not one batch of it.
        //
        // `reclaim_retired` deliberately stops at `RECLAIM_BATCH_LIMIT`, which
        // is right on a mutation path: a delete must not absorb an unbounded
        // backlog. It is wrong here. Vacuum plans from the empty-link registry
        // immediately after this call, so with a backlog larger than one batch
        // it would choose pages from a picture missing everything in the later
        // batches, and a successful vacuum would leave reclaimable space
        // untouched.
        //
        // Bounded by progress rather than by a count: each pass either shrinks
        // the queue or is blocked by a live reader, and the second case stops
        // the loop rather than spinning against a pin that is not going
        // anywhere.
        loop {
            let before = self.pending_retirements.load(Ordering::Acquire);
            if before == 0 {
                return;
            }
            self.reclaim_retired();
            if self.pending_retirements.load(Ordering::Acquire) == before {
                return;
            }
        }
    }

    /// Reclaims only once the queue has grown past [`RECLAIM_BACKLOG_TRIGGER`].
    ///
    /// For paths that retire storage rather than consume it. See that constant
    /// for why they should not be sweeping on every call.
    fn reclaim_if_backlogged(&self) {
        if self.pending_retirements.load(Ordering::Acquire) >= RECLAIM_BACKLOG_TRIGGER {
            self.reclaim_retired();
        }
    }

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
        //
        // Building that set means reading the whole queue, so it is built
        // only when a page retirement is actually queued. Otherwise every
        // sweep would scan the entire backlog to learn there is nothing to
        // skip, which is quadratic in the backlog and shows up the moment
        // reclamation runs behind the mutations feeding it.
        let queued_pages: HashSet<PageId> = if self.queued_page_retirements.load(Ordering::Acquire) == 0 {
            HashSet::new()
        } else {
            retired
                .iter()
                .filter_map(|item| match item {
                    Retired::Page(page_id) => Some(*page_id),
                    _ => None,
                })
                .collect()
        };

        // Freed links are collected and restored together. Reclamation runs
        // in retirement order, so a workload deleting in key order frees a
        // contiguous run, and the registry can merge such a run into a single
        // insertion instead of coalescing once per link.
        let mut freed: Vec<Link> = Vec::new();

        for _ in 0..take {
            let Some(item) = retired.pop_front() else {
                break;
            };
            match item {
                Retired::Link(link) => {
                    if !queued_pages.contains(&link.page_id) {
                        freed.push(link);
                    }
                }
                Retired::Page(page_id) => {
                    // `queued_pages` has already kept this page's links out of
                    // the buffer, so flushing here is not what upholds the
                    // invariant. It keeps the buffer from outliving a purge
                    // regardless: batching is a change to *when* links are
                    // restored, and the one ordering that must not drift is
                    // restoring them after a page they belong to was reset.
                    self.empty_links.push_many(&freed);
                    freed.clear();

                    // Purge stale fragments of this page from the link
                    // allocator before exposing the whole page for reuse.
                    self.empty_links.remove_link_for_page(page_id);
                    self.empty_pages.write().push_back(page_id);
                    self.queued_page_retirements.fetch_sub(1, Ordering::Release);
                }
            }
            self.pending_retirements.fetch_sub(1, Ordering::Release);
        }

        self.empty_links.push_many(&freed);
    }

    pub fn new() -> Self {
        let page = Arc::new(Data::new(1.into()));
        let pages = vec![page];
        Self {
            epoch: EpochDomain::new(),
            retired: Mutex::new(VecDeque::new()),
            reclaimable: Arc::new(AtomicUsize::new(0)),
            pending_retirements: AtomicUsize::new(0),
            queued_page_retirements: AtomicUsize::new(0),
            // We are starting ID's from `1` because `0`'s page in file is info page.
            page_directory: PageDirectory::new(&pages),
            pages: ArcSwap::from_pointee(pages),
            pages_write: Mutex::new(()),
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
            let page_directory = PageDirectory::new(&vec);
            Self {
                epoch: EpochDomain::new(),
                retired: Mutex::new(VecDeque::new()),
                reclaimable: Arc::new(AtomicUsize::new(0)),
                pending_retirements: AtomicUsize::new(0),
                queued_page_retirements: AtomicUsize::new(0),
                page_directory,
                pages: ArcSwap::from_pointee(vec),
                pages_write: Mutex::new(()),
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
            let page = self.page_ref(link.page_id)?;
            let _page_guard = page.access.write();

            match unsafe { page.try_save_row_by_link(&general_row, link) } {
                Ok((link, left_link)) => {
                    if let Some(l) = left_link {
                        self.empty_links.push(l);
                    }
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
                    | DataExecutionError::DeserializeError
                    | DataExecutionError::LiveCellCountOverflow
                    | DataExecutionError::LiveCellCountUnderflow => return Err(e.into()),
                },
            }
        }

        loop {
            let (link, tried_page) = {
                let current_page_id = self.current_page_id.load(Ordering::Acquire);
                let current_page = page_id_mapper(current_page_id as usize);
                let page = self.page_ref(current_page_id.into())?;
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
                                let page = self.page_ref(page_id)?;
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
                    | DataExecutionError::InvalidLink
                    | DataExecutionError::LiveCellCountOverflow
                    | DataExecutionError::LiveCellCountUnderflow => return Err(e.into()),
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
        let _write = self.pages_write.lock();
        if tried_page == page_id_mapper(self.current_page_id.load(Ordering::Acquire) as usize) {
            let index = self.last_page_id.fetch_add(1, Ordering::AcqRel) + 1;
            let pages = self.pages.load_full();
            let mut next = (*pages).clone();
            let page = Arc::new(Data::new(index.into()));
            next.push(page.clone());
            debug_assert_eq!(next.len(), pages.len() + 1);
            debug_assert!(
                next[..pages.len()]
                    .iter()
                    .zip(pages.iter())
                    .all(|(new, old)| Arc::ptr_eq(new, old))
            );
            self.pages.store(Arc::new(next));
            self.publish_page(&page);
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
            let pages = self.pages.load();
            let index = page_id_mapper(page_id.into());
            let page = pages[index].clone();
            {
                let _page_guard = page.access.write();
                page.reset();
            }

            return page;
        }

        let _write = self.pages_write.lock();
        let index = self.last_page_id.fetch_add(1, Ordering::AcqRel) + 1;
        let page = Arc::new(Data::new(index.into()));
        let pages = self.pages.load_full();
        let mut next = (*pages).clone();
        next.push(page.clone());
        debug_assert_eq!(next.len(), pages.len() + 1);
        debug_assert!(
            next[..pages.len()]
                .iter()
                .zip(pages.iter())
                .all(|(new, old)| Arc::ptr_eq(new, old))
        );
        self.pages.store(Arc::new(next));
        self.publish_page(&page);

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
        self.page_row(link).map(|(row, _)| row)
    }

    pub fn select_non_ghosted(&self, link: Link) -> Result<Row, ExecutionError>
    where
        Row: Archive
            + Clone
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived:
            Portable + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        let (row, flags) = self.page_row(link)?;
        if flags & GHOSTED != 0 {
            return Err(ExecutionError::Ghosted);
        }
        if flags & DELETED != 0 {
            return Err(ExecutionError::Deleted);
        }
        Ok(row)
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
        let pages = self.pages.load();
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
        let (row, flags) = self.page_row(link)?;
        if flags & GHOSTED != 0 {
            return Err(ExecutionError::Ghosted);
        }
        if flags & VACUUMED != 0 {
            return Err(ExecutionError::Vacuumed);
        }
        if flags & DELETED != 0 {
            return Err(ExecutionError::Deleted);
        }
        Ok(row)
    }

    #[cfg_attr(feature = "perf_measurements", performance_measurement(prefix_name = "DataPages"))]
    pub fn with_ref<Op, Res>(&self, link: Link, op: Op) -> Result<Res, ExecutionError>
    where
        Row: Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
        Op: Fn(&<<Row as StorableRow>::WrappedRow as Archive>::Archived) -> Res,
    {
        let page = self.page_ref(link.page_id)?;
        let _cell_guard = page.read_cell(link).map_err(ExecutionError::DataPageError)?;
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
        let page = self.page_ref(link.page_id)?;
        let _cell_guard = page.write_cell(link).map_err(ExecutionError::DataPageError)?;
        let res = {
            let gen_row = unsafe {
                page.get_mut_row_ref(link)
                    .map_err(ExecutionError::DataPageError)?
                    .unseal_unchecked()
            };
            op(gen_row)
        };

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
        let page = self.page_ref(link.page_id)?;
        let _cell_guard = page.write_cell(link).map_err(ExecutionError::DataPageError)?;
        let gen_row = <Row as StorableRow>::WrappedRow::from_inner(row.clone());
        let result = unsafe {
            page.save_row_by_link(&gen_row, link)
                .map_err(ExecutionError::DataPageError)
        }?;
        Ok(result)
    }

    /// In-place update of an already-live row at `link`: re-serialize the full
    /// row into the SAME slot and leave it LIVE (unghosted). A live row that is
    /// edited must stay visible to readers.
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
    /// changed. The exact cell guard excludes readers of this cell during the
    /// copy while unrelated cells proceed independently.
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
        let page = self.page_ref(link.page_id)?;
        let _cell_guard = page.write_cell(link).map_err(ExecutionError::DataPageError)?;
        // Write the new bytes into the slot. The preserving variant requires the
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
        // Clear the ghost bit on the stored row. A fresh `from_inner` wrapper
        // is ghosted, but this is an update of an already-live cell.
        unsafe {
            page.get_mut_row_ref(link)
                .map_err(ExecutionError::DataPageError)?
                .unseal_unchecked()
                .unghost();
        }
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
        self.remove_cell(link)?;

        self.row_count.fetch_sub(1, Ordering::Relaxed);
        self.retire(Retired::Link(link));
        self.reclaim_if_backlogged();
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
                Ok(()) => {
                    ghosted += 1;
                    if let Err(error) = self.remove_cell(*link) {
                        failure = Some(error);
                        break;
                    }
                }
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }

        if ghosted > 0 {
            self.row_count.fetch_sub(ghosted as u64, Ordering::Relaxed);
            self.retire_many(links[..ghosted].iter().map(|link| Retired::Link(*link)));
            self.reclaim_if_backlogged();
        }

        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub fn select_raw(&self, link: Link) -> Result<Vec<u8>, ExecutionError> {
        let page = self.page_ref(link.page_id)?;
        let _cell_guard = page.read_cell(link).map_err(ExecutionError::DataPageError)?;
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

        if let Ok(page) = self.page_ref(page_id) {
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
        let pages = self.pages.load();
        let page = pages.get(page_id_mapper(page_id.into()))?;
        Some(page.clone())
    }

    /// Registers an already-indexed cell while rebuilding runtime metadata
    /// for a persisted table.
    pub fn register_cell(&self, link: Link) -> Result<(), ExecutionError> {
        let page = self.page_ref(link.page_id)?;
        page.register_cell(link).map_err(ExecutionError::DataPageError)?;
        Ok(())
    }

    fn remove_cell(&self, link: Link) -> Result<(), ExecutionError> {
        let page = self.page_ref(link.page_id)?;
        page.remove_cell(link).map_err(ExecutionError::DataPageError)?;
        Ok(())
    }

    pub(crate) fn page_has_cells(&self, page_id: PageId) -> Result<bool, ExecutionError> {
        let page = self.page_ref(page_id)?;
        Ok(page.has_live_cells())
    }

    pub(crate) fn page_live_cell_count(&self, page_id: PageId) -> Result<u32, ExecutionError> {
        let page = self.page_ref(page_id)?;
        Ok(page.live_cell_count())
    }

    pub(crate) fn set_loaded_row_count(&self, count: usize) -> Result<(), ExecutionError> {
        let count = u64::try_from(count).map_err(|_| ExecutionError::RowCountOverflow)?;
        self.row_count.store(count, Ordering::Release);
        Ok(())
    }

    /// Completes the vacuum's source-side accounting after every index has
    /// been swung to the destination link.
    pub(crate) fn remove_moved_cell(&self, link: Link) -> Result<(), ExecutionError> {
        self.remove_cell(link)
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
        let pages = self.pages.load();
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
        let from_page = self.page_ref(from_link.page_id)?;
        let to_page = self.page_ref(to_page_id)?;
        // Only the source is reachable from an index while this copy runs.
        // Its exact-cell guard prevents a reader from borrowing the bytes
        // while the vacuum flag is changed. Destination bytes are published
        // only after the complete copy and index swing.
        let _cell_guard = from_page.write_cell(from_link).map_err(ExecutionError::DataPageError)?;

        let raw_data = from_page
            .get_raw_row(from_link)
            .map_err(ExecutionError::DataPageError)?;
        // Copy to the destination BEFORE flagging the source. The vacuumed
        // flag used to be set first, so a failing destination save returned
        // with the flag durably set in the source page image: after a restart
        // the row would load as vacuumed with no copy anywhere (row loss).
        // The source cell guard keeps its bytes private, and destination bytes
        // are unreachable until the caller publishes `new_link`, so readers
        // cannot observe this intermediate state.
        let new_link = to_page.save_raw_row(&raw_data).map_err(ExecutionError::DataPageError)?;
        let archived = unsafe {
            from_page
                .get_mut_row_ref(from_link)
                .map_err(ExecutionError::DataPageError)?
                .unseal_unchecked()
        };
        archived.set_in_vacuum_process();

        Ok((raw_data, new_link))
    }

    pub fn get_page_count(&self) -> usize {
        self.pages.load().len()
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

    /// Data pages allocated, including any currently on the empty list.
    ///
    /// The table's memory is its pages, so this times `DATA_LENGTH` is what it
    /// is holding from the allocator. It is the measure that verifies a
    /// vacuum: reclaiming memory is the whole point of a sweep, and a cost
    /// figure without it cannot be checked, because a sweep that never runs
    /// looks exactly like a sweep that is free.
    pub fn allocated_pages(&self) -> usize {
        self.pages.load().len()
    }

    /// Heap bytes reserved by the fixed-size data-page allocations.
    pub fn allocated_bytes(&self) -> usize {
        self.pages.load().len() * std::mem::size_of::<Data<<Row as StorableRow>::WrappedRow, DATA_LENGTH>>()
    }

    /// Pages allocated but currently on the empty list, so reusable without
    /// asking the allocator for more.
    pub fn reusable_pages(&self) -> usize {
        self.empty_pages.read().len()
    }

    /// Retirements queued but not yet swept into the registry.
    ///
    /// This moves on every delete, where the registry only moves when the
    /// backlog flushes, so it is the signal for "is a delete burst still
    /// running" and the registry's byte total is not: between flushes the
    /// bytes sit still while deletes are streaming.
    pub fn pending_retirements(&self) -> usize {
        self.pending_retirements.load(Ordering::Acquire)
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

    /// Makes an already allocated page the append target for a vacuum pass.
    ///
    /// Vacuum uses this only when the old current page is itself fragmented.
    /// Rotating first lets that old page become a normal source while the new
    /// current page serves as the sweep's first destination. Concurrent
    /// inserts are safe: the insert path rechecks `current_page_id` under the
    /// page barrier before writing and retries if the target changed.
    pub(crate) fn rotate_current_for_vacuum(&self, page_id: PageId) {
        debug_assert!(
            self.get_page(page_id).is_some(),
            "vacuum current page must be allocated"
        );
        self.current_page_id.store(page_id.into(), Ordering::Release);
    }
}

#[derive(Debug, Display, Error, From, PartialEq)]
pub enum ExecutionError {
    DataPageError(DataExecutionError),

    #[display("row count exceeds u64")]
    RowCountOverflow,

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
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;
    use std::time::Instant;

    use parking_lot::RwLock;
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

        pub is_ghosted: bool,

        pub is_deleted: bool,

        pub is_in_vacuum_process: bool,
    }

    impl<Inner> RowWrapper<Inner> for GeneralRow<Inner> {
        fn get_inner(self) -> Inner {
            self.inner
        }

        fn is_ghosted(&self) -> bool {
            self.is_ghosted
        }

        fn is_vacuumed(&self) -> bool {
            self.is_in_vacuum_process
        }

        fn is_deleted(&self) -> bool {
            self.is_deleted
        }

        /// Creates new [`GeneralRow`] from `Inner`.
        fn from_inner(inner: Inner) -> Self {
            Self {
                inner,
                is_ghosted: true,
                is_deleted: false,
                is_in_vacuum_process: false,
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
            self.is_in_vacuum_process = true
        }
        fn delete(&mut self) {
            self.is_deleted = true
        }
        fn is_deleted(&self) -> bool {
            self.is_deleted
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
    fn same_row_reader_waits_while_update_is_incomplete() {
        let pages = Arc::new(DataPages::<TestRow>::new());
        let link = pages.insert(TestRow { a: 0, b: 0 }).unwrap();
        let other_link = pages.insert(TestRow { a: 9, b: 9 }).unwrap();
        unsafe {
            pages.with_mut_ref(link, |row| row.unghost()).unwrap();
            pages.with_mut_ref(other_link, |row| row.unghost()).unwrap();
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

        assert!(
            matches!(
                read_rx.recv_timeout(Duration::from_millis(50)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "same-cell reader must wait instead of observing a torn row"
        );

        let (other_tx, other_rx) = mpsc::channel();
        let other_pages = pages.clone();
        let other_reader = thread::spawn(move || {
            other_tx.send(other_pages.select_non_ghosted(other_link)).unwrap();
        });
        assert_eq!(
            other_rx.recv_timeout(Duration::from_millis(50)).unwrap(),
            Ok(TestRow { a: 9, b: 9 }),
            "a writer on one cell must not block a different cell on the same page"
        );
        other_reader.join().unwrap();

        finish_update_tx.send(()).unwrap();
        writer.join().unwrap();
        assert_eq!(read_rx.recv().unwrap(), Ok(TestRow { a: 1, b: 1 }));
        reader.join().unwrap();
        assert_eq!(pages.select_non_ghosted(link), Ok(TestRow { a: 1, b: 1 }));
    }

    #[test]
    fn failed_exact_length_update_preserves_page_bytes() {
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
        pages.mark_page_empty(old_link.page_id);

        let temporary_page = pages.allocate_new_or_pop_free();
        assert_ne!(
            temporary_page.id, old_link.page_id,
            "vacuumed source page was reused while an old index reader was active"
        );
        assert_eq!(
            pages.select_non_ghosted(old_link),
            Ok(TestRow { a: 1, b: 1 }),
            "the old page must survive until the reader leaves"
        );

        drop(read_guard);
        pages.reclaim_retired();
        assert!(pages.get_empty_pages().contains(&old_link.page_id));

        let reused_page = pages.allocate_new_or_pop_free();
        assert_eq!(reused_page.id, old_link.page_id);
        assert_eq!(reused_page.free_offset.load(Ordering::Acquire), 0);
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

    /// The case a single-sweep test cannot reach: the link's grace period
    /// expires before the page's, so one sweep frees the link while the whole
    /// page retirement is still queued behind it.
    ///
    /// If the link were republished here, an insert could pop it and start
    /// writing through it before the later sweep hands the same page to the
    /// page allocator, which is two allocators owning overlapping storage.
    /// The existing whole-page test processes both retirements in one sweep
    /// and stays green even with the queued-page check disabled entirely;
    /// this one does not.
    #[test]
    fn a_link_is_not_reused_while_its_page_retirement_is_still_queued() {
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

        // Queue the row's retirement and then the page's, with a reader
        // pinned throughout so neither is reclaimed inline.
        let read_guard = pages.read_guard();
        pages.delete(old_link).unwrap();
        pages.mark_page_empty(old_link.page_id);
        drop(read_guard);

        // Let exactly one retirement through. A sweep is capped at
        // `RECLAIM_BATCH_LIMIT` and only drains what has actually expired, so
        // a partial sweep is the ordinary case rather than a contrived one.
        pages.reclaimable.store(1, Ordering::Release);
        pages.reclaim_retired();

        assert!(
            pages
                .get_empty_links()
                .iter()
                .all(|link| link.page_id != old_link.page_id),
            "a link must not be handed back for reuse while its page is queued for whole-page reclamation"
        );

        // And once the page retirement is processed, the page itself is the
        // thing that becomes reusable.
        pages.reclaimable.store(1, Ordering::Release);
        pages.reclaim_retired();
        assert!(pages.get_empty_pages().contains(&old_link.page_id));
        assert!(
            pages
                .get_empty_links()
                .iter()
                .all(|link| link.page_id != old_link.page_id),
            "processing the page must not resurrect its inner links either"
        );
    }

    /// A backlog larger than one reclaim batch must still be fully visible to
    /// whoever asks for a drain.
    ///
    /// `reclaim_retired` stops at `RECLAIM_BATCH_LIMIT`, which is correct on a
    /// mutation path and wrong for vacuum: it plans from the empty-link
    /// registry immediately after asking, so one bounded pass left it choosing
    /// pages from a picture missing everything past the first 256 entries.
    #[test]
    fn reclaim_pending_drains_a_backlog_larger_than_one_batch() {
        let pages = DataPages::<TestRow>::new();

        let mut links = Vec::new();
        for i in 0..(super::RECLAIM_BATCH_LIMIT * 3) {
            let link = pages.insert(TestRow { a: i as u64, b: 0 }).unwrap();
            unsafe {
                pages.with_mut_ref(link, |r| r.unghost()).unwrap();
            }
            links.push(link);
        }
        // Ghost and retire them all without letting any mutation reclaim.
        let guard = pages.read_guard();
        for link in &links {
            pages.delete(*link).unwrap();
        }
        drop(guard);

        assert!(
            pages.pending_retirements.load(Ordering::Acquire) > super::RECLAIM_BATCH_LIMIT,
            "the fixture must build a backlog bigger than one batch"
        );

        pages.reclaim_pending();

        assert_eq!(
            pages.pending_retirements.load(Ordering::Acquire),
            0,
            "reclaim_pending left {} retirements queued, so vacuum would plan against a stale registry",
            pages.pending_retirements.load(Ordering::Acquire)
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
        let vacuumed = pages.with_ref(link, |r| r.is_in_vacuum_process).unwrap();
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

        // The freed link is deliberately not registered for reuse yet:
        // reclamation is driven by the consumer of the storage rather than the
        // producer of it, so a delete queues the retirement and the next
        // insert is what turns it back into free space. Asserting the
        // registry's contents here would be asserting that timing rather than
        // the guarantee, and the guarantee is the line below.
        let row = TestRow { a: 20, b: 20 };
        let new_link = pages.insert(row).unwrap();
        assert_eq!(new_link, link, "the next insert must reuse the deleted row's storage");
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
