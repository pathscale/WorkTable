use alloc::vec::Vec;
use core::cell::UnsafeCell;
use core::fmt::Debug;
use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};
#[cfg(not(wt_loom))]
use core::sync::atomic::{AtomicU32 as CellState, AtomicUsize as CellOwner};
use core::sync::atomic::{AtomicU32, Ordering};
#[cfg(wt_loom)]
use loom::sync::atomic::{AtomicU32 as CellState, AtomicUsize as CellOwner};

use data_bucket::page::INNER_PAGE_SIZE;
use data_bucket::page::PageId;
use data_bucket::{DataPage, GeneralPage};
use derive_more::{Display, Error};
#[cfg(feature = "perf_measurements")]
use performance_measurement_codegen::performance_measurement;
use rkyv::{
    Archive, Deserialize, Portable, Serialize,
    api::high::HighDeserializer,
    rancor::Strategy,
    seal::Seal,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    with::{AtomicLoad, Relaxed, Skip, Unsafe},
};

use crate::in_memory::ArchivedRowWrapper;
use crate::prelude::Link;

#[cfg(all(not(wt_loom), not(any(unix, windows))))]
compile_error!("archived cell locks require a hosted unix or Windows target");

const CELL_LOCK_SLOTS: usize = 256;
const CELL_READER_MASK: u32 = (1_u32 << 31) - 1;
const CELL_WRITER: u32 = 1 << 31;

#[derive(Debug)]
struct CellLocks {
    states: [CellState; CELL_LOCK_SLOTS],
    owners: [CellOwner; CELL_LOCK_SLOTS],
    nested_reads: CellState,
}

impl Default for CellLocks {
    fn default() -> Self {
        Self {
            states: core::array::from_fn(|_| CellState::new(0)),
            owners: core::array::from_fn(|_| CellOwner::new(0)),
            nested_reads: CellState::new(0),
        }
    }
}

#[inline]
fn current_owner() -> usize {
    #[cfg(wt_loom)]
    {
        use core::hash::{Hash, Hasher};

        let mut hasher = rustc_hash::FxHasher::default();
        loom::thread::current().id().hash(&mut hasher);
        (hasher.finish() as usize).max(1)
    }

    #[cfg(all(not(wt_loom), unix))]
    {
        // SAFETY: pthread_self takes no arguments and returns the live calling
        // thread's identity. POSIX keeps it unique until this thread exits;
        // a cell guard necessarily drops before that can happen.
        (unsafe { libc::pthread_self() } as usize).max(1)
    }

    #[cfg(all(not(wt_loom), windows))]
    {
        // SAFETY: GetCurrentThreadId takes no arguments and cannot fail. The
        // ID cannot be reused while the calling thread and its guard are live.
        (unsafe { windows_sys::Win32::System::Threading::GetCurrentThreadId() } as usize).max(1)
    }
}

impl CellLocks {
    #[inline]
    fn start(link: Link) -> usize {
        // Record starts are aligned to the archived row shape, so their low
        // bits alone are a poor stripe selector. Mix all offset bits before
        // taking the power-of-two table index.
        let mut key = link.offset;
        key ^= key >> 16;
        key = key.wrapping_mul(0x7feb_352d);
        key ^= key >> 15;
        key = key.wrapping_mul(0x846c_a68b);
        key ^= key >> 16;
        key as usize & (CELL_LOCK_SLOTS - 1)
    }

    #[inline]
    fn wait(spins: &mut u32) {
        #[cfg(wt_loom)]
        {
            let _ = spins;
            loom::thread::yield_now();
        }
        #[cfg(not(wt_loom))]
        if *spins < 64 {
            core::hint::spin_loop();
            *spins += 1;
        } else {
            crate::util::yield_now();
        }
    }

    fn read(&self, link: Link) -> Result<CellReadGuard<'_>, ExecutionError> {
        let index = Self::start(link);
        let state = &self.states[index];
        let mut spins = 0;
        loop {
            let current = state.load(Ordering::Acquire);
            if current & CELL_WRITER == 0
                && current & CELL_READER_MASK != CELL_READER_MASK
                && state
                    .compare_exchange_weak(current, current + 1, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
            {
                return Ok(CellReadGuard { state });
            }
            if current & CELL_WRITER != 0 && self.owners[index].load(Ordering::Acquire) == current_owner() {
                let writer_key = current & CELL_READER_MASK;
                let requested_key = link.offset.checked_add(1).ok_or(ExecutionError::InvalidLink)?;
                if writer_key == requested_key {
                    return Err(ExecutionError::CellLockReentry);
                }
                if writer_key != 0 {
                    // The outer write owns this entire stripe, so no other
                    // thread can touch either row. Lending a different row to
                    // its callback is therefore safe without incrementing the
                    // state that callback itself must eventually release.
                    let previous = self.nested_reads.fetch_add(1, Ordering::Relaxed);
                    debug_assert_ne!(previous & CELL_READER_MASK, CELL_READER_MASK);
                    return Ok(CellReadGuard {
                        state: &self.nested_reads,
                    });
                }
            }
            Self::wait(&mut spins);
        }
    }

    fn write(&self, link: Link) -> Result<CellWriteGuard<'_>, ExecutionError> {
        let index = Self::start(link);
        let state = &self.states[index];
        let owner = &self.owners[index];
        let mut spins = 0;
        loop {
            let current = state.load(Ordering::Acquire);
            if current & CELL_WRITER == 0
                && state
                    .compare_exchange_weak(current, current | CELL_WRITER, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
            {
                break;
            }
            if current & CELL_WRITER != 0 && owner.load(Ordering::Acquire) == current_owner() {
                return Err(ExecutionError::CellLockReentry);
            }
            Self::wait(&mut spins);
        }
        while state.load(Ordering::Acquire) != CELL_WRITER {
            Self::wait(&mut spins);
        }
        let writer_key = link.offset.checked_add(1).ok_or(ExecutionError::InvalidLink)?;
        debug_assert_eq!(writer_key & CELL_WRITER, 0);
        state.store(CELL_WRITER | writer_key, Ordering::Relaxed);
        owner.store(current_owner(), Ordering::Release);
        Ok(CellWriteGuard {
            state,
            owner,
            _not_send: PhantomData,
        })
    }

    fn reset(&self) {
        for owner in &self.owners {
            owner.store(0, Ordering::Relaxed);
        }
        for state in &self.states {
            state.store(0, Ordering::Release);
        }
        self.nested_reads.store(0, Ordering::Relaxed);
    }
}

/// Shared access to one exact archived cell.
pub(crate) struct CellReadGuard<'a> {
    state: &'a CellState,
}

impl Drop for CellReadGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        let previous = self.state.fetch_sub(1, Ordering::Release);
        debug_assert_ne!(previous & CELL_READER_MASK, 0, "cell reader count underflow");
    }
}

/// Exclusive access to one exact archived cell.
pub(crate) struct CellWriteGuard<'a> {
    state: &'a CellState,
    owner: &'a CellOwner,
    _not_send: PhantomData<*mut ()>,
}

impl Drop for CellWriteGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.owner.store(0, Ordering::Relaxed);
        self.state.store(0, Ordering::Release);
    }
}

/// Length of the [`Data`] page header.
pub const DATA_HEADER_LENGTH: usize = 4;

/// Length of the inner [`Data`] page part.
pub const DATA_INNER_LENGTH: usize = INNER_PAGE_SIZE - DATA_HEADER_LENGTH;

#[derive(Archive, Clone, Copy, Debug, Deserialize, Serialize)]
#[repr(C, align(16))]
pub struct AlignedBytes<const N: usize>(pub [u8; N]);

impl<const N: usize> Deref for AlignedBytes<N> {
    type Target = [u8; N];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const N: usize> DerefMut for AlignedBytes<N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Archive, Deserialize, Debug, Serialize)]
pub struct Data<Row, const DATA_LENGTH: usize = DATA_INNER_LENGTH> {
    /// [`PageId`] of the general page represented by this [`Data`] block.
    #[rkyv(with = Skip)]
    pub id: PageId,

    /// Offset to the first free byte on this [`Data`] page.
    #[rkyv(with = AtomicLoad<Relaxed>)]
    pub free_offset: AtomicU32,

    /// Per-page access barrier for the mutable byte image.
    ///
    /// The exclusive side serializes page allocation/reset and append-position
    /// changes. Existing-cell reads and writes use the exact cell byte below,
    /// so unrelated rows on this page do not contend here. Runtime-only:
    /// skipped by rkyv and reconstructed unlocked on load.
    #[rkyv(with = Skip)]
    pub(crate) access: parking_lot::RwLock<()>,

    /// Runtime-only striped reader/writer coordination for archived cells. A
    /// hash collision may conservatively make unrelated writes wait, while
    /// every read/write pair for one offset always uses the same stripe. The
    /// table is outside the archived row image, so lock state never reaches
    /// disk and the beta.17 wrapper layout remains unchanged.
    #[rkyv(with = Skip)]
    cell_locks: CellLocks,

    /// Number of live cells currently published on this page.
    ///
    /// Vacuum gets move candidates from a transient snapshot of the primary
    /// index. It only needs permanent per-page state to prove a source became
    /// empty before reclaiming it. Keeping that proof as one counter removes
    /// the old four-byte entry for every row (and its locked `Vec`) without
    /// weakening the final reclamation check. Runtime-only and rebuilt from
    /// the primary index when a persisted table is loaded.
    #[rkyv(with = Skip)]
    live_cells: AtomicU32,

    /// Inner array of bytes where deserialized `Row`s will be stored.
    #[rkyv(with = Unsafe)]
    inner_data: UnsafeCell<AlignedBytes<DATA_LENGTH>>,

    /// `Row` phantom data.
    _phantom: PhantomData<Row>,
}

unsafe impl<Row, const DATA_LENGTH: usize> Sync for Data<Row, DATA_LENGTH> {}

impl<Row, const DATA_LENGTH: usize> Data<Row, DATA_LENGTH> {
    fn validate_link(&self, link: Link) -> Result<(), ExecutionError> {
        let start = link.offset as usize;
        let end = start
            .checked_add(link.length as usize)
            .ok_or(ExecutionError::InvalidLink)?;
        let initialized = self.free_offset.load(Ordering::Acquire) as usize;
        if link.length == 0 || end > initialized || end > DATA_LENGTH {
            return Err(ExecutionError::InvalidLink);
        }
        Ok(())
    }

    pub(crate) fn read_cell(&self, link: Link) -> Result<CellReadGuard<'_>, ExecutionError>
    where
        Row: Archive,
        <Row as Archive>::Archived: ArchivedRowWrapper,
    {
        self.validate_link(link)?;
        self.cell_locks.read(link)
    }

    pub(crate) fn write_cell(&self, link: Link) -> Result<CellWriteGuard<'_>, ExecutionError>
    where
        Row: Archive,
        <Row as Archive>::Archived: ArchivedRowWrapper,
    {
        self.validate_link(link)?;
        self.cell_locks.write(link)
    }

    /// Creates new [`Data`] page.
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            free_offset: AtomicU32::default(),
            access: parking_lot::RwLock::new(()),
            cell_locks: CellLocks::default(),
            live_cells: AtomicU32::new(0),
            inner_data: UnsafeCell::new(AlignedBytes::<DATA_LENGTH>([0; DATA_LENGTH])),
            _phantom: PhantomData,
        }
    }

    pub fn from_data_page(page: GeneralPage<DataPage<DATA_LENGTH>>) -> Self {
        Self {
            id: page.header.page_id,
            free_offset: AtomicU32::from(page.header.data_length),
            access: parking_lot::RwLock::new(()),
            cell_locks: CellLocks::default(),
            live_cells: AtomicU32::new(0),
            inner_data: UnsafeCell::new(AlignedBytes::<DATA_LENGTH>(page.inner.data)),
            _phantom: PhantomData,
        }
    }

    /// Keep append allocation out of ranges owned by the restored free list.
    pub(crate) fn reserve_restored_range(&self, link: Link) -> Result<(), ExecutionError> {
        let end = (link.offset as usize)
            .checked_add(link.length as usize)
            .ok_or(ExecutionError::InvalidLink)?;
        if link.page_id != self.id || link.length == 0 || end > DATA_LENGTH {
            return Err(ExecutionError::InvalidLink);
        }
        self.free_offset.fetch_max(end as u32, Ordering::Release);
        Ok(())
    }

    pub fn set_page_id(&mut self, id: PageId) {
        self.id = id;
    }

    #[cfg_attr(feature = "perf_measurements", performance_measurement(prefix_name = "DataRow"))]
    pub fn save_row(&self, row: &Row) -> Result<Link, ExecutionError>
    where
        Row: Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
    {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(row).map_err(|_| ExecutionError::SerializeError)?;
        let length = bytes.len();
        if length > DATA_LENGTH {
            return Err(ExecutionError::PageTooSmall {
                need: length,
                allowed: DATA_LENGTH,
            });
        }
        let length = length as u32;
        let offset = self.free_offset.fetch_add(length, Ordering::AcqRel);
        if offset > DATA_LENGTH as u32 - length {
            // Roll back this call's own reservation, or free_offset inflates
            // permanently: it is persisted as the initialized-bytes bound and
            // fed to used-bytes accounting. Subtracting exactly what was added
            // is safe under concurrent adds.
            self.free_offset.fetch_sub(length, Ordering::AcqRel);
            return Err(ExecutionError::PageIsFull {
                need: length,
                left: DATA_LENGTH as i64 - offset as i64,
            });
        }

        let inner_data = unsafe { &mut *self.inner_data.get() };
        inner_data[offset as usize..][..length as usize].copy_from_slice(bytes.as_slice());

        let link = Link {
            page_id: self.id,
            offset,
            length,
        };

        self.register_cell(link)?;

        Ok(link)
    }

    /// Replaces the complete archived row at an existing link.
    ///
    /// # Safety
    ///
    /// The caller must hold this cell's write guard until the copy finishes.
    /// The serialized replacement must have the same archived layout as the
    /// existing row.
    #[cfg_attr(feature = "perf_measurements", performance_measurement(prefix_name = "DataRow"))]
    pub unsafe fn save_row_by_link(&self, row: &Row, link: Link) -> Result<Link, ExecutionError>
    where
        Row: Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
    {
        let bytes = rkyv::to_bytes(row).map_err(|_| ExecutionError::SerializeError)?;
        let length = bytes.len() as u32;
        if length != link.length {
            return Err(ExecutionError::InvalidLink);
        }
        debug_assert_eq!(
            length, link.length,
            "slot length was checked before archived bytes are overwritten"
        );

        let inner_data = unsafe { &mut *self.inner_data.get() };
        inner_data[link.offset as usize..][..link.length as usize].copy_from_slice(bytes.as_slice());

        Ok(link)
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn try_save_row_by_link(&self, row: &Row, mut link: Link) -> Result<(Link, Option<Link>), ExecutionError>
    where
        Row: Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
    {
        let bytes = rkyv::to_bytes(row).map_err(|_| ExecutionError::SerializeError)?;
        let length = bytes.len() as u32;
        if length > link.length {
            return Err(ExecutionError::InvalidLink);
        }

        let link_diff = link.length - length;
        let link_left = if link_diff > 0 {
            link.length -= link_diff;
            Some(Link {
                page_id: link.page_id,
                offset: link.offset + link.length,
                length: link_diff,
            })
        } else {
            None
        };

        let inner_data = unsafe { &mut *self.inner_data.get() };
        inner_data[link.offset as usize..][..link.length as usize].copy_from_slice(bytes.as_slice());

        self.register_cell(link)?;

        Ok((link, link_left))
    }

    /// # Safety
    /// This function is `unsafe` because it returns a mutable reference to an archived row.
    /// The caller must ensure that there are no other references to the same data
    /// while this function is being used, as it could lead to undefined behavior.
    pub unsafe fn get_mut_row_ref(&self, link: Link) -> Result<Seal<'_, <Row as Archive>::Archived>, ExecutionError>
    where
        Row: Archive,
        <Row as Archive>::Archived: Portable,
    {
        let inner_data = unsafe { &mut *self.inner_data.get() };
        let bytes = &mut inner_data[link.offset as usize..(link.offset + link.length) as usize];
        Ok(unsafe { rkyv::access_unchecked_mut::<<Row as Archive>::Archived>(&mut bytes[..]) })
    }

    #[cfg_attr(feature = "perf_measurements", performance_measurement(prefix_name = "DataRow"))]
    pub fn get_row_ref(&self, link: Link) -> Result<&<Row as Archive>::Archived, ExecutionError>
    where
        Row: Archive,
    {
        let inner_data = unsafe { &*self.inner_data.get() };
        let bytes = &inner_data[link.offset as usize..(link.offset + link.length) as usize];
        Ok(unsafe { rkyv::access_unchecked::<<Row as Archive>::Archived>(bytes) })
    }

    pub fn get_row(&self, link: Link) -> Result<Row, ExecutionError>
    where
        Row: Archive,
        <Row as Archive>::Archived: Deserialize<Row, HighDeserializer<rkyv::rancor::Error>>,
    {
        let row = self.get_row_ref(link)?;
        rkyv::deserialize::<_, rkyv::rancor::Error>(row).map_err(|_| ExecutionError::DeserializeError)
    }

    /// Validates persisted bytes before deserializing them.
    ///
    /// The regular in-memory path only reads bytes written by WorkTable in the
    /// same process. Loading persisted data is different: an abrupt shutdown
    /// may leave a link or archived value only partially written. This method
    /// is deliberately reserved for load-time validation so steady-state row
    /// reads keep their existing cost.
    pub fn get_row_checked(&self, link: Link) -> Result<Row, ExecutionError>
    where
        Row: Archive,
        <Row as Archive>::Archived: Portable
            + Deserialize<Row, HighDeserializer<rkyv::rancor::Error>>
            + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    {
        let start = link.offset as usize;
        let end = start
            .checked_add(link.length as usize)
            .ok_or(ExecutionError::InvalidLink)?;
        let initialized = self.free_offset.load(Ordering::Acquire) as usize;
        if link.length == 0 || end > initialized || end > DATA_LENGTH {
            return Err(ExecutionError::InvalidLink);
        }

        let inner_data = unsafe { &*self.inner_data.get() };
        let archived = rkyv::access::<<Row as Archive>::Archived, rkyv::rancor::Error>(&inner_data[start..end])
            .map_err(|_| ExecutionError::DeserializeError)?;
        rkyv::deserialize::<_, rkyv::rancor::Error>(archived).map_err(|_| ExecutionError::DeserializeError)
    }

    pub fn get_raw_row(&self, link: Link) -> Result<Vec<u8>, ExecutionError> {
        let inner_data = unsafe { &*self.inner_data.get() };
        Ok(inner_data[link.offset as usize..(link.offset + link.length) as usize].to_vec())
    }

    /// Moves data within the page from one location to another.
    /// Used for defragmentation - shifts data left to fill gaps.
    ///
    /// # Safety
    /// Caller must ensure:
    /// - Both `from` and `to` links are valid and point to the same page
    /// - `from.length` equals `to.length`
    /// - No other references exist during this operation
    pub unsafe fn move_from_to(&self, from: Link, to: Link) -> Result<(), ExecutionError> {
        if from.length != to.length {
            return Err(ExecutionError::InvalidLink);
        }

        let inner_data = unsafe { &mut *self.inner_data.get() };
        let src_offset = from.offset as usize;
        let dst_offset = to.offset as usize;
        let length = from.length as usize;

        // Use ptr::copy for overlapping memory regions (safe for shifting left)
        // When moving left (dst_offset < src_offset), this works correctly
        unsafe {
            core::ptr::copy(
                inner_data.as_ptr().add(src_offset),
                inner_data.as_mut_ptr().add(dst_offset),
                length,
            );
        }

        Ok(())
    }

    /// Saves raw serialized bytes to the end of the page.
    /// Used for moving already-serialized data without re-serialization.
    pub fn save_raw_row(&self, data: &[u8]) -> Result<Link, ExecutionError> {
        let length = data.len();
        if length > DATA_LENGTH {
            return Err(ExecutionError::PageTooSmall {
                need: length,
                allowed: DATA_LENGTH,
            });
        }
        let length = length as u32;
        let offset = self.free_offset.fetch_add(length, Ordering::AcqRel);
        if offset > DATA_LENGTH as u32 - length {
            // Same rollback as in save_row: undo this call's own reservation
            // so the persisted initialized-bytes bound stays accurate.
            self.free_offset.fetch_sub(length, Ordering::AcqRel);
            return Err(ExecutionError::PageIsFull {
                need: length,
                left: DATA_LENGTH as i64 - offset as i64,
            });
        }

        let inner_data = unsafe { &mut *self.inner_data.get() };
        inner_data[offset as usize..][..length as usize].copy_from_slice(data);

        let link = Link {
            page_id: self.id,
            offset,
            length,
        };
        self.register_cell(link)?;
        Ok(link)
    }

    pub fn free_space(&self) -> usize {
        DATA_LENGTH.saturating_sub(self.free_offset.load(Ordering::Acquire) as usize)
    }

    pub fn reset(&self) {
        self.free_offset.store(0, Ordering::Release);
        self.cell_locks.reset();
        self.live_cells.store(0, Ordering::Release);
    }

    pub(crate) fn register_cell(&self, link: Link) -> Result<(), ExecutionError> {
        debug_assert_eq!(link.page_id, self.id);
        self.live_cells
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| count.checked_add(1))
            .map(|_| ())
            .map_err(|_| ExecutionError::LiveCellCountOverflow)
    }

    pub(crate) fn remove_cell(&self, link: Link) -> Result<(), ExecutionError> {
        debug_assert_eq!(link.page_id, self.id);
        self.live_cells
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| count.checked_sub(1))
            .map(|_| ())
            .map_err(|_| ExecutionError::LiveCellCountUnderflow)
    }

    #[cfg(feature = "std")]
    pub(crate) fn has_live_cells(&self) -> bool {
        self.live_cells.load(Ordering::Acquire) != 0
    }

    #[cfg(feature = "std")]
    pub(crate) fn live_cell_count(&self) -> u32 {
        self.live_cells.load(Ordering::Acquire)
    }
}

/// Error that can appear on [`Data`] page operations.
#[derive(Copy, Clone, Debug, Display, Error, PartialEq)]
pub enum ExecutionError {
    /// Error of trying to save a row in [`Data`] page with not enough space left.
    #[display("need {}, but {} left", need, left)]
    PageIsFull { need: u32, left: i64 },

    /// Error of trying to save a row in [`Data`] page that has smaller size than required.
    #[display("need {}, but {} allowed", need, allowed)]
    PageTooSmall { need: usize, allowed: usize },

    /// Error of saving `Row` in [`Data`] page.
    SerializeError,

    /// Error of loading `Row` from [`Data`] page.
    DeserializeError,

    /// Link provided for saving `Row` is invalid.
    InvalidLink,

    /// A page's live-cell count cannot represent another row.
    LiveCellCountOverflow,

    /// A row was removed from a page whose live-cell count was already zero.
    LiveCellCountUnderflow,

    /// A callback tried to re-enter a cell stripe already held by this thread.
    CellLockReentry,
}

#[cfg(all(test, not(wt_loom)))]
mod tests {
    use alloc::sync::Arc;
    use core::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::thread;

    use rkyv::{Archive, Deserialize, Serialize};

    use crate::in_memory::DATA_INNER_LENGTH;
    use crate::in_memory::data::{Data, ExecutionError, INNER_PAGE_SIZE};
    use crate::prelude::Link;

    #[derive(Archive, Copy, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
    #[rkyv(compare(PartialEq), derive(Debug))]
    struct TestRow {
        a: u64,
        b: u64,
    }

    #[test]
    fn colliding_rows_keep_using_one_stable_stripe() {
        let locks = super::CellLocks::default();
        let first = Link {
            page_id: 1.into(),
            offset: 7,
            length: 16,
        };
        let second = Link { offset: 14, ..first };
        assert_eq!(super::CellLocks::start(first), super::CellLocks::start(second));
        let preceding = locks.read(first).unwrap();
        let existing = locks.read(second).unwrap();
        drop(preceding);
        let joining = locks.read(second).unwrap();
        assert!(
            core::ptr::eq(existing.state, joining.state),
            "readers of one row must share the state that excludes its writer"
        );
    }

    #[test]
    fn mixed_offsets_do_not_collapse_into_one_low_bit_stripe() {
        let locks = super::CellLocks::default();
        let first = Link {
            page_id: 1.into(),
            offset: 0,
            length: 16,
        };
        let second = Link { offset: 64, ..first };
        assert_ne!(super::CellLocks::start(first), super::CellLocks::start(second));
        let first = locks.write(first).unwrap();
        let second = locks.write(second).unwrap();
        assert!(!core::ptr::eq(first.state, second.state));
    }

    #[test]
    fn callback_can_read_a_different_row_on_its_write_stripe() {
        let locks = super::CellLocks::default();
        let first = Link {
            page_id: 1.into(),
            offset: 7,
            length: 16,
        };
        let second = Link { offset: 14, ..first };
        assert_eq!(super::CellLocks::start(first), super::CellLocks::start(second));

        let _callback_write = locks.write(first).unwrap();
        let _nested_read = locks.read(second).unwrap();
        assert!(matches!(locks.read(first), Err(ExecutionError::CellLockReentry)));
        assert!(matches!(locks.write(second), Err(ExecutionError::CellLockReentry)));
    }

    #[test]
    fn data_page_length_valid() {
        let data = Data::<()>::new(1.into());
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&data).unwrap();

        assert_eq!(bytes.len(), INNER_PAGE_SIZE)
    }

    #[test]
    fn data_page_save_row() {
        let page = Data::<TestRow>::new(1.into());
        let row = TestRow { a: 10, b: 20 };

        let initial_free = page.free_space();
        assert!(initial_free > 0);

        let link = page.save_row(&row).unwrap();
        assert_eq!(link.page_id, page.id);
        assert_eq!(link.length, 16);
        assert_eq!(link.offset, 0);

        assert_eq!(page.free_offset.load(Ordering::Relaxed), link.length);
        assert_eq!(page.free_space(), initial_free - link.length as usize);

        let inner_data = unsafe { &mut *page.inner_data.get() };
        let bytes = &inner_data[link.offset as usize..link.length as usize];
        let archived = unsafe { rkyv::access_unchecked::<ArchivedTestRow>(bytes) };
        assert_eq!(archived, &row)
    }

    #[test]
    fn data_page_overwrite_row() {
        let page = Data::<TestRow>::new(1.into());
        let row = TestRow { a: 10, b: 20 };

        let link = page.save_row(&row).unwrap();

        let new_row = TestRow { a: 20, b: 20 };
        let res = unsafe { page.save_row_by_link(&new_row, link) }.unwrap();

        assert_eq!(res, link);

        let inner_data = unsafe { &mut *page.inner_data.get() };
        let bytes = &inner_data[link.offset as usize..link.length as usize];
        let archived = unsafe { rkyv::access_unchecked::<ArchivedTestRow>(bytes) };
        assert_eq!(archived, &new_row)
    }

    #[test]
    fn data_page_full() {
        let page = Data::<TestRow, 16>::new(1.into());
        let row = TestRow { a: 10, b: 20 };
        let _ = page.save_row(&row).unwrap();

        let new_row = TestRow { a: 20, b: 20 };
        let offset_before = page.free_offset.load(Ordering::Acquire);
        let res = page.save_row(&new_row);

        assert!(matches!(res, Err(ExecutionError::PageIsFull { .. })));
        assert_eq!(
            page.free_offset.load(Ordering::Acquire),
            offset_before,
            "a failed save must roll back its free_offset reservation"
        );
    }

    #[test]
    fn data_page_too_small() {
        let page = Data::<TestRow, 1>::new(1.into());
        let row = TestRow { a: 10, b: 20 };
        let res = page.save_row(&row);

        assert!(matches!(res, Err(ExecutionError::PageTooSmall { .. })));
    }

    #[test]
    fn data_page_full_multithread() {
        let page = Data::<TestRow, 128>::new(1.into());
        let shared = Arc::new(page);

        let (tx, rx) = mpsc::channel();
        let second_shared = shared.clone();

        thread::spawn(move || {
            let mut links = Vec::new();
            for i in 1..10 {
                let row = TestRow { a: 10 + i, b: 20 + i };

                let link = second_shared.save_row(&row);
                links.push(link)
            }

            tx.send(links).unwrap();
        });

        let mut links = Vec::new();
        for i in 1..10 {
            let row = TestRow { a: 30 + i, b: 40 + i };

            let link = shared.save_row(&row);
            links.push(link)
        }
        let _other_links = rx.recv().unwrap();
    }

    #[test]
    fn data_page_save_many_rows() {
        let page = Data::<TestRow>::new(1.into());

        let initial_free = page.free_space();
        let mut total_used = 0;

        let mut rows = Vec::new();
        let mut links = Vec::new();
        for i in 1..10 {
            let row = TestRow { a: 10 + i, b: 20 + i };
            rows.push(row);

            let link = page.save_row(&row);
            total_used += link.as_ref().unwrap().length as usize;
            links.push(link)
        }

        assert_eq!(page.free_space(), initial_free - total_used);

        let inner_data = unsafe { &mut *page.inner_data.get() };

        for (i, link) in links.into_iter().enumerate() {
            let link = link.unwrap();

            let bytes = &inner_data[link.offset as usize..(link.offset + link.length) as usize];
            let archived = unsafe { rkyv::access_unchecked::<ArchivedTestRow>(bytes) };
            let row = rows.get(i).unwrap();

            assert_eq!(row, archived)
        }
    }

    #[test]
    fn data_page_get_row_ref() {
        let page = Data::<TestRow>::new(1.into());
        let row = TestRow { a: 10, b: 20 };

        let link = page.save_row(&row).unwrap();
        let archived = page.get_row_ref(link).unwrap();
        assert_eq!(archived, &row)
    }

    #[test]
    fn data_page_get_row() {
        let page = Data::<TestRow>::new(1.into());
        let row = TestRow { a: 10, b: 20 };

        let link = page.save_row(&row).unwrap();
        let deserialized = page.get_row(link).unwrap();
        assert_eq!(deserialized, row)
    }

    #[test]
    fn multithread() {
        let page = Data::<TestRow>::new(1.into());
        let shared = Arc::new(page);

        let (tx, rx) = mpsc::channel();
        let second_shared = shared.clone();

        thread::spawn(move || {
            let mut links = Vec::new();
            for i in 1..10 {
                let row = TestRow { a: 10 + i, b: 20 + i };

                let link = second_shared.save_row(&row);
                links.push(link)
            }

            tx.send(links).unwrap();
        });

        let mut links = Vec::new();
        for i in 1..10 {
            let row = TestRow { a: 30 + i, b: 40 + i };

            let link = shared.save_row(&row);
            links.push(link)
        }
        let other_links = rx.recv().unwrap();

        let links = other_links
            .into_iter()
            .chain(links)
            .map(|v| v.unwrap())
            .collect::<Vec<_>>();

        for link in links {
            let _ = shared.get_row(link).unwrap();
        }
    }

    /// `get_raw_row` is a pure read and runs under the shared `page_access`
    /// read lock next to other readers of the same page bytes. It used to
    /// create `&mut` over the page image, which aliased those shared readers
    /// (UB Miri flags). Two threads reading the same link concurrently is the
    /// exact shape that was undefined; this must stay valid.
    #[test]
    fn get_raw_row_is_a_shared_read() {
        let page = Data::<TestRow>::new(1.into());
        let row = TestRow { a: 10, b: 20 };
        let link = page.save_row(&row).unwrap();

        let shared = Arc::new(page);
        let other = shared.clone();
        let handle = thread::spawn(move || {
            for _ in 0..100 {
                let raw = other.get_raw_row(link).unwrap();
                assert_eq!(raw.len(), link.length as usize);
            }
        });

        for _ in 0..100 {
            let archived = shared.get_row_ref(link).unwrap();
            assert_eq!(archived, &row);
            let raw = shared.get_raw_row(link).unwrap();
            assert_eq!(raw.len(), link.length as usize);
        }
        handle.join().unwrap();
    }

    #[test]
    fn move_from_to() {
        let page = Data::<TestRow>::new(1.into());

        let row1 = TestRow { a: 100, b: 200 };
        let link1 = page.save_row(&row1).unwrap();
        assert_eq!(link1.offset, 0);

        let row2 = TestRow { a: 300, b: 400 };
        let link2 = page.save_row(&row2).unwrap();
        assert_eq!(link2.offset, 16);

        let new_link = Link {
            page_id: link2.page_id,
            offset: 0,
            length: link2.length,
        };

        unsafe { page.move_from_to(link2, new_link).unwrap() };

        let moved_row = page.get_row(new_link).unwrap();
        assert_eq!(moved_row, row2);
    }

    #[test]
    fn move_from_to_different_lengths() {
        let page = Data::<TestRow>::new(1.into());

        let from = Link {
            page_id: 1.into(),
            offset: 0,
            length: 16,
        };
        let to = Link {
            page_id: 1.into(),
            offset: 32,
            length: 8,
        };

        let result = unsafe { page.move_from_to(from, to) };
        assert!(matches!(result, Err(ExecutionError::InvalidLink)));
    }

    #[test]
    fn save_raw_row_appends_to_page() {
        let page = Data::<TestRow>::new(1.into());
        let row = TestRow { a: 42, b: 99 };

        let link = page.save_row(&row).unwrap();
        let raw_data = page.get_raw_row(link).unwrap();

        let new_link = page.save_raw_row(&raw_data).unwrap();

        assert_eq!(new_link.page_id, page.id);
        assert_eq!(new_link.length, link.length);
        assert_eq!(new_link.offset, link.length);

        let retrieved = page.get_row(new_link).unwrap();
        assert_eq!(retrieved, row);
    }

    #[test]
    fn save_raw_row_page_too_small() {
        let page = Data::<TestRow, 16>::new(1.into());
        let data = vec![0u8; 32];

        let result = page.save_raw_row(&data);
        assert!(matches!(result, Err(ExecutionError::PageTooSmall { .. })));
    }

    #[test]
    fn save_raw_row_page_full() {
        let page = Data::<TestRow, 16>::new(1.into());
        let row = TestRow { a: 1, b: 2 };
        let _ = page.save_row(&row).unwrap();

        let data = vec![0u8; 16];
        let offset_before = page.free_offset.load(Ordering::Acquire);
        let result = page.save_raw_row(&data);
        assert!(matches!(result, Err(ExecutionError::PageIsFull { .. })));
        assert_eq!(
            page.free_offset.load(Ordering::Acquire),
            offset_before,
            "a failed raw save must roll back its free_offset reservation"
        );
    }

    #[test]
    fn save_raw_row_move_between_pages() {
        let page1 = Data::<TestRow>::new(1.into());
        let page2 = Data::<TestRow>::new(2.into());

        let original = TestRow { a: 123, b: 456 };
        let link1 = page1.save_row(&original).unwrap();

        let raw = page1.get_raw_row(link1).unwrap();
        let link2 = page2.save_raw_row(&raw).unwrap();

        let retrieved = page2.get_row(link2).unwrap();
        assert_eq!(retrieved, original);
    }

    #[test]
    fn save_raw_row_multiple_entries() {
        let page = Data::<TestRow>::new(1.into());
        let row = TestRow { a: 77, b: 88 };

        let link = page.save_row(&row).unwrap();
        let raw_data = page.get_raw_row(link).unwrap();
        let row_size = link.length as usize;

        let initial_free = page.free_space();

        let mut links = vec![link];
        for i in 0..5 {
            let new_link = page.save_raw_row(&raw_data).unwrap();
            links.push(new_link);
            let expected_free = initial_free - ((i + 1) as usize * row_size);
            assert_eq!(page.free_space(), expected_free);
        }

        for link in links {
            let retrieved = page.get_row(link).unwrap();
            assert_eq!(retrieved, row);
        }
    }

    #[test]
    fn reset_clears_free_offset() {
        let page = Data::<TestRow>::new(1.into());

        let row1 = TestRow { a: 10, b: 20 };
        let row2 = TestRow { a: 30, b: 40 };
        let link1 = page.save_row(&row1).unwrap();
        let link2 = page.save_row(&row2).unwrap();

        assert!(page.free_offset.load(Ordering::Relaxed) > 0);
        assert_eq!(link1.offset, 0);
        assert_eq!(link2.offset, 16);

        page.reset();

        assert_eq!(page.free_offset.load(Ordering::Relaxed), 0);
        assert_eq!(page.free_space(), DATA_INNER_LENGTH);

        let row3 = TestRow { a: 99, b: 88 };
        let link3 = page.save_row(&row3).unwrap();
        assert_eq!(link3.offset, 0);

        let retrieved = page.get_row(link3).unwrap();
        assert_eq!(retrieved, row3);
    }
}

#[cfg(all(test, wt_loom))]
mod cell_lock_models {
    use super::{CellLocks, Link};
    use loom::{cell::UnsafeCell, sync::Arc, thread};

    struct Protected {
        locks: CellLocks,
        value: UnsafeCell<(u64, u64)>,
    }

    // Every access to value below holds the same row's read or write guard.
    unsafe impl Sync for Protected {}

    #[test]
    fn colliding_offsets_cannot_split_readers_from_a_writer() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(2);
        model.max_branches = 10_000;
        model.check(|| {
            let protected = Arc::new(Protected {
                locks: CellLocks::default(),
                value: UnsafeCell::new((0, 0)),
            });
            let first = Link {
                page_id: 1.into(),
                offset: 7,
                length: 16,
            };
            let second = Link { offset: 14, ..first };
            assert_eq!(CellLocks::start(first), CellLocks::start(second));
            let preceding = protected.locks.read(first).unwrap();
            let existing = protected.locks.read(second).unwrap();
            drop(preceding);
            let reader = {
                let protected = protected.clone();
                thread::spawn(move || {
                    let _guard = protected.locks.read(second).unwrap();
                    protected.value.with(|value| unsafe {
                        let a = (*value).0;
                        thread::yield_now();
                        assert_eq!(a, (*value).1);
                    });
                })
            };
            drop(existing);
            {
                let _guard = protected.locks.write(second).unwrap();
                protected.value.with_mut(|value| unsafe {
                    (*value).0 = 1;
                    thread::yield_now();
                    (*value).1 = 1;
                });
            }
            reader.join().unwrap();
        });
    }

    #[test]
    fn readers_and_writers_never_overlap_and_publish_complete_rows() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(2);
        model.max_branches = 10_000;
        model.check(|| {
            let protected = Arc::new(Protected {
                locks: CellLocks::default(),
                value: UnsafeCell::new((0, 0)),
            });
            let link = Link {
                page_id: 1.into(),
                offset: 64,
                length: 16,
            };
            let mut handles = Vec::new();
            for writer in [false, true] {
                let protected = protected.clone();
                handles.push(thread::spawn(move || {
                    if writer {
                        let _guard = protected.locks.write(link).unwrap();
                        protected.value.with_mut(|value| unsafe {
                            (*value).0 += 1;
                            thread::yield_now();
                            (*value).1 += 1;
                        });
                    } else {
                        let _guard = protected.locks.read(link).unwrap();
                        protected.value.with(|value| unsafe {
                            let first = (*value).0;
                            thread::yield_now();
                            assert_eq!(first, (*value).1);
                        });
                    }
                }));
            }
            for handle in handles {
                handle.join().unwrap();
            }
            let _guard = protected.locks.read(link).unwrap();
            protected.value.with(|value| unsafe {
                assert_eq!(*value, (1, 1));
            });
        });
    }
}
