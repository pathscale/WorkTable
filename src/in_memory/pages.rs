use data_bucket::page::PageId;
use derive_more::{Display, Error, From};
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
use std::collections::VecDeque;
use std::{
    fmt::Debug,
    sync::Arc,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
};

use crate::in_memory::empty_link_registry::EmptyLinkRegistry;
use crate::prelude::ArchivedRowWrapper;
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

#[derive(Debug)]
pub struct DataPages<Row, const DATA_LENGTH: usize = DATA_INNER_LENGTH>
where
    Row: StorableRow,
{
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
    pub fn new() -> Self {
        Self {
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
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <Row as StorableRow>::WrappedRow: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
    {
        let general_row = <Row as StorableRow>::WrappedRow::from_inner(row);

        if let Some(link) = self.empty_links.pop_max() {
            let pages = self.pages.read();
            let current_page: usize = page_id_mapper(link.page_id.into());
            let page = &pages[current_page];

            match unsafe { page.try_save_row_by_link(&general_row, link) } {
                Ok((link, left_link)) => {
                    if let Some(l) = left_link {
                        self.empty_links.push(l);
                    }
                    return Ok(link);
                }
                // Ok(l) => return Ok(l),
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
                let current_page =
                    page_id_mapper(self.current_page_id.load(Ordering::Acquire) as usize);
                let page = &pages[current_page];

                (page.save_row(&general_row), current_page)
            };
            match link {
                Ok(link) => {
                    self.row_count.fetch_add(1, Ordering::Relaxed);
                    return Ok(link);
                }
                Err(e) => match e {
                    DataExecutionError::PageIsFull { .. } => {
                        if tried_page
                            == page_id_mapper(self.current_page_id.load(Ordering::Relaxed) as usize)
                        {
                            let mut g = self.empty_pages.write();
                            if let Some(page_id) = g.pop_front() {
                                let _pages = self.pages.write();
                                self.current_page_id
                                    .store(page_id.into(), Ordering::Release);
                            } else {
                                drop(g);
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
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            > + Clone,
        <Row as StorableRow>::WrappedRow: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
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
    pub fn allocate_new_or_pop_free(
        &self,
    ) -> Arc<Data<<Row as StorableRow>::WrappedRow, DATA_LENGTH>> {
        let page_id = {
            let mut empty_pages = self.empty_pages.write();
            empty_pages.pop_front()
        };

        if let Some(page_id) = page_id {
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

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "DataPages")
    )]
    pub fn select(&self, link: Link) -> Result<Row, ExecutionError>
    where
        Row: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        let pages = self.pages.read();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let gen_row = page.get_row(link).map_err(ExecutionError::DataPageError)?;
        Ok(gen_row.get_inner())
    }

    pub fn select_non_ghosted(&self, link: Link) -> Result<Row, ExecutionError>
    where
        Row: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        let pages = self.pages.read();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let gen_row = page.get_row(link).map_err(ExecutionError::DataPageError)?;
        if gen_row.is_ghosted() {
            return Err(ExecutionError::Ghosted);
        }
        Ok(gen_row.get_inner())
    }

    pub fn select_non_vacuumed(&self, link: Link) -> Result<Row, ExecutionError>
    where
        Row: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        let pages = self.pages.read();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let gen_row = page.get_row(link).map_err(ExecutionError::DataPageError)?;
        if gen_row.is_ghosted() {
            return Err(ExecutionError::Ghosted);
        }
        if gen_row.is_vacuumed() {
            return Err(ExecutionError::Vacuumed);
        }
        Ok(gen_row.get_inner())
    }

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "DataPages")
    )]
    pub fn with_ref<Op, Res>(&self, link: Link, op: Op) -> Result<Res, ExecutionError>
    where
        Row: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        Op: Fn(&<<Row as StorableRow>::WrappedRow as Archive>::Archived) -> Res,
    {
        let pages = self.pages.read();
        let page = pages
            .get::<usize>(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let gen_row = page
            .get_row_ref(link)
            .map_err(ExecutionError::DataPageError)?;
        let res = op(gen_row);
        Ok(res)
    }

    #[allow(clippy::missing_safety_doc)]
    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "DataPages")
    )]
    pub unsafe fn with_mut_ref<Op, Res>(
        &self,
        link: Link,
        mut op: Op,
    ) -> Result<Res, ExecutionError>
    where
        Row: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: Portable,
        Op: FnMut(&mut <<Row as StorableRow>::WrappedRow as Archive>::Archived) -> Res,
    {
        let pages = self.pages.read();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let gen_row = unsafe {
            page.get_mut_row_ref(link)
                .map_err(ExecutionError::DataPageError)?
                .unseal_unchecked()
        };
        let res = op(gen_row);
        Ok(res)
    }

    /// # Safety
    /// This function is `unsafe` because it modifies archived memory directly.
    /// The caller must ensure that:
    /// - The `link` is valid and points to a properly initialized row.
    /// - No other references to the same row exist during modification.
    /// - The operation does not cause data races or memory corruption.
    pub unsafe fn update<const N: usize>(
        &self,
        row: Row,
        link: Link,
    ) -> Result<Link, ExecutionError>
    where
        Row: Archive,
        <Row as StorableRow>::WrappedRow: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
    {
        let pages = self.pages.read();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let gen_row = <Row as StorableRow>::WrappedRow::from_inner(row);
        unsafe {
            page.save_row_by_link(&gen_row, link)
                .map_err(ExecutionError::DataPageError)
        }
    }

    pub fn delete(&self, link: Link) -> Result<(), ExecutionError>
    where
        Row: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <Row as StorableRow>::WrappedRow: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: ArchivedRowWrapper,
    {
        unsafe { self.with_mut_ref(link, |r| r.delete())? }

        self.empty_links.push(link);
        Ok(())
    }

    pub fn select_raw(&self, link: Link) -> Result<Vec<u8>, ExecutionError> {
        let pages = self.pages.read();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        page.get_raw_row(link)
            .map_err(ExecutionError::DataPageError)
    }

    pub fn mark_page_empty(&self, page_id: PageId) {
        if u32::from(page_id) != self.current_page_id.load(Ordering::Acquire) {
            let mut g = self.empty_pages.write();
            g.push_back(page_id);
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

            page.free_offset
                .store(DATA_LENGTH as u32, Ordering::Release);
        }
    }

    pub fn get_empty_pages(&self) -> Vec<PageId> {
        let g = self.empty_pages.read();
        g.iter().copied().collect()
    }

    pub fn get_page(
        &self,
        page_id: PageId,
    ) -> Option<Arc<Data<<Row as StorableRow>::WrappedRow, DATA_LENGTH>>> {
        let pages = self.pages.read();
        let page = pages.get(page_id_mapper(page_id.into()))?;
        Some(page.clone())
    }

    pub fn get_bytes(&self) -> Vec<([u8; DATA_LENGTH], u32)> {
        let pages = self.pages.read();
        pages
            .iter()
            .map(|p| (p.get_bytes(), p.free_offset.load(Ordering::Relaxed)))
            .collect()
    }

    pub fn get_page_count(&self) -> usize {
        self.pages.read().len()
    }

    pub fn get_empty_links(&self) -> Vec<Link> {
        self.empty_links.iter().collect()
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
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Instant;

    use parking_lot::RwLock;
    use rkyv::with::{AtomicLoad, Relaxed};
    use rkyv::{Archive, Deserialize, Serialize};

    use crate::in_memory::data::Data;
    use crate::in_memory::pages::{DataPages, ExecutionError};
    use crate::in_memory::{DATA_INNER_LENGTH, PagesExecutionError, RowWrapper, StorableRow};
    use crate::prelude::ArchivedRowWrapper;

    #[derive(
        Archive, Copy, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
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

        assert_eq!(pages.empty_links.pop_max(), Some(link));
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

        assert_eq!(
            pages.last_page_id.load(Ordering::Relaxed),
            initial_last_id + 1
        );

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

        assert_eq!(
            pages.last_page_id.load(Ordering::Relaxed),
            initial_last_id + 3
        );
        assert_eq!(
            pages.current_page_id.load(Ordering::Relaxed),
            initial_current
        );
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

        assert_eq!(
            new_link.page_id,
            3.into(),
            "New insert should go to page 3, not page 2"
        );
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
        let pages = DataPages::<TestRow>::from_data(vec![
            Arc::new(Data::new(1.into())),
            Arc::new(Data::new(2.into())),
        ]);

        let row = TestRow { a: 10, b: 20 };
        let _link = pages.insert(row).unwrap();

        pages.current_page_id.store(2, Ordering::Release);
        pages.mark_page_full(1.into());

        let empty_links = pages.get_empty_links();
        assert!(!empty_links.is_empty(), "Should have empty links");

        let link = empty_links.first().unwrap();
        assert_eq!(link.page_id, 1.into());
        assert_eq!(
            link.length, 24,
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
        let pages = DataPages::<TestRow>::from_data(vec![
            Arc::new(Data::new(1.into())),
            Arc::new(Data::new(2.into())),
        ]);

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

        assert_eq!(
            page.free_offset.load(Ordering::Relaxed),
            DATA_INNER_LENGTH as u32
        );
    }

    #[test]
    fn mark_page_full_with_no_remaining_space() {
        let pages = DataPages::<TestRow>::from_data(vec![
            Arc::new(Data::new(1.into())),
            Arc::new(Data::new(2.into())),
        ]);

        let page = pages.get_page(1.into()).unwrap();
        page.free_offset
            .store(DATA_INNER_LENGTH as u32, Ordering::Release);

        pages.current_page_id.store(2, Ordering::Release);
        pages.mark_page_full(1.into());

        assert!(pages.get_empty_links().is_empty());
    }
}
