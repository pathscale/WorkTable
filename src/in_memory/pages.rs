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

    /// Allocates new page but **NOT** sets it as `current`.
    pub fn allocate_new_page(&self) -> Arc<Data<<Row as StorableRow>::WrappedRow, DATA_LENGTH>> {
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

    pub fn delete(&self, link: Link) -> Result<(), ExecutionError> {
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

    pub fn get_empty_pages(&self) -> Vec<PageId> {
        let g = self.empty_pages.read();
        g.iter().map(|p| *p).collect()
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

    use crate::in_memory::pages::DataPages;
    use crate::in_memory::{DATA_INNER_LENGTH, PagesExecutionError, RowWrapper, StorableRow};

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
    fn allocate_new_page_creates_page_correctly() {
        let pages = DataPages::<TestRow>::new();

        let initial_last_id = pages.last_page_id.load(Ordering::Relaxed);
        let initial_current = pages.current_page_id.load(Ordering::Relaxed);
        let initial_count = pages.get_page_count();

        let _allocated_page = pages.allocate_new_page();

        assert_eq!(
            pages.last_page_id.load(Ordering::Relaxed),
            initial_last_id + 1
        );

        assert_eq!(
            pages.current_page_id.load(Ordering::Relaxed),
            initial_current,
            "current_page_id should NOT change after allocate_new_page"
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

        let _page2 = pages.allocate_new_page();
        let _page3 = pages.allocate_new_page();
        let _page4 = pages.allocate_new_page();

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

        pages.allocate_new_page();

        let row = TestRow { a: 42, b: 99 };
        let link = pages.insert(row).unwrap();

        assert_eq!(link.page_id, 1.into());
    }

    #[test]
    fn allocate_new_page_concurrent() {
        let pages = Arc::new(DataPages::<TestRow>::new());
        let mut handles = Vec::new();

        for _ in 0..10 {
            let pages_clone = pages.clone();
            let handle = thread::spawn(move || {
                for _ in 0..10 {
                    pages_clone.allocate_new_page();
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

        let allocated = pages.allocate_new_page();

        assert_eq!(allocated.free_offset.load(Ordering::Relaxed), 0);
        assert_eq!(allocated.free_space(), DATA_INNER_LENGTH);
    }

    #[test]
    fn skips_explicitly_allocated_page() {
        let pages = DataPages::<TestRow>::new();

        // Allocate page explicitly
        pages.allocate_new_page();
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
}
