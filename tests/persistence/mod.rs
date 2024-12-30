use std::sync::Arc;

use worktable::prelude::*;
use worktable::worktable;

mod read;
mod space;
mod write;

#[derive(
    Clone,
    rkyv::Archive,
    Debug,
    rkyv::Deserialize,
    rkyv::Serialize,
    From,
    Eq,
    Into,
    PartialEq,
    PartialOrd,
    Ord,
)]
pub struct TestPersistPrimaryKey(u128);
impl TablePrimaryKey for TestPersistPrimaryKey {
    type Generator = ();
}
#[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize, PartialEq)]
#[rkyv(derive(Debug))]
#[repr(C)]
pub struct TestPersistRow {
    pub id: u128,
    pub another: u64,
}
impl TableRow<TestPersistPrimaryKey> for TestPersistRow {
    fn get_primary_key(&self) -> TestPersistPrimaryKey {
        self.id.clone().into()
    }
}
#[derive(rkyv::Archive, Debug, rkyv::Deserialize, rkyv::Serialize)]
#[repr(C)]
pub struct TestPersistWrapper {
    inner: TestPersistRow,
    is_deleted: bool,
    lock: u16,
    id_lock: u16,
    another_lock: u16,
}
impl RowWrapper<TestPersistRow> for TestPersistWrapper {
    fn get_inner(self) -> TestPersistRow {
        self.inner
    }
    fn from_inner(inner: TestPersistRow) -> Self {
        Self {
            inner,
            is_deleted: Default::default(),
            lock: Default::default(),
            id_lock: Default::default(),
            another_lock: Default::default(),
        }
    }
}
impl ArchivedRow for ArchivedTestPersistWrapper {
    fn is_locked(&self) -> Option<u16> {
        if self.lock != 0 {
            return Some(self.lock.into());
        }
        if self.id_lock != 0 {
            return Some(self.id_lock.into());
        }
        if self.another_lock != 0 {
            return Some(self.another_lock.into());
        }
        None
    }
}
impl StorableRow for TestPersistRow {
    type WrappedRow = TestPersistWrapper;
}
#[derive(Debug, Default)]
pub struct TestPersistIndex {
    another_idx: SpaceTreeIndex<
        TreeIndex<u64, std::sync::Arc<LockFreeSet<Link>>>,
        u64,
        std::sync::Arc<LockFreeSet<Link>>,
    >,
}
#[derive(Debug, Default, Clone)]
pub struct TestPersistIndexPersisted {
    another_idx: Vec<GeneralPage<IndexData<TreeIndex>>>,
}
impl TestPersistIndexPersisted {
    pub fn get_intervals(&self) -> std::collections::HashMap<String, Vec<Interval>> {
        let mut map = std::collections::HashMap::new();
        let i = Interval(
            self.another_idx
                .first()
                .expect("at least one page should be presented, even if index contains no values")
                .header
                .page_id
                .into(),
            self.another_idx
                .last()
                .expect("at least one page should be presented, even if index contains no values")
                .header
                .page_id
                .into(),
        );
        map.insert(
            "another_idx".to_string(),
            <[_]>::into_vec(
                #[rustc_box]
                ::alloc::boxed::Box::new([i]),
            ),
        );
        map
    }
    pub fn persist(&mut self, path: &String) -> eyre::Result<()> {
        {
            let mut file = std::fs::File::create(::alloc::__export::must_use({
                let res = ::alloc::fmt::format(::alloc::__export::format_args!(
                    "{}/{}{}",
                    path,
                    "another_idx",
                    ".wt.idx"
                ));
                res
            }))?;
            for mut page in &mut self.another_idx {
                persist_page(&mut page, &mut file)?;
            }
        }
        Ok(())
    }
    pub fn get_last_header_mut(&mut self) -> Option<&mut GeneralHeader> {
        let mut header = None;
        if header.is_none() {
            header = Some(
                &mut self
                    .another_idx
                    .last_mut()
                    .expect(
                        "at least one page should be presented, even if index contains no values",
                    )
                    .header,
            );
        } else {
            let new_header = &mut self
                .another_idx
                .last_mut()
                .expect("at least one page should be presented, even if index contains no values")
                .header;
            let header_page_id = header
                .as_ref()
                .expect("at least one page should be presented, even if index contains no values")
                .page_id;
            if header_page_id < new_header.page_id {
                header = Some(new_header)
            }
        }
        header
    }
    pub fn parse_from_file(
        path: &String,
        map: &std::collections::HashMap<String, Vec<Interval>>,
    ) -> eyre::Result<Self> {
        let another_idx = {
            let mut another_idx = ::alloc::vec::Vec::new();
            let intervals = map.get("another_idx").expect("index name should exist");
            let mut file = std::fs::File::open(::alloc::__export::must_use({
                let res = ::alloc::fmt::format(::alloc::__export::format_args!(
                    "{}/{}{}",
                    path,
                    "another_idx",
                    ".wt.idx"
                ));
                res
            }))?;
            for interval in intervals {
                for page_id in interval.0..interval.1 {
                    let index = parse_page::<IndexData<_>, { TEST_PERSIST_PAGE_SIZE as u32 }>(
                        &mut file,
                        page_id as u32,
                    )?;
                    another_idx.push(index);
                }
                let index = parse_page::<IndexData<_>, { TEST_PERSIST_PAGE_SIZE as u32 }>(
                    &mut file,
                    interval.1 as u32,
                )?;
                another_idx.push(index);
            }
            another_idx
        };
        Ok(Self { another_idx })
    }
}
impl PersistableIndex for TestPersistIndex {
    type PersistedIndex = TestPersistIndexPersisted;
    fn get_index_names(&self) -> Vec<&str> {
        <[_]>::into_vec(
            #[rustc_box]
            ::alloc::boxed::Box::new(["another_idx"]),
        )
    }
    fn get_persisted_index(&self) -> Self::PersistedIndex {
        let mut another_idx =
            map_index_pages_to_general(map_tree_index::<TreeIndex, TEST_PERSIST_PAGE_SIZE>(
                TableIndex::iter(&self.another_idx),
            ));
        Self::PersistedIndex { another_idx }
    }
    fn from_persisted(persisted: Self::PersistedIndex) -> Self {
        let another_idx: SpaceTreeIndex<
            TreeIndex<_, std::sync::Arc<lockfree::set::Set<Link>>>,
            _,
            _,
        > = SpaceTreeIndex::new(TreeIndex::new());
        for page in persisted.another_idx {
            for val in page.inner.index_values {
                if let Some(set) = TableIndex::peek(&another_idx, &val.key) {
                    set.insert(val.link).expect("is ok");
                } else {
                    let set = lockfree::set::Set::new();
                    set.insert(val.link).expect("is ok");
                    TableIndex::insert(&another_idx, val.key, std::sync::Arc::new(set))
                        .expect("index is unique");
                }
            }
        }
        Self { another_idx }
    }
}
impl TableSecondaryIndex<TestPersistRow> for TestPersistIndex {
    fn save_row(
        &self,
        row: TestPersistRow,
        link: Link,
    ) -> core::result::Result<(), WorkTableError> {
        if let Some(set) = TableIndex::peek(&self.another_idx, &row.another) {
            set.insert(link).expect("is ok");
        } else {
            let set = LockFreeSet::new();
            set.insert(link)
                .expect("`Link` should not be already in set");
            TableIndex::insert(&self.another_idx, row.another, std::sync::Arc::new(set))
                .map_err(|_| WorkTableError::AlreadyExists)?;
        }
        core::result::Result::Ok(())
    }
    fn delete_row(
        &self,
        row: TestPersistRow,
        link: Link,
    ) -> core::result::Result<(), WorkTableError> {
        if let Some(set) = TableIndex::peek(&self.another_idx, &row.another) {
            set.remove(&link);
        }
        core::result::Result::Ok(())
    }
}
const TEST_PERSIST_PAGE_SIZE: usize = PAGE_SIZE;
const TEST_PERSIST_INNER_SIZE: usize = TEST_PERSIST_PAGE_SIZE - GENERAL_HEADER_SIZE;
#[derive(Debug, PersistTable)]
pub struct TestPersistWorkTable(
    WorkTable<
        TestPersistRow,
        TestPersistPrimaryKey,
        TreeIndex<TestPersistPrimaryKey, Link>,
        TestPersistIndex,
    >,
    std::sync::Arc<DatabaseManager>,
);
impl TestPersistWorkTable {
    pub fn new(manager: std::sync::Arc<DatabaseManager>) -> Self {
        let mut inner = WorkTable::default();
        inner.table_name = "TestPersist";
        Self(inner, manager)
    }
    pub fn name(&self) -> &'static str {
        &self.0.table_name
    }
    pub fn select(&self, pk: TestPersistPrimaryKey) -> Option<TestPersistRow> {
        self.0.select(pk)
    }
    pub fn insert(
        &self,
        row: TestPersistRow,
    ) -> core::result::Result<TestPersistPrimaryKey, WorkTableError> {
        self.0.insert(row)
    }
    pub async fn upsert(&self, row: TestPersistRow) -> core::result::Result<(), WorkTableError> {
        let pk = row.get_primary_key();
        let need_to_update = {
            let guard = Guard::new();
            if let Some(_) = TableIndex::peek(&self.0.pk_map, &pk) {
                true
            } else {
                false
            }
        };
        if need_to_update {
            self.update(row).await?;
        } else {
            self.insert(row)?;
        }
        core::result::Result::Ok(())
    }
    pub fn iter_with<F: Fn(TestPersistRow) -> core::result::Result<(), WorkTableError>>(
        &self,
        f: F,
    ) -> core::result::Result<(), WorkTableError> {
        let first = TableIndex::iter(&self.0.pk_map)
            .next()
            .map(|(k, v)| (k.clone(), *v));
        let Some((mut k, link)) = first else {
            return Ok(());
        };
        let data = self
            .0
            .data
            .select(link)
            .map_err(WorkTableError::PagesError)?;
        f(data)?;
        let mut ind = false;
        while !ind {
            let next = {
                let guard = Guard::new();
                let mut iter = TableIndex::range(&self.0.pk_map, k.clone()..);
                let next = iter
                    .next()
                    .map(|(k, v)| (k.clone(), *v))
                    .filter(|(key, _)| key != &k);
                if next.is_some() {
                    next
                } else {
                    iter.next().map(|(k, v)| (k.clone(), *v))
                }
            };
            if let Some((key, link)) = next {
                let data = self
                    .0
                    .data
                    .select(link)
                    .map_err(WorkTableError::PagesError)?;
                f(data)?;
                k = key
            } else {
                ind = true;
            };
        }
        core::result::Result::Ok(())
    }
    pub async fn iter_with_async<
        F: Fn(TestPersistRow) -> Fut,
        Fut: std::future::Future<Output = core::result::Result<(), WorkTableError>>,
    >(
        &self,
        f: F,
    ) -> core::result::Result<(), WorkTableError> {
        let first = TableIndex::iter(&self.0.pk_map)
            .next()
            .map(|(k, v)| (k.clone(), *v));
        let Some((mut k, link)) = first else {
            return Ok(());
        };
        let data = self
            .0
            .data
            .select(link)
            .map_err(WorkTableError::PagesError)?;
        f(data).await?;
        let mut ind = false;
        while !ind {
            let next = {
                let guard = Guard::new();
                let mut iter = TableIndex::range(&self.0.pk_map, k.clone()..);
                let next = iter
                    .next()
                    .map(|(k, v)| (k.clone(), *v))
                    .filter(|(key, _)| key != &k);
                if next.is_some() {
                    next
                } else {
                    iter.next().map(|(k, v)| (k.clone(), *v))
                }
            };
            if let Some((key, link)) = next {
                let data = self
                    .0
                    .data
                    .select(link)
                    .map_err(WorkTableError::PagesError)?;
                f(data).await?;
                k = key
            } else {
                ind = true;
            };
        }
        core::result::Result::Ok(())
    }
}
impl TestPersistWorkTable {
    pub fn select_by_another(
        &self,
        by: u64,
    ) -> core::result::Result<SelectResult<TestPersistRow, Self>, WorkTableError> {
        let rows = {
            TableIndex::peek(&self.0.indexes.another_idx, &by)
                .ok_or(WorkTableError::NotFound)?
                .iter()
                .map(|l| *l.as_ref())
                .collect::<Vec<_>>()
        }
        .iter()
        .map(|link| {
            self.0
                .data
                .select(*link)
                .map_err(WorkTableError::PagesError)
        })
        .collect::<Result<Vec<_>, _>>()?;
        core::result::Result::Ok(SelectResult::<TestPersistRow, Self>::new(rows))
    }
}
impl SelectQueryExecutor<'_, TestPersistRow> for TestPersistWorkTable {
    fn execute(
        &self,
        mut q: SelectQueryBuilder<TestPersistRow, Self>,
    ) -> Result<Vec<TestPersistRow>, WorkTableError> {
        if q.params.orders.is_empty() {
            let mut limit = q.params.limit.unwrap_or(usize::MAX);
            let mut offset = q.params.offset.unwrap_or(0);
            let guard = Guard::new();
            let mut iter = TableIndex::iter(&self.0.pk_map);
            let mut rows = ::alloc::vec::Vec::new();
            while let Some((_, l)) = iter.next() {
                if offset != 0 {
                    offset -= 1;
                    continue;
                }
                let next = self.0.data.select(*l).map_err(WorkTableError::PagesError)?;
                rows.push(next);
                if q.params.orders.len() < 2 {
                    limit -= 1;
                    if limit == 0 {
                        break;
                    }
                }
            }
            core::result::Result::Ok(rows)
        } else {
            let (order, column) = q.params.orders.pop_front().unwrap();
            q.params.orders.push_front((order, column.clone()));
            let rows = match column.as_str() {
                "id" => ::core::panicking::panic("not yet implemented"),
                "another" => {
                    let mut limit = q.params.limit.unwrap_or(usize::MAX);
                    let mut offset = q.params.offset.unwrap_or(0);
                    let mut iter = TableIndex::iter(&self.0.indexes.another_idx);
                    let mut rows = ::alloc::vec::Vec::new();
                    while let Some((_, links)) = iter.next() {
                        for l in links.iter() {
                            if q.params.orders.len() < 2 {
                                if offset != 0 {
                                    offset -= 1;
                                    continue;
                                }
                            }
                            let next = self
                                .0
                                .data
                                .select(*l.as_ref())
                                .map_err(WorkTableError::PagesError)?;
                            rows.push(next);
                            if q.params.orders.len() < 2 {
                                limit -= 1;
                                if limit == 0 {
                                    break;
                                }
                            }
                        }
                        if limit == 0 {
                            break;
                        }
                    }
                    rows
                }
                _ => unreachable!(),
            };
            core::result::Result::Ok(
                SelectResult::<_, Self>::new(rows)
                    .with_params(q.params)
                    .execute(),
            )
        }
    }
}
impl SelectResultExecutor<TestPersistRow> for TestPersistWorkTable {
    fn execute(mut q: SelectResult<TestPersistRow, Self>) -> Vec<TestPersistRow> {
        let mut sort: Box<dyn Fn(&TestPersistRow, &TestPersistRow) -> std::cmp::Ordering> =
            Box::new(|left: &TestPersistRow, right: &TestPersistRow| std::cmp::Ordering::Equal);
        while let Some((q, col)) = q.params.orders.pop_front() {
            match col.as_str() {
                "id" => {
                    sort = Box::new(move |left, right| match sort(left, right) {
                        std::cmp::Ordering::Equal => match q {
                            Order::Asc => (&left.id).partial_cmp(&right.id).unwrap(),
                            Order::Desc => (&right.id).partial_cmp(&left.id).unwrap(),
                        },
                        std::cmp::Ordering::Less => std::cmp::Ordering::Less,
                        std::cmp::Ordering::Greater => std::cmp::Ordering::Greater,
                    });
                }
                "another" => {
                    sort = Box::new(move |left, right| match sort(left, right) {
                        std::cmp::Ordering::Equal => match q {
                            Order::Asc => (&left.another).partial_cmp(&right.another).unwrap(),
                            Order::Desc => (&right.another).partial_cmp(&left.another).unwrap(),
                        },
                        std::cmp::Ordering::Less => std::cmp::Ordering::Less,
                        std::cmp::Ordering::Greater => std::cmp::Ordering::Greater,
                    });
                }
                _ => unreachable!(),
            }
        }
        q.vals.sort_by(sort);
        let offset = q.params.offset.unwrap_or(0);
        let mut vals = q.vals.as_slice()[offset..].to_vec();
        if let Some(l) = q.params.limit {
            vals.truncate(l);
            vals
        } else {
            vals
        }
    }
}
impl TestPersistWorkTable {
    pub fn select_all<'a>(&'a self) -> SelectQueryBuilder<'a, TestPersistRow, Self> {
        SelectQueryBuilder::new(&self)
    }
}
impl TestPersistWorkTable {
    pub async fn update(&self, row: TestPersistRow) -> core::result::Result<(), WorkTableError> {
        let pk = row.get_primary_key();
        let op_id = self.0.lock_map.next_id();
        let lock = std::sync::Arc::new(Lock::new());
        self.0.lock_map.insert(op_id.into(), lock.clone());
        let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row)
            .map_err(|_| WorkTableError::SerializeError)?;
        let mut row = unsafe {
            rkyv::access_unchecked_mut::<<TestPersistRow as rkyv::Archive>::Archived>(
                &mut bytes[..],
            )
            .unseal_unchecked()
        };
        let link = TableIndex::peek(&self.0.pk_map, &pk).ok_or(WorkTableError::NotFound)?;
        let id = self
            .0
            .data
            .with_ref(link, |archived| archived.is_locked())
            .map_err(WorkTableError::PagesError)?;
        if let Some(id) = id {
            if let Some(lock) = self.0.lock_map.get(&(id.into())) {
                lock.as_ref().await
            }
        }
        unsafe {
            self.0
                .data
                .with_mut_ref(link, |archived| {
                    archived.lock = op_id.into();
                })
                .map_err(WorkTableError::PagesError)?
        };
        unsafe {
            self.0
                .data
                .with_mut_ref(link, move |archived| {
                    std::mem::swap(&mut archived.inner.id, &mut row.id);
                    std::mem::swap(&mut archived.inner.another, &mut row.another);
                })
                .map_err(WorkTableError::PagesError)?
        };
        unsafe {
            self.0
                .data
                .with_mut_ref(link, |archived| unsafe {
                    archived.lock = 0u16.into();
                })
                .map_err(WorkTableError::PagesError)?
        };
        lock.unlock();
        self.0.lock_map.remove(&op_id.into());
        core::result::Result::Ok(())
    }
}
impl TestPersistWorkTable {
    pub async fn delete(
        &self,
        pk: TestPersistPrimaryKey,
    ) -> core::result::Result<(), WorkTableError> {
        let link = TableIndex::peek(&self.0.pk_map, &pk).ok_or(WorkTableError::NotFound)?;
        let id = self
            .0
            .data
            .with_ref(link, |archived| archived.is_locked())
            .map_err(WorkTableError::PagesError)?;
        if let Some(id) = id {
            if let Some(lock) = self.0.lock_map.get(&(id.into())) {
                lock.as_ref().await
            }
        }
        let row = self.select(pk.clone()).unwrap();
        self.0.indexes.delete_row(row, link)?;
        self.0.pk_map.remove(&pk);
        self.0
            .data
            .delete(link)
            .map_err(WorkTableError::PagesError)?;
        core::result::Result::Ok(())
    }
}

worktable! (
    name: TestWithoutSecondaryIndexes,
    persist: true,
    columns: {
        id: u128 primary_key,
        another: u64,
    },
);

worktable!(
    name: SizeTest,
    columns: {
        id: u32 primary_key,
        number: u64,
    }
);

pub const TEST_ROW_COUNT: usize = 100;

#[test]
fn test_rkyv() {
    let row = SizeTestRow { number: 1, id: 1 };
    let w = SizeTestWrapper {
        inner: row,
        is_deleted: false,
        lock: 1,
        id_lock: 1,
        number_lock: 1,
    };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&w).unwrap();

    println!("{:?}", bytes.len())
}

pub fn get_empty_test_wt() -> TestPersistWorkTable {
    let manager = Arc::new(DatabaseManager {
        config_path: "tests/data".to_string(),
        database_files_dir: "test/data".to_string(),
    });

    TestPersistWorkTable::new(manager)
}

pub fn get_test_wt() -> TestPersistWorkTable {
    let table = get_empty_test_wt();

    for i in 1..100 {
        let row = TestPersistRow {
            another: i as u64,
            id: i,
        };
        table.insert(row).unwrap();
    }

    table
}

pub fn get_test_wt_without_secondary_indexes() -> TestWithoutSecondaryIndexesWorkTable {
    let manager = Arc::new(DatabaseManager {
        config_path: "tests/data".to_string(),
        database_files_dir: "test/data".to_string(),
    });

    let table = TestWithoutSecondaryIndexesWorkTable::new(manager);

    for i in 1..TEST_ROW_COUNT {
        let row = TestWithoutSecondaryIndexesRow {
            another: i as u64,
            id: i as u128,
        };
        table.insert(row).unwrap();
    }

    table
}
