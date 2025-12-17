use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use worktable::prelude::*;
use worktable::worktable;

#[derive(
    Clone,
    rkyv::Archive,
    Debug,
    Default,
    rkyv::Deserialize,
    Hash,
    rkyv::Serialize,
    From,
    Eq,
    Into,
    PartialEq,
    PartialOrd,
    Ord,
    SizeMeasure,
)]
#[rkyv(derive(PartialEq, Eq, PartialOrd, Ord, Debug))]
pub struct TestPrimaryKey(u64);
impl TablePrimaryKey for TestPrimaryKey {
    type Generator = std::sync::atomic::AtomicU64;
}
#[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize, PartialEq, MemStat)]
#[rkyv(derive(Debug))]
#[repr(C)]
pub struct TestRow {
    pub id: u64,
    pub test: i64,
    pub another: u64,
    pub exchange: String,
}
impl TableRow<TestPrimaryKey> for TestRow {
    fn get_primary_key(&self) -> TestPrimaryKey {
        self.id.clone().into()
    }
}
#[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize, PartialEq)]
#[rkyv(derive(Debug))]
#[repr(C)]
pub enum TestRowFields {
    Another,
    Id,
    Exchange,
    Test,
}
impl Query<TestRow> for TestRow {
    fn merge(self, row: TestRow) -> TestRow {
        self
    }
}
#[derive(Clone, Debug, From, PartialEq)]
#[non_exhaustive]
pub enum TestAvaiableTypes {
    #[from]
    STRING(String),
    #[from]
    U64(u64),
    #[from]
    I64(i64),
}
#[derive(rkyv::Archive, Debug, rkyv::Deserialize, rkyv::Serialize)]
#[repr(C)]
pub struct TestWrapper {
    inner: TestRow,
    is_ghosted: bool,
    is_deleted: bool,
}
impl RowWrapper<TestRow> for TestWrapper {
    fn get_inner(self) -> TestRow {
        self.inner
    }
    fn is_ghosted(&self) -> bool {
        self.is_ghosted
    }
    fn from_inner(inner: TestRow) -> Self {
        Self {
            inner,
            is_ghosted: true,
            is_deleted: false,
        }
    }
}
impl StorableRow for TestRow {
    type WrappedRow = TestWrapper;
}
impl GhostWrapper for ArchivedTestWrapper {
    fn unghost(&mut self) {
        self.is_ghosted = false;
    }
}
#[derive(Debug, Clone)]
pub struct TestLock {
    another_lock: Option<std::sync::Arc<Lock>>,
    id_lock: Option<std::sync::Arc<Lock>>,
    exchange_lock: Option<std::sync::Arc<Lock>>,
    test_lock: Option<std::sync::Arc<Lock>>,
}
impl TestLock {
    pub fn new() -> Self {
        Self {
            another_lock: None,
            id_lock: None,
            exchange_lock: None,
            test_lock: None,
        }
    }
}
impl RowLock for TestLock {
    fn is_locked(&self) -> bool {
        self.another_lock
            .as_ref()
            .map(|l| l.is_locked())
            .unwrap_or(false)
            || self
                .id_lock
                .as_ref()
                .map(|l| l.is_locked())
                .unwrap_or(false)
            || self
                .exchange_lock
                .as_ref()
                .map(|l| l.is_locked())
                .unwrap_or(false)
            || self
                .test_lock
                .as_ref()
                .map(|l| l.is_locked())
                .unwrap_or(false)
    }
    #[allow(clippy::mutable_key_type)]
    fn lock(
        &mut self,
        id: u16,
    ) -> (
        std::collections::HashSet<std::sync::Arc<Lock>>,
        std::sync::Arc<Lock>,
    ) {
        let mut set = std::collections::HashSet::new();
        let lock = std::sync::Arc::new(Lock::new(id));
        if let Some(lock) = &self.another_lock {
            set.insert(lock.clone());
        }
        self.another_lock = Some(lock.clone());
        if let Some(lock) = &self.id_lock {
            set.insert(lock.clone());
        }
        self.id_lock = Some(lock.clone());
        if let Some(lock) = &self.exchange_lock {
            set.insert(lock.clone());
        }
        self.exchange_lock = Some(lock.clone());
        if let Some(lock) = &self.test_lock {
            set.insert(lock.clone());
        }
        self.test_lock = Some(lock.clone());
        (set, lock)
    }
    fn with_lock(id: u16) -> (Self, std::sync::Arc<Lock>) {
        let lock = std::sync::Arc::new(Lock::new(id));
        (
            Self {
                another_lock: Some(lock.clone()),
                id_lock: Some(lock.clone()),
                exchange_lock: Some(lock.clone()),
                test_lock: Some(lock.clone()),
            },
            lock,
        )
    }
    #[allow(clippy::mutable_key_type)]
    fn merge(&mut self, other: &mut Self) -> std::collections::HashSet<std::sync::Arc<Lock>> {
        let mut set = std::collections::HashSet::new();
        if let Some(another_lock) = &other.another_lock {
            if self.another_lock.is_none() {
                self.another_lock = Some(another_lock.clone());
            } else {
                set.insert(another_lock.clone());
            }
        }
        other.another_lock = self.another_lock.clone();
        if let Some(id_lock) = &other.id_lock {
            if self.id_lock.is_none() {
                self.id_lock = Some(id_lock.clone());
            } else {
                set.insert(id_lock.clone());
            }
        }
        other.id_lock = self.id_lock.clone();
        if let Some(exchange_lock) = &other.exchange_lock {
            if self.exchange_lock.is_none() {
                self.exchange_lock = Some(exchange_lock.clone());
            } else {
                set.insert(exchange_lock.clone());
            }
        }
        other.exchange_lock = self.exchange_lock.clone();
        if let Some(test_lock) = &other.test_lock {
            if self.test_lock.is_none() {
                self.test_lock = Some(test_lock.clone());
            } else {
                set.insert(test_lock.clone());
            }
        }
        other.test_lock = self.test_lock.clone();
        set
    }
}
#[derive(Debug, MemStat)]
pub struct TestIndex {
    exchnage_idx: IndexMultiMap<String, Link, UnsizedNode<IndexMultiPair<String, Link>>>,
    test_idx: IndexMap<i64, Link>,
    another_idx: IndexMultiMap<u64, Link>,
}
impl TableSecondaryIndex<TestRow, TestAvaiableTypes, TestAvailableIndexes> for TestIndex {
    fn save_row(
        &self,
        row: TestRow,
        link: Link,
    ) -> core::result::Result<(), IndexError<TestAvailableIndexes>> {
        let mut inserted_indexes: Vec<TestAvailableIndexes> = vec![];
        if self
            .exchnage_idx
            .insert_checked(row.exchange.clone(), link)
            .is_none()
        {
            return Err(IndexError::AlreadyExists {
                at: TestAvailableIndexes::ExchnageIdx,
                inserted_already: inserted_indexes.clone(),
            });
        }
        inserted_indexes.push(TestAvailableIndexes::ExchnageIdx);
        if self
            .test_idx
            .insert_checked(row.test.clone(), link)
            .is_none()
        {
            return Err(IndexError::AlreadyExists {
                at: TestAvailableIndexes::TestIdx,
                inserted_already: inserted_indexes.clone(),
            });
        }
        inserted_indexes.push(TestAvailableIndexes::TestIdx);
        if self
            .another_idx
            .insert_checked(row.another.clone(), link)
            .is_none()
        {
            return Err(IndexError::AlreadyExists {
                at: TestAvailableIndexes::AnotherIdx,
                inserted_already: inserted_indexes.clone(),
            });
        }
        inserted_indexes.push(TestAvailableIndexes::AnotherIdx);
        core::result::Result::Ok(())
    }
    fn reinsert_row(
        &self,
        row_old: TestRow,
        link_old: Link,
        row_new: TestRow,
        link_new: Link,
    ) -> core::result::Result<(), IndexError<TestAvailableIndexes>> {
        let mut inserted_indexes: Vec<TestAvailableIndexes> = vec![];
        let row = &row_new;
        let val_new = row.test.clone();
        let row = &row_old;
        let val_old = row.test.clone();
        if val_new != val_old {
            if self
                .test_idx
                .insert_checked(val_new.clone(), link_new)
                .is_none()
            {
                return Err(IndexError::AlreadyExists {
                    at: TestAvailableIndexes::TestIdx,
                    inserted_already: inserted_indexes.clone(),
                });
            }
            inserted_indexes.push(TestAvailableIndexes::TestIdx);
        }
        let row = &row_new;
        let val_new = row.exchange.clone();
        let row = &row_old;
        let val_old = row.exchange.clone();
        self.exchnage_idx.insert(val_new.clone(), link_new);
        TableIndex::remove(&self.exchnage_idx, val_old, link_old);
        let row = &row_new;
        let val_new = row.test.clone();
        let row = &row_old;
        let val_old = row.test.clone();
        if val_new == val_old {
            self.test_idx.insert(val_new.clone(), link_new);
        } else {
            TableIndex::remove(&self.test_idx, val_old, link_old);
        }
        let row = &row_new;
        let val_new = row.another.clone();
        let row = &row_old;
        let val_old = row.another.clone();
        self.another_idx.insert(val_new.clone(), link_new);
        TableIndex::remove(&self.another_idx, val_old, link_old);
        core::result::Result::Ok(())
    }
    fn delete_row(
        &self,
        row: TestRow,
        link: Link,
    ) -> core::result::Result<(), IndexError<TestAvailableIndexes>> {
        self.exchnage_idx.remove(&row.exchange, &link);
        self.test_idx.remove(&row.test);
        self.another_idx.remove(&row.another, &link);
        core::result::Result::Ok(())
    }
    fn process_difference_insert(
        &self,
        link: Link,
        difference: std::collections::HashMap<&str, Difference<TestAvaiableTypes>>,
    ) -> core::result::Result<(), IndexError<TestAvailableIndexes>> {
        let mut inserted_indexes: Vec<TestAvailableIndexes> = vec![];
        if let Some(diff) = difference.get("exchange") {
            if let TestAvaiableTypes::STRING(new) = &diff.new {
                let key_new = new.to_string();
                if TableIndex::insert_checked(&self.exchnage_idx, key_new, link).is_none() {
                    return Err(IndexError::AlreadyExists {
                        at: TestAvailableIndexes::ExchnageIdx,
                        inserted_already: inserted_indexes.clone(),
                    });
                }
                inserted_indexes.push(TestAvailableIndexes::ExchnageIdx);
            }
        }
        if let Some(diff) = difference.get("test") {
            if let TestAvaiableTypes::I64(new) = &diff.new {
                let key_new = *new;
                if TableIndex::insert_checked(&self.test_idx, key_new, link).is_none() {
                    return Err(IndexError::AlreadyExists {
                        at: TestAvailableIndexes::TestIdx,
                        inserted_already: inserted_indexes.clone(),
                    });
                }
                inserted_indexes.push(TestAvailableIndexes::TestIdx);
            }
        }
        if let Some(diff) = difference.get("another") {
            if let TestAvaiableTypes::U64(new) = &diff.new {
                let key_new = *new;
                if TableIndex::insert_checked(&self.another_idx, key_new, link).is_none() {
                    return Err(IndexError::AlreadyExists {
                        at: TestAvailableIndexes::AnotherIdx,
                        inserted_already: inserted_indexes.clone(),
                    });
                }
                inserted_indexes.push(TestAvailableIndexes::AnotherIdx);
            }
        }
        core::result::Result::Ok(())
    }
    fn process_difference_remove(
        &self,
        link: Link,
        difference: std::collections::HashMap<&str, Difference<TestAvaiableTypes>>,
    ) -> core::result::Result<(), IndexError<TestAvailableIndexes>> {
        if let Some(diff) = difference.get("exchange") {
            if let TestAvaiableTypes::STRING(old) = &diff.old {
                let key_old = old.to_string();
                TableIndex::remove(&self.exchnage_idx, key_old, link);
            }
        }
        if let Some(diff) = difference.get("test") {
            if let TestAvaiableTypes::I64(old) = &diff.old {
                let key_old = *old;
                TableIndex::remove(&self.test_idx, key_old, link);
            }
        }
        if let Some(diff) = difference.get("another") {
            if let TestAvaiableTypes::U64(old) = &diff.old {
                let key_old = *old;
                TableIndex::remove(&self.another_idx, key_old, link);
            }
        }
        core::result::Result::Ok(())
    }
    fn delete_from_indexes(
        &self,
        row: TestRow,
        link: Link,
        indexes: Vec<TestAvailableIndexes>,
    ) -> core::result::Result<(), IndexError<TestAvailableIndexes>> {
        for index in indexes {
            match index {
                TestAvailableIndexes::ExchnageIdx => {
                    self.exchnage_idx.remove(&row.exchange, &link);
                }
                TestAvailableIndexes::TestIdx => {
                    self.test_idx.remove(&row.test);
                }
                TestAvailableIndexes::AnotherIdx => {
                    self.another_idx.remove(&row.another, &link);
                }
            }
        }
        core::result::Result::Ok(())
    }
}
impl TableSecondaryIndexInfo for TestIndex {
    fn index_info(&self) -> Vec<IndexInfo> {
        let mut info = Vec::new();
        info.push(IndexInfo {
            name: "exchnage_idx".to_string(),
            index_type: IndexKind::NonUnique,
            key_count: self.exchnage_idx.len(),
            capacity: self.exchnage_idx.capacity(),
            heap_size: self.exchnage_idx.heap_size(),
            used_size: self.exchnage_idx.used_size(),
            node_count: self.exchnage_idx.node_count(),
        });
        info.push(IndexInfo {
            name: "test_idx".to_string(),
            index_type: IndexKind::Unique,
            key_count: self.test_idx.len(),
            capacity: self.test_idx.capacity(),
            heap_size: self.test_idx.heap_size(),
            used_size: self.test_idx.used_size(),
            node_count: self.test_idx.node_count(),
        });
        info.push(IndexInfo {
            name: "another_idx".to_string(),
            index_type: IndexKind::NonUnique,
            key_count: self.another_idx.len(),
            capacity: self.another_idx.capacity(),
            heap_size: self.another_idx.heap_size(),
            used_size: self.another_idx.used_size(),
            node_count: self.another_idx.node_count(),
        });
        info
    }
    fn is_empty(&self) -> bool {
        self.exchnage_idx.len() == 0 && self.test_idx.len() == 0 && self.another_idx.len() == 0
    }
}
impl Default for TestIndex {
    fn default() -> Self {
        Self {
            exchnage_idx: IndexMultiMap::with_maximum_node_size(TEST_INNER_SIZE),
            test_idx: IndexMap::with_maximum_node_size(
                get_index_page_size_from_data_length::<i64>(TEST_INNER_SIZE),
            ),
            another_idx: IndexMultiMap::with_maximum_node_size(
                get_index_page_size_from_data_length::<u64>(TEST_INNER_SIZE),
            ),
        }
    }
}
#[derive(Debug, Clone, Copy, MoreDisplay, PartialEq, PartialOrd, Ord, Hash, Eq)]
pub enum TestAvailableIndexes {
    ExchnageIdx,
    TestIdx,
    AnotherIdx,
}
impl AvailableIndex for TestAvailableIndexes {
    fn to_string_value(&self) -> String {
        ToString::to_string(&self)
    }
}
const TEST_PAGE_SIZE: usize = PAGE_SIZE;
const TEST_INNER_SIZE: usize = TEST_PAGE_SIZE - GENERAL_HEADER_SIZE;
#[derive(Debug)]
pub struct TestWorkTable(
    WorkTable<
        TestRow,
        TestPrimaryKey,
        TestAvaiableTypes,
        TestAvailableIndexes,
        TestIndex,
        TestLock,
        <TestPrimaryKey as TablePrimaryKey>::Generator,
        Vec<IndexPair<TestPrimaryKey, Link>>,
    >,
);
impl Default for TestWorkTable {
    fn default() -> Self {
        let mut inner = WorkTable::default();
        inner.table_name = "Test";
        Self(inner)
    }
}
impl TestWorkTable {
    pub fn name(&self) -> &'static str {
        &self.0.table_name
    }
    pub fn select<Pk>(&self, pk: Pk) -> Option<TestRow>
    where
        TestPrimaryKey: From<Pk>,
    {
        self.0.select(pk.into())
    }
    pub fn insert(&self, row: TestRow) -> core::result::Result<TestPrimaryKey, WorkTableError> {
        self.0.insert(row)
    }
    pub fn reinsert(
        &self,
        row_old: TestRow,
        row_new: TestRow,
    ) -> core::result::Result<TestPrimaryKey, WorkTableError> {
        self.0.reinsert(row_old, row_new)
    }
    pub async fn upsert(&self, row: TestRow) -> core::result::Result<(), WorkTableError> {
        let pk = row.get_primary_key();
        let need_to_update = {
            if let Some(_) = self.0.pk_map.get(&pk) {
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
    pub fn count(&self) -> usize {
        let count = self.0.pk_map.len();
        count
    }
    pub fn get_next_pk(&self) -> TestPrimaryKey {
        self.0.get_next_pk()
    }
    pub fn iter_with<F: Fn(TestRow) -> core::result::Result<(), WorkTableError>>(
        &self,
        f: F,
    ) -> core::result::Result<(), WorkTableError> {
        let first = self.0.pk_map.iter().next().map(|(k, v)| (k.clone(), *v));
        let Some((mut k, link)) = first else {
            return Ok(());
        };
        let data = self
            .0
            .data
            .select_non_ghosted(link)
            .map_err(WorkTableError::PagesError)?;
        f(data)?;
        let mut ind = false;
        while !ind {
            let next = {
                let mut iter = self.0.pk_map.range(k.clone()..);
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
                    .select_non_ghosted(link)
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
        F: Fn(TestRow) -> Fut,
        Fut: std::future::Future<Output = core::result::Result<(), WorkTableError>>,
    >(
        &self,
        f: F,
    ) -> core::result::Result<(), WorkTableError> {
        let first = self.0.pk_map.iter().next().map(|(k, v)| (k.clone(), *v));
        let Some((mut k, link)) = first else {
            return Ok(());
        };
        let data = self
            .0
            .data
            .select_non_ghosted(link)
            .map_err(WorkTableError::PagesError)?;
        f(data).await?;
        let mut ind = false;
        while !ind {
            let next = {
                let mut iter = self.0.pk_map.range(k.clone()..);
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
                    .select_non_ghosted(link)
                    .map_err(WorkTableError::PagesError)?;
                f(data).await?;
                k = key
            } else {
                ind = true;
            };
        }
        core::result::Result::Ok(())
    }
    pub fn system_info(&self) -> SystemInfo {
        self.0.system_info()
    }
}
impl TestWorkTable {
    pub fn select_by_exchange(
        &self,
        by: String,
    ) -> SelectQueryBuilder<
        TestRow,
        impl DoubleEndedIterator<Item = TestRow> + '_,
        TestColumnRange,
        TestRowFields,
    > {
        let rows = self
            .0
            .indexes
            .exchnage_idx
            .get(&by)
            .into_iter()
            .filter_map(|(_, link)| self.0.data.select_non_ghosted(*link).ok())
            .filter(move |r| &r.exchange == &by);
        SelectQueryBuilder::new(rows)
    }
    pub fn select_by_test(&self, by: i64) -> Option<TestRow> {
        let link = self.0.indexes.test_idx.get(&by).map(|kv| kv.get().value)?;
        self.0.data.select_non_ghosted(link).ok()
    }
    pub fn select_by_another(
        &self,
        by: u64,
    ) -> SelectQueryBuilder<
        TestRow,
        impl DoubleEndedIterator<Item = TestRow> + '_,
        TestColumnRange,
        TestRowFields,
    > {
        let rows = self
            .0
            .indexes
            .another_idx
            .get(&by)
            .into_iter()
            .filter_map(|(_, link)| self.0.data.select_non_ghosted(*link).ok())
            .filter(move |r| &r.another == &by);
        SelectQueryBuilder::new(rows)
    }
}
impl<I> SelectQueryExecutor<TestRow, I, TestColumnRange, TestRowFields>
    for SelectQueryBuilder<TestRow, I, TestColumnRange, TestRowFields>
where
    I: DoubleEndedIterator<Item = TestRow> + Sized,
{
    fn where_by<F>(
        self,
        predicate: F,
    ) -> SelectQueryBuilder<
        TestRow,
        impl DoubleEndedIterator<Item = TestRow> + Sized,
        TestColumnRange,
        TestRowFields,
    >
    where
        F: FnMut(&TestRow) -> bool,
    {
        SelectQueryBuilder {
            params: self.params,
            iter: self.iter.filter(predicate),
        }
    }
    fn execute(self) -> Result<Vec<TestRow>, WorkTableError> {
        let mut iter: Box<dyn DoubleEndedIterator<Item = TestRow>> = Box::new(self.iter);
        if !self.params.range.is_empty() {
            for (range, column) in &self.params.range {
                iter = match (column, range.clone().into()) {
                    (TestRowFields::Another, TestColumnRange::U64(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.another)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Another, TestColumnRange::U64Inclusive(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.another)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Another, TestColumnRange::U64From(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.another)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Another, TestColumnRange::U64To(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.another)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Another, TestColumnRange::U64ToInclusive(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.another)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Id, TestColumnRange::U64(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.id)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Id, TestColumnRange::U64Inclusive(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.id)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Id, TestColumnRange::U64From(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.id)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Id, TestColumnRange::U64To(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.id)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Id, TestColumnRange::U64ToInclusive(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.id)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Test, TestColumnRange::I64(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.test)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Test, TestColumnRange::I64Inclusive(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.test)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Test, TestColumnRange::I64From(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.test)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Test, TestColumnRange::I64To(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.test)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Test, TestColumnRange::I64ToInclusive(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.test)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    _ => unreachable!(),
                };
            }
        }
        if !self.params.order.is_empty() {
            let mut items: Vec<TestRow> = iter.collect();
            items.sort_by(|a, b| {
                for (order, col) in &self.params.order {
                    match col {
                        TestRowFields::Another => {
                            let cmp = a
                                .another
                                .partial_cmp(&b.another)
                                .unwrap_or(std::cmp::Ordering::Equal);
                            if cmp != std::cmp::Ordering::Equal {
                                return match order {
                                    Order::Asc => cmp,
                                    Order::Desc => cmp.reverse(),
                                };
                            }
                        }
                        TestRowFields::Id => {
                            let cmp = a.id.partial_cmp(&b.id).unwrap_or(std::cmp::Ordering::Equal);
                            if cmp != std::cmp::Ordering::Equal {
                                return match order {
                                    Order::Asc => cmp,
                                    Order::Desc => cmp.reverse(),
                                };
                            }
                        }
                        TestRowFields::Exchange => {
                            let cmp = a
                                .exchange
                                .partial_cmp(&b.exchange)
                                .unwrap_or(std::cmp::Ordering::Equal);
                            if cmp != std::cmp::Ordering::Equal {
                                return match order {
                                    Order::Asc => cmp,
                                    Order::Desc => cmp.reverse(),
                                };
                            }
                        }
                        TestRowFields::Test => {
                            let cmp = a
                                .test
                                .partial_cmp(&b.test)
                                .unwrap_or(std::cmp::Ordering::Equal);
                            if cmp != std::cmp::Ordering::Equal {
                                return match order {
                                    Order::Asc => cmp,
                                    Order::Desc => cmp.reverse(),
                                };
                            }
                        }
                        _ => continue,
                    }
                }
                std::cmp::Ordering::Equal
            });
            iter = Box::new(items.into_iter());
        }
        let iter_result: Box<dyn Iterator<Item = TestRow>> =
            if let Some(offset) = self.params.offset {
                Box::new(iter.skip(offset))
            } else {
                Box::new(iter)
            };
        let iter_result: Box<dyn Iterator<Item = TestRow>> = if let Some(limit) = self.params.limit
        {
            Box::new(iter_result.take(limit))
        } else {
            Box::new(iter_result)
        };
        Ok(iter_result.collect())
    }
}
#[derive(Debug, Clone)]
pub enum TestColumnRange {
    U64(std::ops::Range<u64>),
    U64Inclusive(std::ops::RangeInclusive<u64>),
    U64From(std::ops::RangeFrom<u64>),
    U64To(std::ops::RangeTo<u64>),
    U64ToInclusive(std::ops::RangeToInclusive<u64>),
    I64(std::ops::Range<i64>),
    I64Inclusive(std::ops::RangeInclusive<i64>),
    I64From(std::ops::RangeFrom<i64>),
    I64To(std::ops::RangeTo<i64>),
    I64ToInclusive(std::ops::RangeToInclusive<i64>),
}
impl From<std::ops::Range<u64>> for TestColumnRange {
    fn from(range: std::ops::Range<u64>) -> Self {
        Self::U64(range)
    }
}
impl From<std::ops::RangeInclusive<u64>> for TestColumnRange {
    fn from(range: std::ops::RangeInclusive<u64>) -> Self {
        Self::U64Inclusive(range)
    }
}
impl From<std::ops::RangeFrom<u64>> for TestColumnRange {
    fn from(range: std::ops::RangeFrom<u64>) -> Self {
        Self::U64From(range)
    }
}
impl From<std::ops::RangeTo<u64>> for TestColumnRange {
    fn from(range: std::ops::RangeTo<u64>) -> Self {
        Self::U64To(range)
    }
}
impl From<std::ops::RangeToInclusive<u64>> for TestColumnRange {
    fn from(range: std::ops::RangeToInclusive<u64>) -> Self {
        Self::U64ToInclusive(range)
    }
}
impl From<std::ops::Range<i64>> for TestColumnRange {
    fn from(range: std::ops::Range<i64>) -> Self {
        Self::I64(range)
    }
}
impl From<std::ops::RangeInclusive<i64>> for TestColumnRange {
    fn from(range: std::ops::RangeInclusive<i64>) -> Self {
        Self::I64Inclusive(range)
    }
}
impl From<std::ops::RangeFrom<i64>> for TestColumnRange {
    fn from(range: std::ops::RangeFrom<i64>) -> Self {
        Self::I64From(range)
    }
}
impl From<std::ops::RangeTo<i64>> for TestColumnRange {
    fn from(range: std::ops::RangeTo<i64>) -> Self {
        Self::I64To(range)
    }
}
impl From<std::ops::RangeToInclusive<i64>> for TestColumnRange {
    fn from(range: std::ops::RangeToInclusive<i64>) -> Self {
        Self::I64ToInclusive(range)
    }
}
#[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize)]
#[repr(C)]
pub struct ExchangeByTestQuery {
    pub exchange: String,
}
impl Query<TestRow> for ExchangeByTestQuery {
    fn merge(self, mut row: TestRow) -> TestRow {
        row.exchange = self.exchange;
        row
    }
}
#[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize)]
#[repr(C)]
pub struct ExchangeByIdQuery {
    pub exchange: String,
}
impl Query<TestRow> for ExchangeByIdQuery {
    fn merge(self, mut row: TestRow) -> TestRow {
        row.exchange = self.exchange;
        row
    }
}
#[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize)]
#[repr(C)]
pub struct ExchangeByAbotherQuery {
    pub exchange: String,
}
impl Query<TestRow> for ExchangeByAbotherQuery {
    fn merge(self, mut row: TestRow) -> TestRow {
        row.exchange = self.exchange;
        row
    }
}
pub type ExchangeByTestBy = i64;
pub type ExchangeByIdBy = u64;
pub type ExchangeByAbotherBy = u64;
impl TestLock {
    #[allow(clippy::mutable_key_type)]
    pub fn lock_update_exchange_by_test(
        &mut self,
        id: u16,
    ) -> (
        std::collections::HashSet<std::sync::Arc<Lock>>,
        std::sync::Arc<Lock>,
    ) {
        let mut set = std::collections::HashSet::new();
        let new_lock = std::sync::Arc::new(Lock::new(id));
        if let Some(lock) = &self.exchange_lock {
            set.insert(lock.clone());
        }
        self.exchange_lock = Some(new_lock.clone());
        (set, new_lock)
    }
    #[allow(clippy::mutable_key_type)]
    pub fn lock_update_exchange_by_id(
        &mut self,
        id: u16,
    ) -> (
        std::collections::HashSet<std::sync::Arc<Lock>>,
        std::sync::Arc<Lock>,
    ) {
        let mut set = std::collections::HashSet::new();
        let new_lock = std::sync::Arc::new(Lock::new(id));
        if let Some(lock) = &self.exchange_lock {
            set.insert(lock.clone());
        }
        self.exchange_lock = Some(new_lock.clone());
        (set, new_lock)
    }
    #[allow(clippy::mutable_key_type)]
    pub fn lock_update_exchange_by_abother(
        &mut self,
        id: u16,
    ) -> (
        std::collections::HashSet<std::sync::Arc<Lock>>,
        std::sync::Arc<Lock>,
    ) {
        let mut set = std::collections::HashSet::new();
        let new_lock = std::sync::Arc::new(Lock::new(id));
        if let Some(lock) = &self.exchange_lock {
            set.insert(lock.clone());
        }
        self.exchange_lock = Some(new_lock.clone());
        (set, new_lock)
    }
}
impl TestWorkTable {
    pub fn select_all(
        &self,
    ) -> SelectQueryBuilder<
        TestRow,
        impl DoubleEndedIterator<Item = TestRow> + '_ + Sized,
        TestColumnRange,
        TestRowFields,
    > {
        let iter = self
            .0
            .pk_map
            .iter()
            .filter_map(|(_, link)| self.0.data.select_non_ghosted(*link).ok());
        SelectQueryBuilder::new(iter)
    }
}
impl TestWorkTable {
    pub async fn update(&self, row: TestRow) -> core::result::Result<(), WorkTableError> {
        let pk = row.get_primary_key();
        let lock = {
            let lock_id = self.0.lock_map.next_id();
            if let Some(lock) = self.0.lock_map.get(&pk) {
                let mut lock_guard = lock.write().await;
                #[allow(clippy::mutable_key_type)]
                let (locks, op_lock) = lock_guard.lock(lock_id);
                drop(lock_guard);
                futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>()).await;
                op_lock
            } else {
                #[allow(clippy::mutable_key_type)]
                let (lock, op_lock) = TestLock::with_lock(lock_id);
                let mut lock = std::sync::Arc::new(tokio::sync::RwLock::new(lock));
                let mut guard = lock.write().await;
                if let Some(old_lock) = self.0.lock_map.insert(pk.clone(), lock.clone()) {
                    let mut old_lock_guard = old_lock.write().await;
                    #[allow(clippy::mutable_key_type)]
                    let locks = guard.merge(&mut *old_lock_guard);
                    drop(old_lock_guard);
                    drop(guard);
                    futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>())
                        .await;
                }
                op_lock
            }
        };
        let link = self
            .0
            .pk_map
            .get(&pk)
            .map(|v| v.get().value)
            .ok_or(WorkTableError::NotFound)?;
        let row_old = self.0.data.select_non_ghosted(link)?;
        self.0.update_state.insert(pk.clone(), row_old);
        let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row)
            .map_err(|_| WorkTableError::SerializeError)?;
        if true {
            lock.unlock();
            let lock = {
                let lock_id = self.0.lock_map.next_id();
                if let Some(lock) = self.0.lock_map.get(&pk) {
                    let mut lock_guard = lock.write().await;
                    #[allow(clippy::mutable_key_type)]
                    let (locks, op_lock) = lock_guard.lock(lock_id);
                    drop(lock_guard);
                    futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>())
                        .await;
                    op_lock
                } else {
                    #[allow(clippy::mutable_key_type)]
                    let (lock, op_lock) = TestLock::with_lock(lock_id);
                    let mut lock = std::sync::Arc::new(tokio::sync::RwLock::new(lock));
                    let mut guard = lock.write().await;
                    if let Some(old_lock) = self.0.lock_map.insert(pk.clone(), lock.clone()) {
                        let mut old_lock_guard = old_lock.write().await;
                        #[allow(clippy::mutable_key_type)]
                        let locks = guard.merge(&mut *old_lock_guard);
                        drop(old_lock_guard);
                        drop(guard);
                        futures::future::join_all(
                            locks.iter().map(|l| l.wait()).collect::<Vec<_>>(),
                        )
                        .await;
                    }
                    op_lock
                }
            };
            let row_old = self.0.data.select_non_ghosted(link)?;
            if let Err(e) = self.reinsert(row_old, row) {
                self.0.update_state.remove(&pk);
                lock.unlock();
                return Err(e);
            }
            self.0.update_state.remove(&pk);
            lock.unlock();
            self.0.lock_map.remove_with_lock_check(&pk).await;
            return core::result::Result::Ok(());
        }
        let mut archived_row = unsafe {
            rkyv::access_unchecked_mut::<<TestRow as rkyv::Archive>::Archived>(&mut bytes[..])
                .unseal_unchecked()
        };
        let op_id = OperationId::Single(uuid::Uuid::now_v7());
        let row_old = self.0.data.select_non_ghosted(link)?;
        let row_new = row.clone();
        let updated_bytes: Vec<u8> = vec![];
        let mut diffs: std::collections::HashMap<&str, Difference<TestAvaiableTypes>> =
            std::collections::HashMap::new();
        let old = &row_old.exchange;
        let new = &row_new.exchange;
        if old != new {
            let diff = Difference::<TestAvaiableTypes> {
                old: old.clone().into(),
                new: new.clone().into(),
            };
            diffs.insert("exchange", diff);
        }
        let old = &row_old.test;
        let new = &row_new.test;
        if old != new {
            let diff = Difference::<TestAvaiableTypes> {
                old: old.clone().into(),
                new: new.clone().into(),
            };
            diffs.insert("test", diff);
        }
        let old = &row_old.another;
        let new = &row_new.another;
        if old != new {
            let diff = Difference::<TestAvaiableTypes> {
                old: old.clone().into(),
                new: new.clone().into(),
            };
            diffs.insert("another", diff);
        }
        let indexes_res = self
            .0
            .indexes
            .process_difference_insert(link, diffs.clone());
        if let Err(e) = indexes_res {
            return match e {
                IndexError::AlreadyExists {
                    at,
                    inserted_already,
                } => {
                    self.0.indexes.delete_from_indexes(
                        row_new.merge(row_old.clone()),
                        link,
                        inserted_already,
                    )?;
                    Err(WorkTableError::AlreadyExists(at.to_string_value()))
                }
                IndexError::NotFound => Err(WorkTableError::NotFound),
            };
        }
        unsafe {
            self.0
                .data
                .with_mut_ref(link, move |archived| {
                    std::mem::swap(&mut archived.inner.another, &mut archived_row.another);
                    std::mem::swap(&mut archived.inner.id, &mut archived_row.id);
                    std::mem::swap(&mut archived.inner.exchange, &mut archived_row.exchange);
                    std::mem::swap(&mut archived.inner.test, &mut archived_row.test);
                })
                .map_err(WorkTableError::PagesError)?
        };
        self.0.indexes.process_difference_remove(link, diffs)?;
        self.0.update_state.remove(&pk);
        lock.unlock();
        self.0.lock_map.remove_with_lock_check(&pk).await;
        core::result::Result::Ok(())
    }
    pub async fn update_exchange_by_test(
        &self,
        row: ExchangeByTestQuery,
        by: ExchangeByTestBy,
    ) -> core::result::Result<(), WorkTableError> {
        let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row)
            .map_err(|_| WorkTableError::SerializeError)?;
        let mut archived_row = unsafe {
            rkyv::access_unchecked_mut::<<ExchangeByTestQuery as rkyv::Archive>::Archived>(
                &mut bytes[..],
            )
            .unseal_unchecked()
        };
        let link = self
            .0
            .indexes
            .test_idx
            .get(&by)
            .map(|kv| kv.get().value)
            .ok_or(WorkTableError::NotFound)?;
        let pk = self
            .0
            .data
            .select_non_ghosted(link)?
            .get_primary_key()
            .clone();
        let lock = {
            let lock_id = self.0.lock_map.next_id();
            if let Some(lock) = self.0.lock_map.get(&pk) {
                let mut lock_guard = lock.write().await;
                #[allow(clippy::mutable_key_type)]
                let (locks, op_lock) = lock_guard.lock_update_exchange_by_test(lock_id);
                drop(lock_guard);
                futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>()).await;
                op_lock
            } else {
                let mut lock = TestLock::new();
                #[allow(clippy::mutable_key_type)]
                let (_, op_lock) = lock.lock_update_exchange_by_test(lock_id);
                let lock = std::sync::Arc::new(tokio::sync::RwLock::new(lock));
                let mut guard = lock.write().await;
                if let Some(old_lock) = self.0.lock_map.insert(pk.clone(), lock.clone()) {
                    let mut old_lock_guard = old_lock.write().await;
                    #[allow(clippy::mutable_key_type)]
                    let locks = guard.merge(&mut *old_lock_guard);
                    drop(old_lock_guard);
                    drop(guard);
                    futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>())
                        .await;
                }
                op_lock
            }
        };
        let link = self
            .0
            .indexes
            .test_idx
            .get(&by)
            .map(|kv| kv.get().value)
            .ok_or(WorkTableError::NotFound)?;
        let op_id = OperationId::Single(uuid::Uuid::now_v7());
        let mut need_to_reinsert = true;
        need_to_reinsert |= archived_row.get_exchange_size() != self.get_exchange_size(link)?;
        if need_to_reinsert {
            lock.unlock();
            let lock = {
                let lock_id = self.0.lock_map.next_id();
                if let Some(lock) = self.0.lock_map.get(&pk) {
                    let mut lock_guard = lock.write().await;
                    #[allow(clippy::mutable_key_type)]
                    let (locks, op_lock) = lock_guard.lock(lock_id);
                    drop(lock_guard);
                    futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>())
                        .await;
                    op_lock
                } else {
                    #[allow(clippy::mutable_key_type)]
                    let (lock, op_lock) = TestLock::with_lock(lock_id);
                    let mut lock = std::sync::Arc::new(tokio::sync::RwLock::new(lock));
                    let mut guard = lock.write().await;
                    if let Some(old_lock) = self.0.lock_map.insert(pk.clone(), lock.clone()) {
                        let mut old_lock_guard = old_lock.write().await;
                        #[allow(clippy::mutable_key_type)]
                        let locks = guard.merge(&mut *old_lock_guard);
                        drop(old_lock_guard);
                        drop(guard);
                        futures::future::join_all(
                            locks.iter().map(|l| l.wait()).collect::<Vec<_>>(),
                        )
                        .await;
                    }
                    op_lock
                }
            };
            let row_old = self
                .0
                .select(pk.clone())
                .expect("should not be deleted by other thread");
            let mut row_new = row_old.clone();
            let pk = row_old.get_primary_key().clone();
            row_new.exchange = row.exchange;
            if let Err(e) = self.reinsert(row_old, row_new) {
                self.0.update_state.remove(&pk);
                lock.unlock();
                return Err(e);
            }
            lock.unlock();
            self.0.lock_map.remove_with_lock_check(&pk).await;
            return core::result::Result::Ok(());
        }
        let row_old = self.0.data.select_non_ghosted(link)?;
        let row_new = row.clone();
        let updated_bytes: Vec<u8> = vec![];
        let mut diffs: std::collections::HashMap<&str, Difference<TestAvaiableTypes>> =
            std::collections::HashMap::new();
        let old = &row_old.exchange;
        let new = &row_new.exchange;
        if old != new {
            let diff = Difference::<TestAvaiableTypes> {
                old: old.clone().into(),
                new: new.clone().into(),
            };
            diffs.insert("exchange", diff);
        }
        let indexes_res = self
            .0
            .indexes
            .process_difference_insert(link, diffs.clone());
        if let Err(e) = indexes_res {
            return match e {
                IndexError::AlreadyExists {
                    at,
                    inserted_already,
                } => {
                    self.0.indexes.delete_from_indexes(
                        row_new.merge(row_old.clone()),
                        link,
                        inserted_already,
                    )?;
                    Err(WorkTableError::AlreadyExists(at.to_string_value()))
                }
                IndexError::NotFound => Err(WorkTableError::NotFound),
            };
        }
        unsafe {
            self.0
                .data
                .with_mut_ref(link, |archived| {
                    std::mem::swap(&mut archived.inner.exchange, &mut archived_row.exchange);
                })
                .map_err(WorkTableError::PagesError)?;
        }
        self.0.indexes.process_difference_remove(link, diffs)?;
        lock.unlock();
        self.0.lock_map.remove_with_lock_check(&pk).await;
        core::result::Result::Ok(())
    }
    pub async fn update_exchange_by_id<Pk>(
        &self,
        row: ExchangeByIdQuery,
        pk: Pk,
    ) -> core::result::Result<(), WorkTableError>
    where
        TestPrimaryKey: From<Pk>,
    {
        let pk = pk.into();
        let lock = {
            let lock_id = self.0.lock_map.next_id();
            if let Some(lock) = self.0.lock_map.get(&pk) {
                let mut lock_guard = lock.write().await;
                #[allow(clippy::mutable_key_type)]
                let (locks, op_lock) = lock_guard.lock_update_exchange_by_id(lock_id);
                drop(lock_guard);
                futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>()).await;
                op_lock
            } else {
                let mut lock = TestLock::new();
                #[allow(clippy::mutable_key_type)]
                let (_, op_lock) = lock.lock_update_exchange_by_id(lock_id);
                let lock = std::sync::Arc::new(tokio::sync::RwLock::new(lock));
                let mut guard = lock.write().await;
                if let Some(old_lock) = self.0.lock_map.insert(pk.clone(), lock.clone()) {
                    let mut old_lock_guard = old_lock.write().await;
                    #[allow(clippy::mutable_key_type)]
                    let locks = guard.merge(&mut *old_lock_guard);
                    drop(old_lock_guard);
                    drop(guard);
                    futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>())
                        .await;
                }
                op_lock
            }
        };
        let link = self
            .0
            .pk_map
            .get(&pk)
            .map(|v| v.get().value)
            .ok_or(WorkTableError::NotFound)?;
        let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row)
            .map_err(|_| WorkTableError::SerializeError)?;
        let mut archived_row = unsafe {
            rkyv::access_unchecked_mut::<<ExchangeByIdQuery as rkyv::Archive>::Archived>(
                &mut bytes[..],
            )
            .unseal_unchecked()
        };
        let op_id = OperationId::Single(uuid::Uuid::now_v7());
        let mut need_to_reinsert = true;
        need_to_reinsert |= archived_row.get_exchange_size() != self.get_exchange_size(link)?;
        if need_to_reinsert {
            lock.unlock();
            let lock = {
                let lock_id = self.0.lock_map.next_id();
                if let Some(lock) = self.0.lock_map.get(&pk) {
                    let mut lock_guard = lock.write().await;
                    #[allow(clippy::mutable_key_type)]
                    let (locks, op_lock) = lock_guard.lock(lock_id);
                    drop(lock_guard);
                    futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>())
                        .await;
                    op_lock
                } else {
                    #[allow(clippy::mutable_key_type)]
                    let (lock, op_lock) = TestLock::with_lock(lock_id);
                    let mut lock = std::sync::Arc::new(tokio::sync::RwLock::new(lock));
                    let mut guard = lock.write().await;
                    if let Some(old_lock) = self.0.lock_map.insert(pk.clone(), lock.clone()) {
                        let mut old_lock_guard = old_lock.write().await;
                        #[allow(clippy::mutable_key_type)]
                        let locks = guard.merge(&mut *old_lock_guard);
                        drop(old_lock_guard);
                        drop(guard);
                        futures::future::join_all(
                            locks.iter().map(|l| l.wait()).collect::<Vec<_>>(),
                        )
                        .await;
                    }
                    op_lock
                }
            };
            let row_old = self
                .0
                .select(pk.clone())
                .expect("should not be deleted by other thread");
            let mut row_new = row_old.clone();
            let pk = row_old.get_primary_key().clone();
            row_new.exchange = row.exchange;
            if let Err(e) = self.reinsert(row_old, row_new) {
                self.0.update_state.remove(&pk);
                lock.unlock();
                return Err(e);
            }
            lock.unlock();
            self.0.lock_map.remove_with_lock_check(&pk).await;
            return core::result::Result::Ok(());
        }
        let row_old = self.0.data.select_non_ghosted(link)?;
        let row_new = row.clone();
        let updated_bytes: Vec<u8> = vec![];
        let mut diffs: std::collections::HashMap<&str, Difference<TestAvaiableTypes>> =
            std::collections::HashMap::new();
        let old = &row_old.exchange;
        let new = &row_new.exchange;
        if old != new {
            let diff = Difference::<TestAvaiableTypes> {
                old: old.clone().into(),
                new: new.clone().into(),
            };
            diffs.insert("exchange", diff);
        }
        let indexes_res = self
            .0
            .indexes
            .process_difference_insert(link, diffs.clone());
        if let Err(e) = indexes_res {
            return match e {
                IndexError::AlreadyExists {
                    at,
                    inserted_already,
                } => {
                    self.0.indexes.delete_from_indexes(
                        row_new.merge(row_old.clone()),
                        link,
                        inserted_already,
                    )?;
                    Err(WorkTableError::AlreadyExists(at.to_string_value()))
                }
                IndexError::NotFound => Err(WorkTableError::NotFound),
            };
        }
        unsafe {
            self.0
                .data
                .with_mut_ref(link, |archived| {
                    std::mem::swap(&mut archived.inner.exchange, &mut archived_row.exchange);
                })
                .map_err(WorkTableError::PagesError)?
        };
        self.0.indexes.process_difference_remove(link, diffs)?;
        lock.unlock();
        self.0.lock_map.remove_with_lock_check(&pk).await;
        core::result::Result::Ok(())
    }
    pub async fn update_exchange_by_abother(
        &self,
        row: ExchangeByAbotherQuery,
        by: ExchangeByAbotherBy,
    ) -> core::result::Result<(), WorkTableError> {
        let links: Vec<_> = self
            .0
            .indexes
            .another_idx
            .get(&by)
            .map(|(_, l)| *l)
            .collect();
        let mut locks = std::collections::HashMap::new();
        for link in links.iter() {
            let pk = self
                .0
                .data
                .select_non_ghosted(*link)?
                .get_primary_key()
                .clone();
            let op_lock = {
                let lock_id = self.0.lock_map.next_id();
                if let Some(lock) = self.0.lock_map.get(&pk) {
                    let mut lock_guard = lock.write().await;
                    #[allow(clippy::mutable_key_type)]
                    let (locks, op_lock) = lock_guard.lock_update_exchange_by_abother(lock_id);
                    drop(lock_guard);
                    futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>())
                        .await;
                    op_lock
                } else {
                    let mut lock = TestLock::new();
                    #[allow(clippy::mutable_key_type)]
                    let (_, op_lock) = lock.lock_update_exchange_by_abother(lock_id);
                    let lock = std::sync::Arc::new(tokio::sync::RwLock::new(lock));
                    let mut guard = lock.write().await;
                    if let Some(old_lock) = self.0.lock_map.insert(pk.clone(), lock.clone()) {
                        let mut old_lock_guard = old_lock.write().await;
                        #[allow(clippy::mutable_key_type)]
                        let locks = guard.merge(&mut *old_lock_guard);
                        drop(old_lock_guard);
                        drop(guard);
                        futures::future::join_all(
                            locks.iter().map(|l| l.wait()).collect::<Vec<_>>(),
                        )
                        .await;
                    }
                    op_lock
                }
            };
            locks.insert(pk, op_lock);
        }
        let links: Vec<_> = self
            .0
            .indexes
            .another_idx
            .get(&by)
            .map(|(_, l)| *l)
            .collect();
        let mut pk_to_unlock: std::collections::HashMap<_, std::sync::Arc<Lock>> =
            std::collections::HashMap::new();
        let op_id = OperationId::Multi(uuid::Uuid::now_v7());
        for link in links.into_iter() {
            let pk = self
                .0
                .data
                .select_non_ghosted(link)?
                .get_primary_key()
                .clone();
            let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row)
                .map_err(|_| WorkTableError::SerializeError)?;
            let mut archived_row = unsafe {
                rkyv::access_unchecked_mut::<<ExchangeByAbotherQuery as rkyv::Archive>::Archived>(
                    &mut bytes[..],
                )
                .unseal_unchecked()
            };
            let mut need_to_reinsert = true;
            need_to_reinsert |= archived_row.get_exchange_size() != self.get_exchange_size(link)?;
            if need_to_reinsert {
                let op_lock = locks
                    .remove(&pk)
                    .expect("should not be deleted as links are unique");
                op_lock.unlock();
                let lock = {
                    let lock_id = self.0.lock_map.next_id();
                    if let Some(lock) = self.0.lock_map.get(&pk) {
                        let mut lock_guard = lock.write().await;
                        #[allow(clippy::mutable_key_type)]
                        let (locks, op_lock) = lock_guard.lock(lock_id);
                        drop(lock_guard);
                        futures::future::join_all(
                            locks.iter().map(|l| l.wait()).collect::<Vec<_>>(),
                        )
                        .await;
                        op_lock
                    } else {
                        #[allow(clippy::mutable_key_type)]
                        let (lock, op_lock) = TestLock::with_lock(lock_id);
                        let mut lock = std::sync::Arc::new(tokio::sync::RwLock::new(lock));
                        let mut guard = lock.write().await;
                        if let Some(old_lock) = self.0.lock_map.insert(pk.clone(), lock.clone()) {
                            let mut old_lock_guard = old_lock.write().await;
                            #[allow(clippy::mutable_key_type)]
                            let locks = guard.merge(&mut *old_lock_guard);
                            drop(old_lock_guard);
                            drop(guard);
                            futures::future::join_all(
                                locks.iter().map(|l| l.wait()).collect::<Vec<_>>(),
                            )
                            .await;
                        }
                        op_lock
                    }
                };
                let row_old = self
                    .select(pk.clone())
                    .expect("should not be deleted by other thread");
                let mut row_new = row_old.clone();
                row_new.exchange = row.exchange.clone();
                if let Err(e) = self.reinsert(row_old, row_new) {
                    self.0.update_state.remove(&pk);
                    lock.unlock();
                    return Err(e);
                }
                lock.unlock();
                self.0.lock_map.remove_with_lock_check(&pk).await;
                continue;
            } else {
                pk_to_unlock.insert(
                    pk.clone(),
                    locks
                        .remove(&pk)
                        .expect("should not be deleted as links are unique"),
                );
            }
            let row_old = self.0.data.select_non_ghosted(link)?;
            let row_new = row.clone();
            let updated_bytes: Vec<u8> = vec![];
            let mut diffs: std::collections::HashMap<&str, Difference<TestAvaiableTypes>> =
                std::collections::HashMap::new();
            let old = &row_old.exchange;
            let new = &row_new.exchange;
            if old != new {
                let diff = Difference::<TestAvaiableTypes> {
                    old: old.clone().into(),
                    new: new.clone().into(),
                };
                diffs.insert("exchange", diff);
            }
            let indexes_res = self
                .0
                .indexes
                .process_difference_insert(link, diffs.clone());
            if let Err(e) = indexes_res {
                return match e {
                    IndexError::AlreadyExists {
                        at,
                        inserted_already,
                    } => {
                        self.0.indexes.delete_from_indexes(
                            row_new.merge(row_old.clone()),
                            link,
                            inserted_already,
                        )?;
                        Err(WorkTableError::AlreadyExists(at.to_string_value()))
                    }
                    IndexError::NotFound => Err(WorkTableError::NotFound),
                };
            }
            unsafe {
                self.0
                    .data
                    .with_mut_ref(link, |archived| {
                        std::mem::swap(&mut archived.inner.exchange, &mut archived_row.exchange);
                    })
                    .map_err(WorkTableError::PagesError)?;
            }
            self.0.indexes.process_difference_remove(link, diffs)?;
        }
        for (pk, lock) in pk_to_unlock {
            lock.unlock();
            self.0.lock_map.remove_with_lock_check(&pk).await;
        }
        core::result::Result::Ok(())
    }
}
impl TestWorkTable {}
impl TestWorkTable {
    pub async fn delete(&self, pk: TestPrimaryKey) -> core::result::Result<(), WorkTableError> {
        let lock = {
            let lock_id = self.0.lock_map.next_id();
            if let Some(lock) = self.0.lock_map.get(&pk) {
                let mut lock_guard = lock.write().await;
                #[allow(clippy::mutable_key_type)]
                let (locks, op_lock) = lock_guard.lock(lock_id);
                drop(lock_guard);
                futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>()).await;
                op_lock
            } else {
                #[allow(clippy::mutable_key_type)]
                let (lock, op_lock) = TestLock::with_lock(lock_id);
                let mut lock = std::sync::Arc::new(tokio::sync::RwLock::new(lock));
                let mut guard = lock.write().await;
                if let Some(old_lock) = self.0.lock_map.insert(pk.clone(), lock.clone()) {
                    let mut old_lock_guard = old_lock.write().await;
                    #[allow(clippy::mutable_key_type)]
                    let locks = guard.merge(&mut *old_lock_guard);
                    drop(old_lock_guard);
                    drop(guard);
                    futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>())
                        .await;
                }
                op_lock
            }
        };
        let link = match self
            .0
            .pk_map
            .get(&pk)
            .map(|v| v.get().value)
            .ok_or(WorkTableError::NotFound)
        {
            Ok(l) => l,
            Err(e) => {
                lock.unlock();
                self.0.lock_map.remove_with_lock_check(&pk).await;

                return Err(e);
            }
        };

        let row = self.select(pk.clone()).unwrap();
        self.0.indexes.delete_row(row, link)?;
        self.0.pk_map.remove(&pk);
        self.0
            .data
            .delete(link)
            .map_err(WorkTableError::PagesError)?;
        lock.unlock();
        self.0.lock_map.remove_with_lock_check(&pk).await;
        core::result::Result::Ok(())
    }
    pub fn delete_without_lock(
        &self,
        pk: TestPrimaryKey,
    ) -> core::result::Result<(), WorkTableError> {
        let link = self
            .0
            .pk_map
            .get(&pk)
            .map(|v| v.get().value)
            .ok_or(WorkTableError::NotFound)?;
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
impl TestWorkTable {
    fn get_exchange_size(&self, link: Link) -> core::result::Result<usize, WorkTableError> {
        self.0
            .data
            .with_ref(link, |row_ref| {
                row_ref.inner.exchange.as_str().to_string().aligned_size()
            })
            .map_err(WorkTableError::PagesError)
    }
}
impl ArchivedExchangeByTestQuery {
    pub fn get_exchange_size(&self) -> usize {
        self.exchange.as_str().to_string().aligned_size()
    }
}
impl ArchivedExchangeByIdQuery {
    pub fn get_exchange_size(&self) -> usize {
        self.exchange.as_str().to_string().aligned_size()
    }
}
impl ArchivedExchangeByAbotherQuery {
    pub fn get_exchange_size(&self) -> usize {
        self.exchange.as_str().to_string().aligned_size()
    }
}
#[tokio::test]
async fn test_update_string_full_row() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();
    let first_link = table.0.pk_map.get(&pk).unwrap().get().value;

    table
        .update(TestRow {
            id: row.id,
            test: 1,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
        })
        .await
        .unwrap();

    let row = table.select_by_test(1).unwrap();

    assert_eq!(
        row,
        TestRow {
            id: 0,
            test: 1,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
        }
    );
    assert_eq!(table.0.data.get_empty_links().first().unwrap(), &first_link)
}

#[tokio::test]
async fn test_update_string_by_unique() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();
    let first_link = table.0.pk_map.get(&pk).unwrap().get().value;

    let row = ExchangeByTestQuery {
        exchange: "bigger test to test string update".to_string(),
    };
    table.update_exchange_by_test(row, 1).await.unwrap();

    let row = table.select_by_test(1).unwrap();

    assert_eq!(
        row,
        TestRow {
            id: 0,
            test: 1,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
        }
    );
    assert_eq!(table.0.data.get_empty_links().first().unwrap(), &first_link)
}

#[tokio::test]
async fn test_update_string_by_pk() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();
    let first_link = table.0.pk_map.get(&pk).unwrap().get().value;

    let row = ExchangeByIdQuery {
        exchange: "bigger test to test string update".to_string(),
    };
    table.update_exchange_by_id(row, pk).await.unwrap();

    let row = table.select_by_test(1).unwrap();

    assert_eq!(
        row,
        TestRow {
            id: 0,
            test: 1,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
        }
    );
    assert_eq!(table.0.data.get_empty_links().first().unwrap(), &first_link)
}

#[tokio::test]
async fn test_update_string_by_non_unique() {
    let table = TestWorkTable::default();
    let row1 = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let pk = table.insert(row1.clone()).unwrap();
    let first_link = table.0.pk_map.get(&pk).unwrap().get().value;
    let row2 = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 1,
        exchange: "test".to_string(),
    };
    let pk = table.insert(row2.clone()).unwrap();
    let second_link = table.0.pk_map.get(&pk).unwrap().get().value;

    let row = ExchangeByAbotherQuery {
        exchange: "bigger test to test string update".to_string(),
    };
    table.update_exchange_by_abother(row, 1).await.unwrap();

    let all = table.select_all().execute().unwrap();

    assert_eq!(all.len(), 2);
    assert_eq!(
        &all[0],
        &TestRow {
            id: 0,
            test: 1,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
        }
    );
    assert_eq!(
        &all[1],
        &TestRow {
            id: 1,
            test: 2,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
        }
    );
    let empty_links = table.0.data.get_empty_links();
    assert_eq!(empty_links.len(), 1);
    let l = Link {
        page_id: first_link.page_id,
        offset: first_link.offset,
        length: first_link.length + second_link.length,
    };
    assert!(empty_links.contains(&l))
}

#[tokio::test]
async fn update_many_times() {
    let table = TestWorkTable::default();
    for i in 0..100 {
        let row = TestRow {
            id: table.get_next_pk().into(),
            test: i + 1,
            another: 1,
            exchange: format!("test_{i}"),
        };
        let _ = table.insert(row.clone()).unwrap();
    }
    let mut i_state = HashMap::new();
    for _ in 0..1000 {
        let val = fastrand::u64(..);
        let id_to_update = fastrand::u64(0..=99);
        table
            .update_exchange_by_id(
                ExchangeByIdQuery {
                    exchange: format!("test_{val}"),
                },
                id_to_update,
            )
            .await
            .unwrap();
        {
            i_state
                .entry(id_to_update as i64 + 1)
                .and_modify(|v| *v = format!("test_{val}"))
                .or_insert(format!("test_{val}"));
        }
    }

    for (test, val) in i_state {
        let row = table.select_by_test(test).unwrap();
        assert_eq!(row.exchange, val)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_parallel() {
    let table = Arc::new(TestWorkTable::default());
    let i_state = Arc::new(Mutex::new(HashMap::new()));
    for i in 0..100 {
        let row = TestRow {
            id: table.get_next_pk().into(),
            test: i + 1,
            another: 1,
            exchange: format!("test_{i}"),
        };
        let _ = table.insert(row.clone()).unwrap();
    }
    let shared = table.clone();
    let shared_i_state = i_state.clone();
    let h = tokio::spawn(async move {
        for _ in 0..1000 {
            let val = fastrand::u64(..);
            let id_to_update = fastrand::i64(1..=100);
            shared
                .update_exchange_by_test(
                    ExchangeByTestQuery {
                        exchange: format!("test_{val}"),
                    },
                    id_to_update,
                )
                .await
                .unwrap();
            {
                let mut guard = shared_i_state.lock();
                guard
                    .entry(id_to_update)
                    .and_modify(|v| *v = format!("test_{val}"))
                    .or_insert(format!("test_{val}"));
            }
            tokio::time::sleep(Duration::from_micros(5)).await;
        }
    });
    tokio::time::sleep(Duration::from_micros(20)).await;
    for _ in 0..1000 {
        let val = fastrand::u64(..);
        let id_to_update = fastrand::u64(0..=99);
        table
            .update_exchange_by_id(
                ExchangeByIdQuery {
                    exchange: format!("test_{val}"),
                },
                id_to_update,
            )
            .await
            .unwrap();
        {
            let mut guard = i_state.lock();
            guard
                .entry(id_to_update as i64 + 1)
                .and_modify(|v| *v = format!("test_{val}"))
                .or_insert(format!("test_{val}"));
        }
        tokio::time::sleep(Duration::from_micros(5)).await;
    }
    h.await.unwrap();

    for (test, val) in i_state.lock_arc().iter() {
        let row = table.select_by_test(*test).unwrap();
        assert_eq!(&row.exchange, val)
    }
}

worktable! (
    name: TestMoreStrings,
    columns: {
        id: u64 primary_key autoincrement,
        test: i64,
        another: u64,
        exchange: String,
        some_string: String,
        other_srting: String,
    },
    indexes: {
        test_idx: test unique,
        exchnage_idx: exchange,
        another_idx: another,
    }
    queries: {
        update: {
            ExchangeAndSomeByTest(exchange, some_string) by test,
            ExchangeAndSomeById(exchange, some_string) by id,
            ExchangeAgainById(exchange) by id,
            SomeById(some_string) by id,
            AnotherById(another) by id,
            ExchangeAndSomeByAnother(exchange, some_string) by another,
            SomeOtherByExchange(some_string, other_srting) by exchange,
        }
    }
);

#[tokio::test]
async fn test_update_many_strings_by_unique() {
    let table = TestMoreStringsWorkTable::default();
    let row = TestMoreStringsRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
        some_string: "some".to_string(),
        other_srting: "other".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();
    let first_link = table.0.pk_map.get(&pk).unwrap().get().value;

    let row = ExchangeAndSomeByTestQuery {
        exchange: "bigger test to test string update".to_string(),
        some_string: "some bigger some to test".to_string(),
    };
    table
        .update_exchange_and_some_by_test(row, 1)
        .await
        .unwrap();

    let row = table.select_by_test(1).unwrap();

    assert_eq!(
        row,
        TestMoreStringsRow {
            id: 0,
            test: 1,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
            some_string: "some bigger some to test".to_string(),
            other_srting: "other".to_string(),
        }
    );
    assert_eq!(table.0.data.get_empty_links().first().unwrap(), &first_link)
}

#[tokio::test]
async fn test_update_many_strings_by_pk() {
    let table = TestMoreStringsWorkTable::default();
    let row = TestMoreStringsRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
        some_string: "some".to_string(),
        other_srting: "other".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();
    let first_link = table.0.pk_map.get(&pk).unwrap().get().value;

    let row = ExchangeAndSomeByIdQuery {
        exchange: "bigger test to test string update".to_string(),
        some_string: "some bigger some to test".to_string(),
    };
    table.update_exchange_and_some_by_id(row, pk).await.unwrap();

    let row = table.select_by_test(1).unwrap();

    assert_eq!(
        row,
        TestMoreStringsRow {
            id: 0,
            test: 1,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
            some_string: "some bigger some to test".to_string(),
            other_srting: "other".to_string(),
        }
    );
    assert_eq!(table.0.data.get_empty_links().first().unwrap(), &first_link)
}

#[tokio::test]
async fn test_update_many_strings_by_non_unique() {
    let table = TestMoreStringsWorkTable::default();
    let row1 = TestMoreStringsRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
        some_string: "some".to_string(),
        other_srting: "other".to_string(),
    };
    let pk = table.insert(row1.clone()).unwrap();
    let first_link = table.0.pk_map.get(&pk).unwrap().get().value;
    let row2 = TestMoreStringsRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 1,
        exchange: "test another".to_string(),
        some_string: "some".to_string(),
        other_srting: "other".to_string(),
    };
    let pk = table.insert(row2.clone()).unwrap();
    let second_link = table.0.pk_map.get(&pk).unwrap().get().value;

    let row = ExchangeAndSomeByAnotherQuery {
        exchange: "bigger test to test string update".to_string(),
        some_string: "some bigger some to test".to_string(),
    };
    table
        .update_exchange_and_some_by_another(row, 1)
        .await
        .unwrap();

    let all = table.select_all().execute().unwrap();

    assert_eq!(all.len(), 2);
    assert_eq!(
        &all[0],
        &TestMoreStringsRow {
            id: 0,
            test: 1,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
            some_string: "some bigger some to test".to_string(),
            other_srting: "other".to_string(),
        }
    );
    assert_eq!(
        &all[1],
        &TestMoreStringsRow {
            id: 1,
            test: 2,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
            some_string: "some bigger some to test".to_string(),
            other_srting: "other".to_string(),
        }
    );
    let empty_links = table.0.data.get_empty_links();
    assert_eq!(empty_links.len(), 1);
    let l = Link {
        page_id: first_link.page_id,
        offset: first_link.offset,
        length: first_link.length + second_link.length,
    };
    assert!(empty_links.contains(&l));
}

#[tokio::test]
async fn test_update_many_strings_by_string() {
    let table = TestMoreStringsWorkTable::default();
    let row1 = TestMoreStringsRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
        some_string: "something".to_string(),
        other_srting: "other er".to_string(),
    };
    let pk = table.insert(row1.clone()).unwrap();
    let first_link = table.0.pk_map.get(&pk).unwrap().get().value;
    let row2 = TestMoreStringsRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 1,
        exchange: "test".to_string(),
        some_string: "some ome".to_string(),
        other_srting: "other".to_string(),
    };
    let pk = table.insert(row2.clone()).unwrap();
    let second_link = table.0.pk_map.get(&pk).unwrap().get().value;

    let row = SomeOtherByExchangeQuery {
        other_srting: "bigger test to test string update".to_string(),
        some_string: "some bigger some to test".to_string(),
    };
    table
        .update_some_other_by_exchange(row, "test".to_string())
        .await
        .unwrap();

    let all = table.select_all().execute().unwrap();

    assert_eq!(all.len(), 2);
    assert_eq!(
        &all[0],
        &TestMoreStringsRow {
            id: 0,
            test: 1,
            another: 1,
            other_srting: "bigger test to test string update".to_string(),
            some_string: "some bigger some to test".to_string(),
            exchange: "test".to_string(),
        }
    );
    assert_eq!(
        &all[1],
        &TestMoreStringsRow {
            id: 1,
            test: 2,
            another: 1,
            other_srting: "bigger test to test string update".to_string(),
            some_string: "some bigger some to test".to_string(),
            exchange: "test".to_string(),
        }
    );
    let empty_links = table.0.data.get_empty_links();
    assert_eq!(empty_links.len(), 1);
    let l = Link {
        page_id: first_link.page_id,
        offset: first_link.offset,
        length: first_link.length + second_link.length,
    };
    assert!(empty_links.contains(&l));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_parallel_more_strings() {
    let table = Arc::new(TestMoreStringsWorkTable::default());
    let e_state = Arc::new(Mutex::new(HashMap::new()));
    let s_state = Arc::new(Mutex::new(HashMap::new()));
    for i in 0..100 {
        let row = TestMoreStringsRow {
            id: table.get_next_pk().into(),
            test: i + 1,
            another: 1,
            exchange: format!("test_{i}"),
            some_string: format!("some_{i}"),
            other_srting: format!("other_{i}"),
        };
        let _ = table.insert(row.clone()).unwrap();
    }
    let shared = table.clone();
    let shared_e_state = e_state.clone();
    let h = tokio::spawn(async move {
        for _ in 0..2000 {
            let val = fastrand::u64(..);
            let id_to_update = fastrand::u64(0..=99);
            shared
                .update_exchange_again_by_id(
                    ExchangeAgainByIdQuery {
                        exchange: format!("test_{val}"),
                    },
                    id_to_update,
                )
                .await
                .unwrap();
            {
                let mut guard = shared_e_state.lock();
                guard
                    .entry(id_to_update)
                    .and_modify(|v| *v = format!("test_{val}"))
                    .or_insert(format!("test_{val}"));
            }
        }
    });
    for _ in 0..2000 {
        let val = fastrand::u64(..);
        let id_to_update = fastrand::u64(0..=99);
        table
            .update_some_by_id(
                SomeByIdQuery {
                    some_string: format!("some_{val}"),
                },
                id_to_update,
            )
            .await
            .unwrap();
        {
            let mut guard = s_state.lock();
            guard
                .entry(id_to_update)
                .and_modify(|v| *v = format!("some_{val}"))
                .or_insert(format!("some_{val}"));
        }
    }
    h.await.unwrap();

    for (id, e) in e_state.lock_arc().iter() {
        let row = table.select(*id).unwrap();
        assert_eq!(&row.exchange, e)
    }
    for (id, s) in s_state.lock_arc().iter() {
        let row = table.select(*id).unwrap();
        assert_eq!(&row.some_string, s)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn update_parallel_more_strings_more_threads() {
    let table = Arc::new(TestMoreStringsWorkTable::default());
    let e_state = Arc::new(Mutex::new(HashMap::new()));
    let s_state = Arc::new(Mutex::new(HashMap::new()));
    let a_state = Arc::new(Mutex::new(HashMap::new()));
    for i in 0..100 {
        let row = TestMoreStringsRow {
            id: table.get_next_pk().into(),
            test: i + 1,
            another: 1,
            exchange: format!("test_{i}"),
            some_string: format!("some_{i}"),
            other_srting: format!("other_{i}"),
        };
        let _ = table.insert(row.clone()).unwrap();
    }
    let shared = table.clone();
    let shared_e_state = e_state.clone();
    let h1 = tokio::spawn(async move {
        for _ in 0..2000 {
            let val = fastrand::u64(..);
            let id_to_update = fastrand::u64(0..=99);
            shared
                .update_exchange_again_by_id(
                    ExchangeAgainByIdQuery {
                        exchange: format!("test_{val}"),
                    },
                    id_to_update,
                )
                .await
                .unwrap();
            {
                let mut guard = shared_e_state.lock();
                guard
                    .entry(id_to_update)
                    .and_modify(|v| *v = format!("test_{val}"))
                    .or_insert(format!("test_{val}"));
            }
        }
    });
    let shared = table.clone();
    let shared_t_state = a_state.clone();
    let h2 = tokio::spawn(async move {
        for _ in 0..5000 {
            let val = fastrand::u64(..);
            let id_to_update = fastrand::u64(0..=99);
            shared
                .update_another_by_id(AnotherByIdQuery { another: val }, id_to_update)
                .await
                .unwrap();
            {
                let mut guard = shared_t_state.lock();
                guard
                    .entry(id_to_update)
                    .and_modify(|v| *v = val)
                    .or_insert(val);
            }
        }
    });
    for _ in 0..2000 {
        let val = fastrand::u64(..);
        let id_to_update = fastrand::u64(0..=99);
        table
            .update_some_by_id(
                SomeByIdQuery {
                    some_string: format!("some_{val}"),
                },
                id_to_update,
            )
            .await
            .unwrap();
        {
            let mut guard = s_state.lock();
            guard
                .entry(id_to_update)
                .and_modify(|v| *v = format!("some_{val}"))
                .or_insert(format!("some_{val}"));
        }
    }
    h1.await.unwrap();
    h2.await.unwrap();

    for (id, e) in e_state.lock_arc().iter() {
        let row = table.select(*id).unwrap();
        assert_eq!(&row.exchange, e)
    }
    for (id, s) in s_state.lock_arc().iter() {
        let row = table.select(*id).unwrap();
        assert_eq!(&row.some_string, s)
    }
    for (id, a) in a_state.lock_arc().iter() {
        let row = table.select(*id).unwrap();
        assert_eq!(&row.another, a)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn update_parallel_more_strings_with_select_non_unique() {
    let table = Arc::new(TestMoreStringsWorkTable::default());
    let e_state = Arc::new(Mutex::new(HashMap::new()));
    let a_state = Arc::new(Mutex::new(HashMap::new()));
    for i in 0..1000 {
        let e_val = fastrand::u8(0..100);
        let s_val = fastrand::u8(0..100);
        let row = TestMoreStringsRow {
            id: table.get_next_pk().into(),
            test: i + 1,
            another: 1,
            exchange: format!("test_{e_val}"),
            some_string: format!("some_{s_val}"),
            other_srting: format!("other_{i}"),
        };
        let _ = table.insert(row.clone()).unwrap();
    }
    let shared = table.clone();
    let shared_e_state = e_state.clone();
    let h1 = tokio::spawn(async move {
        for _ in 0..5_000 {
            let val = fastrand::u8(0..100);
            let id_to_update = fastrand::u64(0..1000);
            shared
                .update_exchange_again_by_id(
                    ExchangeAgainByIdQuery {
                        exchange: format!("test_{val}"),
                    },
                    id_to_update,
                )
                .await
                .unwrap();
            {
                let mut guard = shared_e_state.lock();
                guard
                    .entry(id_to_update)
                    .and_modify(|v| *v = format!("test_{val}"))
                    .or_insert(format!("test_{val}"));
            }
        }
    });
    let shared = table.clone();
    let shared_t_state = a_state.clone();
    let h2 = tokio::spawn(async move {
        for _ in 0..10_000 {
            let val = fastrand::u64(..);
            let id_to_update = fastrand::u64(0..1000);
            shared
                .update_another_by_id(AnotherByIdQuery { another: val }, id_to_update)
                .await
                .unwrap();
            {
                let mut guard = shared_t_state.lock();
                guard
                    .entry(id_to_update)
                    .and_modify(|v| *v = val)
                    .or_insert(val);
            }
        }
    });
    for _ in 0..20_000 {
        let val = fastrand::u8(0..100);
        let vals = table
            .select_by_exchange(format!("test_{val}"))
            .execute()
            .unwrap();
        for v in vals {
            assert_eq!(v.exchange, format!("test_{val}"))
        }
    }
    h1.await.unwrap();
    h2.await.unwrap();

    for (id, e) in e_state.lock_arc().iter() {
        let row = table.select(*id).unwrap();
        assert_eq!(&row.exchange, e)
    }
    for (id, a) in a_state.lock_arc().iter() {
        let row = table.select(*id).unwrap();
        assert_eq!(&row.another, a)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn delete_parallel() {
    let table = Arc::new(TestMoreStringsWorkTable::default());
    let deleted_state = Arc::new(Mutex::new(HashSet::new()));
    for i in 0..1000 {
        let e_val = fastrand::u8(0..100);
        let s_val = fastrand::u8(0..100);
        let row = TestMoreStringsRow {
            id: table.get_next_pk().into(),
            test: i + 1,
            another: 1,
            exchange: format!("test_{e_val}"),
            some_string: format!("some_{s_val}"),
            other_srting: format!("other_{i}"),
        };
        let _ = table.insert(row.clone()).unwrap();
    }
    let shared = table.clone();
    let h1 = tokio::spawn(async move {
        for i in 1_000..6_000 {
            let e_val = fastrand::u8(0..100);
            let s_val = fastrand::u8(0..100);
            let row = TestMoreStringsRow {
                id: shared.get_next_pk().into(),
                test: i + 1,
                another: 1,
                exchange: format!("test_{e_val}"),
                some_string: format!("some_{s_val}"),
                other_srting: format!("other_{i}"),
            };
            let _ = shared.insert(row.clone()).unwrap();
        }
    });
    let shared = table.clone();
    let shared_deleted_state = deleted_state.clone();
    let h2 = tokio::spawn(async move {
        for _ in 0..1_000 {
            let id_to_update = fastrand::u64(0..1000);
            let _ = shared.delete(id_to_update.into()).await;
            {
                let mut guard = shared_deleted_state.lock();
                guard.insert(id_to_update);
            }
        }
    });
    for _ in 0..5_000 {
        let val = fastrand::u8(0..100);
        let vals = table
            .select_by_exchange(format!("test_{val}"))
            .execute()
            .unwrap();
        for v in vals {
            assert_eq!(v.exchange, format!("test_{val}"))
        }
    }
    h1.await.unwrap();
    h2.await.unwrap();

    for id in deleted_state.lock_arc().iter() {
        let row = table.select(*id);
        assert!(row.is_none())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn update_parallel_more_strings_with_select_unique() {
    let table = Arc::new(TestMoreStringsWorkTable::default());
    let e_state = Arc::new(Mutex::new(HashMap::new()));
    let a_state = Arc::new(Mutex::new(HashMap::new()));
    for i in 0..1000 {
        let e_val = fastrand::u8(0..100);
        let s_val = fastrand::u8(0..100);
        let row = TestMoreStringsRow {
            id: table.get_next_pk().into(),
            test: i,
            another: 1,
            exchange: format!("test_{e_val}"),
            some_string: format!("some_{s_val}"),
            other_srting: format!("other_{i}"),
        };
        let _ = table.insert(row.clone()).unwrap();
    }
    let shared = table.clone();
    let shared_e_state = e_state.clone();
    let h1 = tokio::spawn(async move {
        for _ in 0..5_000 {
            let val = fastrand::u8(0..100);
            let id_to_update = fastrand::u64(0..1000);
            shared
                .update_exchange_again_by_id(
                    ExchangeAgainByIdQuery {
                        exchange: format!("test_{val}"),
                    },
                    id_to_update,
                )
                .await
                .unwrap();
            {
                let mut guard = shared_e_state.lock();
                guard
                    .entry(id_to_update)
                    .and_modify(|v| *v = format!("test_{val}"))
                    .or_insert(format!("test_{val}"));
            }
        }
    });
    let shared = table.clone();
    let shared_t_state = a_state.clone();
    let h2 = tokio::spawn(async move {
        for _ in 0..10_000 {
            let val = fastrand::u64(..);
            let id_to_update = fastrand::u64(0..1000);
            shared
                .update_another_by_id(AnotherByIdQuery { another: val }, id_to_update)
                .await
                .unwrap();
            {
                let mut guard = shared_t_state.lock();
                guard
                    .entry(id_to_update)
                    .and_modify(|v| *v = val)
                    .or_insert(val);
            }
        }
    });
    for _ in 0..20_000 {
        let val = fastrand::i64(0..1000);
        let res = table.select_by_test(val);
        assert!(res.is_some())
    }
    h1.await.unwrap();
    h2.await.unwrap();

    for (id, e) in e_state.lock_arc().iter() {
        let row = table.select(*id).unwrap();
        assert_eq!(&row.exchange, e)
    }
}
