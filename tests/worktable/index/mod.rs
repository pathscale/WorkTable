mod insert;
mod update_by_pk;
mod update_full;
mod update_query;

use worktable::prelude::*;
use worktable::worktable;

// The test checks updates for 3 indecies at once
worktable!(
    name: Test3Unique,
    columns: {
        id: u64 primary_key autoincrement,
        val: i64,
        attr1: String,
        attr2: i16,
        attr3: u64,
    },
    indexes: {
        idx1: attr1 unique,
        idx2: attr2 unique,
        idx3: attr3 unique,
    },
    queries: {
        update: {
            UniqueThreeAttrById(attr1, attr2, attr3) by id,
            UniqueTwoAttrByThird(attr1, attr2) by attr3,
        },
        delete: {
            ById() by id,
        }
    }
);

// The test checks updates for 3 indecies at once
worktable!(
    name: Test3NonUnique,
    columns: {
        id: u64 primary_key autoincrement,
        val: i64,
        attr1: String,
        attr2: i16,
        attr3: u64,
    },
    indexes: {
        idx1: attr1,
        idx2: attr2,
        idx3: attr3,
    },
    queries: {
        update: {
            ThreeAttrById(attr1, attr2, attr3) by id,
            TwoAttrByThird(attr1, attr2) by attr3,
        },
        delete: {
            ById() by id,
        }
    }
);

// The test checks updates for 2 indecies at once

worktable!(
    name: Test2,
    columns: {
        id: u64 primary_key autoincrement,
        val: i64,
        attr1: String,
        attr2: i16,
    },
    indexes: {
        idx1: attr1,
        idx2: attr2,
    },
    queries: {
        update: {
            AllAttrById(attr1, attr2) by id,
        },
        delete: {
            ById() by id,
        }
    }
);

#[tokio::test]
async fn update_2_idx() {
    let test_table = Test2WorkTable::default();

    let attr1_old = "TEST".to_string();
    let attr2_old = 1000;

    let row = Test2Row {
        val: 1,
        attr1: attr1_old.clone(),
        attr2: attr2_old,
        id: 0,
    };

    let attr1_new = "OK".to_string();
    let attr2_new = 1337;

    let pk = test_table.insert(row.clone()).unwrap();
    test_table
        .update_all_attr_by_id(
            AllAttrByIdQuery {
                attr1: attr1_new.clone(),
                attr2: attr2_new,
            },
            pk.clone(),
        )
        .await
        .unwrap();

    // Checks idx updated
    let updated = test_table
        .select_by_attr1(attr1_new.clone())
        .execute()
        .expect("rows");
    assert_eq!(updated.first().unwrap().attr1, attr1_new);
    let updated = test_table
        .select_by_attr2(attr2_new)
        .execute()
        .expect("rows");
    assert_eq!(updated.first().unwrap().attr2, attr2_new);

    // Check old idx removed
    let updated = test_table
        .select_by_attr1(attr1_old.clone())
        .execute()
        .expect("rows");
    assert_eq!(updated.first(), None);
    let updated = test_table
        .select_by_attr2(attr2_old)
        .execute()
        .expect("rows");
    assert_eq!(updated.first(), None);
}

#[tokio::test]
async fn update_2_idx_full_row() {
    let test_table = Test2WorkTable::default();

    let attr1_old = "TEST".to_string();
    let attr2_old = 1000;

    let row = Test2Row {
        val: 1,
        attr1: attr1_old.clone(),
        attr2: attr2_old,
        id: 0,
    };

    let attr1_new = "OK".to_string();
    let attr2_new = 1337;

    let pk = test_table.insert(row.clone()).unwrap();
    test_table
        .update(Test2Row {
            id: pk.clone().into(),
            attr1: attr1_new.clone(),
            attr2: attr2_new,
            val: row.val,
        })
        .await
        .unwrap();

    // Checks idx updated
    let updated = test_table
        .select_by_attr1(attr1_new.clone())
        .execute()
        .expect("rows");
    assert_eq!(updated.first().unwrap().attr1, attr1_new);
    let updated = test_table
        .select_by_attr2(attr2_new)
        .execute()
        .expect("rows");
    assert_eq!(updated.first().unwrap().attr2, attr2_new);

    // Check old idx removed
    let updated = test_table
        .select_by_attr1(attr1_old.clone())
        .execute()
        .expect("rows");
    assert_eq!(updated.first(), None);
    let updated = test_table
        .select_by_attr2(attr2_old)
        .execute()
        .expect("rows");
    assert_eq!(updated.first(), None);
}

// The test checks updates for 1 index

// worktable!(
//     name: Test,
//     columns: {
//         id: u64 primary_key autoincrement,
//         val: i64,
//         attr1: String,
//         attr2: i16,
//     },
//     indexes: {
//         idx1: attr1,
//     },
//     queries: {
//         update: {
//             ValByAttr(val) by attr1,
//             Attr1ById(attr1) by id,
//         },
//         delete: {
//             ById() by id,
//         }
//     }
// );

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
    pub val: i64,
    pub attr1: String,
    pub attr2: i16,
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
    Val,
    Attr1,
    Attr2,
    Id,
}
#[derive(Clone, Debug, From, PartialEq)]
#[non_exhaustive]
pub enum TestAvaiableTypes {
    #[from]
    STRING(String),
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
    val_lock: Option<std::sync::Arc<Lock>>,
    attr1_lock: Option<std::sync::Arc<Lock>>,
    attr2_lock: Option<std::sync::Arc<Lock>>,
    id_lock: Option<std::sync::Arc<Lock>>,
}
impl TestLock {
    pub fn new() -> Self {
        Self {
            val_lock: None,
            attr1_lock: None,
            attr2_lock: None,
            id_lock: None,
        }
    }
}
impl RowLock for TestLock {
    fn is_locked(&self) -> bool {
        self.val_lock
            .as_ref()
            .map(|l| l.is_locked())
            .unwrap_or(false)
            || self
                .attr1_lock
                .as_ref()
                .map(|l| l.is_locked())
                .unwrap_or(false)
            || self
                .attr2_lock
                .as_ref()
                .map(|l| l.is_locked())
                .unwrap_or(false)
            || self
                .id_lock
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
        if let Some(lock) = &self.val_lock {
            set.insert(lock.clone());
        }
        self.val_lock = Some(lock.clone());
        if let Some(lock) = &self.attr1_lock {
            set.insert(lock.clone());
        }
        self.attr1_lock = Some(lock.clone());
        if let Some(lock) = &self.attr2_lock {
            set.insert(lock.clone());
        }
        self.attr2_lock = Some(lock.clone());
        if let Some(lock) = &self.id_lock {
            set.insert(lock.clone());
        }
        self.id_lock = Some(lock.clone());
        (set, lock)
    }
    fn with_lock(id: u16) -> (Self, std::sync::Arc<Lock>) {
        let lock = std::sync::Arc::new(Lock::new(id));
        (
            Self {
                val_lock: Some(lock.clone()),
                attr1_lock: Some(lock.clone()),
                attr2_lock: Some(lock.clone()),
                id_lock: Some(lock.clone()),
            },
            lock,
        )
    }
    #[allow(clippy::mutable_key_type)]
    fn merge(&mut self, other: &mut Self) -> std::collections::HashSet<std::sync::Arc<Lock>> {
        let mut set = std::collections::HashSet::new();
        if let Some(val_lock) = &other.val_lock {
            if self.val_lock.is_none() {
                self.val_lock = Some(val_lock.clone());
            } else {
                set.insert(val_lock.clone());
            }
        }
        other.val_lock = self.val_lock.clone();
        if let Some(attr1_lock) = &other.attr1_lock {
            if self.attr1_lock.is_none() {
                self.attr1_lock = Some(attr1_lock.clone());
            } else {
                set.insert(attr1_lock.clone());
            }
        }
        other.attr1_lock = self.attr1_lock.clone();
        if let Some(attr2_lock) = &other.attr2_lock {
            if self.attr2_lock.is_none() {
                self.attr2_lock = Some(attr2_lock.clone());
            } else {
                set.insert(attr2_lock.clone());
            }
        }
        other.attr2_lock = self.attr2_lock.clone();
        if let Some(id_lock) = &other.id_lock {
            if self.id_lock.is_none() {
                self.id_lock = Some(id_lock.clone());
            } else {
                set.insert(id_lock.clone());
            }
        }
        other.id_lock = self.id_lock.clone();
        set
    }
}
#[derive(Debug, MemStat)]
pub struct TestIndex {
    idx1: IndexMultiMap<String, Link, UnsizedNode<IndexMultiPair<String, Link>>>,
}
impl TableSecondaryIndex<TestRow, TestAvaiableTypes, TestAvailableIndexes> for TestIndex {
    fn save_row(
        &self,
        row: TestRow,
        link: Link,
    ) -> core::result::Result<(), IndexError<TestAvailableIndexes>> {
        let mut inserted_indexes: Vec<TestAvailableIndexes> = vec![];
        if let Some(link) = self.idx1.insert(row.attr1.clone(), link) {
            self.idx1.insert(row.attr1, link);
            return Err(IndexError::AlreadyExists {
                at: TestAvailableIndexes::Idx1,
                inserted_already: inserted_indexes.clone(),
            });
        }
        inserted_indexes.push(TestAvailableIndexes::Idx1);
        core::result::Result::Ok(())
    }
    fn reinsert_row(
        &self,
        row_old: TestRow,
        link_old: Link,
        row_new: TestRow,
        link_new: Link,
    ) -> eyre::Result<()> {
        let row = &row_new;
        let val_new = row.attr1.clone();
        self.idx1.insert(val_new.clone(), link_new);
        let row = &row_old;
        let val_old = row.attr1.clone();
        if val_new != val_old {
            TableIndex::remove(&self.idx1, val_old, link_old);
        }
        core::result::Result::Ok(())
    }
    fn delete_row(
        &self,
        row: TestRow,
        link: Link,
    ) -> core::result::Result<(), IndexError<TestAvailableIndexes>> {
        self.idx1.remove(&row.attr1, &link);
        core::result::Result::Ok(())
    }
    fn process_difference(
        &self,
        link: Link,
        difference: std::collections::HashMap<&str, Difference<TestAvaiableTypes>>,
    ) -> core::result::Result<(), WorkTableError> {
        if let Some(diff) = difference.get("attr1") {
            if let TestAvaiableTypes::STRING(old) = &diff.old {
                let key_old = old.to_string();
                TableIndex::remove(&self.idx1, key_old, link);
            }
            if let TestAvaiableTypes::STRING(new) = &diff.new {
                let key_new = new.to_string();
                TableIndex::insert(&self.idx1, key_new, link);
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
                TestAvailableIndexes::Idx1 => {
                    self.idx1.remove(&row.attr1, &link);
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
            name: "idx1".to_string(),
            index_type: IndexKind::NonUnique,
            key_count: self.idx1.len(),
            capacity: self.idx1.capacity(),
            heap_size: self.idx1.heap_size(),
            used_size: self.idx1.used_size(),
            node_count: self.idx1.node_count(),
        });
        info
    }
    fn is_empty(&self) -> bool {
        self.idx1.len() == 0
    }
}
impl Default for TestIndex {
    fn default() -> Self {
        Self {
            idx1: IndexMultiMap::with_maximum_node_size(TEST_INNER_SIZE),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Ord, Hash, Eq)]
pub enum TestAvailableIndexes {
    Idx1,
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
    pub fn select(&self, pk: TestPrimaryKey) -> Option<TestRow> {
        self.0.select(pk)
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
    pub fn select_by_attr1(
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
            .idx1
            .get(&by)
            .into_iter()
            .filter_map(|(_, link)| self.0.data.select_non_ghosted(*link).ok())
            .filter(move |r| &r.attr1 == &by);
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
                    (TestRowFields::Val, TestColumnRange::I64(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.val)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Val, TestColumnRange::I64Inclusive(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.val)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Val, TestColumnRange::I64From(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.val)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Val, TestColumnRange::I64To(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.val)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Val, TestColumnRange::I64ToInclusive(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.val)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Attr2, TestColumnRange::I16(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.attr2)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Attr2, TestColumnRange::I16Inclusive(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.attr2)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Attr2, TestColumnRange::I16From(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.attr2)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Attr2, TestColumnRange::I16To(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.attr2)))
                            as Box<dyn DoubleEndedIterator<Item = TestRow>>
                    }
                    (TestRowFields::Attr2, TestColumnRange::I16ToInclusive(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.attr2)))
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
                    _ => unreachable!(),
                };
            }
        }
        if !self.params.order.is_empty() {
            let mut items: Vec<TestRow> = iter.collect();
            items.sort_by(|a, b| {
                for (order, col) in &self.params.order {
                    match col {
                        TestRowFields::Val => {
                            let cmp = a
                                .val
                                .partial_cmp(&b.val)
                                .unwrap_or(std::cmp::Ordering::Equal);
                            if cmp != std::cmp::Ordering::Equal {
                                return match order {
                                    Order::Asc => cmp,
                                    Order::Desc => cmp.reverse(),
                                };
                            }
                        }
                        TestRowFields::Attr1 => {
                            let cmp = a
                                .attr1
                                .partial_cmp(&b.attr1)
                                .unwrap_or(std::cmp::Ordering::Equal);
                            if cmp != std::cmp::Ordering::Equal {
                                return match order {
                                    Order::Asc => cmp,
                                    Order::Desc => cmp.reverse(),
                                };
                            }
                        }
                        TestRowFields::Attr2 => {
                            let cmp = a
                                .attr2
                                .partial_cmp(&b.attr2)
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
    I16(std::ops::Range<i16>),
    I16Inclusive(std::ops::RangeInclusive<i16>),
    I16From(std::ops::RangeFrom<i16>),
    I16To(std::ops::RangeTo<i16>),
    I16ToInclusive(std::ops::RangeToInclusive<i16>),
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
impl From<std::ops::Range<i16>> for TestColumnRange {
    fn from(range: std::ops::Range<i16>) -> Self {
        Self::I16(range)
    }
}
impl From<std::ops::RangeInclusive<i16>> for TestColumnRange {
    fn from(range: std::ops::RangeInclusive<i16>) -> Self {
        Self::I16Inclusive(range)
    }
}
impl From<std::ops::RangeFrom<i16>> for TestColumnRange {
    fn from(range: std::ops::RangeFrom<i16>) -> Self {
        Self::I16From(range)
    }
}
impl From<std::ops::RangeTo<i16>> for TestColumnRange {
    fn from(range: std::ops::RangeTo<i16>) -> Self {
        Self::I16To(range)
    }
}
impl From<std::ops::RangeToInclusive<i16>> for TestColumnRange {
    fn from(range: std::ops::RangeToInclusive<i16>) -> Self {
        Self::I16ToInclusive(range)
    }
}
#[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize)]
#[repr(C)]
pub struct ValByAttrQuery {
    pub val: i64,
}
#[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize)]
#[repr(C)]
pub struct Attr1ByIdQuery {
    pub attr1: String,
}
pub type ValByAttrBy = String;
pub type Attr1ByIdBy = u64;
impl TestLock {
    #[allow(clippy::mutable_key_type)]
    pub fn lock_update_val_by_attr(
        &mut self,
        id: u16,
    ) -> (
        std::collections::HashSet<std::sync::Arc<Lock>>,
        std::sync::Arc<Lock>,
    ) {
        let mut set = std::collections::HashSet::new();
        let new_lock = std::sync::Arc::new(Lock::new(id));
        if let Some(lock) = &self.val_lock {
            set.insert(lock.clone());
        }
        self.val_lock = Some(new_lock.clone());
        (set, new_lock)
    }
    #[allow(clippy::mutable_key_type)]
    pub fn lock_update_attr_1_by_id(
        &mut self,
        id: u16,
    ) -> (
        std::collections::HashSet<std::sync::Arc<Lock>>,
        std::sync::Arc<Lock>,
    ) {
        let mut set = std::collections::HashSet::new();
        let new_lock = std::sync::Arc::new(Lock::new(id));
        if let Some(lock) = &self.attr1_lock {
            set.insert(lock.clone());
        }
        self.attr1_lock = Some(new_lock.clone());
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
        let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row)
            .map_err(|_| WorkTableError::SerializeError)?;
        if bytes.len() >= link.length as usize {
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
            self.reinsert(row_old, row)?;
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
        let old = &row_old.attr1;
        let new = &row_new.attr1;
        if old != new {
            let diff = Difference::<TestAvaiableTypes> {
                old: old.clone().into(),
                new: new.clone().into(),
            };
            diffs.insert("attr1", diff);
        }
        self.0.indexes.process_difference(link, diffs)?;
        unsafe {
            self.0
                .data
                .with_mut_ref(link, move |archived| {
                    std::mem::swap(&mut archived.inner.val, &mut archived_row.val);
                    std::mem::swap(&mut archived.inner.attr1, &mut archived_row.attr1);
                    std::mem::swap(&mut archived.inner.attr2, &mut archived_row.attr2);
                    std::mem::swap(&mut archived.inner.id, &mut archived_row.id);
                })
                .map_err(WorkTableError::PagesError)?
        };
        lock.unlock();
        self.0.lock_map.remove_with_lock_check(&pk).await;
        core::result::Result::Ok(())
    }
    pub async fn update_val_by_attr(
        &self,
        row: ValByAttrQuery,
        by: ValByAttrBy,
    ) -> core::result::Result<(), WorkTableError> {
        let links: Vec<_> = self.0.indexes.idx1.get(&by).map(|(_, l)| *l).collect();
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
                    let (locks, op_lock) = lock_guard.lock_update_val_by_attr(lock_id);
                    drop(lock_guard);
                    futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>())
                        .await;
                    op_lock
                } else {
                    let mut lock = TestLock::new();
                    #[allow(clippy::mutable_key_type)]
                    let (_, op_lock) = lock.lock_update_val_by_attr(lock_id);
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
        let links: Vec<_> = self.0.indexes.idx1.get(&by).map(|(_, l)| *l).collect();
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
                rkyv::access_unchecked_mut::<<ValByAttrQuery as rkyv::Archive>::Archived>(
                    &mut bytes[..],
                )
                .unseal_unchecked()
            };
            let updated_bytes: Vec<u8> = vec![];
            unsafe {
                self.0
                    .data
                    .with_mut_ref(link, |archived| {
                        std::mem::swap(&mut archived.inner.val, &mut archived_row.val);
                    })
                    .map_err(WorkTableError::PagesError)?;
            }
        }
        for (pk, lock) in pk_to_unlock {
            lock.unlock();
            self.0.lock_map.remove_with_lock_check(&pk).await;
        }
        core::result::Result::Ok(())
    }
    pub async fn update_attr_1_by_id(
        &self,
        row: Attr1ByIdQuery,
        pk: TestPrimaryKey,
    ) -> core::result::Result<(), WorkTableError> {
        let lock = {
            let lock_id = self.0.lock_map.next_id();
            if let Some(lock) = self.0.lock_map.get(&pk) {
                let mut lock_guard = lock.write().await;
                #[allow(clippy::mutable_key_type)]
                let (locks, op_lock) = lock_guard.lock_update_attr_1_by_id(lock_id);
                drop(lock_guard);
                futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>()).await;
                op_lock
            } else {
                let mut lock = TestLock::new();
                #[allow(clippy::mutable_key_type)]
                let (_, op_lock) = lock.lock_update_attr_1_by_id(lock_id);
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
            rkyv::access_unchecked_mut::<<Attr1ByIdQuery as rkyv::Archive>::Archived>(
                &mut bytes[..],
            )
            .unseal_unchecked()
        };
        let op_id = OperationId::Single(uuid::Uuid::now_v7());
        let mut need_to_reinsert = false;
        need_to_reinsert |= archived_row.get_attr1_size() > self.get_attr1_size(link)?;
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
                .select(pk.clone())
                .expect("should not be deleted by other thread");
            let mut row_new = row_old.clone();
            let pk = row_old.get_primary_key().clone();
            row_new.attr1 = row.attr1;
            self.reinsert(row_old, row_new)?;
            lock.unlock();
            self.0.lock_map.remove_with_lock_check(&pk).await;
            return core::result::Result::Ok(());
        }
        let row_old = self.0.data.select_non_ghosted(link)?;
        let row_new = row.clone();
        let updated_bytes: Vec<u8> = vec![];
        let mut diffs: std::collections::HashMap<&str, Difference<TestAvaiableTypes>> =
            std::collections::HashMap::new();
        let old = &row_old.attr1;
        let new = &row_new.attr1;
        if old != new {
            let diff = Difference::<TestAvaiableTypes> {
                old: old.clone().into(),
                new: new.clone().into(),
            };
            diffs.insert("attr1", diff);
        }
        self.0.indexes.process_difference(link, diffs)?;
        unsafe {
            self.0
                .data
                .with_mut_ref(link, |archived| {
                    std::mem::swap(&mut archived.inner.attr1, &mut archived_row.attr1);
                })
                .map_err(WorkTableError::PagesError)?
        };
        lock.unlock();
        self.0.lock_map.remove_with_lock_check(&pk).await;
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
    pub async fn delete_by_id(&self, by: u64) -> core::result::Result<(), WorkTableError> {
        self.iter_with_async(|row| {
            if row.id == by {
                futures::future::Either::Left(async move { self.delete(row.id.into()).await })
            } else {
                futures::future::Either::Right(async { Ok(()) })
            }
        })
        .await?;
        core::result::Result::Ok(())
    }
}
impl TestWorkTable {
    fn get_attr1_size(&self, link: Link) -> core::result::Result<usize, WorkTableError> {
        self.0
            .data
            .with_ref(link, |row_ref| row_ref.inner.attr1.len())
            .map_err(WorkTableError::PagesError)
    }
}
impl ArchivedAttr1ByIdQuery {
    pub fn get_attr1_size(&self) -> usize {
        self.attr1.as_str().to_string().aligned_size()
    }
}

#[tokio::test]
async fn update_1_idx() {
    let test_table = TestWorkTable::default();

    let attr1_old = "TEST".to_string();
    let attr2_old = 1000;

    let row = TestRow {
        val: 1,
        attr1: attr1_old.clone(),
        attr2: attr2_old,
        id: 0,
    };

    let attr1_new = "OK".to_string();

    let pk = test_table.insert(row.clone()).unwrap();
    test_table
        .update_attr_1_by_id(
            Attr1ByIdQuery {
                attr1: attr1_new.clone(),
            },
            pk.clone(),
        )
        .await
        .unwrap();

    // Checks idx updated
    let updated = test_table
        .select_by_attr1(attr1_new.clone())
        .execute()
        .expect("rows");
    assert_eq!(updated.first().unwrap().attr1, attr1_new);

    // Check old idx removed
    let updated = test_table
        .select_by_attr1(attr1_old.clone())
        .execute()
        .expect("rows");
    assert_eq!(updated.first(), None);
}

#[tokio::test]
async fn update_1_idx_full_row() {
    let test_table = TestWorkTable::default();

    let attr1_old = "TEST".to_string();
    let attr2_old = 1000;

    let row = TestRow {
        val: 1,
        attr1: attr1_old.clone(),
        attr2: attr2_old,
        id: 0,
    };

    let attr1_new = "OK".to_string();

    let pk = test_table.insert(row.clone()).unwrap();
    test_table
        .update(TestRow {
            attr2: row.attr2,
            id: pk.clone().into(),
            attr1: attr1_new.clone(),
            val: row.val,
        })
        .await
        .unwrap();

    // Checks idx updated
    let updated = test_table
        .select_by_attr1(attr1_new.clone())
        .execute()
        .expect("rows");
    assert_eq!(updated.first().unwrap().attr1, attr1_new);

    // Check old idx removed
    let updated = test_table
        .select_by_attr1(attr1_old.clone())
        .execute()
        .expect("rows");
    assert_eq!(updated.first(), None);
}
