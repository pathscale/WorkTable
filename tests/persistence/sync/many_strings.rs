use worktable::prelude::*;
use worktable_codegen::worktable;

use crate::remove_dir_if_exists;

// worktable! (
//     name: TestSync,
//     persist: true,
//     columns: {
//         id: String primary_key,
//         field: String,
//         another: u64,
//     },
//     queries: {
//         update: {
//             FieldAnotherById(field, another) by id,
//         },
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
    VariableSizeMeasure,
)]
#[rkyv(derive(PartialEq, Eq, PartialOrd, Ord, Debug))]
pub struct TestSyncPrimaryKey(String);
impl TablePrimaryKey for TestSyncPrimaryKey {
    type Generator = ();
}
#[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize, PartialEq, MemStat)]
#[rkyv(derive(Debug))]
#[repr(C)]
pub struct TestSyncRow {
    pub id: String,
    pub field: String,
    pub another: u64,
}
impl TableRow<TestSyncPrimaryKey> for TestSyncRow {
    fn get_primary_key(&self) -> TestSyncPrimaryKey {
        self.id.clone().into()
    }
}
#[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize, PartialEq)]
#[rkyv(derive(Debug))]
#[repr(C)]
pub enum TestSyncRowFields {
    Field,
    Another,
    Id,
}
impl Query<TestSyncRow> for TestSyncRow {
    fn merge(self, row: TestSyncRow) -> TestSyncRow {
        self
    }
}
type TestSyncAvaiableTypes = ();
#[derive(rkyv::Archive, Debug, rkyv::Deserialize, rkyv::Serialize)]
#[repr(C)]
pub struct TestSyncWrapper {
    inner: TestSyncRow,
    is_ghosted: bool,
    is_deleted: bool,
}
impl RowWrapper<TestSyncRow> for TestSyncWrapper {
    fn get_inner(self) -> TestSyncRow {
        self.inner
    }
    fn is_ghosted(&self) -> bool {
        self.is_ghosted
    }
    fn from_inner(inner: TestSyncRow) -> Self {
        Self {
            inner,
            is_ghosted: true,
            is_deleted: false,
        }
    }
}
impl StorableRow for TestSyncRow {
    type WrappedRow = TestSyncWrapper;
}
impl GhostWrapper for ArchivedTestSyncWrapper {
    fn unghost(&mut self) {
        self.is_ghosted = false;
    }
}
#[derive(Debug, Clone)]
pub struct TestSyncLock {
    field_lock: Option<std::sync::Arc<Lock>>,
    another_lock: Option<std::sync::Arc<Lock>>,
    id_lock: Option<std::sync::Arc<Lock>>,
}
impl TestSyncLock {
    pub fn new() -> Self {
        Self {
            field_lock: None,
            another_lock: None,
            id_lock: None,
        }
    }
}
impl RowLock for TestSyncLock {
    fn is_locked(&self) -> bool {
        self.field_lock
            .as_ref()
            .map(|l| l.is_locked())
            .unwrap_or(false)
            || self
                .another_lock
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
        if let Some(lock) = &self.field_lock {
            set.insert(lock.clone());
        }
        self.field_lock = Some(lock.clone());
        if let Some(lock) = &self.another_lock {
            set.insert(lock.clone());
        }
        self.another_lock = Some(lock.clone());
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
                field_lock: Some(lock.clone()),
                another_lock: Some(lock.clone()),
                id_lock: Some(lock.clone()),
            },
            lock,
        )
    }
    #[allow(clippy::mutable_key_type)]
    fn merge(&mut self, other: &mut Self) -> std::collections::HashSet<std::sync::Arc<Lock>> {
        let mut set = std::collections::HashSet::new();
        if let Some(field_lock) = &other.field_lock {
            if self.field_lock.is_none() {
                self.field_lock = Some(field_lock.clone());
            } else {
                set.insert(field_lock.clone());
            }
        }
        other.field_lock = self.field_lock.clone();
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
        set
    }
}
#[derive(Debug, MemStat, PersistIndex)]
pub struct TestSyncIndex {}
impl TableSecondaryIndex<TestSyncRow, TestSyncAvaiableTypes, TestSyncAvailableIndexes>
    for TestSyncIndex
{
    fn save_row(
        &self,
        row: TestSyncRow,
        link: Link,
    ) -> core::result::Result<(), IndexError<TestSyncAvailableIndexes>> {
        let mut inserted_indexes: Vec<TestSyncAvailableIndexes> = vec![];
        core::result::Result::Ok(())
    }
    fn reinsert_row(
        &self,
        row_old: TestSyncRow,
        link_old: Link,
        row_new: TestSyncRow,
        link_new: Link,
    ) -> core::result::Result<(), IndexError<TestSyncAvailableIndexes>> {
        let mut inserted_indexes: Vec<TestSyncAvailableIndexes> = vec![];
        core::result::Result::Ok(())
    }
    fn delete_row(
        &self,
        row: TestSyncRow,
        link: Link,
    ) -> core::result::Result<(), IndexError<TestSyncAvailableIndexes>> {
        core::result::Result::Ok(())
    }
    fn process_difference_insert(
        &self,
        link: Link,
        difference: std::collections::HashMap<&str, Difference<TestSyncAvaiableTypes>>,
    ) -> core::result::Result<(), IndexError<TestSyncAvailableIndexes>> {
        let mut inserted_indexes: Vec<TestSyncAvailableIndexes> = vec![];
        core::result::Result::Ok(())
    }
    fn process_difference_remove(
        &self,
        link: Link,
        difference: std::collections::HashMap<&str, Difference<TestSyncAvaiableTypes>>,
    ) -> core::result::Result<(), IndexError<TestSyncAvailableIndexes>> {
        core::result::Result::Ok(())
    }
    fn delete_from_indexes(
        &self,
        row: TestSyncRow,
        link: Link,
        indexes: Vec<TestSyncAvailableIndexes>,
    ) -> core::result::Result<(), IndexError<TestSyncAvailableIndexes>> {
        core::result::Result::Ok(())
    }
}
impl TableSecondaryIndexInfo for TestSyncIndex {
    fn index_info(&self) -> Vec<IndexInfo> {
        let mut info = Vec::new();
        info
    }
    fn is_empty(&self) -> bool {
        true
    }
}
impl
    TableSecondaryIndexCdc<
        TestSyncRow,
        TestSyncAvaiableTypes,
        TestSyncSpaceSecondaryIndexEvents,
        TestSyncAvailableIndexes,
    > for TestSyncIndex
{
    fn reinsert_row_cdc(
        &self,
        row_old: TestSyncRow,
        link_old: Link,
        row_new: TestSyncRow,
        link_new: Link,
    ) -> Result<TestSyncSpaceSecondaryIndexEvents, IndexError<TestSyncAvailableIndexes>> {
        let mut inserted_indexes: Vec<TestSyncAvailableIndexes> = vec![];
        core::result::Result::Ok(TestSyncSpaceSecondaryIndexEvents {})
    }
    fn save_row_cdc(
        &self,
        row: TestSyncRow,
        link: Link,
    ) -> Result<TestSyncSpaceSecondaryIndexEvents, IndexError<TestSyncAvailableIndexes>> {
        let mut inserted_indexes: Vec<TestSyncAvailableIndexes> = vec![];
        core::result::Result::Ok(TestSyncSpaceSecondaryIndexEvents {})
    }
    fn delete_row_cdc(
        &self,
        row: TestSyncRow,
        link: Link,
    ) -> Result<TestSyncSpaceSecondaryIndexEvents, IndexError<TestSyncAvailableIndexes>> {
        core::result::Result::Ok(TestSyncSpaceSecondaryIndexEvents {})
    }
    fn process_difference_insert_cdc(
        &self,
        link: Link,
        difference: std::collections::HashMap<&str, Difference<TestSyncAvaiableTypes>>,
    ) -> Result<TestSyncSpaceSecondaryIndexEvents, IndexError<TestSyncAvailableIndexes>> {
        let mut inserted_indexes: Vec<TestSyncAvailableIndexes> = vec![];
        core::result::Result::Ok(TestSyncSpaceSecondaryIndexEvents {})
    }
    fn process_difference_remove_cdc(
        &self,
        link: Link,
        difference: std::collections::HashMap<&str, Difference<TestSyncAvaiableTypes>>,
    ) -> Result<TestSyncSpaceSecondaryIndexEvents, IndexError<TestSyncAvailableIndexes>> {
        core::result::Result::Ok(TestSyncSpaceSecondaryIndexEvents {})
    }
}
impl Default for TestSyncIndex {
    fn default() -> Self {
        Self {}
    }
}
pub type TestSyncAvailableIndexes = ();
const TEST_SYNC_PAGE_SIZE: usize = PAGE_SIZE;
const TEST_SYNC_INNER_SIZE: usize = TEST_SYNC_PAGE_SIZE - GENERAL_HEADER_SIZE;
#[derive(Debug, PersistTable)]
#[table(pk_unsized)]
pub struct TestSyncWorkTable(
    WorkTable<
        TestSyncRow,
        TestSyncPrimaryKey,
        TestSyncAvaiableTypes,
        TestSyncAvailableIndexes,
        TestSyncIndex,
        TestSyncLock,
        <TestSyncPrimaryKey as TablePrimaryKey>::Generator,
        UnsizedNode<IndexPair<TestSyncPrimaryKey, Link>>,
    >,
    PersistenceConfig,
    TestSyncPersistenceTask,
);
impl TestSyncWorkTable {
    pub async fn new(config: PersistenceConfig) -> eyre::Result<Self> {
        let mut inner = WorkTable::default();
        inner.table_name = "TestSync";
        let size = TEST_SYNC_INNER_SIZE;
        inner.pk_map = IndexMap::with_maximum_node_size(size);
        let table_files_path = format!("{}/{}", config.tables_path, "test_sync");
        let engine: TestSyncPersistenceEngine =
            PersistenceEngine::from_table_files_path(table_files_path).await?;
        core::result::Result::Ok(Self(
            inner,
            config,
            TestSyncPersistenceTask::run_engine(engine),
        ))
    }
    pub fn name(&self) -> &'static str {
        &self.0.table_name
    }
    pub fn select<Pk>(&self, pk: Pk) -> Option<TestSyncRow>
    where
        TestSyncPrimaryKey: From<Pk>,
    {
        self.0.select(pk.into())
    }
    pub fn insert(
        &self,
        row: TestSyncRow,
    ) -> core::result::Result<TestSyncPrimaryKey, WorkTableError> {
        let (pk, op) = self.0.insert_cdc(row)?;
        self.2.apply_operation(op);
        core::result::Result::Ok(pk)
    }
    pub fn reinsert(
        &self,
        row_old: TestSyncRow,
        row_new: TestSyncRow,
    ) -> core::result::Result<TestSyncPrimaryKey, WorkTableError> {
        let (pk, op) = self.0.reinsert_cdc(row_old, row_new)?;
        self.2.apply_operation(op);
        core::result::Result::Ok(pk)
    }
    pub async fn upsert(&self, row: TestSyncRow) -> core::result::Result<(), WorkTableError> {
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
    pub fn iter_with<F: Fn(TestSyncRow) -> core::result::Result<(), WorkTableError>>(
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
        F: Fn(TestSyncRow) -> Fut,
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
impl TestSyncWorkTable {}
impl<I> SelectQueryExecutor<TestSyncRow, I, TestSyncColumnRange, TestSyncRowFields>
    for SelectQueryBuilder<TestSyncRow, I, TestSyncColumnRange, TestSyncRowFields>
where
    I: DoubleEndedIterator<Item = TestSyncRow> + Sized,
{
    fn where_by<F>(
        self,
        predicate: F,
    ) -> SelectQueryBuilder<
        TestSyncRow,
        impl DoubleEndedIterator<Item = TestSyncRow> + Sized,
        TestSyncColumnRange,
        TestSyncRowFields,
    >
    where
        F: FnMut(&TestSyncRow) -> bool,
    {
        SelectQueryBuilder {
            params: self.params,
            iter: self.iter.filter(predicate),
        }
    }
    fn execute(self) -> Result<Vec<TestSyncRow>, WorkTableError> {
        let mut iter: Box<dyn DoubleEndedIterator<Item = TestSyncRow>> = Box::new(self.iter);
        if !self.params.range.is_empty() {
            for (range, column) in &self.params.range {
                iter = match (column, range.clone().into()) {
                    (TestSyncRowFields::Another, TestSyncColumnRange::U64(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.another)))
                            as Box<dyn DoubleEndedIterator<Item = TestSyncRow>>
                    }
                    (TestSyncRowFields::Another, TestSyncColumnRange::U64Inclusive(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.another)))
                            as Box<dyn DoubleEndedIterator<Item = TestSyncRow>>
                    }
                    (TestSyncRowFields::Another, TestSyncColumnRange::U64From(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.another)))
                            as Box<dyn DoubleEndedIterator<Item = TestSyncRow>>
                    }
                    (TestSyncRowFields::Another, TestSyncColumnRange::U64To(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.another)))
                            as Box<dyn DoubleEndedIterator<Item = TestSyncRow>>
                    }
                    (TestSyncRowFields::Another, TestSyncColumnRange::U64ToInclusive(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.another)))
                            as Box<dyn DoubleEndedIterator<Item = TestSyncRow>>
                    }
                    _ => unreachable!(),
                };
            }
        }
        if !self.params.order.is_empty() {
            let mut items: Vec<TestSyncRow> = iter.collect();
            items.sort_by(|a, b| {
                for (order, col) in &self.params.order {
                    match col {
                        TestSyncRowFields::Field => {
                            let cmp = a
                                .field
                                .partial_cmp(&b.field)
                                .unwrap_or(std::cmp::Ordering::Equal);
                            if cmp != std::cmp::Ordering::Equal {
                                return match order {
                                    Order::Asc => cmp,
                                    Order::Desc => cmp.reverse(),
                                };
                            }
                        }
                        TestSyncRowFields::Another => {
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
                        TestSyncRowFields::Id => {
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
        let iter_result: Box<dyn Iterator<Item = TestSyncRow>> =
            if let Some(offset) = self.params.offset {
                Box::new(iter.skip(offset))
            } else {
                Box::new(iter)
            };
        let iter_result: Box<dyn Iterator<Item = TestSyncRow>> =
            if let Some(limit) = self.params.limit {
                Box::new(iter_result.take(limit))
            } else {
                Box::new(iter_result)
            };
        Ok(iter_result.collect())
    }
}
#[derive(Debug, Clone)]
pub enum TestSyncColumnRange {
    U64(std::ops::Range<u64>),
    U64Inclusive(std::ops::RangeInclusive<u64>),
    U64From(std::ops::RangeFrom<u64>),
    U64To(std::ops::RangeTo<u64>),
    U64ToInclusive(std::ops::RangeToInclusive<u64>),
}
impl From<std::ops::Range<u64>> for TestSyncColumnRange {
    fn from(range: std::ops::Range<u64>) -> Self {
        Self::U64(range)
    }
}
impl From<std::ops::RangeInclusive<u64>> for TestSyncColumnRange {
    fn from(range: std::ops::RangeInclusive<u64>) -> Self {
        Self::U64Inclusive(range)
    }
}
impl From<std::ops::RangeFrom<u64>> for TestSyncColumnRange {
    fn from(range: std::ops::RangeFrom<u64>) -> Self {
        Self::U64From(range)
    }
}
impl From<std::ops::RangeTo<u64>> for TestSyncColumnRange {
    fn from(range: std::ops::RangeTo<u64>) -> Self {
        Self::U64To(range)
    }
}
impl From<std::ops::RangeToInclusive<u64>> for TestSyncColumnRange {
    fn from(range: std::ops::RangeToInclusive<u64>) -> Self {
        Self::U64ToInclusive(range)
    }
}
#[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize)]
#[repr(C)]
pub struct FieldAnotherByIdQuery {
    pub field: String,
    pub another: u64,
}
impl Query<TestSyncRow> for FieldAnotherByIdQuery {
    fn merge(self, mut row: TestSyncRow) -> TestSyncRow {
        row.field = self.field;
        row.another = self.another;
        row
    }
}
pub type FieldAnotherByIdBy = String;
impl TestSyncLock {
    #[allow(clippy::mutable_key_type)]
    pub fn lock_update_field_another_by_id(
        &mut self,
        id: u16,
    ) -> (
        std::collections::HashSet<std::sync::Arc<Lock>>,
        std::sync::Arc<Lock>,
    ) {
        let mut set = std::collections::HashSet::new();
        let new_lock = std::sync::Arc::new(Lock::new(id));
        if let Some(lock) = &self.field_lock {
            set.insert(lock.clone());
        }
        self.field_lock = Some(new_lock.clone());
        if let Some(lock) = &self.another_lock {
            set.insert(lock.clone());
        }
        self.another_lock = Some(new_lock.clone());
        (set, new_lock)
    }
}
impl TestSyncWorkTable {
    pub fn select_all(
        &self,
    ) -> SelectQueryBuilder<
        TestSyncRow,
        impl DoubleEndedIterator<Item = TestSyncRow> + '_ + Sized,
        TestSyncColumnRange,
        TestSyncRowFields,
    > {
        let iter = self
            .0
            .pk_map
            .iter()
            .filter_map(|(_, link)| self.0.data.select_non_ghosted(*link).ok());
        SelectQueryBuilder::new(iter)
    }
}
impl TestSyncWorkTable {
    pub async fn update(&self, row: TestSyncRow) -> core::result::Result<(), WorkTableError> {
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
                let (lock, op_lock) = TestSyncLock::with_lock(lock_id);
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
                    let (lock, op_lock) = TestSyncLock::with_lock(lock_id);
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
            rkyv::access_unchecked_mut::<<TestSyncRow as rkyv::Archive>::Archived>(&mut bytes[..])
                .unseal_unchecked()
        };
        let op_id = OperationId::Single(uuid::Uuid::now_v7());
        let row_old = self.0.data.select_non_ghosted(link)?;
        let row_new = row.clone();
        let updated_bytes: Vec<u8> = vec![];
        let mut diffs: std::collections::HashMap<&str, Difference<TestSyncAvaiableTypes>> =
            std::collections::HashMap::new();
        let indexes_res = self
            .0
            .indexes
            .process_difference_insert_cdc(link, diffs.clone());
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
        let mut secondary_keys_events = indexes_res.expect("was just checked for correctness");
        let mut op: Operation<
            <<TestSyncPrimaryKey as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
            TestSyncPrimaryKey,
            TestSyncSpaceSecondaryIndexEvents,
        > = Operation::Update(UpdateOperation {
            id: op_id,
            secondary_keys_events,
            bytes: updated_bytes,
            link,
        });
        unsafe {
            self.0
                .data
                .with_mut_ref(link, move |archived| {
                    std::mem::swap(&mut archived.inner.field, &mut archived_row.field);
                    std::mem::swap(&mut archived.inner.another, &mut archived_row.another);
                    std::mem::swap(&mut archived.inner.id, &mut archived_row.id);
                })
                .map_err(WorkTableError::PagesError)?
        };
        let secondary_keys_events_remove =
            self.0.indexes.process_difference_remove_cdc(link, diffs)?;
        op.extend_secondary_key_events(secondary_keys_events_remove);
        self.0.update_state.remove(&pk);
        lock.unlock();
        self.0.lock_map.remove_with_lock_check(&pk).await;
        if let Operation::Update(op) = &mut op {
            op.bytes = self.0.data.select_raw(link)?;
        } else {
            unreachable!("")
        };
        self.2.apply_operation(op);
        core::result::Result::Ok(())
    }
    pub async fn update_field_another_by_id<Pk>(
        &self,
        row: FieldAnotherByIdQuery,
        pk: Pk,
    ) -> core::result::Result<(), WorkTableError>
    where
        TestSyncPrimaryKey: From<Pk>,
    {
        let pk = pk.into();
        let lock = {
            let lock_id = self.0.lock_map.next_id();
            if let Some(lock) = self.0.lock_map.get(&pk) {
                let mut lock_guard = lock.write().await;
                #[allow(clippy::mutable_key_type)]
                let (locks, op_lock) = lock_guard.lock_update_field_another_by_id(lock_id);
                drop(lock_guard);
                futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>()).await;
                op_lock
            } else {
                let mut lock = TestSyncLock::new();
                #[allow(clippy::mutable_key_type)]
                let (_, op_lock) = lock.lock_update_field_another_by_id(lock_id);
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
            rkyv::access_unchecked_mut::<<FieldAnotherByIdQuery as rkyv::Archive>::Archived>(
                &mut bytes[..],
            )
            .unseal_unchecked()
        };
        let op_id = OperationId::Single(uuid::Uuid::now_v7());
        let mut need_to_reinsert = true;
        need_to_reinsert |= archived_row.get_field_size() >= self.get_field_size(link)?;
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
                    let (lock, op_lock) = TestSyncLock::with_lock(lock_id);
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
            row_new.field = row.field;
            row_new.another = row.another;
            if let Err(e) = self.reinsert(row_old, row_new) {
                self.0.update_state.remove(&pk);
                lock.unlock();
                return Err(e);
            }
            lock.unlock();
            self.0.lock_map.remove_with_lock_check(&pk).await;
            return core::result::Result::Ok(());
        }
        let updated_bytes: Vec<u8> = vec![];
        let secondary_keys_events = core::default::Default::default();
        let mut op: Operation<
            <<TestSyncPrimaryKey as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
            TestSyncPrimaryKey,
            TestSyncSpaceSecondaryIndexEvents,
        > = Operation::Update(UpdateOperation {
            id: op_id,
            secondary_keys_events,
            bytes: updated_bytes,
            link,
        });
        unsafe {
            self.0
                .data
                .with_mut_ref(link, |archived| {
                    std::mem::swap(&mut archived.inner.field, &mut archived_row.field);
                    std::mem::swap(&mut archived.inner.another, &mut archived_row.another);
                })
                .map_err(WorkTableError::PagesError)?
        };
        lock.unlock();
        self.0.lock_map.remove_with_lock_check(&pk).await;
        if let Operation::Update(op) = &mut op {
            op.bytes = self.0.data.select_raw(link)?;
        } else {
            unreachable!("")
        };
        self.2.apply_operation(op);
        core::result::Result::Ok(())
    }
}
impl TestSyncWorkTable {}
impl TestSyncWorkTable {
    pub async fn delete(&self, pk: TestSyncPrimaryKey) -> core::result::Result<(), WorkTableError> {
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
                let (lock, op_lock) = TestSyncLock::with_lock(lock_id);
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
        let secondary_keys_events = self.0.indexes.delete_row_cdc(row, link)?;
        let (_, primary_key_events) = TableIndexCdc::remove_cdc(&self.0.pk_map, pk.clone(), link);
        self.0
            .data
            .delete(link)
            .map_err(WorkTableError::PagesError)?;
        let mut op: Operation<
            <<TestSyncPrimaryKey as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
            TestSyncPrimaryKey,
            TestSyncSpaceSecondaryIndexEvents,
        > = Operation::Delete(DeleteOperation {
            id: uuid::Uuid::now_v7().into(),
            secondary_keys_events,
            primary_key_events,
            link,
        });
        self.2.apply_operation(op);
        lock.unlock();
        self.0.lock_map.remove_with_lock_check(&pk).await;
        core::result::Result::Ok(())
    }
    pub fn delete_without_lock(
        &self,
        pk: TestSyncPrimaryKey,
    ) -> core::result::Result<(), WorkTableError> {
        let link = self
            .0
            .pk_map
            .get(&pk)
            .map(|v| v.get().value)
            .ok_or(WorkTableError::NotFound)?;
        let row = self.select(pk.clone()).unwrap();
        let secondary_keys_events = self.0.indexes.delete_row_cdc(row, link)?;
        let (_, primary_key_events) = TableIndexCdc::remove_cdc(&self.0.pk_map, pk.clone(), link);
        self.0
            .data
            .delete(link)
            .map_err(WorkTableError::PagesError)?;
        let mut op: Operation<
            <<TestSyncPrimaryKey as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
            TestSyncPrimaryKey,
            TestSyncSpaceSecondaryIndexEvents,
        > = Operation::Delete(DeleteOperation {
            id: uuid::Uuid::now_v7().into(),
            secondary_keys_events,
            primary_key_events,
            link,
        });
        self.2.apply_operation(op);
        core::result::Result::Ok(())
    }
}
impl TestSyncWorkTable {
    fn get_field_size(&self, link: Link) -> core::result::Result<usize, WorkTableError> {
        self.0
            .data
            .with_ref(link, |row_ref| {
                row_ref.inner.field.as_str().to_string().aligned_size()
            })
            .map_err(WorkTableError::PagesError)
    }
    fn get_id_size(&self, link: Link) -> core::result::Result<usize, WorkTableError> {
        self.0
            .data
            .with_ref(link, |row_ref| {
                row_ref.inner.id.as_str().to_string().aligned_size()
            })
            .map_err(WorkTableError::PagesError)
    }
}
impl ArchivedFieldAnotherByIdQuery {
    pub fn get_field_size(&self) -> usize {
        self.field.as_str().to_string().aligned_size()
    }
}

#[test]
fn test_space_update_query_pk_sync() {
    let config = PersistenceConfig::new(
        "tests/data/unsized_primary_and_other_sync/update_query_pk",
        "tests/data/unsized_primary_and_other_sync/update_query_pk",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists(
            "tests/data/unsized_primary_and_other_sync/update_query_pk".to_string(),
        )
        .await;

        let pk = {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestSyncRow {
                another: 42,
                field: "".to_string(),
                id: "Some string before".to_string(),
            };
            table.insert(row.clone()).unwrap();
            let row = TestSyncRow {
                another: 43,
                field: "".to_string(),
                id: "Some string before 2".to_string(),
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            assert!(table.select(pk.clone()).is_some());
            assert_eq!(table.select(pk.clone()).unwrap().another, 43);
            let q = FieldAnotherByIdQuery {
                field: "Some field value".to_string(),
                another: 0,
            };
            table
                .update_field_another_by_id(q, pk.clone())
                .await
                .unwrap();
            table.wait_for_ops().await;
        }
        {
            let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.clone()).is_some());
            assert_eq!(table.select(pk.clone()).unwrap().another, 0);
            assert_eq!(
                table.select(pk).unwrap().field,
                "Some field value".to_string()
            );
        }
    });
}
