use chrono::TimeDelta;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use worktable::prelude::*;
use worktable::vacuum::{VacuumManager, VacuumManagerConfig};

// worktable!(
//     name: VacuumTest,
//     columns: {
//         id: u64 primary_key autoincrement,
//         value: i64,
//         data: String
//     },
//     indexes: {
//         value_idx: value unique,
//         data_idx: data,
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
    MemStat,
)]
#[rkyv(derive(PartialEq, Eq, PartialOrd, Ord, Debug))]
pub struct VacuumTestPrimaryKey(u64);
impl TablePrimaryKey for VacuumTestPrimaryKey {
    type Generator = std::sync::atomic::AtomicU64;
}
#[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize, PartialEq, MemStat)]
#[rkyv(derive(Debug))]
#[repr(C)]
pub struct VacuumTestRow {
    pub id: u64,
    pub value: i64,
    pub data: String,
}
impl TableRow<VacuumTestPrimaryKey> for VacuumTestRow {
    fn get_primary_key(&self) -> VacuumTestPrimaryKey {
        self.id.clone().into()
    }
}
#[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize, PartialEq)]
#[rkyv(derive(Debug))]
#[repr(C)]
pub enum VacuumTestRowFields {
    Id,
    Value,
    Data,
}
impl Query<VacuumTestRow> for VacuumTestRow {
    fn merge(self, row: VacuumTestRow) -> VacuumTestRow {
        self
    }
}
#[derive(Clone, Debug, From, PartialEq)]
#[non_exhaustive]
pub enum VacuumTestAvaiableTypes {
    #[from]
    STRING(String),
    #[from]
    I64(i64),
}
#[derive(rkyv::Archive, Debug, rkyv::Deserialize, rkyv::Serialize)]
#[repr(C)]
pub struct VacuumTestWrapper {
    inner: VacuumTestRow,
    is_ghosted: bool,
    is_deleted: bool,
    is_in_vacuum_process: bool,
}
impl RowWrapper<VacuumTestRow> for VacuumTestWrapper {
    fn get_inner(self) -> VacuumTestRow {
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
    fn from_inner(inner: VacuumTestRow) -> Self {
        Self {
            inner,
            is_ghosted: true,
            is_deleted: false,
            is_in_vacuum_process: false,
        }
    }
}
impl StorableRow for VacuumTestRow {
    type WrappedRow = VacuumTestWrapper;
}
impl ArchivedRowWrapper for ArchivedVacuumTestWrapper {
    fn unghost(&mut self) {
        self.is_ghosted = false;
    }
    fn set_in_vacuum_process(&mut self) {
        self.is_in_vacuum_process = true;
    }
    fn delete(&mut self) {
        self.is_deleted = true;
    }
    fn is_deleted(&self) -> bool {
        self.is_deleted
    }
}
#[derive(Debug, Clone)]
pub struct VacuumTestLock {
    id_lock: Option<std::sync::Arc<Lock>>,
    value_lock: Option<std::sync::Arc<Lock>>,
    data_lock: Option<std::sync::Arc<Lock>>,
}
impl VacuumTestLock {
    pub fn new() -> Self {
        Self {
            id_lock: None,
            value_lock: None,
            data_lock: None,
        }
    }
}
impl RowLock for VacuumTestLock {
    fn is_locked(&self) -> bool {
        self.id_lock
            .as_ref()
            .map(|l| l.is_locked())
            .unwrap_or(false)
            || self
                .value_lock
                .as_ref()
                .map(|l| l.is_locked())
                .unwrap_or(false)
            || self
                .data_lock
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
        if let Some(lock) = &self.id_lock {
            set.insert(lock.clone());
        }
        self.id_lock = Some(lock.clone());
        if let Some(lock) = &self.value_lock {
            set.insert(lock.clone());
        }
        self.value_lock = Some(lock.clone());
        if let Some(lock) = &self.data_lock {
            set.insert(lock.clone());
        }
        self.data_lock = Some(lock.clone());
        (set, lock)
    }
    fn with_lock(id: u16) -> (Self, std::sync::Arc<Lock>) {
        let lock = std::sync::Arc::new(Lock::new(id));
        (
            Self {
                id_lock: Some(lock.clone()),
                value_lock: Some(lock.clone()),
                data_lock: Some(lock.clone()),
            },
            lock,
        )
    }
    #[allow(clippy::mutable_key_type)]
    fn merge(&mut self, other: &mut Self) -> std::collections::HashSet<std::sync::Arc<Lock>> {
        let mut set = std::collections::HashSet::new();
        if let Some(id_lock) = &other.id_lock {
            if self.id_lock.is_none() {
                self.id_lock = Some(id_lock.clone());
            } else {
                set.insert(id_lock.clone());
            }
        }
        other.id_lock = self.id_lock.clone();
        if let Some(value_lock) = &other.value_lock {
            if self.value_lock.is_none() {
                self.value_lock = Some(value_lock.clone());
            } else {
                set.insert(value_lock.clone());
            }
        }
        other.value_lock = self.value_lock.clone();
        if let Some(data_lock) = &other.data_lock {
            if self.data_lock.is_none() {
                self.data_lock = Some(data_lock.clone());
            } else {
                set.insert(data_lock.clone());
            }
        }
        other.data_lock = self.data_lock.clone();
        set
    }
}
#[derive(Debug, MemStat)]
pub struct VacuumTestIndex {
    value_idx: IndexMap<i64, OffsetEqLink>,
    data_idx:
        IndexMultiMap<String, OffsetEqLink, UnsizedNode<IndexMultiPair<String, OffsetEqLink>>>,
}
impl TableSecondaryIndex<VacuumTestRow, VacuumTestAvaiableTypes, VacuumTestAvailableIndexes>
    for VacuumTestIndex
{
    fn save_row(
        &self,
        row: VacuumTestRow,
        link: Link,
    ) -> core::result::Result<(), IndexError<VacuumTestAvailableIndexes>> {
        let mut inserted_indexes: Vec<VacuumTestAvailableIndexes> = vec![];
        if self
            .value_idx
            .insert_checked(row.value.clone(), link)
            .is_none()
        {
            return Err(IndexError::AlreadyExists {
                at: VacuumTestAvailableIndexes::ValueIdx,
                inserted_already: inserted_indexes.clone(),
            });
        }
        inserted_indexes.push(VacuumTestAvailableIndexes::ValueIdx);
        if self
            .data_idx
            .insert_checked(row.data.clone(), link)
            .is_none()
        {
            return Err(IndexError::AlreadyExists {
                at: VacuumTestAvailableIndexes::DataIdx,
                inserted_already: inserted_indexes.clone(),
            });
        }
        inserted_indexes.push(VacuumTestAvailableIndexes::DataIdx);
        core::result::Result::Ok(())
    }
    fn reinsert_row(
        &self,
        row_old: VacuumTestRow,
        link_old: Link,
        row_new: VacuumTestRow,
        link_new: Link,
    ) -> core::result::Result<(), IndexError<VacuumTestAvailableIndexes>> {
        let mut inserted_indexes: Vec<VacuumTestAvailableIndexes> = vec![];
        let row = &row_new;
        let val_new = row.value.clone();
        let row = &row_old;
        let val_old = row.value.clone();
        if val_new != val_old {
            if self
                .value_idx
                .insert_checked(val_new.clone(), link_new)
                .is_none()
            {
                return Err(IndexError::AlreadyExists {
                    at: VacuumTestAvailableIndexes::ValueIdx,
                    inserted_already: inserted_indexes.clone(),
                });
            }
            inserted_indexes.push(VacuumTestAvailableIndexes::ValueIdx);
        }
        let row = &row_new;
        let val_new = row.value.clone();
        let row = &row_old;
        let val_old = row.value.clone();
        if val_new == val_old {
            TableIndex::insert(&self.value_idx, val_new.clone(), link_new);
        } else {
            TableIndex::remove(&self.value_idx, &val_old, link_old);
        }
        let row = &row_new;
        let val_new = row.data.clone();
        let row = &row_old;
        let val_old = row.data.clone();
        TableIndex::insert(&self.data_idx, val_new.clone(), link_new);
        TableIndex::remove(&self.data_idx, &val_old, link_old);
        core::result::Result::Ok(())
    }
    fn delete_row(
        &self,
        row: VacuumTestRow,
        link: Link,
    ) -> core::result::Result<(), IndexError<VacuumTestAvailableIndexes>> {
        TableIndex::remove(&self.value_idx, &row.value, link);
        TableIndex::remove(&self.data_idx, &row.data, link);
        core::result::Result::Ok(())
    }
    fn process_difference_insert(
        &self,
        link: Link,
        difference: std::collections::HashMap<&str, Difference<VacuumTestAvaiableTypes>>,
    ) -> core::result::Result<(), IndexError<VacuumTestAvailableIndexes>> {
        let mut inserted_indexes: Vec<VacuumTestAvailableIndexes> = vec![];
        if let Some(diff) = difference.get("value") {
            if let VacuumTestAvaiableTypes::I64(new) = &diff.new {
                let key_new = *new;
                if TableIndex::insert_checked(&self.value_idx, key_new, link).is_none() {
                    return Err(IndexError::AlreadyExists {
                        at: VacuumTestAvailableIndexes::ValueIdx,
                        inserted_already: inserted_indexes.clone(),
                    });
                }
                inserted_indexes.push(VacuumTestAvailableIndexes::ValueIdx);
            }
        }
        if let Some(diff) = difference.get("data") {
            if let VacuumTestAvaiableTypes::STRING(new) = &diff.new {
                let key_new = new.to_string();
                if TableIndex::insert_checked(&self.data_idx, key_new, link).is_none() {
                    return Err(IndexError::AlreadyExists {
                        at: VacuumTestAvailableIndexes::DataIdx,
                        inserted_already: inserted_indexes.clone(),
                    });
                }
                inserted_indexes.push(VacuumTestAvailableIndexes::DataIdx);
            }
        }
        core::result::Result::Ok(())
    }
    fn process_difference_remove(
        &self,
        link: Link,
        difference: std::collections::HashMap<&str, Difference<VacuumTestAvaiableTypes>>,
    ) -> core::result::Result<(), IndexError<VacuumTestAvailableIndexes>> {
        if let Some(diff) = difference.get("value") {
            if let VacuumTestAvaiableTypes::I64(old) = &diff.old {
                let key_old = *old;
                TableIndex::remove(&self.value_idx, &key_old, link);
            }
        }
        if let Some(diff) = difference.get("data") {
            if let VacuumTestAvaiableTypes::STRING(old) = &diff.old {
                let key_old = old.to_string();
                TableIndex::remove(&self.data_idx, &key_old, link);
            }
        }
        core::result::Result::Ok(())
    }
    fn delete_from_indexes(
        &self,
        row: VacuumTestRow,
        link: Link,
        indexes: Vec<VacuumTestAvailableIndexes>,
    ) -> core::result::Result<(), IndexError<VacuumTestAvailableIndexes>> {
        for index in indexes {
            match index {
                VacuumTestAvailableIndexes::ValueIdx => {
                    TableIndex::remove(&self.value_idx, &row.value, link);
                }
                VacuumTestAvailableIndexes::DataIdx => {
                    TableIndex::remove(&self.data_idx, &row.data, link);
                }
            }
        }
        core::result::Result::Ok(())
    }
}
impl TableSecondaryIndexInfo for VacuumTestIndex {
    fn index_info(&self) -> Vec<IndexInfo> {
        let mut info = Vec::new();
        info.push(IndexInfo {
            name: "value_idx".to_string(),
            index_type: IndexKind::Unique,
            key_count: self.value_idx.len(),
            capacity: self.value_idx.capacity(),
            heap_size: self.value_idx.heap_size(),
            used_size: self.value_idx.used_size(),
            node_count: self.value_idx.node_count(),
        });
        info.push(IndexInfo {
            name: "data_idx".to_string(),
            index_type: IndexKind::NonUnique,
            key_count: self.data_idx.len(),
            capacity: self.data_idx.capacity(),
            heap_size: self.data_idx.heap_size(),
            used_size: self.data_idx.used_size(),
            node_count: self.data_idx.node_count(),
        });
        info
    }
    fn is_empty(&self) -> bool {
        self.value_idx.len() == 0 && self.data_idx.len() == 0
    }
}
impl Default for VacuumTestIndex {
    fn default() -> Self {
        Self {
            value_idx: IndexMap::with_maximum_node_size(
                get_index_page_size_from_data_length::<i64>(VACUUM_TEST_INNER_SIZE),
            ),
            data_idx: IndexMultiMap::with_maximum_node_size(VACUUM_TEST_INNER_SIZE),
        }
    }
}
#[derive(Debug, Clone, Copy, MoreDisplay, PartialEq, PartialOrd, Ord, Hash, Eq)]
pub enum VacuumTestAvailableIndexes {
    ValueIdx,
    DataIdx,
}
impl AvailableIndex for VacuumTestAvailableIndexes {
    fn to_string_value(&self) -> String {
        ToString::to_string(&self)
    }
}
const VACUUM_TEST_PAGE_SIZE: usize = PAGE_SIZE;
const VACUUM_TEST_INNER_SIZE: usize = VACUUM_TEST_PAGE_SIZE - GENERAL_HEADER_SIZE;
#[derive(Debug)]
pub struct VacuumTestWorkTable(
    WorkTable<
        VacuumTestRow,
        VacuumTestPrimaryKey,
        VacuumTestAvaiableTypes,
        VacuumTestAvailableIndexes,
        VacuumTestIndex,
        VacuumTestLock,
        <VacuumTestPrimaryKey as TablePrimaryKey>::Generator,
        { INNER_PAGE_SIZE },
        Vec<IndexPair<VacuumTestPrimaryKey, OffsetEqLink<VACUUM_TEST_INNER_SIZE>>>,
    >,
);
impl Default for VacuumTestWorkTable {
    fn default() -> Self {
        let mut inner = WorkTable::default();
        inner.table_name = "VacuumTest";
        Self(inner)
    }
}
impl VacuumTestWorkTable {
    pub fn name(&self) -> &'static str {
        &self.0.table_name
    }
    pub fn select<Pk>(&self, pk: Pk) -> Option<VacuumTestRow>
    where
        VacuumTestPrimaryKey: From<Pk>,
    {
        self.0.select(pk.into())
    }
    pub fn insert(
        &self,
        row: VacuumTestRow,
    ) -> core::result::Result<VacuumTestPrimaryKey, WorkTableError> {
        self.0.insert(row)
    }
    pub async fn reinsert(
        &self,
        row_old: VacuumTestRow,
        row_new: VacuumTestRow,
    ) -> core::result::Result<VacuumTestPrimaryKey, WorkTableError> {
        self.0.reinsert(row_old, row_new).await
    }
    pub async fn upsert(&self, row: VacuumTestRow) -> core::result::Result<(), WorkTableError> {
        let pk = row.get_primary_key();
        let need_to_update = {
            if let Some(link) = self.0.primary_index.pk_map.get(&pk) {
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
        let count = self.0.primary_index.pk_map.len();
        count
    }
    pub fn get_next_pk(&self) -> VacuumTestPrimaryKey {
        self.0.get_next_pk()
    }
    pub fn iter_with<F: Fn(VacuumTestRow) -> core::result::Result<(), WorkTableError>>(
        &self,
        f: F,
    ) -> core::result::Result<(), WorkTableError> {
        let first = self
            .0
            .primary_index
            .pk_map
            .iter()
            .next()
            .map(|(k, v)| (k.clone(), v.0));
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
                let mut iter = self.0.primary_index.pk_map.range(k.clone()..);
                let next = iter
                    .next()
                    .map(|(k, v)| (k.clone(), v.0))
                    .filter(|(key, _)| key != &k);
                if next.is_some() {
                    next
                } else {
                    iter.next().map(|(k, v)| (k.clone(), v.0))
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
        F: Fn(VacuumTestRow) -> Fut,
        Fut: std::future::Future<Output = core::result::Result<(), WorkTableError>>,
    >(
        &self,
        f: F,
    ) -> core::result::Result<(), WorkTableError> {
        let first = self
            .0
            .primary_index
            .pk_map
            .iter()
            .next()
            .map(|(k, v)| (k.clone(), v.0));
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
                let mut iter = self.0.primary_index.pk_map.range(k.clone()..);
                let next = iter
                    .next()
                    .map(|(k, v)| (k.clone(), v.0))
                    .filter(|(key, _)| key != &k);
                if next.is_some() {
                    next
                } else {
                    iter.next().map(|(k, v)| (k.clone(), v.0))
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
    pub fn vacuum(&self) -> std::sync::Arc<dyn WorkTableVacuum + std::marker::Send + Sync> {
        std::sync::Arc::new(EmptyDataVacuum::<_, _, _, _, _, _, VacuumTestLock, _>::new(
            "VacuumTest",
            std::sync::Arc::clone(&self.0.data),
            std::sync::Arc::clone(&self.0.lock_manager),
            std::sync::Arc::clone(&self.0.primary_index),
            std::sync::Arc::clone(&self.0.indexes),
        ))
    }
}
impl VacuumTestWorkTable {
    pub fn select_by_value(&self, by: i64) -> Option<VacuumTestRow> {
        let link: Link = self
            .0
            .indexes
            .value_idx
            .get(&by)
            .map(|kv| kv.get().value.into())?;
        self.0.data.select_non_ghosted(link).ok()
    }
    pub fn select_by_data(
        &self,
        by: String,
    ) -> SelectQueryBuilder<
        VacuumTestRow,
        impl DoubleEndedIterator<Item = VacuumTestRow> + '_,
        VacuumTestColumnRange,
        VacuumTestRowFields,
    > {
        let rows = self
            .0
            .indexes
            .data_idx
            .get(&by)
            .into_iter()
            .filter_map(|(_, link)| self.0.data.select_non_ghosted(link.0).ok())
            .filter(move |r| &r.data == &by);
        SelectQueryBuilder::new(rows)
    }
}
impl<I> SelectQueryExecutor<VacuumTestRow, I, VacuumTestColumnRange, VacuumTestRowFields>
    for SelectQueryBuilder<VacuumTestRow, I, VacuumTestColumnRange, VacuumTestRowFields>
where
    I: DoubleEndedIterator<Item = VacuumTestRow> + Sized,
{
    fn where_by<F>(
        self,
        predicate: F,
    ) -> SelectQueryBuilder<
        VacuumTestRow,
        impl DoubleEndedIterator<Item = VacuumTestRow> + Sized,
        VacuumTestColumnRange,
        VacuumTestRowFields,
    >
    where
        F: FnMut(&VacuumTestRow) -> bool,
    {
        SelectQueryBuilder {
            params: self.params,
            iter: self.iter.filter(predicate),
        }
    }
    fn execute(self) -> Result<Vec<VacuumTestRow>, WorkTableError> {
        let mut iter: Box<dyn DoubleEndedIterator<Item = VacuumTestRow>> = Box::new(self.iter);
        if !self.params.range.is_empty() {
            for (range, column) in &self.params.range {
                iter = match (column, range.clone().into()) {
                    (VacuumTestRowFields::Id, VacuumTestColumnRange::U64(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.id)))
                            as Box<dyn DoubleEndedIterator<Item = VacuumTestRow>>
                    }
                    (VacuumTestRowFields::Id, VacuumTestColumnRange::U64Inclusive(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.id)))
                            as Box<dyn DoubleEndedIterator<Item = VacuumTestRow>>
                    }
                    (VacuumTestRowFields::Id, VacuumTestColumnRange::U64From(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.id)))
                            as Box<dyn DoubleEndedIterator<Item = VacuumTestRow>>
                    }
                    (VacuumTestRowFields::Id, VacuumTestColumnRange::U64To(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.id)))
                            as Box<dyn DoubleEndedIterator<Item = VacuumTestRow>>
                    }
                    (VacuumTestRowFields::Id, VacuumTestColumnRange::U64ToInclusive(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.id)))
                            as Box<dyn DoubleEndedIterator<Item = VacuumTestRow>>
                    }
                    (VacuumTestRowFields::Value, VacuumTestColumnRange::I64(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.value)))
                            as Box<dyn DoubleEndedIterator<Item = VacuumTestRow>>
                    }
                    (VacuumTestRowFields::Value, VacuumTestColumnRange::I64Inclusive(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.value)))
                            as Box<dyn DoubleEndedIterator<Item = VacuumTestRow>>
                    }
                    (VacuumTestRowFields::Value, VacuumTestColumnRange::I64From(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.value)))
                            as Box<dyn DoubleEndedIterator<Item = VacuumTestRow>>
                    }
                    (VacuumTestRowFields::Value, VacuumTestColumnRange::I64To(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.value)))
                            as Box<dyn DoubleEndedIterator<Item = VacuumTestRow>>
                    }
                    (VacuumTestRowFields::Value, VacuumTestColumnRange::I64ToInclusive(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.value)))
                            as Box<dyn DoubleEndedIterator<Item = VacuumTestRow>>
                    }
                    _ => unreachable!(),
                };
            }
        }
        if !self.params.order.is_empty() {
            let mut items: Vec<VacuumTestRow> = iter.collect();
            items.sort_by(|a, b| {
                for (order, col) in &self.params.order {
                    match col {
                        VacuumTestRowFields::Id => {
                            let cmp = a.id.partial_cmp(&b.id).unwrap_or(std::cmp::Ordering::Equal);
                            if cmp != std::cmp::Ordering::Equal {
                                return match order {
                                    Order::Asc => cmp,
                                    Order::Desc => cmp.reverse(),
                                };
                            }
                        }
                        VacuumTestRowFields::Value => {
                            let cmp = a
                                .value
                                .partial_cmp(&b.value)
                                .unwrap_or(std::cmp::Ordering::Equal);
                            if cmp != std::cmp::Ordering::Equal {
                                return match order {
                                    Order::Asc => cmp,
                                    Order::Desc => cmp.reverse(),
                                };
                            }
                        }
                        VacuumTestRowFields::Data => {
                            let cmp = a
                                .data
                                .partial_cmp(&b.data)
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
        let iter_result: Box<dyn Iterator<Item = VacuumTestRow>> =
            if let Some(offset) = self.params.offset {
                Box::new(iter.skip(offset))
            } else {
                Box::new(iter)
            };
        let iter_result: Box<dyn Iterator<Item = VacuumTestRow>> =
            if let Some(limit) = self.params.limit {
                Box::new(iter_result.take(limit))
            } else {
                Box::new(iter_result)
            };
        Ok(iter_result.collect())
    }
}
#[derive(Debug, Clone)]
pub enum VacuumTestColumnRange {
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
impl From<std::ops::Range<u64>> for VacuumTestColumnRange {
    fn from(range: std::ops::Range<u64>) -> Self {
        Self::U64(range)
    }
}
impl From<std::ops::RangeInclusive<u64>> for VacuumTestColumnRange {
    fn from(range: std::ops::RangeInclusive<u64>) -> Self {
        Self::U64Inclusive(range)
    }
}
impl From<std::ops::RangeFrom<u64>> for VacuumTestColumnRange {
    fn from(range: std::ops::RangeFrom<u64>) -> Self {
        Self::U64From(range)
    }
}
impl From<std::ops::RangeTo<u64>> for VacuumTestColumnRange {
    fn from(range: std::ops::RangeTo<u64>) -> Self {
        Self::U64To(range)
    }
}
impl From<std::ops::RangeToInclusive<u64>> for VacuumTestColumnRange {
    fn from(range: std::ops::RangeToInclusive<u64>) -> Self {
        Self::U64ToInclusive(range)
    }
}
impl From<std::ops::Range<i64>> for VacuumTestColumnRange {
    fn from(range: std::ops::Range<i64>) -> Self {
        Self::I64(range)
    }
}
impl From<std::ops::RangeInclusive<i64>> for VacuumTestColumnRange {
    fn from(range: std::ops::RangeInclusive<i64>) -> Self {
        Self::I64Inclusive(range)
    }
}
impl From<std::ops::RangeFrom<i64>> for VacuumTestColumnRange {
    fn from(range: std::ops::RangeFrom<i64>) -> Self {
        Self::I64From(range)
    }
}
impl From<std::ops::RangeTo<i64>> for VacuumTestColumnRange {
    fn from(range: std::ops::RangeTo<i64>) -> Self {
        Self::I64To(range)
    }
}
impl From<std::ops::RangeToInclusive<i64>> for VacuumTestColumnRange {
    fn from(range: std::ops::RangeToInclusive<i64>) -> Self {
        Self::I64ToInclusive(range)
    }
}
impl VacuumTestWorkTable {
    pub fn select_all(
        &self,
    ) -> SelectQueryBuilder<
        VacuumTestRow,
        impl DoubleEndedIterator<Item = VacuumTestRow> + '_ + Sized,
        VacuumTestColumnRange,
        VacuumTestRowFields,
    > {
        let iter = self
            .0
            .primary_index
            .pk_map
            .iter()
            .filter_map(|(_, link)| self.0.data.select_non_ghosted(link.0).ok());
        SelectQueryBuilder::new(iter)
    }
}
impl VacuumTestWorkTable {
    pub async fn update(&self, row: VacuumTestRow) -> core::result::Result<(), WorkTableError> {
        let pk = row.get_primary_key();
        let op_lock = {
            let lock_id = self.0.lock_manager.next_id();
            if let Some(lock) = self.0.lock_manager.get(&pk) {
                let mut lock_guard = lock.write().await;
                #[allow(clippy::mutable_key_type)]
                let (locks, op_lock) = lock_guard.lock(lock_id);
                drop(lock_guard);
                futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>()).await;
                op_lock
            } else {
                #[allow(clippy::mutable_key_type)]
                let (lock, op_lock) = VacuumTestLock::with_lock(lock_id);
                let lock = std::sync::Arc::new(tokio::sync::RwLock::new(lock));
                let mut guard = lock.write().await;
                if let Some(old_lock) = self.0.lock_manager.insert(pk.clone(), lock.clone()) {
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
        let _guard = LockGuard::new(op_lock, self.0.lock_manager.clone(), pk.clone());
        let mut link: Link = self
            .0
            .primary_index
            .pk_map
            .get(&pk)
            .map(|v| v.get().value.into())
            .ok_or(WorkTableError::NotFound)?;
        let row_old = self.0.data.select_non_ghosted(link)?;
        self.0.update_state.insert(pk.clone(), row_old);
        let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row)
            .map_err(|_| WorkTableError::SerializeError)?;
        if true {
            drop(_guard);
            let op_lock = {
                let lock_id = self.0.lock_manager.next_id();
                if let Some(lock) = self.0.lock_manager.get(&pk) {
                    let mut lock_guard = lock.write().await;
                    #[allow(clippy::mutable_key_type)]
                    let (locks, op_lock) = lock_guard.lock(lock_id);
                    drop(lock_guard);
                    futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>())
                        .await;
                    op_lock
                } else {
                    #[allow(clippy::mutable_key_type)]
                    let (lock, op_lock) = VacuumTestLock::with_lock(lock_id);
                    let lock = std::sync::Arc::new(tokio::sync::RwLock::new(lock));
                    let mut guard = lock.write().await;
                    if let Some(old_lock) = self.0.lock_manager.insert(pk.clone(), lock.clone()) {
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
            let _guard = LockGuard::new(op_lock, self.0.lock_manager.clone(), pk.clone());
            let row_old = self.0.data.select_non_ghosted(link)?;
            if let Err(e) = self.reinsert(row_old, row).await {
                self.0.update_state.remove(&pk);
                return Err(e);
            }
            self.0.update_state.remove(&pk);
            return core::result::Result::Ok(());
        }
        let mut archived_row = unsafe {
            rkyv::access_unchecked_mut::<<VacuumTestRow as rkyv::Archive>::Archived>(&mut bytes[..])
                .unseal_unchecked()
        };
        let op_id = OperationId::Single(uuid::Uuid::now_v7());
        let row_old = self.0.data.select_non_ghosted(link)?;
        let row_new = row.clone();
        let updated_bytes: Vec<u8> = vec![];
        let mut diffs: std::collections::HashMap<&str, Difference<VacuumTestAvaiableTypes>> =
            std::collections::HashMap::new();
        let old = &row_old.value;
        let new = &row_new.value;
        if old != new {
            let diff = Difference::<VacuumTestAvaiableTypes> {
                old: old.clone().into(),
                new: new.clone().into(),
            };
            diffs.insert("value", diff);
        }
        let old = &row_old.data;
        let new = &row_new.data;
        if old != new {
            let diff = Difference::<VacuumTestAvaiableTypes> {
                old: old.clone().into(),
                new: new.clone().into(),
            };
            diffs.insert("data", diff);
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
                    std::mem::swap(&mut archived.inner.id, &mut archived_row.id);
                    std::mem::swap(&mut archived.inner.value, &mut archived_row.value);
                    std::mem::swap(&mut archived.inner.data, &mut archived_row.data);
                })
                .map_err(WorkTableError::PagesError)?
        };
        self.0.indexes.process_difference_remove(link, diffs)?;
        self.0.update_state.remove(&pk);
        core::result::Result::Ok(())
    }
}
impl VacuumTestWorkTable {}
impl VacuumTestWorkTable {
    pub async fn delete<Pk>(&self, pk: Pk) -> core::result::Result<(), WorkTableError>
    where
        VacuumTestPrimaryKey: From<Pk>,
    {
        let pk: VacuumTestPrimaryKey = pk.into();
        let op_lock = {
            let lock_id = self.0.lock_manager.next_id();
            if let Some(lock) = self.0.lock_manager.get(&pk) {
                let mut lock_guard = lock.write().await;
                #[allow(clippy::mutable_key_type)]
                let (locks, op_lock) = lock_guard.lock(lock_id);
                drop(lock_guard);
                futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>()).await;
                op_lock
            } else {
                #[allow(clippy::mutable_key_type)]
                let (lock, op_lock) = VacuumTestLock::with_lock(lock_id);
                let lock = std::sync::Arc::new(tokio::sync::RwLock::new(lock));
                let mut guard = lock.write().await;
                if let Some(old_lock) = self.0.lock_manager.insert(pk.clone(), lock.clone()) {
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
        let _guard = LockGuard::new(op_lock, self.0.lock_manager.clone(), pk.clone());
        let link = match self
            .0
            .primary_index
            .pk_map
            .get(&pk)
            .map(|v| v.get().value.into())
            .ok_or(WorkTableError::NotFound)
        {
            Ok(l) => l,
            Err(e) => {
                println!("Error getting primary index: {} for {:?}", e, pk.clone());
                return Err(e);
            }
        };

        let Some(row) = self.0.select(pk.clone()) else {
            println!("Found link {:?} for {:?}", link, pk);
            panic!("Should exist")
        };
        self.0.indexes.delete_row(row, link)?;
        self.0.primary_index.remove(&pk, link);
        self.0
            .data
            .delete(link)
            .map_err(WorkTableError::PagesError)?;
        println!("Deleted link {:?} for {:?}", link, pk);
        core::result::Result::Ok(())
    }
    pub async fn delete_without_lock<Pk>(&self, pk: Pk) -> core::result::Result<(), WorkTableError>
    where
        VacuumTestPrimaryKey: From<Pk>,
    {
        let pk: VacuumTestPrimaryKey = pk.into();
        let link = self
            .0
            .primary_index
            .pk_map
            .get(&pk)
            .map(|v| v.get().value.into())
            .ok_or(WorkTableError::NotFound)?;
        let row = self.0.select(pk.clone()).unwrap();
        self.0.indexes.delete_row(row, link)?;
        self.0.primary_index.remove(&pk, link);
        self.0
            .data
            .delete(link)
            .map_err(WorkTableError::PagesError)?;
        core::result::Result::Ok(())
    }
}
impl VacuumTestWorkTable {
    fn get_data_size(&self, link: Link) -> core::result::Result<usize, WorkTableError> {
        self.0
            .data
            .with_ref(link, |row_ref| {
                row_ref.inner.data.as_str().to_string().aligned_size()
            })
            .map_err(WorkTableError::PagesError)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn vacuum_parallel_with_selects() {
    let config = VacuumManagerConfig {
        check_interval: Duration::from_millis(5),
        ..Default::default()
    };
    let vacuum_manager = Arc::new(VacuumManager::with_config(config));
    let table = Arc::new(VacuumTestWorkTable::default());

    // Insert 2000 rows
    let mut rows = Vec::new();
    for i in 0..2000 {
        let row = VacuumTestRow {
            id: table.get_next_pk().into(),
            value: i,
            data: format!("test_data_{}", i),
        };
        let id = row.id;
        table.insert(row.clone()).unwrap();
        rows.push((id, row));
    }
    let rows = Arc::new(rows);

    let vacuum = table.vacuum();
    vacuum_manager.register(vacuum);
    let _h = vacuum_manager.run_vacuum_task();

    let delete_table = table.clone();
    let ids_to_delete: Arc<Vec<_>> = Arc::new(rows.iter().step_by(2).map(|p| p.0).collect());
    let task_ids = ids_to_delete.clone();
    let delete_task = tokio::spawn(async move {
        for id in task_ids.iter() {
            delete_table.delete(*id).await.unwrap();
        }
    });

    for _ in 0..10 {
        // Verify all remaining rows are still accessible multiple times while vacuuming
        for (id, expected) in rows.iter().filter(|(i, _)| !ids_to_delete.contains(i)) {
            let row = table.select(*id);
            assert_eq!(row, Some(expected.clone()));
            let row = row.unwrap();
            let by_value = table.select_by_value(row.value);
            assert_eq!(by_value, Some(expected.clone()));
        }
    }

    delete_task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn vacuum_parallel_with_inserts() {
    let config = VacuumManagerConfig {
        check_interval: Duration::from_millis(5),
        ..Default::default()
    };
    let vacuum_manager = Arc::new(VacuumManager::with_config(config));
    let table = Arc::new(VacuumTestWorkTable::default());

    // Insert 2000 rows
    let mut rows = Vec::new();
    for i in 0..2000 {
        let row = VacuumTestRow {
            id: table.get_next_pk().into(),
            value: i,
            data: format!("test_data_{}", i),
        };
        let id = row.id;
        table.insert(row.clone()).unwrap();
        rows.push((id, row));
    }
    let rows = Arc::new(rows);

    let vacuum = table.vacuum();
    vacuum_manager.register(vacuum);
    let _h = vacuum_manager.run_vacuum_task();

    let delete_table = table.clone();
    let ids_to_delete: Arc<Vec<_>> = Arc::new(rows.iter().step_by(2).map(|p| p.0).collect());
    let task_ids = ids_to_delete.clone();
    let delete_task = tokio::spawn(async move {
        for id in task_ids.iter() {
            delete_table.delete(*id).await.unwrap();
        }
    });

    let mut inserted_rows = Vec::new();
    for i in 2001..3000 {
        let row = VacuumTestRow {
            id: table.get_next_pk().into(),
            value: i,
            data: format!("test_data_{}", i),
        };
        let id = row.id;
        table.insert(row.clone()).unwrap();
        inserted_rows.push((id, row));
    }

    // Verify all remaining rows are still accessible
    for (id, expected) in rows.iter().filter(|(i, _)| !ids_to_delete.contains(i)) {
        let row = table.select(*id);
        assert_eq!(row, Some(expected.clone()));
        let row = row.unwrap();
        let by_value = table.select_by_value(row.value);
        assert_eq!(by_value, Some(expected.clone()));
    }
    // Verify all inserted rows are accessible
    for (id, expected) in inserted_rows.iter() {
        let row = table.select(*id);
        assert_eq!(row, Some(expected.clone()));
        let row = row.unwrap();
        let by_value = table.select_by_value(row.value);
        assert_eq!(by_value, Some(expected.clone()));
    }

    delete_task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn vacuum_parallel_with_upserts() {
    let config = VacuumManagerConfig {
        check_interval: Duration::from_millis(5),
        ..Default::default()
    };
    let vacuum_manager = Arc::new(VacuumManager::with_config(config));
    let table = Arc::new(VacuumTestWorkTable::default());

    // Insert 3000 rows
    let mut rows = Vec::new();
    for i in 0..3000 {
        let row = VacuumTestRow {
            id: table.get_next_pk().into(),
            value: i,
            data: format!("test_data_{}", i),
        };
        let id = row.id;
        table.insert(row.clone()).unwrap();
        rows.push((id, row));
    }
    let rows = Arc::new(rows);

    let vacuum = table.vacuum();
    vacuum_manager.register(vacuum);
    let _h = vacuum_manager.run_vacuum_task();

    let delete_table = table.clone();
    let ids_to_delete: Arc<Vec<_>> = Arc::new(rows.iter().step_by(2).map(|p| p.0).collect());
    let row_state = Arc::new(Mutex::new(rows.iter().cloned().collect::<HashMap<_, _>>()));
    let task_ids = ids_to_delete.clone();
    let task_row_state = Arc::clone(&row_state);
    let delete_task = tokio::spawn(async move {
        for id in task_ids.iter() {
            delete_table.delete(*id).await.unwrap();
            {
                let mut g = task_row_state.lock();
                g.remove(id);
            }
        }
    });

    for _ in 0..3000 {
        let id = fastrand::u64(0..3000);
        let i = fastrand::i64(0..3000);
        let row = VacuumTestRow {
            id,
            value: id as i64,
            data: format!("test_data_{}", i),
        };
        let id = row.id;
        table.upsert(row.clone()).await.unwrap();
        {
            let mut g = row_state.lock();
            g.entry(id).and_modify(|r| *r = row.clone()).or_insert(row);
        }
    }

    delete_task.await.unwrap();

    let g = row_state.lock();

    // Verify all inserted rows are accessible
    for (id, expected) in g.iter() {
        let row = table.select(*id);
        assert_eq!(row, Some(expected.clone()));
        let row = row.unwrap();
        let by_value = table.select_by_value(row.value);
        assert_eq!(by_value, Some(expected.clone()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn vacuum_loop_test() {
    let config = VacuumManagerConfig {
        check_interval: Duration::from_millis(1_000),
        ..Default::default()
    };
    let vacuum_manager = Arc::new(VacuumManager::with_config(config));
    let table = Arc::new(VacuumTestWorkTable::default());

    // Insert 3000 rows
    for i in 0..3000 {
        let row = VacuumTestRow {
            id: table.get_next_pk().into(),
            value: chrono::Utc::now().timestamp_nanos_opt().unwrap(),
            data: format!("test_data_{}", i),
        };
        table.insert(row.clone()).unwrap();
    }

    let vacuum = table.vacuum();
    vacuum_manager.register(vacuum);
    let _h = vacuum_manager.run_vacuum_task();

    let insert_table = table.clone();
    let _task = tokio::spawn(async move {
        let mut i = 3001;
        loop {
            let row = VacuumTestRow {
                id: insert_table.get_next_pk().into(),
                value: chrono::Utc::now().timestamp_nanos_opt().unwrap(),
                data: format!("test_data_{}", i),
            };
            insert_table.insert(row.clone()).unwrap();
            println!("Inserted {:?}", row.id);
            tokio::time::sleep(Duration::from_micros(500)).await;
            i += 1;
        }
    });

    tokio::time::sleep(Duration::from_millis(1_000)).await;

    loop {
        tokio::time::sleep(Duration::from_millis(1_000)).await;

        let outdated_ts = chrono::Utc::now()
            .checked_sub_signed(TimeDelta::new(1, 0).unwrap())
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap();
        let ids_to_remove = table
            .0
            .indexes
            .value_idx
            .range(..outdated_ts)
            .map(|(_, l)| table.0.data.select_non_ghosted(**l).unwrap())
            .collect::<Vec<_>>();
        println!("Ids to Remove {:?}", ids_to_remove);
        for row in ids_to_remove {
            table.delete(row.id).await.unwrap();
            println!("Removed {:?}", row.id);
        }
    }
}
