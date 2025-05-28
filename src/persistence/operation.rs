use std::collections::HashMap;
use std::fmt::Debug;

use crate::persistence::space::{BatchChangeEvent, BatchData};
use crate::persistence::task::{QueueInnerRowFields, QueueInnerWorkTable};
use crate::prelude::{From, Order, SelectQueryExecutor};
use data_bucket::{Link, SizeMeasurable};
use derive_more::Display;
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;
use worktable_codegen::MemStat;

/// Represents page's identifier. Is unique within the table bounds
#[derive(
    Archive,
    Copy,
    Clone,
    Deserialize,
    Debug,
    Display,
    Eq,
    From,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[rkyv(derive(Debug, PartialOrd, PartialEq, Eq, Ord))]
pub enum OperationId {
    #[from]
    Single(Uuid),
    Multi(Uuid),
}

impl SizeMeasurable for OperationId {
    fn aligned_size(&self) -> usize {
        Uuid::default().aligned_size()
    }
}

impl Default for OperationId {
    fn default() -> Self {
        OperationId::Single(Uuid::now_v7())
    }
}

#[derive(
    Archive,
    Clone,
    Copy,
    Debug,
    Default,
    Deserialize,
    Serialize,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Hash,
)]
#[rkyv(compare(PartialEq), derive(Debug))]
#[repr(u8)]
pub enum OperationType {
    #[default]
    Insert,
    Update,
    Delete,
}

impl SizeMeasurable for OperationType {
    fn aligned_size(&self) -> usize {
        u8::default().aligned_size()
    }
}

#[derive(Debug)]
pub struct BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    pub ops: Vec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    pub info_wt: QueueInnerWorkTable,
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
    BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
where
    PrimaryKeyGenState: Debug + Clone,
    PrimaryKey: Debug + Clone,
    SecondaryKeys: Debug + Clone,
{
    pub fn get_pk_gen_state(&self) -> eyre::Result<Option<PrimaryKeyGenState>> {
        let row = self
            .info_wt
            .select_by_op_type(OperationType::Insert)
            .order_on(QueueInnerRowFields::OperationId, Order::Desc)
            .limit(1)
            .execute()?;
        Ok(row.into_iter().next().map(|r| {
            let pos = r.pos;
            let op = self.ops.get(pos).expect("available as pos in wt");
            op.pk_gen_state().expect("is insert operation").clone()
        }))
    }

    pub fn get_primary_key_evs(&self) -> eyre::Result<BatchChangeEvent<PrimaryKey>> {
        let mut data = vec![];
        let mut rows = self.info_wt.select_all().execute()?;
        rows.sort_by(|l, r| l.operation_id.cmp(&r.operation_id));
        for row in rows {
            let pos = row.pos;
            let op = self
                .ops
                .get(pos)
                .expect("pos should be correct as was set while batch build");
            if let Some(evs) = op.primary_key_events() {
                data.extend(evs.iter().cloned())
            }
        }

        Ok(data)
    }

    pub fn get_batch_data_op(&self) -> eyre::Result<BatchData> {
        let mut data = HashMap::new();
        for link in self.info_wt.iter_links() {
            let last_op = self
                .info_wt
                .select_by_link(link)
                .order_on(QueueInnerRowFields::OperationId, Order::Desc)
                .limit(1)
                .execute()?;
            let op_row = last_op
                .into_iter()
                .next()
                .expect("if link is in info_wt at least one row exists");
            let pos = op_row.pos;
            let op = self
                .ops
                .get(pos)
                .expect("pos should be correct as was set while batch build");
            if let Some(data_bytes) = op.bytes() {
                let link = op.link();
                data.entry(link.page_id)
                    .and_modify(|v: &mut Vec<_>| v.push((link, data_bytes.to_vec())))
                    .or_insert(vec![(link, data_bytes.to_vec())]);
            }
        }

        Ok(data)
    }
}

#[derive(Clone, Debug)]
pub enum Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    Insert(InsertOperation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>),
    Update(UpdateOperation<SecondaryKeys>),
    Delete(DeleteOperation<PrimaryKey, SecondaryKeys>),
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
    Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
{
    pub fn operation_type(&self) -> OperationType {
        match &self {
            Operation::Insert(_) => OperationType::Insert,
            Operation::Update(_) => OperationType::Update,
            Operation::Delete(_) => OperationType::Delete,
        }
    }

    pub fn operation_id(&self) -> OperationId {
        match &self {
            Operation::Insert(insert) => insert.id,
            Operation::Update(update) => update.id,
            Operation::Delete(delete) => delete.id,
        }
    }

    pub fn link(&self) -> Link {
        match &self {
            Operation::Insert(insert) => insert.link,
            Operation::Update(update) => update.link,
            Operation::Delete(delete) => delete.link,
        }
    }

    pub fn bytes(&self) -> Option<&[u8]> {
        match &self {
            Operation::Insert(insert) => Some(&insert.bytes),
            Operation::Update(update) => Some(&update.bytes),
            Operation::Delete(_) => None,
        }
    }

    pub fn primary_key_events(&self) -> Option<&Vec<ChangeEvent<Pair<PrimaryKey, Link>>>> {
        match &self {
            Operation::Insert(insert) => Some(&insert.primary_key_events),
            Operation::Update(_) => None,
            Operation::Delete(delete) => Some(&delete.primary_key_events),
        }
    }

    pub fn pk_gen_state(&self) -> Option<&PrimaryKeyGenState> {
        match &self {
            Operation::Insert(insert) => Some(&insert.pk_gen_state),
            Operation::Update(_) => None,
            Operation::Delete(_) => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct InsertOperation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    pub id: OperationId,
    pub primary_key_events: Vec<ChangeEvent<Pair<PrimaryKey, Link>>>,
    pub secondary_keys_events: SecondaryKeys,
    pub pk_gen_state: PrimaryKeyGenState,
    pub bytes: Vec<u8>,
    pub link: Link,
}

#[derive(Clone, Debug)]
pub struct UpdateOperation<SecondaryKeys> {
    pub id: OperationId,
    pub secondary_keys_events: SecondaryKeys,
    pub bytes: Vec<u8>,
    pub link: Link,
}

#[derive(Clone, Debug)]
pub struct DeleteOperation<PrimaryKey, SecondaryKeys> {
    pub id: OperationId,
    pub primary_key_events: Vec<ChangeEvent<Pair<PrimaryKey, Link>>>,
    pub secondary_keys_events: SecondaryKeys,
    pub link: Link,
}
