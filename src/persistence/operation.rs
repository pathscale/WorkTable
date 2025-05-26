use std::collections::HashMap;
use std::fmt::Debug;

use data_bucket::{Link, SizeMeasurable};
use derive_more::Display;
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

use crate::persistence::space::BatchData;
use crate::persistence::task::{QueueInnerRowFields, QueueInnerWorkTable};
use crate::prelude::{From, Order, SelectQueryExecutor};

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

#[derive(Debug)]
pub struct BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    pub ops: Vec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    pub info_wt: QueueInnerWorkTable,
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
    BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
where
    PrimaryKeyGenState: Debug,
    PrimaryKey: Debug,
    SecondaryKeys: Debug,
{
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
            Operation::Delete(delete) => None,
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
