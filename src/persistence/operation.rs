use crate::prelude::From;
use data_bucket::Link;
use derive_more::Display;
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

/// Represents page's identifier. Is unique within the table bounds
#[derive(
    Archive,
    Copy,
    Clone,
    Deserialize,
    Debug,
    Default,
    Display,
    Eq,
    From,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
pub struct OperationId(Uuid);

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
