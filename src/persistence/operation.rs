use data_bucket::Link;
use derive_more::Display;
use rkyv::{Archive, Deserialize, Serialize};

use crate::prelude::From;

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
pub struct OperationId(u32);

pub enum Operation<PrimaryKey, SecondaryKeys> {
    Insert(InsertOperation<PrimaryKey, SecondaryKeys>),
    Update(UpdateOperation<SecondaryKeys>),
}

pub struct InsertOperation<PrimaryKey, SecondaryKeys> {
    pub id: OperationId,
    pub primary_key: PrimaryKey,
    pub secondary_keys: SecondaryKeys,
    pub bytes: Vec<u8>,
    pub link: Link,
}

pub struct UpdateOperation<SecondaryKeys> {
    pub id: OperationId,
    pub old_secondary_keys: SecondaryKeys,
    pub new_secondary_keys: SecondaryKeys,
    pub bytes: Vec<u8>,
    pub link: Link,
}
