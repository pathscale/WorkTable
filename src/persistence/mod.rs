use crate::persistence::operation::Operation;

mod mappers;
mod operation;
mod space;

use crate::persistence::space::SpaceSecondaryIndexOps;
pub use space::{IndexTableOfContents, SpaceData, SpaceDataOps, SpaceIndex, SpaceIndexOps};

pub struct PersistenceEngine<SpaceData, SpacePrimaryIndex, SpaceSecondaryIndexes> {
    pub data: SpaceData,
    pub primary_index: SpacePrimaryIndex,
    pub secondary_indexes: SpaceSecondaryIndexes,
}

impl<SpaceData, SpacePrimaryIndex, SpaceSecondaryIndexes>
    PersistenceEngine<SpaceData, SpacePrimaryIndex, SpaceSecondaryIndexes>
where
    SpaceData: SpaceDataOps,
{
    pub fn apply_operation<PrimaryKey, SecondaryIndexEvents>(
        &mut self,
        op: Operation<PrimaryKey, SecondaryIndexEvents>,
    ) -> eyre::Result<()>
    where
        SpacePrimaryIndex: SpaceIndexOps<PrimaryKey>,
        SpaceSecondaryIndexes: SpaceSecondaryIndexOps<SecondaryIndexEvents>,
    {
        match op {
            Operation::Insert(insert) => {
                self.data.save_data(insert.link, insert.bytes.as_ref())?;
                self.primary_index
                    .process_change_event(insert.primary_key_event)?;
                self.secondary_indexes
                    .process_change_events(insert.secondary_keys_events)
            }
            Operation::Update(update) => {
                self.data.save_data(update.link, update.bytes.as_ref())?;
                self.secondary_indexes
                    .process_change_events(update.secondary_keys_events)
            }
        }
    }
}
