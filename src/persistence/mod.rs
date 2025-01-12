use crate::persistence::operation::Operation;

mod mappers;
mod operation;
mod space;
mod space_index;

pub use space::{IndexTableOfContents, SpaceData, SpaceDataOps, SpaceIndex};
pub use space_index::SpaceTreeIndex;

pub struct PersistenceEngine<Space> {
    pub space: Space,
}

impl<Space> PersistenceEngine<Space>
where
    Space: SpaceDataOps,
{
    pub fn apply_operation<PrimaryKey, SecondaryKeys>(
        &mut self,
        op: Operation<PrimaryKey, SecondaryKeys>,
    ) -> eyre::Result<()> {
        match op {
            Operation::Insert(insert) => self.space.save_data(insert.link, insert.bytes.as_ref()),
            Operation::Update(update) => self.space.save_data(update.link, update.bytes.as_ref()),
        }
    }
}
