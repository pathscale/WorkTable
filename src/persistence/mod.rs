use crate::persistence::operation::Operation;

mod operation;
mod space;
mod space_index;

pub use space::SpaceData;

pub struct PersistenceEngine<Space> {
    pub space: Space,
}

impl<Space> PersistenceEngine<Space>
where
    Space: SpaceData,
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
