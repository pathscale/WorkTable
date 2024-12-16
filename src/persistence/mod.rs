use crate::persistence::operation::Operation;
use crate::persistence::space::SpaceData;

mod operation;
mod space;
mod space_index;

pub struct PersistenceEngine<Space> {
    pub space: Space,
}

impl<Space> PersistenceEngine<Space>
where Space: SpaceData {
    pub fn apply_operation<PrimaryKey, SecondaryKeys>(
        &self,
        op: Operation<PrimaryKey, SecondaryKeys>,
    ) {
        match op {
            Operation::Insert(insert) => {
                self.space.save_data(insert.link, insert.bytes.as_ref())
            }
            Operation::Update(update) => {
                self.space.save_data(update.link, update.bytes.as_ref())
            }
        }
    }
}
