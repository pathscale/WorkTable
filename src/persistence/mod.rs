use crate::persistence::operation::Operation;

mod operation;
mod space;

pub struct PersistenceEngine<Space> {
    pub space: Space
}

impl<Space> PersistenceEngine<Space> {
    pub fn apply_operation<PrimaryKey, SecondaryKeys>(&self, op: Operation<PrimaryKey, SecondaryKeys>) {

    }


}