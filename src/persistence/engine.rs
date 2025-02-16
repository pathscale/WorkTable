use crate::persistence::operation::Operation;
use crate::persistence::{SpaceDataOps, SpaceIndexOps, SpaceSecondaryIndexOps};
use std::fs;
use std::marker::PhantomData;
use std::path::Path;

#[derive(Debug)]
pub struct PersistenceEngine<
    SpaceData,
    SpacePrimaryIndex,
    SpaceSecondaryIndexes,
    PrimaryKey,
    SecondaryIndexEvents,
> {
    pub data: SpaceData,
    pub primary_index: SpacePrimaryIndex,
    pub secondary_indexes: SpaceSecondaryIndexes,
    phantom_data: PhantomData<(PrimaryKey, SecondaryIndexEvents)>,
}

impl<SpaceData, SpacePrimaryIndex, SpaceSecondaryIndexes, PrimaryKey, SecondaryIndexEvents>
    PersistenceEngine<
        SpaceData,
        SpacePrimaryIndex,
        SpaceSecondaryIndexes,
        PrimaryKey,
        SecondaryIndexEvents,
    >
where
    PrimaryKey: Ord,
    SpaceData: SpaceDataOps,
    SpacePrimaryIndex: SpaceIndexOps<PrimaryKey>,
    SpaceSecondaryIndexes: SpaceSecondaryIndexOps<SecondaryIndexEvents>,
{
    pub fn from_table_files_path<S: AsRef<str> + Clone>(path: S) -> eyre::Result<Self> {
        let table_path = Path::new(path.as_ref());
        if !table_path.exists() {
            fs::create_dir_all(table_path)?;
        }

        println!("engine ok");

        Ok(Self {
            data: SpaceData::from_table_files_path(path.clone())?,
            primary_index: SpacePrimaryIndex::primary_from_table_files_path(path.clone())?,
            secondary_indexes: SpaceSecondaryIndexes::from_table_files_path(path)?,
            phantom_data: PhantomData,
        })
    }

    pub fn apply_operation(
        &mut self,
        op: Operation<PrimaryKey, SecondaryIndexEvents>,
    ) -> eyre::Result<()> {
        match op {
            Operation::Insert(insert) => {
                self.data.save_data(insert.link, insert.bytes.as_ref())?;
                for event in insert.primary_key_events {
                    self.primary_index.process_change_event(event)?;
                }
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
