use crate::persistence::engine::PersistenceEngine;
use crate::persistence::{SpaceDataOps, SpaceIndexOps, SpaceSecondaryIndexOps};

#[derive(Debug)]
pub struct PersistenceManager<
    SpaceData,
    SpacePrimaryIndex,
    SpaceSecondaryIndexes,
    PrimaryKey,
    SecondaryIndexEvents,
> {
    pub config_path: String,
    pub table_files_dir: String,
    pub engine: PersistenceEngine<
        SpaceData,
        SpacePrimaryIndex,
        SpaceSecondaryIndexes,
        PrimaryKey,
        SecondaryIndexEvents,
    >,
}

impl<SpaceData, SpacePrimaryIndex, SpaceSecondaryIndexes, PrimaryKey, SecondaryIndexEvents>
    PersistenceManager<
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
    pub fn new(config_path: String, table_files_dir: String) -> eyre::Result<Self> {
        Ok(Self {
            config_path,
            table_files_dir: table_files_dir.clone(),
            engine: PersistenceEngine::from_table_files_path(table_files_dir)?,
        })
    }
}
