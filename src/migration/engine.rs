use rkyv::{Archive, Deserialize, Serialize};
use rkyv::api::high::HighDeserializer;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use tokio::fs::File;

use crate::prelude::{WT_DATA_EXTENSION, GeneralPage, SpaceInfoPage, parse_page, Persistable};

/// Detect version from SpaceInfoPage at page 0 in .wt.data.
pub async fn detect_version<PkGenState>(table_path: &str) -> eyre::Result<u32>
where
    PkGenState: Default + Clone + Archive + Send,
    for<'a> PkGenState: Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
    <PkGenState as Archive>::Archived: Deserialize<PkGenState, HighDeserializer<rkyv::rancor::Error>>,
    SpaceInfoPage<PkGenState>: Persistable,
{
    let data_file_path = format!("{}/{}", table_path, WT_DATA_EXTENSION);
    let mut file = File::open(&data_file_path).await?;
    let info: GeneralPage<SpaceInfoPage<PkGenState>> =
        parse_page::<_, 4096>(&mut file, 0).await?;
    Ok(info.inner.version)
}
