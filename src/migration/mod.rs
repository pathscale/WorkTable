use data_bucket::DEFAULT_PAGE_STRIDE;
use rkyv::api::high::HighDeserializer;
use rkyv::rancor::Strategy;
use rkyv::ser::Serializer;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

use crate::prelude::{Persistable, SpaceInfoPage, WT_DATA_EXTENSION};
use core::marker::PhantomData;

/// Declares the source archive layout from known provenance, not the target schema.
pub struct SourceLayout<PkGenState>(PhantomData<fn() -> PkGenState>);

impl<PkGenState> SourceLayout<PkGenState> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<PkGenState> Default for SourceLayout<PkGenState> {
    fn default() -> Self {
        Self::new()
    }
}

/// Fallible preflight using the caller-declared source primary-key state layout.
pub async fn detect_source_version<PkGenState>(
    table_path: &str,
    _source_layout: SourceLayout<PkGenState>,
) -> eyre::Result<u32>
where
    PkGenState: Archive,
    PkGenState::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    let data_file_path = format!("{}/{}", table_path, WT_DATA_EXTENSION);
    let mut file = crate::fsx::open_read_only(&data_file_path).await?;
    Ok(SpaceInfoPage::<PkGenState>::read_version::<DEFAULT_PAGE_STRIDE>(&mut file).await?)
}

pub trait Migration<FromRow, ToRow> {
    type Context: Default + Send + Sync;

    fn migrate(row: FromRow, ctx: &Self::Context) -> ToRow;
}

/// Detect version from SpaceInfoPage at page 0 in .wt.data.
pub async fn detect_version<PkGenState>(table_path: &str) -> eyre::Result<u32>
where
    PkGenState: Default + Clone + Archive + Send,
    for<'a> PkGenState: Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>,
    <PkGenState as Archive>::Archived: Deserialize<PkGenState, HighDeserializer<rkyv::rancor::Error>>,
    PkGenState::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    SpaceInfoPage<PkGenState>: Persistable,
{
    detect_source_version(table_path, SourceLayout::<PkGenState>::new()).await
}
