use crate::in_memory::StorableRow;
use crate::persistence::space::open_or_create_file;
use crate::persistence::SpaceDataOps;
use crate::prelude::WT_DATA_EXTENSION;
use convert_case::{Case, Casing};
use data_bucket::{
    parse_page, persist_page, update_at, GeneralHeader, GeneralPage, Link, PageType, Persistable,
    SpaceInfoPage,
};
use rkyv::api::high::HighDeserializer;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use std::fs::File;
use std::path::Path;

#[derive(Debug)]
pub struct SpaceData<PkGenState, const DATA_LENGTH: u32> {
    pub info: GeneralPage<SpaceInfoPage<PkGenState>>,
    pub data_file: File,
}

impl<PkGenState, const DATA_LENGTH: u32> SpaceDataOps for SpaceData<PkGenState, DATA_LENGTH>
where
    PkGenState: Default
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        > + Archive,
    <PkGenState as Archive>::Archived:
        Deserialize<PkGenState, HighDeserializer<rkyv::rancor::Error>>,
    SpaceInfoPage<PkGenState>: Persistable,
{
    fn from_table_files_path<S: AsRef<str>>(table_path: S) -> eyre::Result<Self> {
        let path = format!("{}/{}", table_path.as_ref(), WT_DATA_EXTENSION);
        let mut data_file = if !Path::new(&path).exists() {
            let name = table_path
                .as_ref()
                .split("/")
                .last()
                .expect("is not in root...")
                .to_string()
                .from_case(Case::Snake)
                .to_case(Case::Pascal);
            let mut data_file = open_or_create_file(path)?;
            Self::bootstrap(&mut data_file, name)?;
            data_file
        } else {
            open_or_create_file(path)?
        };
        println!("Before info ok");
        let info = parse_page::<_, DATA_LENGTH>(&mut data_file, 0)?;

        println!("Data ok");
        Ok(Self { data_file, info })
    }

    fn bootstrap(file: &mut File, table_name: String) -> eyre::Result<()> {
        let info = SpaceInfoPage {
            id: 0.into(),
            page_count: 0,
            name: table_name,
            row_schema: vec![],
            primary_key_fields: vec![],
            secondary_index_types: vec![],
            pk_gen_state: Default::default(),
            empty_links_list: vec![],
        };
        let mut page = GeneralPage {
            header: GeneralHeader::new(0.into(), PageType::SpaceInfo, 0.into()),
            inner: info,
        };
        persist_page(&mut page, file)
    }

    fn save_data(&mut self, link: Link, bytes: &[u8]) -> eyre::Result<()> {
        update_at::<{ DATA_LENGTH }>(&mut self.data_file, link, bytes)
    }
}
