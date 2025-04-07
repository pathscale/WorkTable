use std::fmt::Debug;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use convert_case::{Case, Casing};
use data_bucket::page::PageId;
use data_bucket::{
    get_index_page_size_from_data_length, parse_page, persist_page, GeneralHeader, GeneralPage,
    IndexPage, IndexValue, Link, PageType, SizeMeasurable, SpaceId, SpaceInfoPage,
    UnsizedIndexPage, VariableSizeMeasurable, GENERAL_HEADER_SIZE,
};
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{rancor, Archive, Deserialize, Serialize};
use tokio::fs::File;

use crate::persistence::space::open_or_create_file;
use crate::persistence::{IndexTableOfContents, SpaceIndex, SpaceIndexOps};
use crate::prelude::WT_INDEX_EXTENSION;

#[derive(Debug)]
pub struct SpaceIndexUnsized<T: Ord + Eq, const DATA_LENGTH: u32> {
    space_id: SpaceId,
    table_of_contents: IndexTableOfContents<T, DATA_LENGTH>,
    next_page_id: Arc<AtomicU32>,
    index_file: File,
    #[allow(dead_code)]
    info: GeneralPage<SpaceInfoPage<()>>,
}

impl<T, const DATA_LENGTH: u32> SpaceIndexUnsized<T, DATA_LENGTH>
where
    T: Archive
        + Ord
        + Eq
        + Clone
        + Default
        + Debug
        + SizeMeasurable
        + VariableSizeMeasurable
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>
        + Send
        + Sync
        + 'static,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>> + Ord + Eq,
{
    pub async fn new<S: AsRef<str>>(index_file_path: S, space_id: SpaceId) -> eyre::Result<Self> {
        let space_index = SpaceIndex::<T, DATA_LENGTH>::new(index_file_path, space_id).await?;
        Ok(Self {
            space_id,
            table_of_contents: space_index.table_of_contents,
            next_page_id: space_index.next_page_id,
            index_file: space_index.index_file,
            info: space_index.info,
        })
    }

    async fn add_new_index_page(
        &mut self,
        node_id: Pair<T, Link>,
        page_id: PageId,
    ) -> eyre::Result<()> {
        let value = IndexValue {
            key: node_id.key.clone(),
            link: node_id.value,
        };
        let page = UnsizedIndexPage::new(node_id.key.clone(), value)?;
        self.add_index_page(page, page_id).await
    }

    async fn add_index_page(
        &mut self,
        node: UnsizedIndexPage<T, DATA_LENGTH>,
        page_id: PageId,
    ) -> eyre::Result<()> {
        let header = GeneralHeader::new(page_id, PageType::Index, self.space_id);
        let mut general_page = GeneralPage {
            inner: node,
            header,
        };
        persist_page(&mut general_page, &mut self.index_file).await?;
        Ok(())
    }

    async fn process_create_node(&mut self, node_id: Pair<T, Link>) -> eyre::Result<()> {
        let page_id = if let Some(id) = self.table_of_contents.pop_empty_page_id() {
            id
        } else {
            self.next_page_id.fetch_add(1, Ordering::Relaxed).into()
        };
        self.table_of_contents.insert(node_id.key.clone(), page_id);
        self.table_of_contents.persist(&mut self.index_file).await?;
        self.add_new_index_page(node_id, page_id).await?;

        Ok(())
    }
}

impl<T, const DATA_LENGTH: u32> SpaceIndexOps<T> for SpaceIndexUnsized<T, DATA_LENGTH>
where
    T: Archive
        + Ord
        + Eq
        + Clone
        + Default
        + Debug
        + SizeMeasurable
        + VariableSizeMeasurable
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>
        + Send
        + Sync
        + 'static,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>> + Ord + Eq,
{
    async fn primary_from_table_files_path<S: AsRef<str> + Send>(
        table_path: S,
    ) -> eyre::Result<Self> {
        let path = format!("{}/primary{}", table_path.as_ref(), WT_INDEX_EXTENSION);
        Self::new(path, 0.into()).await
    }

    async fn secondary_from_table_files_path<S1: AsRef<str> + Send, S2: AsRef<str> + Send>(
        table_path: S1,
        name: S2,
    ) -> eyre::Result<Self>
    where
        Self: Sized,
    {
        let path = format!(
            "{}/{}{}",
            table_path.as_ref(),
            name.as_ref(),
            WT_INDEX_EXTENSION
        );
        Self::new(path, 0.into()).await
    }

    async fn bootstrap(file: &mut File, table_name: String) -> eyre::Result<()> {
        SpaceIndex::<T, DATA_LENGTH>::bootstrap(file, table_name).await
    }

    async fn process_change_event(
        &mut self,
        event: ChangeEvent<Pair<T, Link>>,
    ) -> eyre::Result<()> {
        match event {
            // ChangeEvent::InsertAt {
            //     max_value: node_id,
            //     value,
            //     index,
            // } => self.process_insert_at(node_id.key, value, index).await,
            // ChangeEvent::RemoveAt {
            //     max_value: node_id,
            //     value,
            //     index,
            // } => self.process_remove_at(node_id.key, value, index).await,
            ChangeEvent::CreateNode { max_value: node_id } => {
                self.process_create_node(node_id).await
            }
            // ChangeEvent::RemoveNode { max_value: node_id } => {
            //     self.process_remove_node(node_id.key).await
            // }
            // ChangeEvent::SplitNode {
            //     max_value: node_id,
            //     split_index,
            // } => self.process_split_node(node_id.key, split_index).await,
            _ => todo!(),
        }
    }
}
