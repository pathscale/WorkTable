mod table_of_contents;

use std::fmt::Debug;
use std::fs::File;
use std::hash::Hash;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use data_bucket::page::{IndexValue, PageId};
use data_bucket::{
    align8, parse_page, persist_page, GeneralHeader, GeneralPage, Link, NewIndexPage, PageType,
    SizeMeasurable, SpaceId, GENERAL_HEADER_SIZE,
};
use eyre::eyre;
use indexset::cdc::change::ChangeEvent;
use indexset::Pair;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{rancor, Archive, Deserialize, Serialize};

pub use table_of_contents::IndexTableOfContents;

pub fn get_size_from_data_length<T>(length: usize) -> usize
where
    T: Default + SizeMeasurable,
{
    let node_id_size = T::default().aligned_size();
    let slot_size = u16::default().aligned_size();
    let index_value_size = align8(T::default().aligned_size() + Link::default().aligned_size());
    let vec_util_size = 8;
    let size =
        (length - node_id_size - slot_size - vec_util_size * 2) / (slot_size + index_value_size);
    size
}

#[derive(Debug)]
pub struct SpaceIndex<T, const DATA_LENGTH: u32> {
    space_id: SpaceId,
    table_of_contents: IndexTableOfContents<T, DATA_LENGTH>,
    next_page_id: Arc<AtomicU32>,
    index_file: File,
}

impl<T, const DATA_LENGTH: u32> SpaceIndex<T, DATA_LENGTH>
where
    T: Archive
        + Ord
        + Eq
        + Clone
        + SizeMeasurable
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>> + Ord + Eq,
{
    pub fn new(mut index_file: File, space_id: SpaceId) -> eyre::Result<Self> {
        let file_length = index_file.metadata()?.len();
        let page_id = file_length / (DATA_LENGTH as u64 + GENERAL_HEADER_SIZE as u64) + 1;
        let next_page_id = Arc::new(AtomicU32::new(page_id as u32));
        let table_of_contents =
            IndexTableOfContents::parse_from_file(&mut index_file, space_id, next_page_id.clone())?;
        Ok(Self {
            space_id,
            table_of_contents,
            next_page_id,
            index_file,
        })
    }

    fn add_new_index_page(&mut self, node_id: Pair<T, Link>, page_id: PageId) -> eyre::Result<()>
    where
        T: Archive
            + Clone
            + Default
            + SizeMeasurable
            + Ord
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
    {
        let size = get_size_from_data_length::<T>(DATA_LENGTH as usize);
        let mut page = NewIndexPage::new(node_id.key.clone(), size);
        page.current_index = 1;
        page.slots[0] = 0;
        page.index_values[0] = IndexValue {
            key: node_id.key,
            link: node_id.value,
        };
        self.add_index_page(page, page_id)
    }

    fn add_index_page(&mut self, node: NewIndexPage<T>, page_id: PageId) -> eyre::Result<()>
    where
        T: Archive
            + Clone
            + Default
            + SizeMeasurable
            + Ord
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
    {
        let header = GeneralHeader::new(page_id.into(), PageType::Index, self.space_id);
        let mut general_page = GeneralPage {
            inner: node,
            header,
        };
        persist_page(&mut general_page, &mut self.index_file)?;
        Ok(())
    }

    pub fn process_change_event(&mut self, event: ChangeEvent<Pair<T, Link>>) -> eyre::Result<()>
    where
        T: Archive
            + Clone
            + Default
            + Debug
            + SizeMeasurable
            + Ord
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
    {
        match event {
            ChangeEvent::InsertAt {
                max_value: node_id,
                value,
                index,
            } => self.process_insert_at(node_id.key, value, index),
            ChangeEvent::RemoveAt {
                max_value: node_id,
                value,
                index,
            } => self.process_remove_at(node_id.key, value, index),
            ChangeEvent::CreateNode { max_value: node_id } => self.process_create_node(node_id),
            ChangeEvent::RemoveNode { max_value: node_id } => self.process_remove_node(node_id.key),
            ChangeEvent::SplitNode {
                max_value: node_id,
                split_index,
            } => self.process_split_node(node_id.key, split_index),
        }
    }

    fn insert_on_index_page(
        &mut self,
        page_id: PageId,
        node_id: T,
        index: usize,
        value: Pair<T, Link>,
    ) -> eyre::Result<Option<T>>
    where
        T: Archive
            + Clone
            + Default
            + SizeMeasurable
            + Ord
            + Eq
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>,
    {
        let mut new_node_id = None;

        let size = get_size_from_data_length::<T>(DATA_LENGTH as usize);
        let mut utility =
            NewIndexPage::<T>::parse_index_page_utility(&mut self.index_file, page_id)?;
        utility.slots.insert(index, utility.current_index);
        utility.slots.remove(size);
        let index_value = IndexValue {
            key: value.key.clone(),
            link: value.value,
        };
        utility.current_index = NewIndexPage::<T>::persist_value(
            &mut self.index_file,
            page_id,
            size,
            index_value,
            utility.current_index,
        )?;

        if &node_id < &value.key {
            utility.node_id = value.key.clone();
            new_node_id = Some(value.key);
        }

        NewIndexPage::<T>::persist_index_page_utility(&mut self.index_file, page_id, utility)?;

        Ok(new_node_id)
    }

    fn remove_from_index_page(
        &mut self,
        page_id: PageId,
        node_id: T,
        index: usize,
        value: Pair<T, Link>,
    ) -> eyre::Result<Option<T>>
    where
        T: Archive
            + Default
            + Clone
            + SizeMeasurable
            + Ord
            + Eq
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>,
    {
        let mut new_node_id = None;

        let size = get_size_from_data_length::<T>(DATA_LENGTH as usize);
        let mut utility =
            NewIndexPage::<T>::parse_index_page_utility(&mut self.index_file, page_id)?;
        utility.current_index = *utility
            .slots
            .get(index)
            .expect("Slots should exist for every index within `size`");
        utility.slots.remove(index);
        utility.slots.push(0);
        NewIndexPage::<T>::remove_value(
            &mut self.index_file,
            page_id,
            size,
            utility.current_index,
        )?;

        if &node_id == &value.key {
            let index = *utility
                .slots
                .get(index - 1)
                .expect("slots always should exist in `size` bounds");
            utility.node_id = NewIndexPage::<T>::read_value_with_index(
                &mut self.index_file,
                page_id,
                size,
                index as usize,
            )?
            .key;
            new_node_id = Some(utility.node_id.clone())
        }

        NewIndexPage::<T>::persist_index_page_utility(&mut self.index_file, page_id, utility)?;

        Ok(new_node_id)
    }

    fn process_insert_at(
        &mut self,
        node_id: T,
        value: Pair<T, Link>,
        index: usize,
    ) -> eyre::Result<()>
    where
        T: Archive
            + Default
            + Debug
            + SizeMeasurable
            + Ord
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
    {
        let page_id = self
            .table_of_contents
            .get(&node_id)
            .ok_or(eyre!("Node with {:?} id is not found", node_id))?;
        if let Some(new_node_id) =
            self.insert_on_index_page(page_id, node_id.clone(), index, value)?
        {
            self.table_of_contents.update_key(&node_id, new_node_id);
            self.table_of_contents.persist(&mut self.index_file)?;
        }
        Ok(())
    }
    fn process_remove_at(
        &mut self,
        node_id: T,
        value: Pair<T, Link>,
        index: usize,
    ) -> eyre::Result<()>
    where
        T: Archive
            + Default
            + Debug
            + SizeMeasurable
            + Ord
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
    {
        let page_id = self
            .table_of_contents
            .get(&node_id)
            .ok_or(eyre!("Node with {:?} id is not found", node_id))?;
        if let Some(new_node_id) =
            self.remove_from_index_page(page_id, node_id.clone(), index, value)?
        {
            self.table_of_contents.update_key(&node_id, new_node_id);
            self.table_of_contents.persist(&mut self.index_file)?;
        }
        Ok(())
    }
    fn process_create_node(&mut self, node_id: Pair<T, Link>) -> eyre::Result<()>
    where
        T: Archive
            + Clone
            + Default
            + SizeMeasurable
            + Ord
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
    {
        let page_id = if let Some(id) = self.table_of_contents.pop_empty_page_id() {
            id
        } else {
            self.next_page_id.fetch_add(1, Ordering::Relaxed).into()
        };
        self.table_of_contents.insert(node_id.key.clone(), page_id);
        self.table_of_contents.persist(&mut self.index_file)?;
        self.add_new_index_page(node_id, page_id)?;

        Ok(())
    }

    fn process_remove_node(&mut self, node_id: T) -> eyre::Result<()>
    where
        T: Archive
            + Clone
            + Default
            + SizeMeasurable
            + Ord
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
    {
        self.table_of_contents.remove(&node_id);
        self.table_of_contents.persist(&mut self.index_file)?;
        Ok(())
    }

    fn process_split_node(&mut self, node_id: T, split_index: usize) -> eyre::Result<()>
    where
        T: Archive
            + Clone
            + Default
            + Debug
            + SizeMeasurable
            + Ord
            + Eq
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>,
    {
        let page_id = self
            .table_of_contents
            .get(&node_id)
            .ok_or(eyre!("Node with {:?} id is not found", node_id))?;
        let mut page =
            parse_page::<NewIndexPage<T>, DATA_LENGTH>(&mut self.index_file, page_id.into())?;
        let splitted_page = page.inner.split(split_index);
        let new_page_id = if let Some(id) = self.table_of_contents.pop_empty_page_id() {
            id
        } else {
            self.next_page_id.fetch_add(1, Ordering::Relaxed).into()
        };

        self.table_of_contents
            .update_key(&node_id, page.inner.node_id.clone());
        self.table_of_contents
            .insert(splitted_page.node_id.clone(), new_page_id);
        self.table_of_contents.persist(&mut self.index_file)?;

        self.add_index_page(splitted_page, new_page_id)?;
        persist_page(&mut page, &mut self.index_file)?;

        Ok(())
    }
}
