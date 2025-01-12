mod table_of_contents;

use std::fs::File;
use std::hash::Hash;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use data_bucket::page::{IndexValue, PageId};
use data_bucket::{
    align, persist_page, GeneralHeader, GeneralPage, Link, NewIndexPage, PageType, SizeMeasurable,
    SpaceId,
};
use indexset::cdc::change::ChangeEvent;
use indexset::Pair;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{rancor, Archive, Deserialize, Serialize};
use table_of_contents::IndexTableOfContents;

#[derive(Debug)]
pub struct SpaceIndex<T, const DATA_LENGTH: u32> {
    space_id: SpaceId,
    table_of_contents: IndexTableOfContents<T, DATA_LENGTH>,
    next_page_id: Arc<AtomicU32>,
    index_file: File,
}

impl<T, const DATA_LENGTH: u32> SpaceIndex<T, DATA_LENGTH>
where
    T: Archive + Hash + Eq + Ord,
    <T as Archive>::Archived: Hash + Eq + Deserialize<T, Strategy<Pool, rancor::Error>>,
{
    pub fn new(mut index_file: File, space_id: SpaceId) -> eyre::Result<Self> {
        let next_page_id = Arc::new(AtomicU32::new(0));
        let table_of_contents =
            IndexTableOfContents::parse_from_file(&mut index_file, space_id, next_page_id.clone())?;
        Ok(Self {
            space_id,
            table_of_contents,
            next_page_id,
            index_file,
        })
    }

    fn get_size_from_data_length() -> usize
    where
        T: Default + SizeMeasurable,
    {
        let node_id_size = T::default().aligned_size();
        let slot_size = u16::default().aligned_size();
        let index_value_size = align(T::default().aligned_size() + Link::default().aligned_size());
        let size = (DATA_LENGTH as usize - node_id_size) / (slot_size + index_value_size);
        size
    }

    fn add_index_page(&mut self, node_id: Pair<T, Link>, page_id: PageId) -> eyre::Result<()>
    where
        T: Archive
            + Clone
            + Default
            + SizeMeasurable
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
    {
        let size = Self::get_size_from_data_length();
        let mut page = NewIndexPage::new(node_id.key.clone(), size);
        page.values_count = 1;
        page.slots[0] = 0;
        page.index_values.push(IndexValue {
            key: node_id.key,
            link: node_id.value,
        });
        let header = GeneralHeader::new(page_id.into(), PageType::Index, self.space_id);
        let mut general_page = GeneralPage {
            inner: page,
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
            + SizeMeasurable
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
        index: usize,
        value: Pair<T, Link>,
    ) -> eyre::Result<()>
    where
        T: Archive
            + Default
            + SizeMeasurable
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
    {
        let size = Self::get_size_from_data_length();
        let (mut slots, values_count) =
            NewIndexPage::<T>::parse_slots_and_values_count(&mut self.index_file, page_id, size)?;
        slots.insert(index, values_count);
        NewIndexPage::<T>::persist_slots(&mut self.index_file, page_id, slots, values_count + 1)?;
        let index_value = IndexValue {
            key: value.key,
            link: value.value,
        };
        NewIndexPage::<T>::persist_value(
            &mut self.index_file,
            page_id,
            size,
            index_value,
            values_count,
        )?;

        Ok(())
    }

    fn process_insert_at(
        &mut self,
        node_id: T,
        value: Pair<T, Link>,
        index: usize,
    ) -> eyre::Result<()> {
        let page_id = self.table_of_contents.get(&node_id).expect("should exist");

        Ok(())
    }
    fn process_remove_at(
        &mut self,
        node_id: T,
        value: Pair<T, Link>,
        index: usize,
    ) -> eyre::Result<()> {
        todo!()
    }
    fn process_create_node(&mut self, node_id: Pair<T, Link>) -> eyre::Result<()>
    where
        T: Archive
            + Clone
            + Default
            + SizeMeasurable
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
    {
        let page_id = self.next_page_id.fetch_add(1, Ordering::Relaxed);
        self.table_of_contents
            .insert(node_id.key.clone(), page_id.into());
        self.add_index_page(node_id, page_id.into())?;

        Ok(())
    }
    fn process_remove_node(&mut self, node_id: T) -> eyre::Result<()> {
        todo!()
    }
    fn process_split_node(&mut self, node_id: T, split_index: usize) -> eyre::Result<()> {
        todo!()
    }
}
