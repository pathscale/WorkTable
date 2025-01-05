use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::sync::atomic::{AtomicU32, Ordering};

use data_bucket::page::PageId;
use indexset::cdc::change::ChangeEvent;

pub struct SpaceIndex<T, const DATA_LENGTH: usize> {
    table_of_contents: HashMap<T, PageId>,
    next_page_id: AtomicU32,
    index_file: File,
}

impl<T, const DATA_LENGTH: usize> SpaceIndex<T, DATA_LENGTH>
where
    T: Hash + Eq,
{
    pub fn new(index_file: File) -> Self {
        Self {
            table_of_contents: HashMap::new(),
            next_page_id: AtomicU32::new(1),
            index_file,
        }
    }

    pub fn process_change_event(&mut self, event: ChangeEvent<T>) -> eyre::Result<()> {
        match event {
            ChangeEvent::InsertAt {
                max_value: node_id,
                value,
                index,
            } => self.process_insert_at(node_id, value, index),
            ChangeEvent::RemoveAt {
                max_value: node_id,
                value,
                index,
            } => self.process_remove_at(node_id, value, index),
            ChangeEvent::CreateNode { max_value: node_id } => self.process_create_node(node_id),
            ChangeEvent::RemoveNode { max_value: node_id } => self.process_remove_node(node_id),
            ChangeEvent::SplitNode {
                max_value: node_id,
                split_index,
            } => self.process_split_node(node_id, split_index),
        }
    }

    fn process_insert_at(&mut self, node_id: T, value: T, index: usize) -> eyre::Result<()> {
        todo!()
    }
    fn process_remove_at(&mut self, node_id: T, value: T, index: usize) -> eyre::Result<()> {
        todo!()
    }
    fn process_create_node(&mut self, node_id: T) -> eyre::Result<()> {
        let page_id = self.next_page_id.fetch_add(1, Ordering::Relaxed);
        self.table_of_contents.insert(node_id, page_id.into());
        Ok(())
    }
    fn process_remove_node(&mut self, node_id: T) -> eyre::Result<()> {
        todo!()
    }
    fn process_split_node(&mut self, node_id: T, split_index: usize) -> eyre::Result<()> {
        todo!()
    }
}
