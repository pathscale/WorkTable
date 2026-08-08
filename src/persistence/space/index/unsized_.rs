use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use data_bucket::page::PageId;
use data_bucket::{
    GeneralHeader, GeneralPage, IndexPageUtility, IndexValue, Link, PageType, SizeMeasurable, SpaceId, SpaceInfoPage,
    UnsizedIndexPage, UnsizedIndexPageUtility, VariableSizeMeasurable, parse_page, persist_page, persist_pages_batch,
};
use eyre::eyre;
use indexset::cdc::change::ChangeEvent;
use indexset::concurrent::map::BTreeMap;
use indexset::core::pair::Pair;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::Serializer;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize, rancor};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::UnsizedNode;
use crate::persistence::space::BatchChangeEvent;
use crate::persistence::{IndexTableOfContents, SpaceIndex, SpaceIndexOps};
use crate::prelude::WT_INDEX_EXTENSION;

#[derive(Debug)]
pub struct SpaceIndexUnsized<T: Ord + Eq, const DATA_LENGTH: u32> {
    space_id: SpaceId,
    table_of_contents: IndexTableOfContents<(T, Link), DATA_LENGTH>,
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
        + Hash
        + Clone
        + Default
        + Debug
        + SizeMeasurable
        + VariableSizeMeasurable
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>
        + Send
        + Sync
        + 'static,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>
        + Ord
        + Eq
        + Debug
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rancor::Error>>,
{
    fn compact_page_if_needed(page: &mut UnsizedIndexPage<T, DATA_LENGTH>) -> eyre::Result<()> {
        let persisted_size =
            UnsizedIndexPageUtility::<T>::persisted_size(page.slots_size as usize, page.node_id_size as usize)
                + page.last_value_offset as usize;
        if persisted_size <= DATA_LENGTH as usize {
            return Ok(());
        }

        // Removed variable-width entries leave holes at the page tail. After
        // reload the in-memory node contains only live values, so it cannot
        // see that physical fragmentation. Compact before the growing slot
        // directory can overlap the next tail value.
        // An empty page cannot pass the occupancy guard above, which also
        // protects `UnsizedIndexPage::rebuild` from its non-empty assumption.
        page.rebuild();
        let compacted_size =
            UnsizedIndexPageUtility::<T>::persisted_size(page.slots_size as usize, page.node_id_size as usize)
                + page.last_value_offset as usize;
        if compacted_size > DATA_LENGTH as usize {
            return Err(eyre!(
                "unsized index page requires {compacted_size} bytes after compaction, but its capacity is {DATA_LENGTH}"
            ));
        }
        Ok(())
    }

    pub async fn new<S: AsRef<str>>(index_file_path: S, space_id: SpaceId, version: u32) -> eyre::Result<Self> {
        let space_index = SpaceIndex::<T, DATA_LENGTH>::new(index_file_path, space_id, version).await?;
        Ok(Self {
            space_id,
            table_of_contents: space_index.table_of_contents,
            next_page_id: space_index.next_page_id,
            index_file: space_index.index_file,
            info: space_index.info,
        })
    }

    async fn add_new_index_page(&mut self, node_id: Pair<T, Link>, page_id: PageId) -> eyre::Result<()> {
        let page = UnsizedIndexPage::new(node_id.clone().into())?;
        self.add_index_page(page, page_id).await
    }

    async fn add_index_page(&mut self, node: UnsizedIndexPage<T, DATA_LENGTH>, page_id: PageId) -> eyre::Result<()> {
        let header = GeneralHeader::new(page_id, PageType::Index, self.space_id);
        let mut general_page = GeneralPage { inner: node, header };
        persist_page(&mut general_page, &mut self.index_file).await?;
        Ok(())
    }

    async fn process_create_node(&mut self, node_id: Pair<T, Link>) -> eyre::Result<()> {
        let page_id = if let Some(id) = self.table_of_contents.pop_empty_page_id() {
            id
        } else {
            self.next_page_id.fetch_add(1, Ordering::Relaxed).into()
        };
        self.table_of_contents
            .insert((node_id.key.clone(), node_id.value), page_id);
        self.table_of_contents.persist(&mut self.index_file).await?;
        self.add_new_index_page(node_id, page_id).await?;

        Ok(())
    }

    async fn process_remove_node(&mut self, node_id: Pair<T, Link>) -> eyre::Result<()> {
        self.table_of_contents.remove(&(node_id.key, node_id.value));
        self.table_of_contents.persist(&mut self.index_file).await?;
        Ok(())
    }

    async fn process_insert_at(
        &mut self,
        node_id: Pair<T, Link>,
        value: Pair<T, Link>,
        index: usize,
    ) -> eyre::Result<()> {
        let page_id = self
            .table_of_contents
            .get(&(node_id.key.clone(), node_id.value))
            .ok_or(eyre!("Node with {:?} id is not found", node_id))?;
        if let Some(new_node_id) = self
            .insert_on_index_page(page_id, node_id.clone(), index, value)
            .await?
        {
            self.table_of_contents
                .update_key(&(node_id.key, node_id.value), (new_node_id.key, new_node_id.value));
            self.table_of_contents.persist(&mut self.index_file).await?;
        }
        Ok(())
    }

    async fn insert_on_index_page(
        &mut self,
        page_id: PageId,
        node_id: Pair<T, Link>,
        index: usize,
        value: Pair<T, Link>,
    ) -> eyre::Result<Option<Pair<T, Link>>> {
        let mut new_node_id = None;

        let mut utility =
            UnsizedIndexPage::<T, DATA_LENGTH>::parse_index_page_utility(&mut self.index_file, page_id).await?;
        let index_value = IndexValue {
            key: value.key.clone(),
            link: value.value,
        };
        let value_size = index_value.aligned_size();
        let future_node_id_size = if node_id.key < value.key {
            value_size
        } else {
            utility.node_id_size as usize
        };
        let future_utility_size =
            UnsizedIndexPageUtility::<T>::persisted_size(utility.slots_size as usize + 1, future_node_id_size);
        if future_utility_size + utility.last_value_offset as usize + value_size > DATA_LENGTH as usize {
            let mut page =
                parse_page::<UnsizedIndexPage<T, DATA_LENGTH>, DATA_LENGTH>(&mut self.index_file, page_id.into())
                    .await?;
            page.inner.apply_change_event(ChangeEvent::InsertAt {
                // The page mutation ignores event ids; this synthetic event
                // exists only to reuse the same insertion accounting.
                event_id: 0.into(),
                max_value: node_id,
                value,
                index,
            })?;
            Self::compact_page_if_needed(&mut page.inner)?;
            let changed_node_id =
                (page.inner.node_id.key != utility.node_id.key).then(|| Pair::from(page.inner.node_id.clone()));
            persist_page(&mut page, &mut self.index_file).await?;
            return Ok(changed_node_id);
        }
        let previous_offset = utility.last_value_offset;
        let value_offset = UnsizedIndexPage::<T, DATA_LENGTH>::persist_value(
            &mut self.index_file,
            page_id,
            previous_offset,
            index_value,
        )
        .await?;
        utility.slots_size += 1;
        utility.last_value_offset = value_offset;
        utility
            .slots
            .insert(index, (value_offset, (value_offset - previous_offset) as u16));

        if node_id.key < value.key {
            utility.update_node_id(value.clone().into())?;
            new_node_id = Some(value);
        }

        UnsizedIndexPage::<T, DATA_LENGTH>::persist_index_page_utility(&mut self.index_file, page_id, utility).await?;

        Ok(new_node_id)
    }

    async fn process_remove_at(
        &mut self,
        node_id: Pair<T, Link>,
        value: Pair<T, Link>,
        index: usize,
    ) -> eyre::Result<()> {
        let page_id = self
            .table_of_contents
            .get(&(node_id.key.clone(), node_id.value))
            .ok_or(eyre!("Node with {:?} id is not found", node_id))?;
        if let Some(new_node_id) = self
            .remove_from_index_page(page_id, node_id.clone(), index, value)
            .await?
        {
            self.table_of_contents
                .update_key(&(node_id.key, node_id.value), (new_node_id.key, new_node_id.value));
            self.table_of_contents.persist(&mut self.index_file).await?;
        }
        Ok(())
    }

    async fn remove_from_index_page(
        &mut self,
        page_id: PageId,
        node_id: Pair<T, Link>,
        index: usize,
        value: Pair<T, Link>,
    ) -> eyre::Result<Option<Pair<T, Link>>> {
        let mut new_node_id = None;

        let mut utility =
            UnsizedIndexPage::<T, DATA_LENGTH>::parse_index_page_utility(&mut self.index_file, page_id).await?;
        utility.slots.remove(index);
        utility.slots_size -= 1;

        if node_id.key == value.key {
            let (offset, len) = *utility
                .slots
                .get(index - 1)
                .expect("slots always should exist in `size` bounds");
            let node_id =
                UnsizedIndexPage::<T, DATA_LENGTH>::read_value_with_offset(&mut self.index_file, page_id, offset, len)
                    .await?;
            utility.update_node_id(node_id)?;
            new_node_id = Some(utility.node_id.clone().into())
        }

        UnsizedIndexPage::<T, DATA_LENGTH>::persist_index_page_utility(&mut self.index_file, page_id, utility).await?;

        Ok(new_node_id)
    }

    async fn process_split_node(&mut self, node_id: Pair<T, Link>, split_index: usize) -> eyre::Result<()> {
        let page_id = self
            .table_of_contents
            .get(&(node_id.key.clone(), node_id.value))
            .ok_or(eyre!("Node with {:?} id is not found", node_id))?;
        let mut page =
            parse_page::<UnsizedIndexPage<T, DATA_LENGTH>, DATA_LENGTH>(&mut self.index_file, page_id.into()).await?;
        let splitted_page = page.inner.split(split_index);
        let new_page_id = if let Some(id) = self.table_of_contents.pop_empty_page_id() {
            id
        } else {
            self.next_page_id.fetch_add(1, Ordering::Relaxed).into()
        };

        self.table_of_contents.update_key(
            &(node_id.key, node_id.value),
            (page.inner.node_id.key.clone(), page.inner.node_id.link),
        );
        self.table_of_contents.insert(
            (splitted_page.node_id.key.clone(), splitted_page.node_id.link),
            new_page_id,
        );
        self.table_of_contents.persist(&mut self.index_file).await?;

        self.add_index_page(splitted_page, new_page_id).await?;
        persist_page(&mut page, &mut self.index_file).await?;

        Ok(())
    }

    pub async fn parse_indexset(&mut self) -> eyre::Result<BTreeMap<T, Link, UnsizedNode<Pair<T, Link>>>> {
        let indexset = BTreeMap::<T, Link, UnsizedNode<Pair<T, Link>>>::with_maximum_node_size(DATA_LENGTH as usize);
        for (_, page_id) in self.table_of_contents.iter() {
            let page =
                parse_page::<UnsizedIndexPage<T, DATA_LENGTH>, DATA_LENGTH>(&mut self.index_file, (*page_id).into())
                    .await?;
            let node = page.inner.get_node();
            indexset.attach_node(UnsizedNode::from_inner(node, DATA_LENGTH as usize))
        }

        Ok(indexset)
    }
}

impl<T, const INNER_PAGE_SIZE: u32> SpaceIndexOps<T> for SpaceIndexUnsized<T, INNER_PAGE_SIZE>
where
    T: Archive
        + Ord
        + Eq
        + Hash
        + Clone
        + Default
        + Debug
        + SizeMeasurable
        + VariableSizeMeasurable
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>
        + Send
        + Sync
        + 'static,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>
        + Ord
        + Eq
        + Debug
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rancor::Error>>,
{
    async fn primary_from_table_files_path<S: AsRef<str> + Send>(table_path: S, version: u32) -> eyre::Result<Self> {
        let path = format!("{}/primary{}", table_path.as_ref(), WT_INDEX_EXTENSION);
        Self::new(path, 0.into(), version).await
    }

    async fn secondary_from_table_files_path<S1: AsRef<str> + Send, S2: AsRef<str> + Send>(
        table_path: S1,
        name: S2,
        version: u32,
    ) -> eyre::Result<Self>
    where
        Self: Sized,
    {
        let path = format!("{}/{}{}", table_path.as_ref(), name.as_ref(), WT_INDEX_EXTENSION);
        Self::new(path, 0.into(), version).await
    }

    async fn bootstrap(file: &mut File, table_name: String, version: u32) -> eyre::Result<()> {
        SpaceIndex::<T, INNER_PAGE_SIZE>::bootstrap(file, table_name, version).await
    }

    async fn process_change_event(&mut self, event: ChangeEvent<Pair<T, Link>>) -> eyre::Result<()> {
        match event {
            ChangeEvent::InsertAt {
                event_id: _,
                max_value: node_id,
                value,
                index,
            } => self.process_insert_at(node_id, value, index).await,
            ChangeEvent::RemoveAt {
                event_id: _,
                max_value: node_id,
                value,
                index,
            } => self.process_remove_at(node_id, value, index).await,
            ChangeEvent::CreateNode {
                event_id: _,
                max_value: node_id,
            } => self.process_create_node(node_id).await,
            ChangeEvent::RemoveNode {
                event_id: _,
                max_value: node_id,
            } => self.process_remove_node(node_id).await,
            ChangeEvent::SplitNode {
                event_id: _,
                max_value: node_id,
                split_index,
            } => self.process_split_node(node_id, split_index).await,
        }?;
        // The partial page writes above can end with a buffered `write_all`
        // that `tokio::fs::File` completes on a background blocking task.
        // Flush before reporting the event processed so the bytes are visible
        // to any other handle that opens this file afterwards.
        self.index_file.flush().await?;
        Ok(())
    }

    async fn process_change_event_batch(&mut self, events: BatchChangeEvent<T>) -> eyre::Result<()> {
        let mut pages: HashMap<PageId, _> = HashMap::new();
        // A split can change a page's maximum and therefore its table-of-
        // contents key while later events in the same CDC batch still refer
        // to the pre-split maximum. Keep those historical identities scoped
        // to this batch so the event reaches the page it was generated from.
        // At most two transitional identities per buffered page. Replacing a
        // page maximum consumes its prior aliases; a split may retain both the
        // event's identity and the page's actual pre-split identity for the
        // new right page. Memory is therefore bounded by pages, not operations.
        let mut page_aliases = PageAliases::default();
        for ev in events {
            match &ev {
                ChangeEvent::InsertAt { max_value, .. } | ChangeEvent::RemoveAt { max_value, .. } => {
                    let event_page_key = (max_value.key.clone(), max_value.value);

                    let page_index = self
                        .table_of_contents
                        .get(&event_page_key)
                        .or_else(|| page_aliases.get(&event_page_key))
                        .ok_or_else(|| {
                            eyre!(
                                "unsized index event references a missing page (toc_segments={}, buffered_pages={}, aliases={})",
                                self.table_of_contents.pages.len(),
                                pages.len(),
                                page_aliases.len()
                            )
                        })?;
                    let page = pages.get_mut(&page_index);
                    let page_to_update = if let Some(page) = page {
                        page
                    } else {
                        let page = parse_page::<UnsizedIndexPage<T, INNER_PAGE_SIZE>, INNER_PAGE_SIZE>(
                            &mut self.index_file,
                            page_index.into(),
                        )
                        .await?;
                        pages.insert(page_index, page);
                        pages
                            .get_mut(&page_index)
                            .expect("should be available as was just inserted before")
                    };
                    let current_page_key = (
                        page_to_update.inner.node_id.key.clone(),
                        page_to_update.inner.node_id.link,
                    );
                    page_to_update.inner.apply_change_event(ev.clone())?;
                    if page_to_update.inner.node_id.key != current_page_key.0
                        || page_to_update.inner.node_id.link != current_page_key.1
                    {
                        let updated_page_key = (
                            page_to_update.inner.node_id.key.clone(),
                            page_to_update.inner.node_id.link,
                        );
                        self.table_of_contents.update_key(&current_page_key, updated_page_key);
                        page_aliases.replace(page_index, [current_page_key]);
                    }
                }
                ChangeEvent::CreateNode { event_id: _, max_value } => {
                    let page_id = if let Some(id) = self.table_of_contents.pop_empty_page_id() {
                        id
                    } else {
                        self.next_page_id.fetch_add(1, Ordering::Relaxed).into()
                    };
                    self.table_of_contents
                        .insert((max_value.key.clone(), max_value.value), page_id);

                    let page = UnsizedIndexPage::<T, INNER_PAGE_SIZE>::new(max_value.clone().into())?;
                    let header = GeneralHeader::new(page_id, PageType::IndexUnsized, self.space_id);
                    let general_page = GeneralPage { inner: page, header };
                    pages.insert(page_id, general_page);
                    self.table_of_contents
                        .insert((max_value.key.clone(), max_value.value), page_id)
                }
                ChangeEvent::RemoveNode { event_id: _, max_value } => {
                    self.table_of_contents.remove(&(max_value.key.clone(), max_value.value));
                }
                ChangeEvent::SplitNode {
                    event_id: _,
                    max_value,
                    split_index,
                } => {
                    let event_page_key = (max_value.key.clone(), max_value.value);

                    let page_index = self
                        .table_of_contents
                        .get(&event_page_key)
                        .or_else(|| page_aliases.get(&event_page_key))
                        .ok_or_else(|| {
                            eyre!(
                                "unsized index split references a missing page (toc_segments={}, buffered_pages={}, aliases={})",
                                self.table_of_contents.pages.len(),
                                pages.len(),
                                page_aliases.len()
                            )
                        })?;
                    let page = pages.get_mut(&page_index);
                    let page_to_update = if let Some(page) = page {
                        page
                    } else {
                        let page = parse_page::<UnsizedIndexPage<T, INNER_PAGE_SIZE>, INNER_PAGE_SIZE>(
                            &mut self.index_file,
                            page_index.into(),
                        )
                        .await?;
                        pages.insert(page_index, page);
                        pages
                            .get_mut(&page_index)
                            .expect("should be available as was just inserted before")
                    };
                    let current_page_key = (
                        page_to_update.inner.node_id.key.clone(),
                        page_to_update.inner.node_id.link,
                    );
                    let splitted_page = page_to_update.inner.split(*split_index);

                    let new_page_id = if let Some(id) = self.table_of_contents.pop_empty_page_id() {
                        id
                    } else {
                        self.next_page_id.fetch_add(1, Ordering::Relaxed).into()
                    };

                    self.table_of_contents.update_key(
                        &current_page_key,
                        (
                            page_to_update.inner.node_id.key.clone(),
                            page_to_update.inner.node_id.link,
                        ),
                    );
                    self.table_of_contents.insert(
                        (splitted_page.node_id.key.clone(), splitted_page.node_id.link),
                        new_page_id,
                    );
                    let header = GeneralHeader::new(new_page_id, PageType::Index, self.space_id);
                    let general_page = GeneralPage {
                        inner: splitted_page,
                        header,
                    };
                    pages.insert(new_page_id, general_page);
                    // The pre-split maximum remains the right page's identity.
                    // A following remove/insert pair can still name it even
                    // after the remove temporarily lowers that maximum.
                    page_aliases.remove_page(page_index);
                    page_aliases.replace(new_page_id, [event_page_key, current_page_key]);
                }
            }
        }

        for page in pages.values_mut() {
            Self::compact_page_if_needed(&mut page.inner)?;
        }

        self.table_of_contents.persist(&mut self.index_file).await?;
        persist_pages_batch(pages.values().cloned().collect(), &mut self.index_file).await?;
        // The batch's last page write is a buffered `write_all`; flush so the
        // batch is visible to other handles once it reports done.
        self.index_file.flush().await?;
        Ok(())
    }
}

struct PageAliases<T> {
    by_key: HashMap<Arc<(T, Link)>, PageId>,
    by_page: HashMap<PageId, Vec<Arc<(T, Link)>>>,
}

impl<T> Default for PageAliases<T> {
    fn default() -> Self {
        Self {
            by_key: HashMap::new(),
            by_page: HashMap::new(),
        }
    }
}

impl<T: Hash + Eq> PageAliases<T> {
    fn get(&self, key: &(T, Link)) -> Option<PageId> {
        self.by_key.get(key).copied()
    }

    fn len(&self) -> usize {
        self.by_key.len()
    }

    fn remove_page(&mut self, page_id: PageId) {
        let Some(keys) = self.by_page.remove(&page_id) else {
            return;
        };
        for key in keys {
            if self.by_key.get(key.as_ref()) == Some(&page_id) {
                self.by_key.remove(key.as_ref());
            }
        }
    }

    fn replace(&mut self, page_id: PageId, keys: impl IntoIterator<Item = (T, Link)>) {
        self.remove_page(page_id);
        let mut page_keys = Vec::with_capacity(2);
        for key in keys {
            if page_keys
                .iter()
                .any(|existing: &Arc<(T, Link)>| existing.as_ref() == &key)
            {
                continue;
            }

            let key = Arc::new(key);
            let existing = self
                .by_key
                .get_key_value(key.as_ref())
                .map(|(stored, previous_page)| (stored.clone(), *previous_page));
            let shared_key = if let Some((stored, previous_page)) = existing {
                *self
                    .by_key
                    .get_mut(stored.as_ref())
                    .expect("alias found immediately before update") = page_id;
                if previous_page != page_id {
                    self.remove_key_from_page(previous_page, stored.as_ref());
                }
                stored
            } else {
                self.by_key.insert(key.clone(), page_id);
                key
            };
            page_keys.push(shared_key);
        }
        if !page_keys.is_empty() {
            self.by_page.insert(page_id, page_keys);
        }
    }

    fn remove_key_from_page(&mut self, page_id: PageId, key: &(T, Link)) {
        let remove_page = if let Some(keys) = self.by_page.get_mut(&page_id) {
            keys.retain(|existing| existing.as_ref() != key);
            keys.is_empty()
        } else {
            false
        };
        if remove_page {
            self.by_page.remove(&page_id);
        }
    }
}

#[cfg(test)]
mod alias_tests {
    use super::*;

    fn link(offset: u32) -> Link {
        Link {
            page_id: 1.into(),
            offset,
            length: 8,
        }
    }

    #[test]
    fn repeated_maximum_changes_keep_one_alias_per_page() {
        let page_id = PageId::from(7);
        let mut aliases = PageAliases::default();
        for revision in 0..1_000 {
            aliases.replace(page_id, [(format!("key-{revision}"), link(revision))]);
        }

        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases.get(&("key-999".into(), link(999))), Some(page_id));
    }

    #[test]
    fn split_keeps_only_the_two_live_transitional_identities() {
        let old_page = PageId::from(3);
        let right_page = PageId::from(4);
        let mut aliases = PageAliases::default();
        aliases.replace(old_page, [("older".to_string(), link(1))]);
        aliases.remove_page(old_page);
        aliases.replace(
            right_page,
            [("event".to_string(), link(2)), ("current".to_string(), link(3))],
        );

        assert_eq!(aliases.len(), 2);
        assert_eq!(aliases.by_page.get(&right_page).map(Vec::len), Some(2));
    }

    #[test]
    fn replacing_many_pages_preserves_constant_work_per_page() {
        let mut aliases = PageAliases::default();
        for page in 1..=1_000 {
            aliases.replace(PageId::from(page), [(format!("old-{page}"), link(page))]);
        }
        for page in 1..=1_000 {
            aliases.replace(PageId::from(page), [(format!("new-{page}"), link(page + 1_000))]);
        }

        assert_eq!(aliases.len(), 1_000);
        assert_eq!(aliases.by_page.len(), 1_000);
        for page in 1..=1_000 {
            assert_eq!(
                aliases.get(&(format!("new-{page}"), link(page + 1_000))),
                Some(PageId::from(page))
            );
        }
    }
}
