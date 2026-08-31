mod page_aliases;
mod reconstruct;
mod table_of_contents;
mod unsized_;
mod util;

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use convert_case::{Case, Casing};
use data_bucket::page::{IndexValue, PageId};
use data_bucket::{
    GENERAL_HEADER_SIZE, GeneralHeader, GeneralPage, IndexPage, IndexPageUtility, Link, PageType, SizeMeasurable,
    SpaceId, SpaceInfoPage, get_index_page_size_from_data_length, parse_page, persist_page, persist_pages_batch,
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

use crate::persistence::SpaceIndexOps;
use crate::persistence::space::{BatchChangeEvent, open_or_create_file};
use crate::prelude::WT_INDEX_EXTENSION;

pub use reconstruct::reconstruct_multi_index_nodes;
pub use table_of_contents::{IndexTableOfContents, TocEntryOversizedError};
pub use unsized_::SpaceIndexUnsized;
pub use util::{map_index_pages_to_toc_and_general, map_unsized_index_pages_to_toc_and_general};

#[derive(Debug)]
pub struct SpaceIndex<T: Ord + Eq, const INNER_PAGE_SIZE: u32> {
    space_id: SpaceId,
    table_of_contents: IndexTableOfContents<(T, Link), INNER_PAGE_SIZE>,
    next_page_id: Arc<AtomicU32>,
    index_file: File,
    #[allow(dead_code)]
    info: GeneralPage<SpaceInfoPage<()>>,
}

impl<T, const INNER_PAGE_SIZE: u32> SpaceIndex<T, INNER_PAGE_SIZE>
where
    T: Archive
        + Ord
        + Eq
        + Hash
        + Clone
        + Default
        + Debug
        + SizeMeasurable
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
    /// Resolves a batch event's page identity to a page id: first through the
    /// table of contents (canonical identity), then through the batch-scoped
    /// aliases (historical identities whose page was re-keyed earlier in the
    /// same batch). An alias hit also returns the page's current canonical
    /// identity so callers never have to predict page mutation semantics.
    fn resolve_batch_page(
        &self,
        aliases: &page_aliases::PageAliases<T>,
        event_page_key: &(T, Link),
    ) -> Option<(PageId, Option<(T, Link)>)> {
        self.table_of_contents
            .get(event_page_key)
            .map(|page_id| (page_id, None))
            .or_else(|| {
                aliases
                    .resolve(event_page_key)
                    .map(|(page_id, current_key)| (page_id, Some(current_key.clone())))
            })
    }

    pub async fn new<S: AsRef<str>>(index_file_path: S, space_id: SpaceId, version: u32) -> eyre::Result<Self> {
        let mut index_file = if !Path::new(index_file_path.as_ref()).exists() {
            let name = index_file_path
                .as_ref()
                .split("/")
                .collect::<Vec<_>>()
                .iter()
                .rev()
                .nth(1)
                .expect("is not in root...")
                .to_string()
                .from_case(Case::Snake)
                .to_case(Case::Pascal);
            let mut index_file = open_or_create_file(index_file_path.as_ref()).await?;
            Self::bootstrap(&mut index_file, name, version).await?;
            index_file
        } else {
            open_or_create_file(index_file_path).await?
        };
        let info = parse_page::<_, INNER_PAGE_SIZE>(&mut index_file, 0).await?;

        let file_length = index_file.metadata().await?.len();
        let page_id = if file_length % (INNER_PAGE_SIZE as u64 + GENERAL_HEADER_SIZE as u64) == 0 {
            file_length / (INNER_PAGE_SIZE as u64 + GENERAL_HEADER_SIZE as u64)
        } else {
            file_length / (INNER_PAGE_SIZE as u64 + GENERAL_HEADER_SIZE as u64) + 1
        };
        let next_page_id = Arc::new(AtomicU32::new(page_id as u32));
        let table_of_contents =
            IndexTableOfContents::parse_from_file(&mut index_file, space_id, next_page_id.clone()).await?;
        Ok(Self {
            space_id,
            table_of_contents,
            next_page_id,
            index_file,
            info,
        })
    }

    async fn add_new_index_page(&mut self, node_id: Pair<T, Link>, page_id: PageId) -> eyre::Result<()> {
        let size = get_index_page_size_from_data_length::<T>(INNER_PAGE_SIZE as usize);
        let mut page = IndexPage::new(node_id.clone().into(), size);
        page.current_index = 1;
        page.current_length = 1;
        page.slots[0] = 0;
        page.index_values[0] = IndexValue {
            key: node_id.key,
            link: node_id.value,
        };
        self.add_index_page(page, page_id).await
    }

    async fn add_index_page(&mut self, node: IndexPage<T>, page_id: PageId) -> eyre::Result<()> {
        let header = GeneralHeader::new(page_id, PageType::Index, self.space_id);
        let mut general_page = GeneralPage { inner: node, header };
        persist_page(&mut general_page, &mut self.index_file).await?;
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

        let size = get_index_page_size_from_data_length::<T>(INNER_PAGE_SIZE as usize);
        let mut utility = IndexPage::<T>::parse_index_page_utility(&mut self.index_file, page_id).await?;
        utility.slots.insert(index, utility.current_index);
        utility.slots.remove(size);
        utility.current_length += 1;
        let index_value = IndexValue {
            key: value.key.clone(),
            link: value.value,
        };
        utility.current_index =
            IndexPage::<T>::persist_value(&mut self.index_file, page_id, size, index_value, utility.current_index)
                .await?;

        if node_id.key < value.key {
            utility.node_id = value.clone().into();
            new_node_id = Some(value);
        }

        IndexPage::<T>::persist_index_page_utility(&mut self.index_file, page_id, utility).await?;

        Ok(new_node_id)
    }

    async fn remove_from_index_page(
        &mut self,
        page_id: PageId,
        node_id: Pair<T, Link>,
        index: usize,
        value: Pair<T, Link>,
    ) -> eyre::Result<Option<Pair<T, Link>>> {
        let mut new_node_id = None;

        let size = get_index_page_size_from_data_length::<T>(INNER_PAGE_SIZE as usize);
        let mut utility = IndexPage::<T>::parse_index_page_utility(&mut self.index_file, page_id).await?;
        let value_position = *utility
            .slots
            .get(index)
            .expect("Slots should exist for every index within `size`");
        if value_position < utility.current_index {
            utility.current_index = value_position;
        }
        utility.slots.remove(index);
        utility.slots.push(0);
        utility.current_length -= 1;
        IndexPage::<T>::remove_value(&mut self.index_file, page_id, size, utility.current_index).await?;

        if node_id.key == value.key {
            let index = *utility
                .slots
                .get(index - 1)
                .expect("slots always should exist in `size` bounds");
            utility.node_id =
                IndexPage::<T>::read_value_with_index(&mut self.index_file, page_id, size, index as usize).await?;
            new_node_id = Some(utility.node_id.clone().into())
        }

        IndexPage::<T>::persist_index_page_utility(&mut self.index_file, page_id, utility).await?;

        Ok(new_node_id)
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
                .update_key(&(node_id.key, node_id.value), (new_node_id.key, new_node_id.value))?;
            self.table_of_contents.persist(&mut self.index_file).await?;
        }
        Ok(())
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
                .update_key(&(node_id.key, node_id.value), (new_node_id.key, new_node_id.value))?;
            self.table_of_contents.persist(&mut self.index_file).await?;
        }
        Ok(())
    }
    async fn process_create_node(&mut self, node_id: Pair<T, Link>) -> eyre::Result<()> {
        let page_id = if let Some(id) = self.table_of_contents.pop_empty_page_id() {
            id
        } else {
            self.next_page_id.fetch_add(1, Ordering::Relaxed).into()
        };
        self.table_of_contents
            .try_insert((node_id.key.clone(), node_id.value), page_id)?;
        self.table_of_contents.persist(&mut self.index_file).await?;
        self.add_new_index_page(node_id, page_id).await?;

        Ok(())
    }

    async fn process_remove_node(&mut self, node_id: Pair<T, Link>) -> eyre::Result<()> {
        self.table_of_contents.remove(&(node_id.key, node_id.value));
        self.table_of_contents.persist(&mut self.index_file).await?;
        Ok(())
    }

    async fn process_split_node(&mut self, node_id: Pair<T, Link>, split_index: usize) -> eyre::Result<()> {
        let page_id = self
            .table_of_contents
            .get(&(node_id.key.clone(), node_id.value))
            .ok_or(eyre!("Node with {:?} id is not found", node_id))?;
        let mut page = parse_page::<IndexPage<T>, INNER_PAGE_SIZE>(&mut self.index_file, page_id.into()).await?;
        let splitted_page = page.inner.split(split_index);
        let new_page_id = if let Some(id) = self.table_of_contents.pop_empty_page_id() {
            id
        } else {
            self.next_page_id.fetch_add(1, Ordering::Relaxed).into()
        };

        self.table_of_contents.update_key(
            &(node_id.key.clone(), node_id.value),
            (page.inner.node_id.key.clone(), page.inner.node_id.link),
        )?;
        self.table_of_contents.try_insert(
            (splitted_page.node_id.key.clone(), splitted_page.node_id.link),
            new_page_id,
        )?;
        self.table_of_contents.persist(&mut self.index_file).await?;

        self.add_index_page(splitted_page, new_page_id).await?;
        persist_page(&mut page, &mut self.index_file).await?;

        Ok(())
    }

    pub async fn parse_indexset(&mut self) -> eyre::Result<BTreeMap<T, Link>> {
        let size = get_index_page_size_from_data_length::<T>(INNER_PAGE_SIZE as usize);
        let indexset = BTreeMap::<T, Link>::with_maximum_node_size(size);
        for (_, page_id) in self.table_of_contents.iter() {
            let page = parse_page::<IndexPage<T>, INNER_PAGE_SIZE>(&mut self.index_file, (*page_id).into()).await?;
            let node = page.inner.get_node();
            indexset.attach_node(node)
        }

        Ok(indexset)
    }
}

impl<T, const INNER_PAGE_SIZE: u32> SpaceIndexOps<T> for SpaceIndex<T, INNER_PAGE_SIZE>
where
    T: Archive
        + Ord
        + Eq
        + Hash
        + Clone
        + Default
        + Debug
        + SizeMeasurable
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
        let info = SpaceInfoPage {
            id: 0.into(),
            page_count: 0,
            name: table_name,
            version,
            row_schema: vec![],
            primary_key_fields: vec![],
            secondary_index_types: vec![],
            pk_gen_state: (),
            empty_links_list: vec![],
        };
        let mut page = GeneralPage {
            header: GeneralHeader::new(0.into(), PageType::SpaceInfo, 0.into()),
            inner: info,
        };
        persist_page(&mut page, file).await
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
        // At most two transitional identities per buffered page: the event's
        // identity and the page's actual pre-apply identity. Keeping both is
        // required when a split is followed by a remove/insert pair that still
        // names the pre-split maximum. Memory is bounded by pages, not events.
        let mut page_aliases = page_aliases::PageAliases::default();
        for ev in events {
            match &ev {
                ChangeEvent::InsertAt { max_value, .. } | ChangeEvent::RemoveAt { max_value, .. } => {
                    let event_page_key = (max_value.key.clone(), max_value.value);
                    // A direct TOC hit means the event key is the page's
                    // canonical pre-event identity. An alias hit carries the
                    // current canonical identity captured when the alias was
                    // installed. This lets us compare the actual post-apply
                    // identity without predicting DataBucket's mutation rules.
                    let Some((page_index, aliased_page_key)) = self.resolve_batch_page(&page_aliases, &event_page_key)
                    else {
                        return Err(eyre!(
                            "index event references a missing page (toc_segments={}, buffered_pages={}, aliases={})",
                            self.table_of_contents.pages.len(),
                            pages.len(),
                            page_aliases.len()
                        ));
                    };
                    let page = pages.get_mut(&page_index);
                    let page_to_update = if let Some(page) = page {
                        page
                    } else {
                        let page = parse_page::<IndexPage<T>, INNER_PAGE_SIZE>(&mut self.index_file, page_index.into())
                            .await?;
                        pages.insert(page_index, page);
                        pages
                            .get_mut(&page_index)
                            .expect("should be available as was just inserted before")
                    };
                    let canonical_page_key = aliased_page_key.as_ref().unwrap_or(&event_page_key);
                    page_to_update.inner.apply_change_event(ev.clone())?;
                    if page_to_update.inner.node_id.key != canonical_page_key.0
                        || page_to_update.inner.node_id.link != canonical_page_key.1
                    {
                        let pre_event_page_key = aliased_page_key.unwrap_or_else(|| event_page_key.clone());
                        let updated_page_key = (
                            page_to_update.inner.node_id.key.clone(),
                            page_to_update.inner.node_id.link,
                        );
                        // The TOC owns the buffered page's actual identity.
                        // `event_page_key` may be a historical alias, so using
                        // it as the canonical update target can remove or
                        // rewrite the wrong segment.
                        if !self
                            .table_of_contents
                            .try_update_key(&pre_event_page_key, updated_page_key.clone())?
                        {
                            return Err(eyre!(
                                "index page identity is absent from the table of contents (page={page_index:?}, toc_segments={})",
                                self.table_of_contents.pages.len()
                            ));
                        }
                        if self.table_of_contents.get(&updated_page_key) != Some(page_index) {
                            return Err(eyre!(
                                "index page identity update did not become canonical (page={page_index:?})"
                            ));
                        }
                        page_aliases.replace(page_index, updated_page_key, event_page_key, pre_event_page_key)?;
                    }
                }
                ChangeEvent::CreateNode { event_id: _, max_value } => {
                    let page_id = if let Some(id) = self.table_of_contents.pop_empty_page_id() {
                        id
                    } else {
                        self.next_page_id.fetch_add(1, Ordering::Relaxed).into()
                    };
                    self.table_of_contents
                        .try_insert((max_value.key.clone(), max_value.value), page_id)?;

                    let size = get_index_page_size_from_data_length::<T>(INNER_PAGE_SIZE as usize);
                    let mut page = IndexPage::new(max_value.clone().into(), size);
                    let ev = ChangeEvent::InsertAt {
                        event_id: 0.into(),
                        max_value: max_value.clone(),
                        value: max_value.clone(),
                        index: 0,
                    };
                    page.apply_change_event(ev)?;
                    let header = GeneralHeader::new(page_id, PageType::Index, self.space_id);
                    let general_page = GeneralPage { inner: page, header };
                    pages.insert(page_id, general_page);
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
                    let Some((page_index, aliased_page_key)) = self.resolve_batch_page(&page_aliases, &event_page_key)
                    else {
                        return Err(eyre!(
                            "index split references a missing page (toc_segments={}, buffered_pages={}, aliases={})",
                            self.table_of_contents.pages.len(),
                            pages.len(),
                            page_aliases.len()
                        ));
                    };
                    let page = pages.get_mut(&page_index);
                    let page_to_update = if let Some(page) = page {
                        page
                    } else {
                        let page = parse_page::<IndexPage<T>, INNER_PAGE_SIZE>(&mut self.index_file, page_index.into())
                            .await?;
                        pages.insert(page_index, page);
                        pages
                            .get_mut(&page_index)
                            .expect("should be available as was just inserted before")
                    };
                    let canonical_page_key = aliased_page_key.as_ref().unwrap_or(&event_page_key);
                    if page_to_update.inner.node_id.key != canonical_page_key.0
                        || page_to_update.inner.node_id.link != canonical_page_key.1
                    {
                        return Err(eyre!(
                            "index split found a buffered page with a mismatched identity (page={page_index:?})"
                        ));
                    }
                    let pre_split_page_key = aliased_page_key.unwrap_or_else(|| event_page_key.clone());
                    let splitted_page = page_to_update.inner.split(*split_index);

                    let new_page_id = if let Some(id) = self.table_of_contents.pop_empty_page_id() {
                        id
                    } else {
                        self.next_page_id.fetch_add(1, Ordering::Relaxed).into()
                    };

                    let left_page_key = (
                        page_to_update.inner.node_id.key.clone(),
                        page_to_update.inner.node_id.link,
                    );
                    if !self
                        .table_of_contents
                        .try_update_key(&pre_split_page_key, left_page_key)?
                    {
                        return Err(eyre!(
                            "index split identity is absent from the table of contents (page={page_index:?})"
                        ));
                    }
                    let right_page_key = (splitted_page.node_id.key.clone(), splitted_page.node_id.link);
                    self.table_of_contents.try_insert(right_page_key.clone(), new_page_id)?;
                    if self.table_of_contents.get(&right_page_key) != Some(new_page_id) {
                        return Err(eyre!(
                            "index split identity did not become canonical (page={new_page_id:?})"
                        ));
                    }
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
                    page_aliases.replace(new_page_id, right_page_key, event_page_key, pre_split_page_key)?;
                }
            }
        }

        self.table_of_contents.persist(&mut self.index_file).await?;
        persist_pages_batch(pages.values().cloned().collect(), &mut self.index_file).await?;
        // The batch's last page write is a buffered `write_all`; flush so the
        // batch is visible to other handles once it reports done.
        self.index_file.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use data_bucket::{INNER_PAGE_SIZE, IndexPage, IndexValue, Persistable, get_index_page_size_from_data_length};

    #[test]
    fn test_size_measure() {
        let size = get_index_page_size_from_data_length::<u32>(INNER_PAGE_SIZE);
        let page = IndexPage::new(
            IndexValue {
                key: 0,
                link: Default::default(),
            },
            size,
        );
        assert!(page.as_bytes().as_ref().len() <= INNER_PAGE_SIZE)
    }
}
