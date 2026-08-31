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

// Normal persistence batches begin at 16 source pages. Keep that common case
// inline while allowing analyzer retries to grow beyond it without turning a
// recovery batch into a terminal capacity error.
const INLINE_BATCH_ALIASED_PAGES: usize = 16;

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
    fn resolve_batch_page(
        &self,
        aliases: &PageAliases<T>,
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
        // Unsized index pages are tagged IndexUnsized everywhere: the batch
        // create path and the bulk-load mapping already did, while this single
        // path (and the batch split) wrote PageType::Index for the same page
        // layout, leaving the on-disk tag dependent on which code path
        // happened to create the page.
        let header = GeneralHeader::new(page_id, PageType::IndexUnsized, self.space_id);
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
                .update_key(&(node_id.key, node_id.value), (new_node_id.key, new_node_id.value))?;
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
        // At most two transitional identities per buffered page: the event's
        // identity and the page's actual pre-apply identity. Keeping both is
        // required when a split is followed by a remove/insert pair that still
        // names the pre-split maximum. Memory is bounded by pages, not events.
        let mut page_aliases = PageAliases::default();
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
                            "unsized index event references a missing page (toc_segments={}, buffered_pages={}, aliases={})",
                            self.table_of_contents.pages.len(),
                            pages.len(),
                            page_aliases.len()
                        ));
                    };
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
                                "unsized index page identity is absent from the table of contents (page={page_index:?}, toc_segments={})",
                                self.table_of_contents.pages.len()
                            ));
                        }
                        if self.table_of_contents.get(&updated_page_key) != Some(page_index) {
                            return Err(eyre!(
                                "unsized index page identity update did not become canonical (page={page_index:?})"
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

                    let page = UnsizedIndexPage::<T, INNER_PAGE_SIZE>::new(max_value.clone().into())?;
                    let header = GeneralHeader::new(page_id, PageType::IndexUnsized, self.space_id);
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
                            "unsized index split references a missing page (toc_segments={}, buffered_pages={}, aliases={})",
                            self.table_of_contents.pages.len(),
                            pages.len(),
                            page_aliases.len()
                        ));
                    };
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
                    let canonical_page_key = aliased_page_key.as_ref().unwrap_or(&event_page_key);
                    if page_to_update.inner.node_id.key != canonical_page_key.0
                        || page_to_update.inner.node_id.link != canonical_page_key.1
                    {
                        return Err(eyre!(
                            "unsized index split found a buffered page with a mismatched identity (page={page_index:?})"
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
                            "unsized index split identity is absent from the table of contents (page={page_index:?})"
                        ));
                    }
                    let right_page_key = (splitted_page.node_id.key.clone(), splitted_page.node_id.link);
                    self.table_of_contents.try_insert(right_page_key.clone(), new_page_id)?;
                    if self.table_of_contents.get(&right_page_key) != Some(new_page_id) {
                        return Err(eyre!(
                            "unsized index split identity did not become canonical (page={new_page_id:?})"
                        ));
                    }
                    let header = GeneralHeader::new(new_page_id, PageType::IndexUnsized, self.space_id);
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

/// Transitional event identities for pages whose canonical TOC key changed
/// earlier in the same CDC batch.
///
/// Each page owns at most the event identity and its actual pre-event identity.
/// The current canonical identity is retained alongside them so alias lookup
/// never has to predict page mutation semantics. Normal batches use the inline
/// slots; analyzer retries may spill into `overflow` without losing events.
struct PageAliases<T> {
    inline: [Option<PageAliasEntry<T>>; INLINE_BATCH_ALIASED_PAGES],
    overflow: Vec<PageAliasEntry<T>>,
}

struct PageAliasEntry<T> {
    page_id: PageId,
    current_key: (T, Link),
    aliases: [Option<(T, Link)>; 2],
}

impl<T> Default for PageAliases<T> {
    fn default() -> Self {
        Self {
            inline: std::array::from_fn(|_| None),
            overflow: Vec::new(),
        }
    }
}

impl<T: Eq> PageAliases<T> {
    fn entries(&self) -> impl Iterator<Item = &PageAliasEntry<T>> {
        self.inline.iter().flatten().chain(self.overflow.iter())
    }

    fn resolve(&self, key: &(T, Link)) -> Option<(PageId, &(T, Link))> {
        self.entries().find_map(|entry| {
            entry
                .aliases
                .iter()
                .flatten()
                .any(|alias| alias == key)
                .then_some((entry.page_id, &entry.current_key))
        })
    }

    #[cfg(test)]
    fn get(&self, key: &(T, Link)) -> Option<PageId> {
        self.resolve(key).map(|(page_id, _)| page_id)
    }

    fn len(&self) -> usize {
        self.entries().map(|entry| entry.aliases.iter().flatten().count()).sum()
    }

    #[cfg(test)]
    fn page_len(&self) -> usize {
        self.entries().count()
    }

    fn remove_page(&mut self, page_id: PageId) {
        if let Some(slot) = self
            .inline
            .iter_mut()
            .find(|slot| slot.as_ref().is_some_and(|entry| entry.page_id == page_id))
        {
            *slot = None;
        } else if let Some(index) = self.overflow.iter().position(|entry| entry.page_id == page_id) {
            self.overflow.swap_remove(index);
        }
    }

    fn replace(
        &mut self,
        page_id: PageId,
        current_key: (T, Link),
        event_key: (T, Link),
        pre_event_key: (T, Link),
    ) -> eyre::Result<()> {
        let first_alias = (event_key != current_key).then_some(event_key);
        let second_alias =
            (pre_event_key != current_key && first_alias.as_ref() != Some(&pre_event_key)).then_some(pre_event_key);

        if first_alias.is_none() && second_alias.is_none() {
            self.remove_page(page_id);
            return Ok(());
        }

        for alias in [first_alias.as_ref(), second_alias.as_ref()].into_iter().flatten() {
            if let Some(owner) = self
                .entries()
                .find(|entry| entry.page_id != page_id && entry.aliases.iter().flatten().any(|stored| stored == alias))
            {
                return Err(eyre!(
                    "page alias ownership collision between {:?} and {page_id:?}",
                    owner.page_id
                ));
            }
        }

        let entry = PageAliasEntry {
            page_id,
            current_key,
            aliases: [first_alias, second_alias],
        };
        if let Some(slot) = self
            .inline
            .iter_mut()
            .find(|slot| slot.as_ref().is_some_and(|stored| stored.page_id == page_id))
        {
            *slot = Some(entry);
        } else if let Some(slot) = self.overflow.iter_mut().find(|stored| stored.page_id == page_id) {
            *slot = entry;
        } else if let Some(slot) = self.inline.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(entry);
        } else {
            self.overflow.push(entry);
        }
        Ok(())
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
            let current = (format!("current-{revision}"), link(revision + 1_000));
            aliases
                .replace(
                    page_id,
                    current,
                    (format!("key-{revision}"), link(revision)),
                    (format!("key-{revision}"), link(revision)),
                )
                .unwrap();
        }

        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases.get(&("key-999".into(), link(999))), Some(page_id));
        assert_eq!(aliases.page_len(), 1);
    }

    #[test]
    fn canonical_only_transition_stores_no_alias_entry() {
        let page_id = PageId::from(7);
        let canonical = ("current".to_string(), link(1));
        let mut aliases = PageAliases::default();

        aliases
            .replace(page_id, canonical.clone(), canonical.clone(), canonical)
            .unwrap();

        assert_eq!(aliases.page_len(), 0);
        assert_eq!(aliases.len(), 0);
    }

    #[test]
    fn split_keeps_only_the_two_live_transitional_identities() {
        let old_page = PageId::from(3);
        let right_page = PageId::from(4);
        let mut aliases = PageAliases::default();
        aliases
            .replace(
                old_page,
                ("old-current".to_string(), link(9)),
                ("older".to_string(), link(1)),
                ("older".to_string(), link(1)),
            )
            .unwrap();
        aliases.remove_page(old_page);
        aliases
            .replace(
                right_page,
                ("right-current".to_string(), link(4)),
                ("event".to_string(), link(2)),
                ("pre-split".to_string(), link(3)),
            )
            .unwrap();

        assert_eq!(aliases.len(), 2);
        assert_eq!(aliases.page_len(), 1);
    }

    #[test]
    fn split_remove_insert_preserves_the_event_identity() {
        let right_page = PageId::from(4);
        let pre_split = ("pre-split".to_string(), link(1));
        let post_split = ("post-split".to_string(), link(2));
        let after_remove = ("after-remove".to_string(), link(3));
        let mut aliases = PageAliases::default();

        aliases
            .replace(right_page, post_split.clone(), pre_split.clone(), pre_split.clone())
            .unwrap();
        aliases
            .replace(right_page, after_remove.clone(), pre_split.clone(), post_split.clone())
            .unwrap();

        assert_eq!(aliases.get(&pre_split), Some(right_page));
        assert_eq!(aliases.get(&post_split), Some(right_page));
        assert_eq!(
            aliases.resolve(&pre_split).map(|(_, current)| current),
            Some(&after_remove)
        );
    }

    #[test]
    fn batches_beyond_inline_capacity_preserve_every_alias() {
        let mut aliases = PageAliases::default();
        let page_count = INLINE_BATCH_ALIASED_PAGES as u32 + 8;
        for page in 1..=page_count {
            aliases
                .replace(
                    PageId::from(page),
                    (format!("current-{page}"), link(page + 1_000)),
                    (format!("old-{page}"), link(page)),
                    (format!("old-{page}"), link(page)),
                )
                .unwrap();
        }
        assert_eq!(aliases.page_len(), page_count as usize);
        assert_eq!(aliases.overflow.len(), 8);
        assert_eq!(aliases.get(&("old-1".into(), link(1))), Some(PageId::from(1)));
        assert_eq!(
            aliases.get(&(format!("old-{page_count}"), link(page_count))),
            Some(PageId::from(page_count))
        );
    }

    #[test]
    fn alias_invariants_fail_without_corrupting_existing_ownership() {
        let first_page = PageId::from(1);
        let second_page = PageId::from(2);
        let shared = ("shared".to_string(), link(1));
        let mut aliases = PageAliases::default();
        aliases
            .replace(
                first_page,
                ("first-current".to_string(), link(9)),
                shared.clone(),
                shared.clone(),
            )
            .unwrap();

        assert!(
            aliases
                .replace(
                    second_page,
                    ("second-current".to_string(), link(10)),
                    shared.clone(),
                    shared.clone(),
                )
                .is_err()
        );
        assert_eq!(aliases.get(&shared), Some(first_page));
        assert_eq!(aliases.page_len(), 1);
    }
}
