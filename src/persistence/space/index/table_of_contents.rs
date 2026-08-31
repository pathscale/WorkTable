use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use data_bucket::page::PageId;
use data_bucket::{
    GeneralHeader, GeneralPage, PageType, SizeMeasurable, SpaceId, TableOfContentsPage, parse_page, persist_page,
};
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::Serializer;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize, rancor};
use tokio::fs::File;

#[derive(Debug)]
pub struct IndexTableOfContents<T: Ord + Eq, const DATA_LENGTH: u32> {
    current_page: usize,
    next_page_id: Arc<AtomicU32>,
    pub pages: Vec<GeneralPage<TableOfContentsPage<T>>>,
}

impl<T, const DATA_LENGTH: u32> IndexTableOfContents<T, DATA_LENGTH>
where
    T: Debug + SizeMeasurable + Ord + Eq,
{
    pub fn new(space_id: SpaceId, next_page_id: Arc<AtomicU32>) -> Self {
        let page_id = next_page_id.fetch_add(1, Ordering::Relaxed);
        let header = GeneralHeader::new(page_id.into(), PageType::IndexTableOfContents, space_id);
        let page = GeneralPage {
            header,
            inner: TableOfContentsPage::default(),
        };
        Self {
            current_page: 0,
            next_page_id,
            pages: vec![page],
        }
    }

    pub fn get(&self, node_id: &T) -> Option<PageId> {
        for page in &self.pages {
            if page.inner.contains(node_id) {
                return Some(page.inner.get(node_id).expect("should exist as checked in `contains`"));
            }
        }

        None
    }

    fn get_current_page_mut(&mut self) -> &mut GeneralPage<TableOfContentsPage<T>> {
        &mut self.pages[self.current_page]
    }

    /// Inserts a fresh page identity into the table of contents.
    ///
    /// This restores source compatibility with the public infallible API that
    /// preceded PR #63. A fully constructed table of contents owns its complete
    /// segment chain, so failure here is an internal invariant violation and
    /// retains the historical panic-level contract. Loaded persistence paths,
    /// where a truncated chain can be reported to the caller, must use
    /// [`Self::try_insert`] instead.
    pub fn insert(&mut self, node_id: T, page_id: PageId)
    where
        T: Clone + SizeMeasurable,
    {
        self.try_insert(node_id, page_id)
            .expect("table-of-contents chain should be fully loaded");
    }

    /// Fallible variant of [`Self::insert`] for persistence paths.
    pub fn try_insert(&mut self, node_id: T, page_id: PageId) -> eyre::Result<()>
    where
        T: Clone + SizeMeasurable,
    {
        let next_page_id = self.next_page_id.clone();
        let entry_size = (node_id.clone(), page_id).aligned_size();

        loop {
            let page = &mut self.pages[self.current_page];
            if page.inner.estimated_size() + entry_size <= DATA_LENGTH as usize {
                page.inner.insert(node_id, page_id);
                return Ok(());
            }

            if !page.header.next_id.is_empty() {
                let next = self.current_page + 1;
                if next >= self.pages.len() {
                    return Err(eyre::eyre!(
                        "table-of-contents segment {} links past the loaded chain of {} segments",
                        self.current_page,
                        self.pages.len()
                    ));
                }
                self.current_page = next;
                continue;
            }

            let next_page_id = next_page_id.fetch_add(1, Ordering::Relaxed);
            let header = page.header.follow_with_page_id(next_page_id.into());
            page.header.next_id = next_page_id.into();
            let mut next_page = GeneralPage {
                header,
                inner: TableOfContentsPage::default(),
            };
            // Preserve the old behavior for an entry larger than one segment:
            // it gets its own page rather than creating pages forever.
            next_page.inner.insert(node_id, page_id);
            self.pages.push(next_page);
            self.current_page = self.pages.len() - 1;
            return Ok(());
        }
    }

    pub fn remove(&mut self, node_id: &T)
    where
        T: Clone + SizeMeasurable,
    {
        let mut removed = false;
        let mut i = 0;
        while !removed {
            let page = &mut self.pages[i];
            if page.inner.contains(node_id) {
                page.inner.remove(node_id);
                self.current_page = i;
                removed = true;
            }
            i += 1;
            if self.pages.len() == i {
                removed = true;
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&T, &PageId)> {
        self.pages.iter().flat_map(|v| v.inner.iter())
    }

    pub fn update_key(&mut self, old_key: &T, new_key: T)
    where
        T: Clone + Debug,
    {
        assert!(
            self.try_update_key(old_key, new_key),
            "Page with key {old_key:?} not found"
        );
    }

    /// Updates a page identity without panicking when the old identity is
    /// absent. Batch replay uses this checked form because an absent key is a
    /// persistence invariant failure that must surface through `Result`.
    pub fn try_update_key(&mut self, old_key: &T, new_key: T) -> bool
    where
        T: Clone,
    {
        let page = self.get_current_page_mut();
        if page.inner.update_key(old_key, new_key.clone()).is_none() {
            for page in self.pages.iter_mut() {
                if page.inner.update_key(old_key, new_key.clone()).is_some() {
                    return true;
                }
            }
            false
        } else {
            true
        }
    }

    pub fn pop_empty_page_id(&mut self) -> Option<PageId> {
        let page = self.get_current_page_mut();
        page.inner.pop_empty_page()
    }

    pub async fn persist(&mut self, file: &mut File) -> eyre::Result<()>
    where
        T: Archive
            + Clone
            + SizeMeasurable
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>
            + Send
            + Sync,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>
            + Ord
            + Eq
            + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rancor::Error>>,
    {
        for page in &mut self.pages {
            persist_page(page, file).await?;
        }

        Ok(())
    }

    pub async fn parse_from_file(file: &mut File, space_id: SpaceId, next_page_id: Arc<AtomicU32>) -> eyre::Result<Self>
    where
        T: Archive
            + Clone
            + SizeMeasurable
            + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>
            + Ord
            + Eq
            + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rancor::Error>>,
    {
        let first_page = parse_page::<TableOfContentsPage<T>, DATA_LENGTH>(file, 1).await;
        let page = match first_page {
            Ok(page) => page,
            Err(error) => {
                // Only a genuinely fresh space may fall back to an empty table
                // of contents: right after bootstrap the file holds nothing
                // beyond page 0, so page 1 simply does not exist yet. If the
                // file extends into page 1's slot, the parse failure means a
                // torn or truncated table of contents, and silently starting
                // empty would discard the whole index.
                let file_length = file.metadata().await?.len();
                if file_length <= data_bucket::PAGE_SIZE as u64 {
                    return Ok(Self::new(space_id, next_page_id));
                }
                return Err(error.wrap_err(format!(
                    "table of contents page 1 failed to parse in a {file_length}-byte index file that should contain it"
                )));
            }
        };
        {
            if page.header.next_id.is_empty() {
                Ok(Self {
                    current_page: 0,
                    next_page_id,
                    pages: vec![page],
                })
            } else {
                let mut table_of_contents_pages = vec![page];
                let mut index = table_of_contents_pages[0].header.next_id.into();
                let mut ind = false;

                while !ind {
                    let page = parse_page::<TableOfContentsPage<T>, DATA_LENGTH>(file, index).await?;
                    ind = page.header.next_id.is_empty();
                    index = page.header.next_id.into();
                    table_of_contents_pages.push(page);
                }

                Ok(Self {
                    current_page: 0,
                    next_page_id,
                    pages: table_of_contents_pages,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::persistence::space::index::table_of_contents::IndexTableOfContents;
    use data_bucket::page::PageId;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn empty() {
        let toc = IndexTableOfContents::<u8, 128>::new(0.into(), Arc::new(AtomicU32::new(0)));
        assert_eq!(
            toc.current_page, 0,
            "`current_page` is not set to 0, it is {}",
            toc.current_page
        );
        assert_eq!(toc.pages.len(), 1, "`table_of_contents_pages` is empty")
    }

    #[test]
    fn insert_to_empty() {
        let mut toc = IndexTableOfContents::<u8, 128>::new(0.into(), Arc::new(AtomicU32::new(0)));
        let key = 1;
        toc.insert(key, 1.into());

        let page = toc.pages[toc.current_page].clone();
        assert!(
            page.inner.contains(&key),
            "`page` not contains value {}, keys are {:?}",
            key,
            page.inner.into_iter().collect::<Vec<_>>()
        );
        assert!(
            page.inner.estimated_size() > 0,
            "`estimated_size` is zero, but it shouldn't"
        );
    }

    #[test]
    fn checked_update_reports_a_missing_identity_without_mutating_the_toc() {
        let mut toc = IndexTableOfContents::<u8, 128>::new(0.into(), Arc::new(AtomicU32::new(1)));
        toc.insert(7, 2.into());

        assert!(!toc.try_update_key(&8, 9));
        assert_eq!(toc.get(&7), Some(2.into()));
        assert_eq!(toc.get(&9), None);
    }

    #[test]
    fn insert_more_than_one_page() {
        let mut toc = IndexTableOfContents::<u8, 20>::new(0.into(), Arc::new(AtomicU32::new(0)));
        let mut keys = vec![];
        for key in 0..10 {
            toc.insert(key, 1.into());
            keys.push(key);
        }

        assert!(
            toc.current_page > 0,
            "`current_page` not moved forward and is {}",
            toc.current_page,
        );

        for i in 0..toc.current_page + 1 {
            let page = toc.pages[i].clone();
            for (k, _) in page.inner.into_iter() {
                let pos = keys.binary_search(&k).expect("value should exist");
                keys.remove(pos);
            }
        }

        assert!(keys.is_empty(), "Some keys was not inserted: {keys:?}")
    }

    #[test]
    fn insert_reaches_existing_tail_after_reload_resets_cursor() {
        let mut toc = IndexTableOfContents::<u8, 20>::new(0.into(), Arc::new(AtomicU32::new(0)));
        for key in 0..10 {
            toc.insert(key, u32::from(key).into());
        }
        assert!(toc.pages.len() > 1, "fixture must span TOC pages");

        // `parse_from_file` starts at the first TOC segment. When that segment
        // is already full and has a successor, a new page identity must carry
        // forward until an existing or newly-created tail can accept it.
        toc.current_page = 0;
        let before_sizes: Vec<_> = toc.pages.iter().map(|page| page.inner.estimated_size()).collect();
        toc.insert(200, PageId::from(200));

        assert_eq!(toc.get(&200), Some(PageId::from(200)));
        for (page, before_size) in toc.pages.iter().zip(before_sizes) {
            if page.inner.contains(&200) {
                continue;
            }
            assert_eq!(
                page.inner.estimated_size(),
                before_size,
                "probing a full TOC segment must not change its persisted size"
            );
        }
    }

    #[test]
    fn insert_reports_a_truncated_segment_chain() {
        let mut toc = IndexTableOfContents::<u8, 20>::new(0.into(), Arc::new(AtomicU32::new(0)));
        for key in 0..10 {
            toc.insert(key, u32::from(key).into());
        }
        assert!(!toc.pages[0].header.next_id.is_empty());
        toc.pages.truncate(1);
        toc.current_page = 0;

        let error = toc.try_insert(200, PageId::from(200)).unwrap_err();

        assert!(error.to_string().contains("links past the loaded chain"));
    }

    #[test]
    fn reinsert_on_empty_space() {
        let mut toc = IndexTableOfContents::<u8, 20>::new(0.into(), Arc::new(AtomicU32::new(0)));
        let mut keys = vec![];
        for key in 0..10 {
            toc.insert(key, 1.into());
            keys.push(key);
        }

        assert!(
            toc.current_page > 0,
            "`current_page` not moved forward and is {}",
            toc.current_page,
        );
        let before_remove_current_page = toc.current_page;

        let key_to_remove = keys[5];
        toc.remove(&key_to_remove);
        assert!(
            before_remove_current_page > toc.current_page,
            "`current_page` not moved backwards on remove and is still {}",
            toc.current_page,
        );
        assert_eq!(
            toc.get_current_page_mut().inner.clone().pop_empty_page(),
            Some(1.into()),
            "Current page not contains any empty page",
        );
        let after_remove_current_page = toc.current_page;

        let new_key = keys.last().unwrap() + 1;
        let id = toc.pop_empty_page_id().unwrap();
        let before_insert_segments = toc.pages.len();
        toc.insert(new_key, id);
        assert_eq!(toc.get(&new_key), Some(id), "reused page id was not recorded");
        assert_eq!(
            toc.pages.len(),
            before_insert_segments + 1,
            "the fixture's full successor chain should append one segment"
        );
        assert_eq!(
            toc.current_page,
            toc.pages.len() - 1,
            "the cursor should name the segment that accepted the identity"
        );
        assert_eq!(
            toc.pages[after_remove_current_page].inner.clone().pop_empty_page(),
            None,
            "After insertion page contains empty page {:?}, but shouldn't",
            toc.pages[after_remove_current_page].inner.clone().pop_empty_page(),
        );
    }
}
