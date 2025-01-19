use std::fs::File;
use std::hash::Hash;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use data_bucket::page::PageId;
use data_bucket::{
    parse_page, persist_page, GeneralHeader, GeneralPage, PageType, SizeMeasurable, SpaceId,
    TableOfContentsPage,
};
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{rancor, Archive, Deserialize, Serialize};

#[derive(Debug)]
pub struct IndexTableOfContents<T, const DATA_LENGTH: u32> {
    current_page: usize,
    next_page_id: Arc<AtomicU32>,
    table_of_contents_pages: Vec<GeneralPage<TableOfContentsPage<T>>>,
}

impl<T, const DATA_LENGTH: u32> IndexTableOfContents<T, DATA_LENGTH>
where
    T: SizeMeasurable,
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
            table_of_contents_pages: vec![page],
        }
    }

    pub fn persist(&mut self, file: &mut File) -> eyre::Result<()>
    where
        T: Archive
            + Hash
            + Eq
            + Clone
            + SizeMeasurable
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
        <T as Archive>::Archived: Hash + Eq,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>> + Hash + Eq,
    {
        for page in &mut self.table_of_contents_pages {
            persist_page(page, file)?;
        }

        Ok(())
    }

    pub fn parse_from_file(
        file: &mut File,
        space_id: SpaceId,
        next_page_id: Arc<AtomicU32>,
    ) -> eyre::Result<Self>
    where
        T: Archive
            + Hash
            + Eq
            + Clone
            + SizeMeasurable
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
        <T as Archive>::Archived: Hash + Eq,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>> + Hash + Eq,
    {
        let first_page = parse_page::<TableOfContentsPage<T>, DATA_LENGTH>(file, 1);
        if let Ok(page) = first_page {
            if page.inner.is_last() {
                Ok(Self {
                    current_page: 0,
                    next_page_id,
                    table_of_contents_pages: vec![page],
                })
            } else {
                let mut table_of_contents_pages = vec![page];
                let mut index = 2;
                let mut ind = false;

                while !ind {
                    let page = parse_page::<TableOfContentsPage<T>, DATA_LENGTH>(file, index)?;
                    ind = page.inner.is_last();
                    table_of_contents_pages.push(page);
                    index += 1;
                }

                Ok(Self {
                    current_page: 0,
                    next_page_id,
                    table_of_contents_pages,
                })
            }
        } else {
            Ok(Self::new(space_id, next_page_id))
        }
    }

    fn get_current_page_mut(&mut self) -> &mut GeneralPage<TableOfContentsPage<T>> {
        &mut self.table_of_contents_pages[self.current_page]
    }

    pub fn insert(&mut self, node_id: T, page_id: PageId)
    where
        T: Clone + Hash + Eq + SizeMeasurable,
    {
        let next_page_id = self.next_page_id.clone();

        let page = self.get_current_page_mut();
        page.inner.insert(node_id.clone(), page_id);
        if page.inner.estimated_size() > DATA_LENGTH as usize {
            page.inner.remove(&node_id);
            if page.inner.is_last() {
                let next_page_id = next_page_id.fetch_add(1, Ordering::Relaxed);
                let header = page.header.follow_with_page_id(next_page_id.into());
                page.inner.mark_not_last(next_page_id.into());
                self.table_of_contents_pages.push(GeneralPage {
                    header,
                    inner: TableOfContentsPage::default(),
                });
                self.current_page += 1;

                let page = self.get_current_page_mut();
                page.inner.insert(node_id.clone(), page_id);
            }
        }
    }

    pub fn get(&self, node_id: &T) -> Option<PageId>
    where
        T: Hash + Eq,
    {
        for page in &self.table_of_contents_pages {
            if page.inner.contains(node_id) {
                return Some(
                    page.inner
                        .get(node_id)
                        .expect("should exist as checked in `contains`"),
                );
            }
        }

        None
    }

    pub fn remove(&mut self, node_id: &T)
    where
        T: Clone + Hash + Eq + SizeMeasurable,
    {
        let mut removed = false;
        let mut i = 0;
        while !removed {
            let mut page = &mut self.table_of_contents_pages[i];
            if page.inner.contains(node_id) {
                page.inner.remove(node_id);
                removed = true;
            }
            i += 1;
            if self.table_of_contents_pages.len() == i {
                removed = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::persistence::space::index::table_of_contents::IndexTableOfContents;
    use std::sync::atomic::AtomicU32;
    use std::sync::Arc;

    #[test]
    fn empty() {
        let toc = IndexTableOfContents::<u8, 128>::new(0.into(), Arc::new(AtomicU32::new(0)));
        assert_eq!(
            toc.current_page, 0,
            "`current_page` is not set to 0, it is {}",
            toc.current_page
        );
        assert_eq!(
            toc.table_of_contents_pages.len(),
            1,
            "`table_of_contents_pages` is empty"
        )
    }

    #[test]
    fn insert_to_empty() {
        let mut toc = IndexTableOfContents::<u8, 128>::new(0.into(), Arc::new(AtomicU32::new(0)));
        let key = 1;
        toc.insert(key, 1.into());

        let page = toc.table_of_contents_pages[toc.current_page].clone();
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
            let page = toc.table_of_contents_pages[i].clone();
            for (k, i) in page.inner.into_iter() {
                let pos = keys.binary_search(&k).expect("value should exist");
                keys.remove(pos);
            }
        }

        assert!(keys.is_empty(), "Some keys was not inserted: {:?}", keys)
    }
}
