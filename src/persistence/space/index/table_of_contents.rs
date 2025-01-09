use std::fs::File;
use std::hash::Hash;

use data_bucket::page::PageId;
use data_bucket::{parse_page, SizeMeasurable, TableOfContentsPage};
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::{rancor, Archive, Deserialize, Serialize};

#[derive(Debug)]
pub struct IndexTableOfContents<T, const DATA_LENGTH: u32> {
    current_page: usize,
    table_of_contents_pages: Vec<TableOfContentsPage<T>>,
}

impl<T, const DATA_LENGTH: u32> Default for IndexTableOfContents<T, DATA_LENGTH> {
    fn default() -> Self {
        Self {
            current_page: 0,
            table_of_contents_pages: vec![TableOfContentsPage::default()],
        }
    }
}

impl<T, const DATA_LENGTH: u32> IndexTableOfContents<T, DATA_LENGTH> {
    pub fn parse_from_file(file: &mut File) -> eyre::Result<Self>
    where
        T: Archive + Hash + Eq,
        <T as Archive>::Archived: Hash + Eq + Deserialize<T, Strategy<Pool, rancor::Error>>,
    {
        let first_page = parse_page::<TableOfContentsPage<T>, DATA_LENGTH>(file, 1);
        if let Ok(page) = first_page {
            if page.inner.is_last() {
                Ok(Self {
                    current_page: 0,
                    table_of_contents_pages: vec![page.inner],
                })
            } else {
                let mut table_of_contents_pages = vec![page.inner];
                let mut index = 2;
                let mut ind = false;

                while !ind {
                    let page = parse_page::<TableOfContentsPage<T>, DATA_LENGTH>(file, index)?;
                    ind = page.inner.is_last();
                    table_of_contents_pages.push(page.inner);
                    index += 1;
                }

                Ok(Self {
                    current_page: 0,
                    table_of_contents_pages,
                })
            }
        } else {
            Ok(Self::default())
        }
    }

    fn get_current_page_mut(&mut self) -> &mut TableOfContentsPage<T> {
        &mut self.table_of_contents_pages[self.current_page]
    }

    pub fn insert(&mut self, node_id: T, page_id: PageId)
    where
        T: Clone + Hash + Eq + SizeMeasurable,
    {
        let page = self.get_current_page_mut();
        page.insert(node_id.clone(), page_id);
        if page.estimated_size() > DATA_LENGTH as usize {
            page.remove(&node_id);
            if !page.is_last() {
                page.mark_not_last();
                self.table_of_contents_pages
                    .push(TableOfContentsPage::default());
                self.current_page += 1;

                let page = self.get_current_page_mut();
                page.insert(node_id.clone(), page_id);
            }
        }
    }

    pub fn remove(&mut self, node_id: &T)
    where
        T: Clone + Hash + Eq + SizeMeasurable,
    {
        let mut removed = false;
        let mut i = 0;
        while !removed {
            let mut page = &mut self.table_of_contents_pages[i];
            if page.contains(node_id) {
                page.remove(node_id);
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

    #[test]
    fn empty() {
        let toc = IndexTableOfContents::<u8, 128>::default();
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
        let mut toc = IndexTableOfContents::<u8, 128>::default();
        let key = 1;
        toc.insert(key, 1.into());

        let page = toc.table_of_contents_pages[toc.current_page].clone();
        assert!(
            page.contains(&key),
            "`page` not contains value {}, keys are {:?}",
            key,
            page.into_iter().collect::<Vec<_>>()
        );
        assert!(
            page.estimated_size() > 0,
            "`estimated_size` is zero, but it shouldn't"
        );
    }

    #[test]
    fn insert_more_than_one_page() {
        let mut toc = IndexTableOfContents::<u8, 20>::default();
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
            for (k, i) in page.into_iter() {
                let pos = keys.binary_search(&k).expect("value should exist");
                keys.remove(pos);
            }
        }

        assert!(keys.is_empty(), "Some keys was not inserted: {:?}", keys)
    }
}
