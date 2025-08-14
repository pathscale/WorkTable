use data_bucket::{DefaultSizeMeasurable, Link};

use crate::IndexMultiMap;
use crate::in_memory::RowLength;
use crate::in_memory::empty_links_registry::EmptyLinksRegistry;

#[derive(Debug, Default)]
pub struct UnsizedEmptyLinkRegistry(IndexMultiMap<u32, Link>);

impl EmptyLinksRegistry for UnsizedEmptyLinkRegistry {
    fn add_empty_link(&self, link: Link) {
        self.0.insert(link.length, link);
    }

    fn find_link_with_length(&self, size: u32) -> Option<Link> {
        if let Some(link) = self.0.remove_max().map(|(_, l)| l) {
            if link.length < size + RowLength::default_aligned_size() as u32 {
                self.0.insert(link.length, link);
                None
            } else {
                Some(link)
            }
        } else {
            None
        }
    }

    fn as_vec(&self) -> Vec<Link> {
        self.0.drain().into_iter().map(|p| p.value).collect()
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}
