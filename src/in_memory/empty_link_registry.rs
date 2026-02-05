use std::sync::atomic::{AtomicU32, Ordering};

use data_bucket::Link;
use data_bucket::page::PageId;
use derive_more::Into;
use indexset::concurrent::multimap::BTreeMultiMap;
use indexset::concurrent::set::BTreeSet;
use parking_lot::FairMutex;

use crate::in_memory::DATA_INNER_LENGTH;

/// A link wrapper that implements `Ord` based on absolute index calculation.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Into)]
pub struct IndexOrdLink<const DATA_LENGTH: usize = DATA_INNER_LENGTH>(pub Link);

impl<const DATA_LENGTH: usize> IndexOrdLink<DATA_LENGTH> {
    /// Calculates the absolute index of the link.
    fn absolute_index(&self) -> u64 {
        let page_id: u32 = self.0.page_id.into();
        (page_id as u64 * DATA_LENGTH as u64) + self.0.offset as u64
    }

    fn unite_with_right_neighbor(&self, other: &Self) -> Option<Self> {
        let self_end = self.absolute_index() + self.0.length as u64;
        let other_start = other.absolute_index();

        if self.0.page_id != other.0.page_id {
            return None;
        }

        if self_end == other_start {
            let new_length = self.0.length + other.0.length;
            Some(IndexOrdLink(Link {
                page_id: self.0.page_id,
                offset: self.0.offset,
                length: new_length,
            }))
        } else {
            None
        }
    }

    fn unite_with_left_neighbor(&self, other: &Self) -> Option<Self> {
        let other_end = other.absolute_index() + other.0.length as u64;
        let self_start = self.absolute_index();

        if self.0.page_id != other.0.page_id {
            return None;
        }

        if other_end == self_start {
            let new_offset = other.0.offset;
            let new_length = self.0.length + other.0.length;
            Some(IndexOrdLink(Link {
                page_id: other.0.page_id,
                offset: new_offset,
                length: new_length,
            }))
        } else {
            None
        }
    }
}

impl<const DATA_LENGTH: usize> PartialOrd for IndexOrdLink<DATA_LENGTH> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<const DATA_LENGTH: usize> Ord for IndexOrdLink<DATA_LENGTH> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.absolute_index().cmp(&other.absolute_index())
    }
}

#[derive(Debug)]
pub struct EmptyLinkRegistry<const DATA_LENGTH: usize = DATA_INNER_LENGTH> {
    index_ord_links: BTreeSet<IndexOrdLink<DATA_LENGTH>>,
    length_ord_links: BTreeMultiMap<u32, Link>,

    pub(crate) page_links_map: BTreeMultiMap<PageId, Link>,

    sum_links_len: AtomicU32,

    pub(crate) op_lock: FairMutex<()>,
    vacuum_lock: tokio::sync::Mutex<()>,
}

impl<const DATA_LENGTH: usize> Default for EmptyLinkRegistry<DATA_LENGTH> {
    fn default() -> Self {
        Self {
            index_ord_links: BTreeSet::new(),
            length_ord_links: BTreeMultiMap::new(),
            page_links_map: BTreeMultiMap::new(),
            sum_links_len: Default::default(),
            op_lock: Default::default(),
            vacuum_lock: Default::default(),
        }
    }
}

impl<const DATA_LENGTH: usize> EmptyLinkRegistry<DATA_LENGTH> {
    pub fn remove_link<L: Into<Link>>(&self, link: L) {
        let link = link.into();
        self.index_ord_links.remove(&IndexOrdLink(link));
        self.length_ord_links.remove(&link.length, &link);
        self.page_links_map.remove(&link.page_id, &link);

        self.sum_links_len.fetch_sub(link.length, Ordering::AcqRel);
    }

    fn insert_link<L: Into<Link>>(&self, link: L) {
        let link = link.into();
        self.index_ord_links.insert(IndexOrdLink(link));
        self.length_ord_links.insert(link.length, link);
        self.page_links_map.insert(link.page_id, link);

        self.sum_links_len.fetch_add(link.length, Ordering::AcqRel);
    }

    pub fn remove_link_for_page(&self, page_id: PageId) {
        let _g = self.op_lock.lock();
        let links = self
            .page_links_map
            .get(&page_id)
            .map(|(_, l)| *l)
            .collect::<Vec<_>>();
        for l in links {
            self.remove_link(l);
        }
    }

    pub fn push(&self, link: Link) {
        let mut index_ord_link = IndexOrdLink(link);
        let _g = self.op_lock.lock();

        {
            let mut iter = self.index_ord_links.range(..index_ord_link).rev();
            if let Some(possible_left_neighbor) = iter.next() {
                let possible_left_neighbor = *possible_left_neighbor;
                if let Some(united_link) =
                    index_ord_link.unite_with_left_neighbor(&possible_left_neighbor)
                {
                    drop(iter);

                    // Remove left neighbor
                    self.remove_link(possible_left_neighbor);

                    index_ord_link = united_link;
                }
            }
        }

        {
            let mut iter = self.index_ord_links.range(index_ord_link..);
            if let Some(possible_right_neighbor) = iter.next() {
                let possible_right_neighbor = *possible_right_neighbor;
                if let Some(united_link) =
                    index_ord_link.unite_with_right_neighbor(&possible_right_neighbor)
                {
                    drop(iter);

                    // Remove right neighbor
                    self.remove_link(possible_right_neighbor);

                    index_ord_link = united_link;
                }
            }
        }

        self.insert_link(index_ord_link);
    }

    pub fn pop_max(&self) -> Option<Link> {
        if self.vacuum_lock.try_lock().is_err() {
            return None;
        }

        let _g = self.op_lock.lock();

        let mut iter = self.length_ord_links.iter().rev();
        let (_, max_length_link) = iter.next()?;
        drop(iter);

        self.remove_link(*max_length_link);

        Some(*max_length_link)
    }

    pub fn iter(&self) -> impl Iterator<Item = Link> + '_ {
        self.index_ord_links.iter().map(|l| l.0)
    }

    pub fn get_empty_links_size_bytes(&self) -> u32 {
        self.sum_links_len.load(Ordering::Acquire)
    }

    pub async fn lock_vacuum(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.vacuum_lock.lock().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unite_with_right_neighbor() {
        let left = IndexOrdLink::<DATA_INNER_LENGTH>(Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        });

        let right = IndexOrdLink::<DATA_INNER_LENGTH>(Link {
            page_id: 1.into(),
            offset: 100,
            length: 50,
        });

        let united = left.unite_with_right_neighbor(&right).unwrap();
        assert_eq!(united.0.page_id, 1.into());
        assert_eq!(united.0.offset, 0);
        assert_eq!(united.0.length, 150);
    }

    #[test]
    fn test_unite_with_left_neighbor() {
        let left = IndexOrdLink::<DATA_INNER_LENGTH>(Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        });

        let right = IndexOrdLink::<DATA_INNER_LENGTH>(Link {
            page_id: 1.into(),
            offset: 100,
            length: 50,
        });

        let united = right.unite_with_left_neighbor(&left).unwrap();
        assert_eq!(united.0.page_id, 1.into());
        assert_eq!(united.0.offset, 0);
        assert_eq!(united.0.length, 150);
    }

    #[test]
    fn test_unite_fails_on_gap() {
        let link1 = IndexOrdLink::<DATA_INNER_LENGTH>(Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        });

        let link2 = IndexOrdLink::<DATA_INNER_LENGTH>(Link {
            page_id: 1.into(),
            offset: 200,
            length: 50,
        });

        assert!(link1.unite_with_right_neighbor(&link2).is_none());
        assert!(link2.unite_with_left_neighbor(&link1).is_none());
    }

    #[test]
    fn test_unite_fails_on_different_pages() {
        let link1 = IndexOrdLink::<DATA_INNER_LENGTH>(Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        });

        let link2 = IndexOrdLink::<DATA_INNER_LENGTH>(Link {
            page_id: 2.into(),
            offset: 100,
            length: 50,
        });

        assert!(link1.unite_with_right_neighbor(&link2).is_none());
        assert!(link2.unite_with_left_neighbor(&link1).is_none());
    }

    #[test]
    fn test_index_ord_link_ordering() {
        const TEST_DATA_LENGTH: usize = 1000;

        let link1 = IndexOrdLink::<TEST_DATA_LENGTH>(Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        });

        let link2 = IndexOrdLink::<TEST_DATA_LENGTH>(Link {
            page_id: 1.into(),
            offset: 100,
            length: 50,
        });

        let link3 = IndexOrdLink::<TEST_DATA_LENGTH>(Link {
            page_id: 2.into(),
            offset: 0,
            length: 200,
        });

        assert!(link1 < link2);
        assert!(link2 < link3);
        assert!(link1 < link3);
    }

    #[test]
    fn test_push_merges_both_sides() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        let left = Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        };

        let middle = Link {
            page_id: 1.into(),
            offset: 100,
            length: 50,
        };

        let right = Link {
            page_id: 1.into(),
            offset: 150,
            length: 75,
        };

        registry.push(left);
        registry.push(right);
        registry.push(middle);

        let result = registry.pop_max().unwrap();
        assert_eq!(result.page_id, 1.into());
        assert_eq!(result.offset, 0);
        assert_eq!(result.length, 225);
    }

    #[test]
    fn test_push_non_adjacent_no_merge() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        let link1 = Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        };

        let link2 = Link {
            page_id: 1.into(),
            offset: 200,
            length: 50,
        };

        registry.push(link1);
        registry.push(link2);

        let pop1 = registry.pop_max().unwrap();
        let pop2 = registry.pop_max().unwrap();

        assert_eq!(pop1.length, 100);
        assert_eq!(pop2.length, 50);
    }

    #[test]
    fn test_pop_max_returns_largest() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        let small = Link {
            page_id: 1.into(),
            offset: 0,
            length: 50,
        };

        let large = Link {
            page_id: 1.into(),
            offset: 100,
            length: 200,
        };

        let medium = Link {
            page_id: 1.into(),
            offset: 300,
            length: 100,
        };

        registry.push(small);
        registry.push(large);
        registry.push(medium);

        assert_eq!(registry.pop_max().unwrap().length, 300); // two links were united
        assert_eq!(registry.pop_max().unwrap().length, 50);
    }

    #[test]
    fn test_iter_returns_all_links() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        let link1 = Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        };

        let link2 = Link {
            page_id: 2.into(),
            offset: 0,
            length: 150,
        };

        let link3 = Link {
            page_id: 3.into(),
            offset: 0,
            length: 200,
        };

        registry.push(link1);
        registry.push(link2);
        registry.push(link3);

        let links: Vec<Link> = registry.iter().collect();
        assert_eq!(links.len(), 3);
    }

    #[test]
    fn test_empty_registry() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        assert_eq!(registry.pop_max(), None);
        assert_eq!(registry.iter().count(), 0);
    }

    #[test]
    fn test_sum_links_counter() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        let link1 = Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        };

        let link2 = Link {
            page_id: 1.into(),
            offset: 100,
            length: 150,
        };

        registry.push(link1);
        assert_eq!(registry.sum_links_len.load(Ordering::Acquire), 100);

        registry.push(link2);
        assert_eq!(registry.sum_links_len.load(Ordering::Acquire), 250);

        registry.pop_max();
        assert_eq!(registry.sum_links_len.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn test_lock_vacuum_prevents_pop() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        let link = Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        };

        registry.push(link);

        let popped = registry.pop_max();
        assert!(popped.is_some());
        assert_eq!(popped.unwrap().length, 100);

        registry.push(Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        });

        let _lock = registry.lock_vacuum().await;
        let popped_locked = registry.pop_max();
        assert!(
            popped_locked.is_none(),
            "pop_max should return None when vacuum lock is held"
        );

        drop(_lock);
        let popped_after_unlock = registry.pop_max();
        assert!(
            popped_after_unlock.is_some(),
            "pop_max should return link after vacuum lock is released"
        );
        assert_eq!(popped_after_unlock.unwrap().length, 100);
    }
}
