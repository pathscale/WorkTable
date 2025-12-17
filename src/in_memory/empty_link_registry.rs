use crate::in_memory::DATA_INNER_LENGTH;
use data_bucket::Link;
use indexset::concurrent::multimap::BTreeMultiMap;
use indexset::concurrent::set::BTreeSet;
use parking_lot::FairMutex;

/// A link wrapper that implements `Ord` based on absolute index calculation.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord)]
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
        Some(self.absolute_index().cmp(&other.absolute_index()))
    }
}

#[derive(Debug)]
pub struct EmptyLinkRegistry<const DATA_LENGTH: usize = DATA_INNER_LENGTH> {
    index_ord_links: BTreeSet<IndexOrdLink<DATA_LENGTH>>,
    length_ord_links: BTreeMultiMap<u32, Link>,
    op_lock: FairMutex<()>,
}

impl<const DATA_LENGTH: usize> Default for EmptyLinkRegistry<DATA_LENGTH> {
    fn default() -> Self {
        Self {
            index_ord_links: BTreeSet::new(),
            length_ord_links: BTreeMultiMap::new(),
            op_lock: Default::default(),
        }
    }
}

impl<const DATA_LENGTH: usize> EmptyLinkRegistry<DATA_LENGTH> {
    pub fn push(&self, link: Link) {
        let mut index_ord_link = IndexOrdLink(link.clone());
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
                    self.index_ord_links.remove(&possible_left_neighbor);
                    self.length_ord_links
                        .remove(&possible_left_neighbor.0.length, &possible_left_neighbor.0);

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
                    self.index_ord_links.remove(&possible_right_neighbor);
                    self.length_ord_links.remove(
                        &possible_right_neighbor.0.length,
                        &possible_right_neighbor.0,
                    );

                    index_ord_link = united_link;
                }
            }
        }

        self.index_ord_links.insert(index_ord_link);
        self.length_ord_links
            .insert(index_ord_link.0.length, index_ord_link.0);
    }

    pub fn pop_max(&self) -> Option<Link> {
        let _g = self.op_lock.lock();

        let mut iter = self.length_ord_links.iter().rev();
        let (len, max_length_link) = iter.next()?;
        let index_ord_link = IndexOrdLink(*max_length_link);
        drop(iter);

        self.length_ord_links.remove(len, max_length_link);
        self.index_ord_links.remove(&index_ord_link);

        Some(index_ord_link.0)
    }

    pub fn len(&self) -> usize {
        self.index_ord_links.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = Link> + '_ {
        self.index_ord_links.iter().map(|l| l.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_link_registry_insert_and_pop() {
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

        let link3 = Link {
            page_id: 2.into(),
            offset: 0,
            length: 200,
        };

        registry.push(link1.clone());
        registry.push(link2.clone());
        registry.push(link3.clone());

        // After inserting link1 and link2, they should be united
        let united_link = Link {
            page_id: 1.into(),
            offset: 0,
            length: 250,
        };

        assert_eq!(registry.pop_max(), Some(united_link));
        assert_eq!(registry.pop_max(), Some(link3));
        assert_eq!(registry.pop_max(), None);
    }
}
