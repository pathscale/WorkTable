//! A link wrapper with equality based on absolute position.
//!
//! [`OffsetEqLink`] wraps a [`Link`] and implements `Eq`, `Ord`, and `Hash`
//! based on its absolute index within the data pages, rather than on the
//! raw `Link` fields.

use data_bucket::{Link, SizeMeasurable};
use derive_more::From;

use crate::in_memory::DATA_INNER_LENGTH;
use crate::prelude::Into;

/// A link wrapper that implements `Eq` based on absolute index.
#[derive(Copy, Clone, Debug, Default, Into, From)]
pub struct OffsetEqLink<const DATA_LENGTH: usize = DATA_INNER_LENGTH>(pub Link);

impl<const DATA_LENGTH: usize> OffsetEqLink<DATA_LENGTH> {
    /// Calculates the absolute index of the link.
    fn absolute_index(&self) -> u64 {
        let page_id: u32 = self.0.page_id.into();
        (page_id as u64 * DATA_LENGTH as u64) + self.0.offset as u64
    }
}

impl<const DATA_LENGTH: usize> std::hash::Hash for OffsetEqLink<DATA_LENGTH> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.absolute_index().hash(state);
    }
}

impl<const DATA_LENGTH: usize> PartialOrd for OffsetEqLink<DATA_LENGTH> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<const DATA_LENGTH: usize> Ord for OffsetEqLink<DATA_LENGTH> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.absolute_index().cmp(&other.absolute_index())
    }
}

impl<const DATA_LENGTH: usize> PartialEq for OffsetEqLink<DATA_LENGTH> {
    fn eq(&self, other: &Self) -> bool {
        self.absolute_index().eq(&other.absolute_index())
    }
}

impl<const DATA_LENGTH: usize> Eq for OffsetEqLink<DATA_LENGTH> {}

impl<const DATA_LENGTH: usize> std::ops::Deref for OffsetEqLink<DATA_LENGTH> {
    type Target = Link;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const DATA_LENGTH: usize> AsRef<Link> for OffsetEqLink<DATA_LENGTH> {
    fn as_ref(&self) -> &Link {
        &self.0
    }
}

impl<const DATA_LENGTH: usize> PartialEq<Link> for OffsetEqLink<DATA_LENGTH> {
    fn eq(&self, other: &Link) -> bool {
        self.0.eq(other)
    }
}

impl<const DATA_LENGTH: usize> SizeMeasurable for OffsetEqLink<DATA_LENGTH> {
    fn aligned_size(&self) -> usize {
        self.0.aligned_size()
    }

    fn align() -> Option<usize> {
        Link::align()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_bucket::page::PageId;
    use std::collections::HashSet;

    const TEST_DATA_LENGTH: usize = 4096;

    #[test]
    fn test_same_position_different_length_are_equal() {
        let link1 = OffsetEqLink::<TEST_DATA_LENGTH>(Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        });
        let link2 = OffsetEqLink::<TEST_DATA_LENGTH>(Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 100,
        });

        assert_eq!(link1, link2);
    }

    #[test]
    fn test_different_page_not_equal() {
        let link1 = OffsetEqLink::<TEST_DATA_LENGTH>(Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        });
        let link2 = OffsetEqLink::<TEST_DATA_LENGTH>(Link {
            page_id: PageId::from(2),
            offset: 100,
            length: 50,
        });

        assert_ne!(link1, link2);
    }

    #[test]
    fn test_different_offset_not_equal() {
        let link1 = OffsetEqLink::<TEST_DATA_LENGTH>(Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        });
        let link2 = OffsetEqLink::<TEST_DATA_LENGTH>(Link {
            page_id: PageId::from(1),
            offset: 200,
            length: 50,
        });

        assert_ne!(link1, link2);
    }

    #[test]
    fn test_ordering_same_page() {
        let link1 = OffsetEqLink::<TEST_DATA_LENGTH>(Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        });
        let link2 = OffsetEqLink::<TEST_DATA_LENGTH>(Link {
            page_id: PageId::from(1),
            offset: 200,
            length: 50,
        });

        assert!(link1 < link2);
    }

    #[test]
    fn test_ordering_different_pages() {
        let link1 = OffsetEqLink::<TEST_DATA_LENGTH>(Link {
            page_id: PageId::from(1),
            offset: 4000,
            length: 50,
        });
        let link2 = OffsetEqLink::<TEST_DATA_LENGTH>(Link {
            page_id: PageId::from(2),
            offset: 100,
            length: 50,
        });

        assert!(link1 < link2); // page 1 end < page 2 start
    }

    #[test]
    fn test_ordering_within_page_boundaries() {
        let link1 = OffsetEqLink::<TEST_DATA_LENGTH>(Link {
            page_id: PageId::from(0),
            offset: 0,
            length: 10,
        });
        let link2 = OffsetEqLink::<TEST_DATA_LENGTH>(Link {
            page_id: PageId::from(0),
            offset: TEST_DATA_LENGTH as u32 - 1,
            length: 10,
        });
        let link3 = OffsetEqLink::<TEST_DATA_LENGTH>(Link {
            page_id: PageId::from(1),
            offset: 0,
            length: 10,
        });

        assert!(link1 < link2);
        assert!(link2 < link3);
    }

    #[test]
    fn test_hash_consistent_with_equality() {
        let link1 = OffsetEqLink::<TEST_DATA_LENGTH>(Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        });
        let link2 = OffsetEqLink::<TEST_DATA_LENGTH>(Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 100,
        });
        let link3 = OffsetEqLink::<TEST_DATA_LENGTH>(Link {
            page_id: PageId::from(1),
            offset: 200,
            length: 50,
        });

        let mut set = HashSet::new();
        set.insert(link1);
        set.insert(link2);
        set.insert(link3);

        // link1 and link2 are equal, so only 2 elements in set
        assert_eq!(set.len(), 2);
    }
}
