//! Combined storage for primary and reverse indexes.
//!
//! [`PrimaryIndex`] keeps both the primary key index (PK → [`OffsetEqLink`])
//! and the reverse index ([`OffsetEqLink`] → PK) in sync.

use std::fmt::Debug;
use std::hash::Hash;

use data_bucket::Link;
use indexset::cdc::change::ChangeEvent;
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;

use crate::util::OffsetEqLink;
use crate::{IndexMap, TableIndex, TableIndexCdc, convert_change_events};

/// Combined storage for primary and reverse indexes.
///
/// Maintains bidirectional mapping between primary keys and their data locations:
/// - **Forward index**: `PrimaryKey` → [`OffsetEqLink`] (primary lookups)
/// - **Reverse index**: [`OffsetEqLink`] → `PrimaryKey` (vacuum, position queries)
#[derive(Debug)]
pub struct PrimaryIndex<
    PrimaryKey,
    const DATA_LENGTH: usize,
    PkNodeType = Vec<Pair<PrimaryKey, OffsetEqLink<DATA_LENGTH>>>,
> where
    PrimaryKey: Clone + Ord + Send + 'static + std::hash::Hash,
    PkNodeType: NodeLike<Pair<PrimaryKey, OffsetEqLink<DATA_LENGTH>>> + Send + 'static,
{
    pub pk_map: IndexMap<PrimaryKey, OffsetEqLink<DATA_LENGTH>, PkNodeType>,
    pub reverse_pk_map: IndexMap<OffsetEqLink<DATA_LENGTH>, PrimaryKey>,
}

impl<PrimaryKey, const DATA_LENGTH: usize, PkNodeType> Default
    for PrimaryIndex<PrimaryKey, DATA_LENGTH, PkNodeType>
where
    PrimaryKey: Clone + Ord + Send + 'static + std::hash::Hash,
    PkNodeType: NodeLike<Pair<PrimaryKey, OffsetEqLink<DATA_LENGTH>>> + Send + 'static,
{
    fn default() -> Self {
        Self {
            pk_map: IndexMap::default(),
            reverse_pk_map: IndexMap::default(),
        }
    }
}

impl<PrimaryKey, const DATA_LENGTH: usize, PkNodeType> TableIndex<PrimaryKey>
    for PrimaryIndex<PrimaryKey, DATA_LENGTH, PkNodeType>
where
    PrimaryKey: Debug + Eq + Hash + Clone + Send + Ord,
    PkNodeType: NodeLike<Pair<PrimaryKey, OffsetEqLink<DATA_LENGTH>>> + Send + 'static,
{
    fn insert(&self, value: PrimaryKey, link: Link) -> Option<Link> {
        let offset_link = OffsetEqLink(link);
        let old = self.pk_map.insert(value.clone(), offset_link);
        if let Some(old_link) = old {
            // Update reverse index
            self.reverse_pk_map.remove(&old_link);
        }
        self.reverse_pk_map.insert(offset_link, value);
        old.map(|l| l.0)
    }

    fn insert_checked(&self, value: PrimaryKey, link: Link) -> Option<()> {
        let offset_link = OffsetEqLink(link);
        self.pk_map.checked_insert(value.clone(), offset_link)?;
        self.reverse_pk_map.checked_insert(offset_link, value)?;
        Some(())
    }

    fn remove(&self, value: &PrimaryKey, _: Link) -> Option<(PrimaryKey, Link)> {
        let (_, old_link) = self.pk_map.remove(value)?;
        self.reverse_pk_map.remove(&old_link);
        Some((value.clone(), old_link.0))
    }
}

impl<PrimaryKey, const DATA_LENGTH: usize, PkNodeType> TableIndexCdc<PrimaryKey>
    for PrimaryIndex<PrimaryKey, DATA_LENGTH, PkNodeType>
where
    PrimaryKey: Debug + Eq + Hash + Clone + Send + Ord,
    PkNodeType: NodeLike<Pair<PrimaryKey, OffsetEqLink<DATA_LENGTH>>> + Send + 'static,
{
    fn insert_cdc(
        &self,
        value: PrimaryKey,
        link: Link,
    ) -> (Option<Link>, Vec<ChangeEvent<Pair<PrimaryKey, Link>>>) {
        let offset_link = OffsetEqLink(link);
        let (res, evs) = self.pk_map.insert_cdc(value.clone(), offset_link);
        let res_link = res.map(|l| l.0);
        if let Some(res) = res {
            self.reverse_pk_map.remove(&res);
        }
        self.reverse_pk_map.insert(offset_link, value);

        (res_link, convert_change_events(evs))
    }

    fn insert_checked_cdc(
        &self,
        value: PrimaryKey,
        link: Link,
    ) -> Option<Vec<ChangeEvent<Pair<PrimaryKey, Link>>>> {
        let offset_link = OffsetEqLink(link);
        let res = self.pk_map.checked_insert_cdc(value.clone(), offset_link);

        if let Some(evs) = res {
            self.reverse_pk_map.insert(offset_link, value);
            Some(convert_change_events(evs))
        } else {
            None
        }
    }

    fn remove_cdc(
        &self,
        value: PrimaryKey,
        _: Link,
    ) -> (
        Option<(PrimaryKey, Link)>,
        Vec<ChangeEvent<Pair<PrimaryKey, Link>>>,
    ) {
        let (res, evs) = self.pk_map.remove_cdc(&value);

        if let Some((pk, old_link)) = res {
            let offset_link = OffsetEqLink(old_link.0);
            self.reverse_pk_map.remove(&offset_link);
            (Some((pk, old_link.0)), convert_change_events(evs))
        } else {
            (None, convert_change_events(evs))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_bucket::page::PageId;

    const TEST_DATA_LENGTH: usize = 4096;

    type TestPrimaryIndex =
        PrimaryIndex<u64, { TEST_DATA_LENGTH }, Vec<Pair<u64, OffsetEqLink<TEST_DATA_LENGTH>>>>;

    #[test]
    fn test_default_creates_empty_indexes() {
        let index = TestPrimaryIndex::default();
        assert_eq!(index.pk_map.len(), 0);
        assert_eq!(index.reverse_pk_map.len(), 0);
    }

    #[test]
    fn test_insert_creates_bidirectional_mapping() {
        let index = TestPrimaryIndex::default();
        let link = Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        };

        index.insert(42, link);

        assert_eq!(index.pk_map.get(&42).map(|v| v.get().value.0), Some(link));
        assert_eq!(
            index
                .reverse_pk_map
                .get(&OffsetEqLink(link))
                .map(|v| v.get().value),
            Some(42)
        );
    }

    #[test]
    fn test_insert_returns_old_link_on_duplicate() {
        let index = TestPrimaryIndex::default();
        let link1 = Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        };
        let link2 = Link {
            page_id: PageId::from(2),
            offset: 200,
            length: 50,
        };

        index.insert(42, link1);
        let old = index.insert(42, link2);

        assert_eq!(old, Some(link1));
        assert_eq!(index.pk_map.get(&42).map(|v| v.get().value.0), Some(link2));
        assert_eq!(
            index
                .reverse_pk_map
                .get(&OffsetEqLink(link2))
                .map(|v| v.get().value),
            Some(42)
        );
        assert!(index.reverse_pk_map.get(&OffsetEqLink(link1)).is_none());
    }

    #[test]
    fn test_insert_checked_succeeds_on_new_key() {
        let index = TestPrimaryIndex::default();
        let link = Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        };

        let result = index.insert_checked(42, link);
        assert_eq!(result, Some(()));

        assert_eq!(index.pk_map.get(&42).map(|v| v.get().value.0), Some(link));
        assert_eq!(
            index
                .reverse_pk_map
                .get(&OffsetEqLink(link))
                .map(|v| v.get().value),
            Some(42)
        );
    }

    #[test]
    fn test_insert_checked_fails_on_duplicate() {
        let index = TestPrimaryIndex::default();
        let link1 = Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        };
        let link2 = Link {
            page_id: PageId::from(2),
            offset: 200,
            length: 50,
        };

        index.insert_checked(42, link1).unwrap();
        let result = index.insert_checked(42, link2);

        assert_eq!(result, None);
        assert_eq!(index.pk_map.get(&42).map(|v| v.get().value.0), Some(link1));
    }

    #[test]
    fn test_removing_existing_key() {
        let index = TestPrimaryIndex::default();
        let link = Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        };

        index.insert(42, link);
        let removed = index.remove(&42, link);

        assert_eq!(removed, Some((42, link)));
        assert!(index.pk_map.get(&42).is_none());
        assert!(index.reverse_pk_map.get(&OffsetEqLink(link)).is_none());
    }

    #[test]
    fn test_removing_nonexistent_key_returns_none() {
        let index = TestPrimaryIndex::default();
        let link = Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        };

        let removed = index.remove(&42, link);
        assert_eq!(removed, None);
    }

    #[test]
    fn test_insert_cdc_new_key() {
        let index = TestPrimaryIndex::default();
        let link = Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        };

        let (old_link, _events) = index.insert_cdc(42, link);

        assert_eq!(old_link, None);
        assert_eq!(index.pk_map.get(&42).map(|v| v.get().value.0), Some(link));
        assert_eq!(
            index
                .reverse_pk_map
                .get(&OffsetEqLink(link))
                .map(|v| v.get().value),
            Some(42)
        );
    }

    #[test]
    fn test_insert_cdc_existing_key() {
        let index = TestPrimaryIndex::default();
        let link1 = Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        };
        let link2 = Link {
            page_id: PageId::from(2),
            offset: 200,
            length: 50,
        };

        index.insert_cdc(42, link1);
        let (old_link, _events) = index.insert_cdc(42, link2);

        assert_eq!(old_link, Some(link1));
        assert_eq!(index.pk_map.get(&42).map(|v| v.get().value.0), Some(link2));
        assert!(index.reverse_pk_map.get(&OffsetEqLink(link1)).is_none());
        assert_eq!(
            index
                .reverse_pk_map
                .get(&OffsetEqLink(link2))
                .map(|v| v.get().value),
            Some(42)
        );
    }

    #[test]
    fn test_insert_checked_cdc_new_key() {
        let index = TestPrimaryIndex::default();
        let link = Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        };

        let events = index.insert_checked_cdc(42, link);

        assert!(events.is_some());
        assert_eq!(index.pk_map.get(&42).map(|v| v.get().value.0), Some(link));
    }

    #[test]
    fn test_insert_checked_cdc_existing_key() {
        let index = TestPrimaryIndex::default();
        let link1 = Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        };
        let link2 = Link {
            page_id: PageId::from(2),
            offset: 200,
            length: 50,
        };

        index.insert_checked_cdc(42, link1).unwrap();
        let events = index.insert_checked_cdc(42, link2);

        assert!(events.is_none());
        assert_eq!(index.pk_map.get(&42).map(|v| v.get().value.0), Some(link1));
    }

    #[test]
    fn test_remove_cdc_existing_key() {
        let index = TestPrimaryIndex::default();
        let link = Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        };

        index.insert_cdc(42, link);
        let (removed, _events) = index.remove_cdc(42, link);

        assert_eq!(removed, Some((42, link)));
        assert!(index.pk_map.get(&42).is_none());
        assert!(index.reverse_pk_map.get(&OffsetEqLink(link)).is_none());
    }

    #[test]
    fn test_remove_cdc_nonexistent_key() {
        let index = TestPrimaryIndex::default();
        let link = Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        };

        let (removed, _events) = index.remove_cdc(42, link);

        assert_eq!(removed, None);
    }

    #[test]
    fn test_multiple_keys_maintain_separate_mappings() {
        let index = TestPrimaryIndex::default();
        let link1 = Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        };
        let link2 = Link {
            page_id: PageId::from(2),
            offset: 200,
            length: 50,
        };
        let link3 = Link {
            page_id: PageId::from(3),
            offset: 300,
            length: 50,
        };

        index.insert(1, link1);
        index.insert(2, link2);
        index.insert(3, link3);

        assert_eq!(index.pk_map.get(&1).map(|v| v.get().value.0), Some(link1));
        assert_eq!(index.pk_map.get(&2).map(|v| v.get().value.0), Some(link2));
        assert_eq!(index.pk_map.get(&3).map(|v| v.get().value.0), Some(link3));

        assert_eq!(
            index
                .reverse_pk_map
                .get(&OffsetEqLink(link1))
                .map(|v| v.get().value),
            Some(1)
        );
        assert_eq!(
            index
                .reverse_pk_map
                .get(&OffsetEqLink(link2))
                .map(|v| v.get().value),
            Some(2)
        );
        assert_eq!(
            index
                .reverse_pk_map
                .get(&OffsetEqLink(link3))
                .map(|v| v.get().value),
            Some(3)
        );
    }

    #[test]
    fn test_reverse_lookup_by_link() {
        let index = TestPrimaryIndex::default();
        let link = Link {
            page_id: PageId::from(1),
            offset: 100,
            length: 50,
        };

        index.insert(42, link);

        let pk = index
            .reverse_pk_map
            .get(&OffsetEqLink(link))
            .map(|v| v.get().value);
        assert_eq!(pk, Some(42));
    }
}
