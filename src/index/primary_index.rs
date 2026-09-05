//! Primary-key to row-location index.

use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

use data_bucket::Link;
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;

use crate::util::OffsetEqLink;
use crate::{IndexMap, TableIndex, TableIndexCdc, UniqueIndex};

/// Primary-key to physical-row mapping.
///
/// Vacuum groups a transient snapshot of this map by page. Keeping a second
/// link-to-key index here would duplicate every key for the table's lifetime
/// solely to accelerate an occasional maintenance pass.
#[derive(Debug)]
pub struct PrimaryIndex<PrimaryKey, const DATA_LENGTH: usize, PkMap = IndexMap<PrimaryKey, OffsetEqLink<DATA_LENGTH>>>
where
    PrimaryKey: Clone + Ord + Send + 'static + Hash,
    PkMap: UniqueIndex<PrimaryKey, OffsetEqLink<DATA_LENGTH>>,
{
    pub pk_map: PkMap,
    marker: PhantomData<fn() -> PrimaryKey>,
}

impl<PrimaryKey, const DATA_LENGTH: usize, PkMap> PrimaryIndex<PrimaryKey, DATA_LENGTH, PkMap>
where
    PrimaryKey: Clone + Ord + Send + 'static + Hash,
    PkMap: UniqueIndex<PrimaryKey, OffsetEqLink<DATA_LENGTH>>,
{
    pub fn from_map(pk_map: PkMap) -> Self {
        Self {
            pk_map,
            marker: PhantomData,
        }
    }
}

impl<PrimaryKey, const DATA_LENGTH: usize, PkMap> Default for PrimaryIndex<PrimaryKey, DATA_LENGTH, PkMap>
where
    PrimaryKey: Clone + Ord + Send + 'static + Hash,
    PkMap: UniqueIndex<PrimaryKey, OffsetEqLink<DATA_LENGTH>>,
{
    fn default() -> Self {
        Self::from_map(PkMap::default())
    }
}

impl<PrimaryKey, const DATA_LENGTH: usize, PkMap> TableIndex<PrimaryKey>
    for PrimaryIndex<PrimaryKey, DATA_LENGTH, PkMap>
where
    PrimaryKey: Debug + Eq + Hash + Clone + Send + Ord + 'static,
    PkMap: UniqueIndex<PrimaryKey, OffsetEqLink<DATA_LENGTH>>,
{
    fn insert(&self, value: PrimaryKey, link: Link) -> Option<Link> {
        self.pk_map.insert_value(value, OffsetEqLink(link)).map(|old| old.0)
    }

    fn insert_checked(&self, value: PrimaryKey, link: Link) -> Option<()> {
        self.pk_map.insert_value_checked(value, OffsetEqLink(link))
    }

    fn remove(&self, value: &PrimaryKey, _: Link) -> Option<(PrimaryKey, Link)> {
        let (_, old_link) = self.pk_map.remove_value(value)?;
        Some((value.clone(), old_link.0))
    }
}

impl<PrimaryKey, const DATA_LENGTH: usize, PkMap> TableIndexCdc<PrimaryKey>
    for PrimaryIndex<PrimaryKey, DATA_LENGTH, PkMap>
where
    PrimaryKey: Debug + Eq + Hash + Clone + Send + Ord + 'static,
    PkMap: UniqueIndex<PrimaryKey, OffsetEqLink<DATA_LENGTH>> + TableIndexCdc<PrimaryKey>,
{
    fn insert_cdc(&self, value: PrimaryKey, link: Link) -> (Option<Link>, Vec<ChangeEvent<Pair<PrimaryKey, Link>>>) {
        TableIndexCdc::insert_cdc(&self.pk_map, value, link)
    }

    fn insert_checked_cdc(&self, value: PrimaryKey, link: Link) -> Option<Vec<ChangeEvent<Pair<PrimaryKey, Link>>>> {
        TableIndexCdc::insert_checked_cdc(&self.pk_map, value, link)
    }

    fn remove_cdc(
        &self,
        value: PrimaryKey,
        link: Link,
    ) -> (Option<(PrimaryKey, Link)>, Vec<ChangeEvent<Pair<PrimaryKey, Link>>>) {
        TableIndexCdc::remove_cdc(&self.pk_map, value, link)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_bucket::page::PageId;

    const TEST_DATA_LENGTH: usize = 4096;
    type TestPrimaryIndex = PrimaryIndex<u64, TEST_DATA_LENGTH>;

    fn link(page: u32, offset: u32) -> Link {
        Link {
            page_id: PageId::from(page),
            offset,
            length: 16,
        }
    }

    #[test]
    fn default_is_empty() {
        assert_eq!(TestPrimaryIndex::default().pk_map.len(), 0);
    }

    #[test]
    fn insert_replaces_the_forward_location() {
        let index = TestPrimaryIndex::default();
        let first = link(1, 0);
        let second = link(2, 16);

        assert_eq!(index.insert(42, first), None);
        assert_eq!(index.insert(42, second), Some(first));
        assert_eq!(index.pk_map.get_value(&42), Some(OffsetEqLink(second)));
    }

    #[test]
    fn checked_insert_rejects_an_existing_key() {
        let index = TestPrimaryIndex::default();
        let first = link(1, 0);

        assert_eq!(index.insert_checked(42, first), Some(()));
        assert_eq!(index.insert_checked(42, link(2, 0)), None);
        assert_eq!(index.pk_map.get_value(&42), Some(OffsetEqLink(first)));
    }

    #[test]
    fn remove_returns_the_indexed_location() {
        let index = TestPrimaryIndex::default();
        let row_link = link(1, 0);
        index.insert(42, row_link);

        assert_eq!(index.remove(&42, row_link), Some((42, row_link)));
        assert_eq!(index.pk_map.get_value(&42), None);
    }
}
