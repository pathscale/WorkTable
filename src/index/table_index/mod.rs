use std::fmt::Debug;
use std::hash::Hash;

use data_bucket::Link;
use indexset::core::multipair::MultiPair;
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;
use vanilla_indexset::core::node::NodeLike as VanillaNodeLike;
use vanilla_indexset::core::pair::Pair as VanillaPair;

use crate::util::OffsetEqLink;
use crate::{
    ArcticIndex, ArcticKey, ArcticMultiIndex, CongeeIndex, CongeeKey, IndexMap, IndexMultiMap, PersistentArcticIndex,
    PersistentArcticMultiIndex, PersistentCongeeIndex, PersistentWtiIndex, UniqueIndex, UpstreamIndexMap,
};

mod cdc;
pub mod util;

pub use cdc::TableIndexCdc;
pub use util::{convert_change_events, convert_multi_change_events, convert_upstream_change_events};

pub trait TableIndex<T> {
    fn insert(&self, value: T, link: Link) -> Option<Link>;
    fn insert_checked(&self, value: T, link: Link) -> Option<()>;
    fn remove(&self, value: &T, link: Link) -> Option<(T, Link)>;
}

impl<T, Node> TableIndex<T> for IndexMultiMap<T, OffsetEqLink, Node>
where
    T: Debug + Eq + Hash + Clone + Send + Ord,
    Node: NodeLike<MultiPair<T, OffsetEqLink>> + Send + 'static,
{
    fn insert(&self, value: T, link: Link) -> Option<Link> {
        self.insert(value, OffsetEqLink(link)).map(|l| l.0)
    }

    fn insert_checked(&self, value: T, link: Link) -> Option<()> {
        if self.insert(value, OffsetEqLink(link)).is_some() {
            None
        } else {
            Some(())
        }
    }

    fn remove(&self, value: &T, link: Link) -> Option<(T, Link)> {
        self.remove(value, &OffsetEqLink(link)).map(|(v, l)| (v, l.0))
    }
}

#[inline]
fn unique_insert<T, I>(index: &I, value: T, link: Link) -> Option<Link>
where
    T: Debug + Eq + Hash + Clone + Send + Ord,
    I: UniqueIndex<T, OffsetEqLink>,
{
    index.insert_value(value, OffsetEqLink(link)).map(|l| l.0)
}

#[inline]
fn unique_insert_checked<T, I>(index: &I, value: T, link: Link) -> Option<()>
where
    T: Debug + Eq + Hash + Clone + Send + Ord,
    I: UniqueIndex<T, OffsetEqLink>,
{
    index.insert_value_checked(value, OffsetEqLink(link))
}

#[inline]
fn unique_remove<T, I>(index: &I, value: &T) -> Option<(T, Link)>
where
    T: Debug + Eq + Hash + Clone + Send + Ord,
    I: UniqueIndex<T, OffsetEqLink>,
{
    index.remove_value(value).map(|(v, l)| (v, l.0))
}

macro_rules! impl_unique_table_index {
    ($index:ty, [$($bound:tt)*]) => {
        impl<T> TableIndex<T> for $index
        where
            T: $($bound)*,
        {
            fn insert(&self, value: T, link: Link) -> Option<Link> {
                unique_insert(self, value, link)
            }

            fn insert_checked(&self, value: T, link: Link) -> Option<()> {
                unique_insert_checked(self, value, link)
            }

            fn remove(&self, value: &T, _: Link) -> Option<(T, Link)> {
                unique_remove(self, value)
            }
        }
    };
}

impl<T, Node> TableIndex<T> for IndexMap<T, OffsetEqLink, Node>
where
    T: Debug + Eq + Hash + Clone + Send + Ord,
    Node: NodeLike<Pair<T, OffsetEqLink>> + Send + 'static,
{
    fn insert(&self, value: T, link: Link) -> Option<Link> {
        unique_insert(self, value, link)
    }

    fn insert_checked(&self, value: T, link: Link) -> Option<()> {
        unique_insert_checked(self, value, link)
    }

    fn remove(&self, value: &T, _: Link) -> Option<(T, Link)> {
        unique_remove(self, value)
    }
}

impl<T, Node> TableIndex<T> for UpstreamIndexMap<T, OffsetEqLink, Node>
where
    T: Debug + Eq + Hash + Clone + Send + Ord,
    Node: VanillaNodeLike<VanillaPair<T, OffsetEqLink>> + Send + 'static,
{
    fn insert(&self, value: T, link: Link) -> Option<Link> {
        unique_insert(self, value, link)
    }

    fn insert_checked(&self, value: T, link: Link) -> Option<()> {
        unique_insert_checked(self, value, link)
    }

    fn remove(&self, value: &T, _: Link) -> Option<(T, Link)> {
        unique_remove(self, value)
    }
}

impl<T, Node> TableIndex<T> for PersistentWtiIndex<T, OffsetEqLink, Node>
where
    T: Debug + Eq + Hash + Clone + Send + Ord + 'static,
    Node: NodeLike<Pair<T, OffsetEqLink>> + Send + 'static,
{
    fn insert(&self, value: T, link: Link) -> Option<Link> {
        unique_insert(self, value, link)
    }

    fn insert_checked(&self, value: T, link: Link) -> Option<()> {
        unique_insert_checked(self, value, link)
    }

    fn remove(&self, value: &T, _: Link) -> Option<(T, Link)> {
        unique_remove(self, value)
    }
}

/// Multiset semantics, mirroring the WorkTablesIndex multimap: inserting is
/// always possible for a non-unique index, so `insert` reports no displaced
/// link and `insert_checked` always succeeds. `remove` drops exactly the
/// requested `(key, link)` pair.
impl<T> TableIndex<T> for ArcticMultiIndex<T, OffsetEqLink>
where
    T: ArcticKey + Eq + Hash,
{
    fn insert(&self, value: T, link: Link) -> Option<Link> {
        self.insert_pair(value, OffsetEqLink(link));
        None
    }

    fn insert_checked(&self, value: T, link: Link) -> Option<()> {
        self.insert_pair(value, OffsetEqLink(link));
        Some(())
    }

    fn remove(&self, value: &T, link: Link) -> Option<(T, Link)> {
        self.remove_pair(value, &OffsetEqLink(link))
            .map(|removed| (value.clone(), removed.0))
    }
}

/// Same multiset semantics as the memory-only adapter above; durable events
/// come from the separate `TableIndexCdc` implementation.
impl<T> TableIndex<T> for PersistentArcticMultiIndex<T, OffsetEqLink>
where
    T: ArcticKey + Eq + Hash,
{
    fn insert(&self, value: T, link: Link) -> Option<Link> {
        self.insert_pair(value, OffsetEqLink(link));
        None
    }

    fn insert_checked(&self, value: T, link: Link) -> Option<()> {
        self.insert_pair(value, OffsetEqLink(link));
        Some(())
    }

    fn remove(&self, value: &T, link: Link) -> Option<(T, Link)> {
        self.remove_pair(value, &OffsetEqLink(link))
            .map(|removed| (value.clone(), removed.0))
    }
}

impl_unique_table_index!(CongeeIndex<T, OffsetEqLink>, [CongeeKey + Eq + Hash]);
impl_unique_table_index!(ArcticIndex<T, OffsetEqLink>, [
    ArcticKey + Eq + Hash
]);
impl_unique_table_index!(PersistentCongeeIndex<T, OffsetEqLink>, [
    CongeeKey + Eq + Hash
]);
impl_unique_table_index!(PersistentArcticIndex<T, OffsetEqLink>, [
    ArcticKey + Eq + Hash
]);
