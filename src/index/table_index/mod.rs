use std::fmt::Debug;
use std::hash::Hash;

use data_bucket::Link;
use indexset::core::multipair::MultiPair;
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;

use crate::util::OffsetEqLink;
use crate::{IndexMap, IndexMultiMap};

mod cdc;
pub mod util;

pub use cdc::TableIndexCdc;
pub use util::convert_change_events;

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
        self.remove(value, &OffsetEqLink(link))
            .map(|(v, l)| (v, l.0))
    }
}

impl<T, Node> TableIndex<T> for IndexMap<T, OffsetEqLink, Node>
where
    T: Debug + Eq + Hash + Clone + Send + Ord,
    Node: NodeLike<Pair<T, OffsetEqLink>> + Send + 'static,
{
    fn insert(&self, value: T, link: Link) -> Option<Link> {
        self.insert(value, OffsetEqLink(link)).map(|l| l.0)
    }

    fn insert_checked(&self, value: T, link: Link) -> Option<()> {
        self.checked_insert(value, OffsetEqLink(link))
    }

    fn remove(&self, value: &T, _: Link) -> Option<(T, Link)> {
        self.remove(value).map(|(v, l)| (v, l.0))
    }
}
