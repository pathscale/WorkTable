use std::hash::Hash;

use data_bucket::Link;
use derive_more::From;

mod table_secondary_index;

pub use indexset::concurrent::map::BTreeMap as IndexMap;
pub use indexset::concurrent::multimap::BTreeMultiMap as IndexMultiMap;
pub use table_secondary_index::{TableSecondaryIndex, TableSecondaryIndexCdc};

#[derive(Debug)]
pub struct Difference<AvailableTypes> {
    pub old: AvailableTypes,
    pub new: AvailableTypes,
}

pub trait TableIndex<T> {
    fn insert(&self, value: T, link: Link) -> Option<Link>;
    fn remove(&self, value: T, link: Link) -> Option<(T, Link)>;
}

impl<T> TableIndex<T> for IndexMultiMap<T, Link>
where
    T: Eq + Hash + Clone + Send + Ord,
{
    fn insert(&self, value: T, link: Link) -> Option<Link> {
        self.insert(value, link)
    }

    fn remove(&self, value: T, link: Link) -> Option<(T, Link)> {
        self.remove(&value, &link)
    }
}

impl<T> TableIndex<T> for IndexMap<T, Link>
where
    T: Eq + Hash + Clone + Send + Ord,
{
    fn insert(&self, value: T, link: Link) -> Option<Link> {
        self.insert(value, link)
    }

    fn remove(&self, value: T, _: Link) -> Option<(T, Link)> {
        self.remove(&value)
    }
}
