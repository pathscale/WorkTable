use std::marker::PhantomData;

use derive_more::From;

mod table_secondary_index;

pub use indexset::concurrent::map::BTreeMap as IndexMap;
pub use indexset::concurrent::multimap::BTreeMultiMap as IndexMultiMap;
pub use table_secondary_index::TableSecondaryIndex;

// #[derive(Debug, From)]
// pub enum IndexType<'a, Index, K, V>
// where
//     Index: TableIndex<K, V>,
// {
//     Primary(&'a Index),
//     Secondary(&'a Index, String),
//     Phantom(PhantomData<(K, V)>),
// }
