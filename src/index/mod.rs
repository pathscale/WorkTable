use std::marker::PhantomData;

use derive_more::From;

mod table_index;
mod table_secondary_index;

pub use table_index::{IndexSet, KeyValue, LockFreeMap, LockedHashMap, TableIndex};
pub use table_secondary_index::TableSecondaryIndex;

#[derive(Debug, From)]
pub enum IndexType<'a, Index, K, V>
where
    Index: TableIndex<K, V>,
{
    Primary(&'a Index),
    Secondary(&'a Index, String),
    Phantom(PhantomData<(K, V)>),
}
