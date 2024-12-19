use crate::{IndexType, TableIndex};
use data_bucket::Link;
use std::ops::RangeBounds;

pub trait SpaceIndex<Index, K, V>
where
    Index: TableIndex<K, V>,
{
    fn save_index(&self, index: IndexType<Index, K, V>) -> eyre::Result<()>;
    fn load_index(&self, index: IndexType<Index, K, V>) -> eyre::Result<()>;
    fn save_index_range<R: RangeBounds<K>>(
        &self,
        index: IndexType<Index, K, V>,
        range: R,
    ) -> eyre::Result<()>;
    fn load_index_range<R: RangeBounds<K>>(
        &self,
        index: IndexType<Index, K, V>,
        range: R,
    ) -> eyre::Result<()>;
}

pub trait SpaceData {
    fn save_data(&mut self, link: Link, bytes: &[u8]) -> eyre::Result<()>;
}
