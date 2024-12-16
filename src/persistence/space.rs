use std::ops::RangeBounds;
use data_bucket::Link;
use crate::{IndexType, TableIndex};

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
    fn save_data(&self, link: Link, bytes: &[u8]);
}
