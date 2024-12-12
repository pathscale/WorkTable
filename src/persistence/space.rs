use std::ops::RangeBounds;
use crate::TableIndex;

pub trait SpaceIndex<Index, K, V>
where Index: TableIndex<K, V>{
    fn save_index(&self, index: &Index) -> eyre::Result<()>;
    fn load_index(&self, index: &Index) -> eyre::Result<()>;
    fn save_index_range<R: RangeBounds<K>>(&self, index: &Index, range: R) -> eyre::Result<()>;
    fn load_index_range<R: RangeBounds<K>>(&self, index: &Index, range: R) -> eyre::Result<()>;
}