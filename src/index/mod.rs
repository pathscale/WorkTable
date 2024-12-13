use std::sync::Arc;

use data_bucket::{Link, SizeMeasurable};
use scc::TreeIndex;

use crate::prelude::LockFreeSet;

mod table_index;
mod table_secondary_index;

pub use table_index::{IndexSet, KeyValue, MeasuredTreeIndex, TableIndex};
pub use table_secondary_index::TableSecondaryIndex;

pub enum IndexType<'a, T>
where
    T: Clone + Ord + Send + Sync + 'static + SizeMeasurable,
{
    Unique(&'a MeasuredTreeIndex<TreeIndex<T, Link>, T, Link>),
    NonUnique(
        &'a MeasuredTreeIndex<TreeIndex<T, Arc<LockFreeSet<Link>>>, T, Arc<LockFreeSet<Link>>>,
    ),
}
