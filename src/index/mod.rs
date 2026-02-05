mod available_index;
mod multipair;
mod primary_index;
mod table_index;
mod table_secondary_index;
mod unsized_node;

pub use available_index::AvailableIndex;
pub use indexset::concurrent::map::BTreeMap as IndexMap;
pub use indexset::concurrent::multimap::BTreeMultiMap as IndexMultiMap;
pub use multipair::MultiPairRecreate;
pub use primary_index::PrimaryIndex;
pub use table_index::{TableIndex, TableIndexCdc, convert_change_events};
pub use table_secondary_index::{
    IndexError, TableSecondaryIndex, TableSecondaryIndexCdc, TableSecondaryIndexEventsOps,
    TableSecondaryIndexInfo,
};
pub use unsized_node::UnsizedNode;

#[derive(Clone, Debug)]
pub struct Difference<AvailableTypes> {
    pub old: AvailableTypes,
    pub new: AvailableTypes,
}
