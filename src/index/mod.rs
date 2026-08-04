mod arctic;
mod available_index;
mod congee;
mod multipair;
mod persistent_art;
mod persistent_wti;
mod primary_index;
mod table_index;
mod table_secondary_index;
mod unique;
mod unsized_node;

pub use arctic::{ArcticIndex, ArcticKey};
pub use available_index::AvailableIndex;
pub use congee::{CongeeIndex, CongeeKey};
pub use indexset::concurrent::map::BTreeMap as IndexMap;
pub use indexset::concurrent::multimap::BTreeMultiMap as IndexMultiMap;
pub use multipair::MultiPairRecreate;
pub use persistent_art::{PersistentArcticIndex, PersistentArtIndex, PersistentCongeeIndex};
pub use persistent_wti::PersistentWtiIndex;
pub use primary_index::PrimaryIndex;
pub use table_index::{TableIndex, TableIndexCdc, convert_change_events, convert_upstream_change_events};
pub use table_secondary_index::{
    IndexError, TableSecondaryIndex, TableSecondaryIndexCdc, TableSecondaryIndexEventsOps, TableSecondaryIndexInfo,
};
pub use unique::{UniqueIndex, UpstreamIndexMap, UpstreamIndexPair};
pub use unsized_node::UnsizedNode;

#[derive(Clone, Debug)]
pub struct Difference<AvailableTypes> {
    pub old: AvailableTypes,
    pub new: AvailableTypes,
}
