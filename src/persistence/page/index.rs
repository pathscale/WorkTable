use std::sync::Arc;

use rkyv::{Archive, Deserialize, Serialize};
use scc::TreeIndex;

use crate::in_memory::data;
use crate::prelude::Guard;

#[derive(
    Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct IndexValue<T> {
    pub key: T,
    pub link: data::Link
}

#[derive(
    Archive, Clone, Deserialize, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct IndexPage<T> {
    pub  index_values: Vec<IndexValue<T>>
}

// pub fn map_tree_index<T>(index: Arc<TreeIndex<T, data::Link>>) -> Vec<IndexPage<T>>
// where T: Clone + Ord
// {
//     let guard = Guard::new();
//     let pages = vec![];
//     let values = vec![];
//     let mut current_page = IndexPage::default();
//     let mut current_size = 0;
//
//     for (key, &link) in index.iter(&guard) {
//         let index_value = IndexValue {
//             key: key.clone(),
//             link,
//         };
//
//
//     }
//
//     pages
// }