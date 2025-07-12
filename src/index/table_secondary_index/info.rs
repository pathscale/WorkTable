use crate::prelude::IndexInfo;

pub trait TableSecondaryIndexInfo {
    fn index_info(&self) -> Vec<IndexInfo>;
    fn is_empty(&self) -> bool;
}

impl TableSecondaryIndexInfo for () {
    fn index_info(&self) -> Vec<IndexInfo> {
        vec![]
    }

    fn is_empty(&self) -> bool {
        true
    }
}
