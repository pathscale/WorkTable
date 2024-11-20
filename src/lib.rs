pub mod in_memory;
mod index;
pub mod lock;
mod primary_key;
mod row;
mod table;
pub use data_bucket as persistence;
mod database;
pub mod util;

// mod ty;
// mod value;
//
// pub use column::*;
// pub use field::*;
pub use index::*;
pub use row::*;
pub use table::*;

pub use worktable_codegen::worktable;

pub mod prelude {
    pub use crate::database::DatabaseManager;
    pub use crate::in_memory::{ArchivedRow, RowWrapper, StorableRow};
    pub use crate::primary_key::{PrimaryKeyGenerator, TablePrimaryKey};
    use crate::table;
    pub use crate::{lock::Lock, TableIndex, TableRow, WorkTable, WorkTableError};
    pub use data_bucket::{
        align, map_data_pages_to_general, map_index_pages_to_general, map_tree_index,
        map_unique_tree_index, persist_page, DataPage, GeneralHeader, GeneralPage, IndexData,
        Interval, Link, PageType, PersistIndex, PersistTable, Persistable, PersistableIndex,
        SizeMeasurable, SizeMeasure, SpaceInfoData, INNER_PAGE_LENGTH, PAGE_SIZE, HEADER_LENGTH
    };
    pub use derive_more::{From, Into};
    pub use lockfree::set::Set as LockFreeSet;
    pub use scc::{ebr::Guard, tree_index::TreeIndex};
    pub use table::select::{
        Order, SelectQueryBuilder, SelectQueryExecutor, SelectResult, SelectResultExecutor,
    };
}
