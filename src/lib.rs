pub mod in_memory;
mod index;
pub mod lock;
mod primary_key;
mod row;
mod table;
pub use data_bucket;
mod database;
mod persistence;

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
    pub use crate::in_memory::{ArchivedRow, Data, DataPages, RowWrapper, StorableRow};
    pub use crate::lock::LockMap;
    pub use crate::persistence::{IndexTableOfContents, SpaceData, SpaceDataOps, SpaceIndex};
    pub use crate::primary_key::{PrimaryKeyGenerator, PrimaryKeyGeneratorState, TablePrimaryKey};
    pub use crate::table::select::{
        Order, SelectQueryBuilder, SelectQueryExecutor, SelectResult, SelectResultExecutor,
    };
    pub use crate::{
        lock::Lock, IndexMap, IndexMultiMap, TableRow, TableSecondaryIndex, WorkTable,
        WorkTableError,
    };
    pub use data_bucket::{
        align, get_index_page_size_from_data_length, map_data_pages_to_general,
        map_index_pages_to_general, parse_data_page, parse_page, persist_page, seek_to_page_start,
        update_at, DataPage, GeneralHeader, GeneralPage, IndexPage, Interval, Link, PageType,
        Persistable, PersistableIndex, SizeMeasurable, SizeMeasure, SpaceInfoPage, DATA_VERSION,
        GENERAL_HEADER_SIZE, INNER_PAGE_SIZE, PAGE_SIZE,
    };

    pub use derive_more::{From, Into};
    pub use lockfree::set::Set as LockFreeSet;
    pub use scc::{ebr::Guard, tree_index::TreeIndex};
    pub use worktable_codegen::{PersistIndex, PersistTable};
}
