use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable::worktable;

mod bulk_load_stall;
mod concurrent;
mod duplicate_key_index_reload;
mod failure;
mod index_page;
mod loaded_index_growth;
mod read;
mod schema;
mod space_index;
mod sync;
mod toc;
mod torn_shutdown;
mod tuple_primary_key;
mod vacuum;

#[cfg(feature = "s3-support")]
mod s3;

worktable! (
    name: TestPersist,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        another: u64,
    },
    indexes: {
        another_idx: another,
    },
    queries: {
        update: {
            AnotherById(another) by id,
        },
        delete: {
             ByAnother() by another,
        }
    }
);

worktable! (
    name: TestWithoutSecondaryIndexes,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        another: u64,
    },
);

worktable!(
    name: SizeTest,
    columns: {
        id: u32 primary_key,
        number: u64,
    }
);
