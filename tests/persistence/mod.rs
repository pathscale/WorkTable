use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable::worktable;

mod concurrent;
mod failure;
mod index_page;
mod read;
mod space_index;
mod sync;
mod toc;

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

pub async fn get_empty_test_wt() -> TestPersistWorkTable {
    let config =
        DiskConfig::new_with_table_name("tests/data", TestPersistWorkTable::name_snake_case(), TestPersistWorkTable::version());
    let engine = TestPersistPersistenceEngine::new(config).await.unwrap();
    TestPersistWorkTable::new(engine).await.unwrap()
}

pub async fn get_test_wt() -> TestPersistWorkTable {
    let table = get_empty_test_wt().await;

    for i in 1..100 {
        let row = TestPersistRow { another: i, id: i };
        table.insert(row).unwrap();
    }

    table
}
