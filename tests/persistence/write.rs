use std::sync::Arc;

use crate::check_if_files_are_same;

use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: Test,
    persist: true,
    columns: {
        id: u128 primary_key,
        another: u64,
    },
    indexes: {
        another_idx: another,
    },
);

#[test]
fn test_persist() {
    let manager = Arc::new(DatabaseManager {
        config_path: "tests/data".to_string(),
    });
    let table = TestWorkTable::new(manager);

    for i in 1..100 {
        let row = TestRow {
            another: i as u64,
            id: i,
        };
        table.insert(row).unwrap();
    }
    let mut space: TestSpace = table.into_space();
    // this call will save space file to `tests/db`. It will be `tests/data/test.wt`
    // TODO: How to config this? Maybe we will need to have DATABASE_CONFIG env
    space.persist().unwrap();

    assert!(check_if_files_are_same(
        "tests/data/test.wt".to_string(),
        "tests/data/expected/test_persist.wt".to_string()
    ))
}
