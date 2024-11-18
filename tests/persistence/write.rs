use std::sync::Arc;

use worktable::worktable;
use worktable::prelude::*;
use uuid::Uuid;

worktable! (
    name: Test,
    persist: true,
    columns: {
        id: Uuid primary_key,
        another: i64,
    },
    indexes: {
        another_idx: another,
    },
);

#[test]
fn test_persist () {
    let manager = Arc::new(DatabaseManager {
        config_path: "tests/data".to_string()
    });
    let table = TestWorkTable::new(manager);

    for i in 1..100 {
        let row = TestRow {
            another: i,
            id: Uuid::new_v4(),
        };
        table.insert(row).unwrap();
    }
    let space: TestSpace = table.into_space();
    let a = 1;
    // this call will save space file to `tests/db`. It will be `tests/db/test.wt`
    // TODO: How to config this? Maybe we will need to have DATABASE_CONFIG env
    space.persist().unwrap();

    // check if file is same to expected
}