use worktable::worktable;
use worktable::prelude::*;
use uuid::Uuid;


worktable! (
    name: Test,
    persistence: true,
    columns: {
        id: Uuid primary_key,
        another: i64,
    },
    indexes: {
        another_idx: another,
    }
);

#[test]
fn test_persist () {
    let table = TestWorkTable::new();
    let space: TestSpace = table.into_space();
    // this call will save space file to `tests/db`. It will be `tests/db/test.wt`
    // TODO: How to config this? Maybe we will need to have DATABASE_CONFIG env
    space.persist();

    // check if file is same to expected
}