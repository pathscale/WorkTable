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

fn test_read () {
    // this call will read space file from `tests/db`. It will be `tests/db/test.wt`
    // TODO: How to config this? Maybe we will need to have DATABASE_CONFIG env
    let space = TestSpace::read();
    let table = space.into_table();

    // Check tables data
}
