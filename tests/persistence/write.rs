use worktable::worktable;
use worktable::prelude::*;
use uuid::Uuid;


worktable! (
    name: Test,
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
    let persistence_config = PersistenceEngineConfig {
        path: "tests/db/",
    };
    let engine = Arc::new(PersistenceEngine::new(persistence_config));

    let table = TestWorkTable::new(engine.clone());
    let space: TestSpace = table.into_space();
    // this call will save space file to `tests/db`. It will be `tests/db/test.wt`
    space.persist();

    // check if file is same to expected
}