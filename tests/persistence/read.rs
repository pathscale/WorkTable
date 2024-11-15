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
    let persistence_config = PersistenceEngineConfig {
        path: "tests/db",
    };
    let engine = Arc::new(PersistenceEngine::new(persistence_config));

    let space = TestSpace::read(engine);
    let table = space.into_table();
}
