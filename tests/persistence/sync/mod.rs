use crate::persistence::{get_test_wt, TestPersistRow, TestPersistWorkTable};
use crate::remove_dir_if_exists;
use worktable::prelude::PersistenceConfig;

#[test]
fn test_space_parse() {
    remove_dir_if_exists("tests/data/sync".to_string());

    let config = PersistenceConfig::new("tests/data/sync", "tests/data/sync").unwrap();
    let row = TestPersistRow {
        another: 42,
        id: 100,
    };
    {
        let table = TestPersistWorkTable::load_from_file(config.clone()).unwrap();
        table.insert(row.clone()).unwrap();
    }
    {
        let table = TestPersistWorkTable::load_from_file(config).unwrap();
        assert!(table.select(row.id.into()).is_some())
    }
}
