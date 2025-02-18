use crate::persistence::{get_test_wt, TestPersistRow, TestPersistWorkTable};
use crate::remove_dir_if_exists;
use worktable::prelude::{PersistenceConfig, PrimaryKeyGeneratorState};

#[test]
fn test_space_parse() {
    remove_dir_if_exists("tests/data/sync".to_string());

    let config = PersistenceConfig::new("tests/data/sync", "tests/data/sync").unwrap();

    let pk = {
        let table = TestPersistWorkTable::load_from_file(config.clone()).unwrap();
        let row = TestPersistRow {
            another: 42,
            id: table.get_next_pk().0,
        };
        table.insert(row.clone()).unwrap();
        row.id
    };
    {
        let table = TestPersistWorkTable::load_from_file(config).unwrap();
        assert!(table.select(pk.into()).is_some());
        assert_eq!(table.0.pk_gen.get_state(), pk)
    }
}
