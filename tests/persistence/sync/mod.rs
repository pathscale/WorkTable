use crate::persistence::{get_test_wt, AnotherByIdQuery, TestPersistRow, TestPersistWorkTable};
use crate::remove_dir_if_exists;
use worktable::prelude::{PersistenceConfig, PrimaryKeyGeneratorState};

#[test]
fn test_space_insert_sync() {
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

#[tokio::test]
async fn test_space_update_full_sync() {
    remove_dir_if_exists("tests/data/sync".to_string());

    let config = PersistenceConfig::new("tests/data/sync", "tests/data/sync").unwrap();

    let pk = {
        let table = TestPersistWorkTable::load_from_file(config.clone()).unwrap();
        let row = TestPersistRow {
            another: 42,
            id: table.get_next_pk().0,
        };
        table.insert(row.clone()).unwrap();
        table
            .update(TestPersistRow {
                another: 13,
                id: row.id,
            })
            .await
            .unwrap();
        row.id
    };
    {
        let table = TestPersistWorkTable::load_from_file(config).unwrap();
        assert!(table.select(pk.into()).is_some());
        assert_eq!(table.select(pk.into()).unwrap().another, 13);
        assert_eq!(table.0.pk_gen.get_state(), pk)
    }
}

#[tokio::test]
async fn test_space_update_query_sync() {
    remove_dir_if_exists("tests/data/sync".to_string());

    let config = PersistenceConfig::new("tests/data/sync", "tests/data/sync").unwrap();

    let pk = {
        let table = TestPersistWorkTable::load_from_file(config.clone()).unwrap();
        let row = TestPersistRow {
            another: 42,
            id: table.get_next_pk().0,
        };
        table.insert(row.clone()).unwrap();
        table
            .update_another_by_id(AnotherByIdQuery { another: 13 }, row.id.into())
            .await
            .unwrap();
        row.id
    };
    {
        let table = TestPersistWorkTable::load_from_file(config).unwrap();
        assert!(table.select(pk.into()).is_some());
        assert_eq!(table.select(pk.into()).unwrap().another, 13);
        assert_eq!(table.0.pk_gen.get_state(), pk)
    }
}

#[tokio::test]
async fn test_space_delete_sync() {
    remove_dir_if_exists("tests/data/sync".to_string());

    let config = PersistenceConfig::new("tests/data/sync", "tests/data/sync").unwrap();

    let pk = {
        let table = TestPersistWorkTable::load_from_file(config.clone()).unwrap();
        let row = TestPersistRow {
            another: 42,
            id: table.get_next_pk().0,
        };
        table.insert(row.clone()).unwrap();
        table.delete(row.id.into()).await.unwrap();
        row.id
    };
    {
        let table = TestPersistWorkTable::load_from_file(config).unwrap();
        assert!(table.select(pk.into()).is_none());
        assert_eq!(table.0.pk_gen.get_state(), pk)
    }
}

#[tokio::test]
async fn test_space_delete_query_sync() {
    remove_dir_if_exists("tests/data/sync".to_string());

    let config = PersistenceConfig::new("tests/data/sync", "tests/data/sync").unwrap();

    let pk = {
        let table = TestPersistWorkTable::load_from_file(config.clone()).unwrap();
        let row = TestPersistRow {
            another: 42,
            id: table.get_next_pk().0,
        };
        table.insert(row.clone()).unwrap();
        table.delete_by_another(row.another).await.unwrap();
        row.id
    };
    {
        let table = TestPersistWorkTable::load_from_file(config).unwrap();
        assert!(table.select(pk.into()).is_none());
        assert_eq!(table.0.pk_gen.get_state(), pk)
    }
}
