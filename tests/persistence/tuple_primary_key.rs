use worktable::prelude::*;
use worktable::worktable;

use crate::remove_dir_if_exists;

worktable! (
    name: PersistedTuplePrimaryKey,
    persist: true,
    columns: {
        tenant_id: u64 primary_key using worktables_index,
        record_id: u64 primary_key using worktables_index,
        value: i64,
    },
);

#[tokio::test]
async fn composite_primary_key_survives_mutations_and_reload() {
    let path = "tests/data/persisted_tuple_primary_key/reload";
    remove_dir_if_exists(path.to_string()).await;

    let config = DiskConfig::new_with_table_name(
        path,
        PersistedTuplePrimaryKeyWorkTable::name_snake_case(),
        PersistedTuplePrimaryKeyWorkTable::version(),
    );
    let rows = [
        PersistedTuplePrimaryKeyRow {
            tenant_id: 7,
            record_id: 41,
            value: -10,
        },
        PersistedTuplePrimaryKeyRow {
            tenant_id: 7,
            record_id: 42,
            value: -11,
        },
        PersistedTuplePrimaryKeyRow {
            tenant_id: 8,
            record_id: 1,
            value: -12,
        },
    ];

    {
        let engine = PersistedTuplePrimaryKeyPersistenceEngine::new(config.clone())
            .await
            .unwrap();
        let table = PersistedTuplePrimaryKeyWorkTable::load(engine).await.unwrap();
        for row in &rows {
            table.insert(row.clone()).await.unwrap();
        }
        table.wait_for_ops().await.unwrap();
        for row in &rows {
            assert_eq!(table.select((row.tenant_id, row.record_id)), Some(row.clone()));
        }
        table.close().await.unwrap();
    }

    {
        let engine = PersistedTuplePrimaryKeyPersistenceEngine::new(config.clone())
            .await
            .unwrap();
        let table = PersistedTuplePrimaryKeyWorkTable::load(engine).await.unwrap();
        for row in &rows {
            assert_eq!(table.select((row.tenant_id, row.record_id)), Some(row.clone()));
        }

        let range = table.select_by_pk_range((7, 41)..=(7, 42)).execute().unwrap();
        assert_eq!(range, rows[..2]);

        let updated = PersistedTuplePrimaryKeyRow {
            value: 99,
            ..rows[1].clone()
        };
        table.update(updated.clone()).await.unwrap();
        table.delete((7, 41)).await.unwrap();
        assert_eq!(table.select((7, 42)), Some(updated));
        assert!(table.select((7, 41)).is_none());
        table.close().await.unwrap();
    }

    {
        let engine = PersistedTuplePrimaryKeyPersistenceEngine::new(config).await.unwrap();
        let table = PersistedTuplePrimaryKeyWorkTable::load(engine).await.unwrap();
        assert!(table.select((7, 41)).is_none());
        assert_eq!(table.select((7, 42)).unwrap().value, 99);
        assert_eq!(table.select((8, 1)), Some(rows[2].clone()));
        table.close().await.unwrap();
    }
}
