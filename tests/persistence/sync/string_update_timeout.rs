use crate::remove_dir_if_exists;
use std::time::Duration;
use tokio::time::timeout;
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: User,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        public_id: u64,
        fk_app_id: u64,
        fk_app_pub_id: u64,
        username: String,
        display_name: String optional,
        telegram_username: String optional,
        telegram_confirmed: bool,
        status: u64,
        honey_app_role: u64,
    },
    indexes: {
        public_id_idx: public_id unique,
        username_idx: username,
        fk_app_id_idx: fk_app_id,
        fk_app_public_id_idx: fk_app_pub_id,
    },
    queries: {
        update: {
            DisplayNameByPublicId(display_name) by public_id,
            UsernameByPublicId(username) by public_id,
            StatusByPublicId(status) by public_id,
        },
        delete: {
            ByFkAppId() by fk_app_id,
            ByFkAppPubId() by fk_app_pub_id,
            ByPubId() by public_id,
        }
    }
);

#[test]
fn test_string_update_doesnt_block_persistence() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/sync/string_update_timeout",
        UserWorkTable::name_snake_case(),
        UserWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/sync/string_update_timeout".to_string()).await;

        // Phase 1: Insert initial row with String fields
        let row = {
            let engine = UserPersistenceEngine::new(config.clone()).await.unwrap();
            let table = UserWorkTable::load(engine).await.unwrap();

            let row = UserRow {
                id: table.get_next_pk().0,
                public_id: 1001,
                fk_app_id: 42,
                fk_app_pub_id: 9999,
                username: "test_user".to_string(),
                display_name: None,
                telegram_username: None,
                telegram_confirmed: false,
                status: 1,
                honey_app_role: 2,
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row
        };

        {
            let engine = UserPersistenceEngine::new(config.clone()).await.unwrap();
            let table = UserWorkTable::load(engine).await.unwrap();

            table.update(row.clone()).await.unwrap();

            let wait_result = timeout(Duration::from_secs(4), table.wait_for_ops()).await;

            if wait_result.is_err() {
                panic!(
                    "BUG DETECTED: Persistence system is stuck! \
                     wait_for_ops() timed out after String update."
                );
            }
        }

        // Phase 3: Reload and verify persisted values
        {
            let engine = UserPersistenceEngine::new(config).await.unwrap();
            let table = UserWorkTable::load(engine).await.unwrap();

            let persisted_row = table.select(row.id).unwrap();
            assert_eq!(persisted_row.username, row.username);
            assert_eq!(persisted_row.display_name, row.display_name);
            assert_eq!(persisted_row.public_id, row.public_id);
            assert_eq!(persisted_row.fk_app_id, row.fk_app_id);
            assert_eq!(persisted_row.fk_app_pub_id, row.fk_app_pub_id);
            assert_eq!(persisted_row.telegram_username, row.telegram_username);
            assert_eq!(persisted_row.telegram_confirmed, row.telegram_confirmed);
            assert_eq!(persisted_row.status, row.status);
            assert_eq!(persisted_row.honey_app_role, row.honey_app_role);
        }
    });
}
