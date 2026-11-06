use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable::worktable;

async fn remove_dir_if_exists(path: String) {
    match std::fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("fixture cleanup failed: {error}"),
    }
}

worktable!(
    name: TotpDeleteReuse,
    persist: true,
    columns: {
        user_id: u64 primary_key using worktables_index,
        secret: String,
        algorithm: String,
        enabled_at_secs: i64,
        last_verified_at_secs: i64 optional,
        last_used_step: u64 optional,
    },
    indexes: {
        enabled_at_idx: enabled_at_secs,
    },
    queries: {
        delete: {
            ByUserId() by user_id,
        }
    }
);

#[derive(Clone, Copy)]
enum DeletePath {
    Direct,
    GeneratedByPrimary,
}

fn row(user_id: u64, secret: &str) -> TotpDeleteReuseRow {
    TotpDeleteReuseRow {
        user_id,
        secret: secret.to_owned(),
        algorithm: "SHA1".to_owned(),
        enabled_at_secs: 1_800_000_000,
        last_verified_at_secs: Some(1_800_000_001),
        last_used_step: Some(30_000_000),
    }
}

async fn exercise_delete_reuse(dir: &str, delete_path: DeletePath, empty_table: bool) {
    remove_dir_if_exists(dir.to_owned()).await;

    let config = DiskConfig::new_with_table_name(
        dir,
        TotpDeleteReuseWorkTable::name_snake_case(),
        TotpDeleteReuseWorkTable::version(),
    );

    {
        let engine = TotpDeleteReusePersistenceEngine::new(config.clone()).await.unwrap();
        let table = TotpDeleteReuseWorkTable::load(engine).await.unwrap();

        for (user_id, secret) in [(11, "secret-0001"), (22, "secret-0002"), (33, "secret-0003")] {
            table.insert(row(user_id, secret)).await.unwrap();
        }
        table.wait_for_ops().await.unwrap();
        table.close().await.unwrap();
        let engine = TotpDeleteReusePersistenceEngine::new(config.clone()).await.unwrap();
        let table = TotpDeleteReuseWorkTable::load(engine).await.unwrap();

        let previous_links: Vec<_> = [11, 22, 33]
            .into_iter()
            .map(|key| {
                table
                    .0
                    .primary_index
                    .pk_map
                    .get_value(&TotpDeleteReusePrimaryKey(key))
                    .unwrap()
                    .0
            })
            .collect();
        if empty_table {
            table.delete(11).await.unwrap();
            table.delete(33).await.unwrap();
        }
        let mut verified = row(22, "secret-0002");
        verified.last_verified_at_secs = Some(1_800_000_099);
        verified.last_used_step = Some(30_000_003);
        table.replace(verified).await.unwrap();
        let old_link = table
            .0
            .primary_index
            .pk_map
            .get_value(&TotpDeleteReusePrimaryKey(22))
            .unwrap()
            .0;
        match delete_path {
            DeletePath::Direct => {
                table.delete(22).await.unwrap();
            }
            DeletePath::GeneratedByPrimary => {
                table.delete_by_user_id(22).await.unwrap();
            }
        }
        table.wait_for_ops().await.unwrap();
        for _ in 0..8 {
            table.0.data.reclaim_pending();
        }
        // Match the deleted row's serialized size so the next insert can reuse its link.
        table.insert(row(44, "secret-0004")).await.unwrap();
        let new_link = table
            .0
            .primary_index
            .pk_map
            .get_value(&TotpDeleteReusePrimaryKey(44))
            .unwrap()
            .0;
        if empty_table {
            assert!(
                previous_links.contains(&new_link) || new_link == old_link,
                "fixture must reuse a deleted slot"
            );
        } else {
            assert_eq!(old_link, new_link, "fixture must actually reuse the deleted slot");
        }
        table.wait_for_ops().await.unwrap();
        table.close().await.unwrap();
    }

    {
        let engine = TotpDeleteReusePersistenceEngine::new(config.clone()).await.unwrap();
        let table = TotpDeleteReuseWorkTable::load(engine).await.unwrap();
        assert!(table.select(22).is_none(), "deleted key returned after reload");
        assert_eq!(table.select(11), (!empty_table).then(|| row(11, "secret-0001")));
        assert_eq!(table.select(33), (!empty_table).then(|| row(33, "secret-0003")));
        assert_eq!(table.select(44), Some(row(44, "secret-0004")));
        table.close().await.unwrap();
    }

    {
        let engine = TotpDeleteReusePersistenceEngine::new(config).await.unwrap();
        let table = TotpDeleteReuseWorkTable::load(engine).await.unwrap();
        assert!(table.select(22).is_none(), "deleted key returned after clean shutdown");
        assert_eq!(table.select(11), (!empty_table).then(|| row(11, "secret-0001")));
        assert_eq!(table.select(33), (!empty_table).then(|| row(33, "secret-0003")));
        assert_eq!(table.select(44), Some(row(44, "secret-0004")));
        table.close().await.unwrap();
    }

    remove_dir_if_exists(dir.to_owned()).await;
}

#[tokio::test]
async fn delete_then_same_size_insert_survives_reopens() {
    let base = std::env::var("WT_TOTP_REPRO_DIR")
        .unwrap_or_else(|_| format!("{}/target/test-data/totp-delete-reuse", env!("CARGO_MANIFEST_DIR")));
    exercise_delete_reuse(&format!("{base}/direct"), DeletePath::Direct, false).await;
    exercise_delete_reuse(&format!("{base}/generated"), DeletePath::GeneratedByPrimary, false).await;
    exercise_delete_reuse(&format!("{base}/empty-direct"), DeletePath::Direct, true).await;
    exercise_delete_reuse(&format!("{base}/empty-generated"), DeletePath::GeneratedByPrimary, true).await;
}
