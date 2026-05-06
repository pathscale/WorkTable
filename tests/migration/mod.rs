use crate::remove_dir_if_exists;
use worktable::migration::Migration;
use worktable::prelude::*;
use worktable_codegen::{migration_engine, worktable};

mod v1 {
    use super::*;

    worktable!(
        name: User,
        version: 1,
        persist: true,
        columns: {
            id: u64 primary_key autoincrement,
            name: String,
        },
    );
}

mod v2 {
    use super::*;

    worktable!(
        name: User,
        version: 2,
        persist: true,
        columns: {
            id: u64 primary_key autoincrement,
            name: String,
            email: String,
        },
    );
}

worktable!(
    name: User,
    version: 3,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        name: String,
        email: String,
        created_at: u64,
    },
    indexes: {
        name_idx: name,
    },
);

#[derive(Default)]
pub struct UserMigrationContext {
    pub default_email: String,
    pub default_created_at: u64,
}

pub struct UserMigration;

impl Migration<v1::UserRow, v2::UserRow> for UserMigration {
    type Context = UserMigrationContext;

    fn migrate(row: v1::UserRow, ctx: &Self::Context) -> v2::UserRow {
        v2::UserRow {
            id: row.id,
            name: row.name,
            email: ctx.default_email.clone(),
        }
    }
}

impl Migration<v2::UserRow, UserRow> for UserMigration {
    type Context = UserMigrationContext;

    fn migrate(row: v2::UserRow, ctx: &Self::Context) -> UserRow {
        UserRow {
            id: row.id,
            name: row.name,
            email: row.email,
            created_at: ctx.default_created_at,
        }
    }
}

migration_engine!(
    migration: UserMigration,
    current: UserWorkTable,
    ctx: UserMigrationContext,
    version_tables: {
        1 => v1::UserWorkTable,
        2 => v2::UserWorkTable,
    },
);

/// v1 → current: create v1 data, migrate to current, verify data
#[test]
fn test_migrate_v1_to_current() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        let src = "tests/data/migration/v1_to_current";
        let dst = "tests/data/migration/v1_to_current/dst";
        remove_dir_if_exists(src.to_string()).await;

        // Write v1 data
        {
            let config = DiskConfig::new_with_table_name(
                src,
                v1::UserWorkTable::name_snake_case(),
                v1::UserWorkTable::version(),
            );
            let engine = v1::UserPersistenceEngine::new(config).await.unwrap();
            let table = v1::UserWorkTable::load(engine).await.unwrap();

            table
                .insert(v1::UserRow {
                    id: table.get_next_pk().into(),
                    name: "Alice".to_string(),
                })
                .unwrap();
            table
                .insert(v1::UserRow {
                    id: table.get_next_pk().into(),
                    name: "Bob".to_string(),
                })
                .unwrap();

            table.wait_for_ops().await;
        }

        // Verify source data is readable
        {
            let config = DiskConfig::new_with_table_name(
                src,
                v1::UserWorkTable::name_snake_case(),
                v1::UserWorkTable::version(),
            );
            let engine = ReadOnlyPersistenceEngine::create(config).await.unwrap();
            let table = v1::UserWorkTable::load(engine).await.unwrap();
            let count = table.count();
            assert_eq!(count, 2, "v1 table should have 2 rows, got {}", count);
        }

        let ctx = UserMigrationContext {
            default_email: "unknown@example.com".to_string(),
            default_created_at: chrono::Utc::now().timestamp() as u64,
        };

        let report = UserMigrationEngine::migrate(src, dst, &ctx).await.unwrap();
        assert_eq!(report.source_version, v1::UserWorkTable::version());

        {
            let config =
                DiskConfig::new_with_table_name(dst, UserWorkTable::name_snake_case(), UserWorkTable::version());
            let engine = UserPersistenceEngine::new(config).await.unwrap();
            let table = UserWorkTable::load(engine).await.unwrap();

            let rows = table.select_all().execute().unwrap();
            assert_eq!(rows.len(), 2);

            for row in &rows {
                assert_eq!(row.email, ctx.default_email);
                assert_eq!(row.created_at, ctx.default_created_at);
            }

            let names: Vec<_> = rows.iter().map(|r| r.name.clone()).collect();
            assert!(names.contains(&"Alice".to_string()));
            assert!(names.contains(&"Bob".to_string()));
        }
    });
}

#[test]
fn test_migrate_v2_to_current() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        let src = "tests/data/migration/v2_to_current";
        let dst = "tests/data/migration/v2_to_current/dst";
        remove_dir_if_exists(src.to_string()).await;

        {
            let config = DiskConfig::new_with_table_name(
                src,
                v2::UserWorkTable::name_snake_case(),
                v2::UserWorkTable::version(),
            );
            let engine = v2::UserPersistenceEngine::new(config).await.unwrap();
            let table = v2::UserWorkTable::load(engine).await.unwrap();

            table
                .insert(v2::UserRow {
                    id: table.get_next_pk().into(),
                    name: "Charlie".to_string(),
                    email: "charlie@test.com".to_string(),
                })
                .unwrap();
            table
                .insert(v2::UserRow {
                    id: table.get_next_pk().into(),
                    name: "Diana".to_string(),
                    email: "diana@test.com".to_string(),
                })
                .unwrap();

            table.wait_for_ops().await;
        }

        let ctx = UserMigrationContext {
            default_email: "unknown@example.com".to_string(),
            default_created_at: chrono::Utc::now().timestamp() as u64,
        };

        let report = UserMigrationEngine::migrate(src, dst, &ctx).await.unwrap();
        assert_eq!(report.source_version, v2::UserWorkTable::version());

        {
            let config =
                DiskConfig::new_with_table_name(dst, UserWorkTable::name_snake_case(), UserWorkTable::version());
            let engine = UserPersistenceEngine::new(config).await.unwrap();
            let table = UserWorkTable::load(engine).await.unwrap();

            let rows = table.select_all().execute().unwrap();
            assert_eq!(rows.len(), 2);

            let charlie = rows.iter().find(|r| r.name == "Charlie").unwrap();
            assert_eq!(charlie.email, "charlie@test.com");
            assert_eq!(charlie.created_at, ctx.default_created_at);

            let diana = rows.iter().find(|r| r.name == "Diana").unwrap();
            assert_eq!(diana.email, "diana@test.com");
            assert_eq!(diana.created_at, ctx.default_created_at);
        }
    });
}

/// Nonexistent source returns an error
#[test]
fn test_nonexistent_source_error() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        let dst = "tests/data/migration/nonexistent_new";
        remove_dir_if_exists(dst.to_string()).await;

        let ctx = UserMigrationContext::default();
        let result = UserMigrationEngine::migrate("tests/data/migration/does_not_exist", dst, &ctx).await;
        assert!(result.is_err());
    });
}
