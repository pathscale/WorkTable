use tokio::fs::File;

use super::*;
use crate::remove_dir_if_exists;

worktable!(
    name: SchemaMetadata,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        email: String,
        score: i64,
    },
    indexes: {
        email_idx: email unique,
        score_idx: score,
    },
);

worktable!(
    name: IncompatibleSchema,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        display_name: String,
    },
);

#[tokio::test]
async fn generated_schema_is_persisted_and_mismatches_are_rejected() {
    let root = "tests/data/persisted_schema_metadata";
    remove_dir_if_exists(root.to_owned()).await;
    let table_path = format!("{root}/shared");
    let config = DiskConfig::new(root, &table_path, SchemaMetadataWorkTable::version());

    let engine = SchemaMetadataPersistenceEngine::new(config.clone()).await.unwrap();
    let table = SchemaMetadataWorkTable::load(engine).await.unwrap();
    table.close().await.unwrap();

    let mut file = File::open(format!("{table_path}/{}", WT_DATA_EXTENSION)).await.unwrap();
    let info = parse_page::<SpaceInfoPage<u64>, { PAGE_SIZE as u32 }>(&mut file, 0)
        .await
        .unwrap();
    assert_eq!(
        info.inner.row_schema,
        vec![
            ("id".to_owned(), "u64".to_owned()),
            ("email".to_owned(), "String".to_owned()),
            ("score".to_owned(), "i64".to_owned()),
        ]
    );
    assert_eq!(info.inner.primary_key_fields, vec!["id"]);
    assert_eq!(
        info.inner.secondary_index_types,
        vec![
            ("email_idx".to_owned(), "String".to_owned()),
            ("score_idx".to_owned(), "i64".to_owned()),
        ]
    );

    let incompatible = DiskConfig::new(root, &table_path, IncompatibleSchemaWorkTable::version());
    let engine = IncompatibleSchemaPersistenceEngine::new(incompatible).await.unwrap();
    let error = IncompatibleSchemaWorkTable::load(engine).await.unwrap_err();
    assert!(error.to_string().contains("persisted schema mismatch"));
}

#[tokio::test]
async fn loading_a_legacy_empty_schema_does_not_rewrite_the_file() {
    let root = "tests/data/persisted_schema_legacy";
    remove_dir_if_exists(root.to_owned()).await;
    let table_path = format!("{root}/shared");
    let config = DiskConfig::new(root, &table_path, SchemaMetadataWorkTable::version());

    // Opening the raw engine bootstraps the same empty metadata written by
    // pre-schema WorkTable releases. Dropping it before constructing a table
    // makes the next engine observe an existing legacy file.
    drop(SchemaMetadataPersistenceEngine::new(config.clone()).await.unwrap());

    let engine = SchemaMetadataPersistenceEngine::new(config).await.unwrap();
    let table = SchemaMetadataWorkTable::load(engine).await.unwrap();
    table.close().await.unwrap();

    let mut file = File::open(format!("{table_path}/{}", WT_DATA_EXTENSION)).await.unwrap();
    let info = parse_page::<SpaceInfoPage<u64>, { PAGE_SIZE as u32 }>(&mut file, 0)
        .await
        .unwrap();
    assert!(info.inner.row_schema.is_empty());
    assert!(info.inner.primary_key_fields.is_empty());
    assert!(info.inner.secondary_index_types.is_empty());
}
