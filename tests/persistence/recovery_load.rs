use std::collections::BTreeSet;

use crate::remove_dir_if_exists;
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: RecoveryLoad,
    persist: true,
    columns: {
        id: String primary_key,
        project_id: String,
        body: String,
    },
    indexes: {
        project_idx: project_id,
    },
);

const DIR: &str = "tests/data/recovery_load/persisted";
const CORRUPT_DIR: &str = "tests/data/recovery_load/corrupt_row";

fn config(dir: &str) -> DiskConfig {
    DiskConfig::new_with_table_name(
        dir,
        RecoveryLoadWorkTable::name_snake_case(),
        RecoveryLoadWorkTable::version(),
    )
}

async fn engine(dir: &str) -> RecoveryLoadPersistenceEngine {
    RecoveryLoadPersistenceEngine::new(config(dir)).await.unwrap()
}

fn row(id: &str, project_id: &str) -> RecoveryLoadRow {
    RecoveryLoadRow {
        id: id.to_owned(),
        project_id: project_id.to_owned(),
        body: format!("body-{id}"),
    }
}

#[tokio::test]
async fn recovery_mode_reads_valid_rows_through_a_surviving_secondary_index() {
    remove_dir_if_exists(DIR.to_owned()).await;

    let table = RecoveryLoadWorkTable::load(engine(DIR).await).await.unwrap();
    table.insert(row("row-1", "project-a")).unwrap();
    table.insert(row("row-2", "project-a")).unwrap();
    table.insert(row("row-3", "project-b")).unwrap();
    table.close().await.unwrap();

    let table_dir = format!("{DIR}/{}", RecoveryLoadWorkTable::name_snake_case());
    let primary_path = format!("{table_dir}/primary{WT_INDEX_EXTENSION}");
    tokio::fs::rename(&primary_path, format!("{primary_path}.damaged"))
        .await
        .unwrap();

    let error = RecoveryLoadWorkTable::load(engine(DIR).await).await.unwrap_err();
    let typed = error
        .downcast_ref::<PersistenceLoadError>()
        .expect("strict load must return a typed corruption error");
    assert!(
        typed.reason().contains("project_idx"),
        "unexpected strict-load reason: {}",
        typed.reason()
    );

    let table = RecoveryLoadWorkTable::load_with(engine(DIR).await, LoadMode::Recovery)
        .await
        .unwrap();
    assert!(table.select("row-1".to_owned()).is_none());

    let project_a: BTreeSet<_> = table
        .select_by_project_id("project-a".to_owned())
        .execute()
        .unwrap()
        .into_iter()
        .map(|row| row.id)
        .collect();
    assert_eq!(project_a, BTreeSet::from(["row-1".to_owned(), "row-2".to_owned()]));

    let project_b: BTreeSet<_> = table
        .select_by_project_id("project-b".to_owned())
        .execute()
        .unwrap()
        .into_iter()
        .map(|row| row.id)
        .collect();
    assert_eq!(project_b, BTreeSet::from(["row-3".to_owned()]));

    table.close().await.unwrap();
    remove_dir_if_exists(DIR.to_owned()).await;
}

#[tokio::test]
async fn recovery_mode_rejects_corrupt_rows_reached_through_a_secondary_index() {
    remove_dir_if_exists(CORRUPT_DIR.to_owned()).await;

    let table = RecoveryLoadWorkTable::load(engine(CORRUPT_DIR).await).await.unwrap();
    let id = table.insert(row("row-corrupt", "project-a")).unwrap();
    let link = table.0.primary_index.pk_map.get_value(&id).unwrap().0;
    table.close().await.unwrap();

    let table_dir = format!("{CORRUPT_DIR}/{}", RecoveryLoadWorkTable::name_snake_case());
    let primary_path = format!("{table_dir}/primary{WT_INDEX_EXTENSION}");
    tokio::fs::rename(&primary_path, format!("{primary_path}.damaged"))
        .await
        .unwrap();

    let data_path = format!("{table_dir}/{WT_DATA_EXTENSION}");
    let page_id: u32 = link.page_id.into();
    let byte_offset = u64::from(page_id) * PAGE_SIZE as u64 + GENERAL_HEADER_SIZE as u64 + u64::from(link.offset);
    {
        use std::io::{Seek, SeekFrom, Write};

        let mut file = std::fs::OpenOptions::new().write(true).open(data_path).unwrap();
        file.seek(SeekFrom::Start(byte_offset)).unwrap();
        file.write_all(&vec![0; link.length as usize]).unwrap();
        file.sync_all().unwrap();
    }

    let error = RecoveryLoadWorkTable::load_with(engine(CORRUPT_DIR).await, LoadMode::Recovery)
        .await
        .unwrap_err();
    let typed = error
        .downcast_ref::<PersistenceLoadError>()
        .expect("recovery must return a typed corruption error");
    assert!(
        typed.reason().contains("project_idx") && typed.reason().contains("key does not match"),
        "unexpected recovery-load reason: {}",
        typed.reason()
    );

    remove_dir_if_exists(CORRUPT_DIR.to_owned()).await;
}
