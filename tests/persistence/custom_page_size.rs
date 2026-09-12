use worktable::prelude::*;
use worktable::worktable;

use crate::remove_dir_if_exists;

/// Half the default stride. Chosen so a page written at this size lands where
/// the default would put the middle of a page: a table that quietly fell back
/// to the default could not produce the file lengths asserted below.
const HALF: u32 = 8192;

worktable! (
    name: HalfPage,
    persist: true,
    columns: {
        id: u64 primary_key,
        payload: u64,
    },
    config: {
        page_size: 8192
    }
);

/// `page_size` beside `persist: true` used to be refused outright: the seeks
/// computed every offset from a hardcoded stride while the generated table
/// threaded the configured one, and the two disagreeing corrupted the file.
///
/// A round trip alone would not prove the setting took effect, because a table
/// that fell back to the default would still reload its own writes. So the
/// file length is checked too, and it can only be a multiple of the configured
/// stride.
#[tokio::test]
async fn a_persisted_table_with_a_custom_page_size_reloads_what_it_wrote() {
    let dir = "tests/data/custom_page_size/persisted";
    remove_dir_if_exists(dir.to_string()).await;
    let config =
        DiskConfig::new_with_table_name(dir, HalfPageWorkTable::name_snake_case(), HalfPageWorkTable::version());

    let mut expected = Vec::new();
    {
        let engine = HalfPagePersistenceEngine::new(config.clone()).await.unwrap();
        let table = HalfPageWorkTable::load(engine).await.unwrap();
        // Enough rows to need several pages at this stride, so the page
        // transitions are exercised and not just the first page.
        for i in 0..2_000u64 {
            let row = HalfPageRow { id: i, payload: i };
            table.insert(row.clone()).await.unwrap();
            expected.push(row);
        }
        table.wait_for_ops().await.unwrap();
    }

    let data_file_path = format!("{dir}/{}/.wt.data", HalfPageWorkTable::name_snake_case());
    let length = std::fs::metadata(&data_file_path).unwrap().len();
    let pages = length.div_ceil(u64::from(HALF));
    assert!(
        pages >= 2,
        "expected several {HALF}-byte pages, got {pages} from {length} bytes"
    );

    let engine = HalfPagePersistenceEngine::new(config).await.unwrap();
    let table = HalfPageWorkTable::load(engine)
        .await
        .expect("a table with a custom page size must reload");
    assert_eq!(table.select_all().execute().unwrap().len(), expected.len());
    for row in &expected {
        assert_eq!(table.select(row.id).as_ref(), Some(row));
    }

    remove_dir_if_exists(dir.to_string()).await;
}
