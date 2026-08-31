use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;

use crate::persistence::{TEST_PERSIST_PAGE_SIZE, TestPersistPersistenceEngine, TestPersistRow, TestPersistWorkTable};
use crate::remove_dir_if_exists;

/// Regression test for the generated `parse_file` page-count arithmetic: the
/// data-page loop used `1..=(file_length / stride)`, which parses one page
/// past EOF whenever the data file ends exactly on a page boundary (a final
/// data page that exactly fills its slot). Such a table failed to load at
/// all. The test constructs the boundary-aligned length directly by padding
/// the tail of the last page's slot; parsed pages are length-prefixed, so the
/// padding is inert, only the file length changes.
#[tokio::test]
async fn table_whose_data_file_ends_on_an_exact_page_boundary_loads() {
    let dir = "tests/data/exact_boundary/persisted";
    remove_dir_if_exists(dir.to_string()).await;
    let config = DiskConfig::new_with_table_name(
        dir,
        TestPersistWorkTable::name_snake_case(),
        TestPersistWorkTable::version(),
    );

    let mut expected = Vec::new();
    {
        let engine = TestPersistPersistenceEngine::new(config.clone()).await.unwrap();
        let table = TestPersistWorkTable::load(engine).await.unwrap();
        for i in 0..100u64 {
            let row = TestPersistRow {
                id: table.get_next_pk().into(),
                another: i,
            };
            table.insert(row.clone()).unwrap();
            expected.push(row);
        }
        table.wait_for_ops().await.unwrap();
    }

    // Pad the data file to the next exact page-size multiple.
    let data_file_path = format!("{dir}/{}/.wt.data", TestPersistWorkTable::name_snake_case());
    let stride = TEST_PERSIST_PAGE_SIZE as u64;
    let len = std::fs::metadata(&data_file_path).unwrap().len();
    assert!(
        len % stride != 0,
        "fixture must start off the boundary for the padding below to construct it"
    );
    let padded = len.div_ceil(stride) * stride;
    let file = std::fs::OpenOptions::new().write(true).open(&data_file_path).unwrap();
    file.set_len(padded).unwrap();
    drop(file);

    let engine = TestPersistPersistenceEngine::new(config).await.unwrap();
    let table = TestPersistWorkTable::load(engine)
        .await
        .expect("a data file ending exactly on a page boundary must load");
    assert_eq!(table.select_all().execute().unwrap().len(), expected.len());
    for row in &expected {
        assert_eq!(table.select(row.id).as_ref(), Some(row));
    }

    drop(table);
    remove_dir_if_exists(dir.to_string()).await;
}
