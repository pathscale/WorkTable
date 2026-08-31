use std::collections::HashMap;

use data_bucket::{INNER_PAGE_SIZE, Link, PAGE_SIZE, parse_general_header_by_index};
use worktable::prelude::{SpaceData, SpaceDataOps};

type TestSpaceData = SpaceData<(), INNER_PAGE_SIZE, { PAGE_SIZE as u32 }>;

fn test_dir(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("worktable-space-data-{name}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.display().to_string()
}

fn link(page_id: u32, offset: u32, length: u32) -> Link {
    Link {
        page_id: page_id.into(),
        offset,
        length,
    }
}

#[tokio::test]
async fn rewriting_one_link_does_not_inflate_the_persisted_data_length() {
    let dir = test_dir("hot-row");
    let mut space = TestSpaceData::from_table_files_path(&dir, 1).await.unwrap();

    let row = [7u8; 24];
    let hot = link(1, 0, row.len() as u32);
    for _ in 0..2_000 {
        space.save_data(hot, &row).await.unwrap();
    }
    assert_eq!(space.current_data_length, row.len() as u32);
    drop(space);

    // Reload from disk: the persisted last-page length must match the actually
    // occupied extent, not 2000 accumulated rewrites of the same 24 bytes.
    let mut space = TestSpaceData::from_table_files_path(&dir, 1).await.unwrap();
    assert_eq!(space.last_page_id, 1);
    assert_eq!(space.current_data_length, row.len() as u32);

    // A batch persist parses the last page with its recorded length and
    // re-serializes `data[..length]`. An inflated length made this slice out
    // of range and killed the persistence worker.
    let batch_row = [9u8; 24];
    let batch = HashMap::from([(1.into(), vec![(link(1, 24, 24), batch_row.to_vec())])]);
    space.save_batch_data(batch).await.unwrap();
    assert_eq!(space.current_data_length, 48);

    let header = parse_general_header_by_index(&mut space.data_file, 1).await.unwrap();
    assert_eq!(header.data_length, 48);

    drop(space);
    std::fs::remove_dir_all(&dir).unwrap();
}
