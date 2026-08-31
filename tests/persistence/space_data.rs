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

#[tokio::test]
async fn reclaiming_thousands_of_pages_bounds_the_info_page_instead_of_corrupting_page_one() {
    use data_bucket::{GENERAL_HEADER_SIZE, PageType, Persistable};

    let dir = test_dir("free-list-bound");
    let mut space = TestSpaceData::from_table_files_path(&dir, 1).await.unwrap();

    // A live row on page 1: exactly what an overflowing info page destroys,
    // since the serialized info is written unchecked from the start of page 0.
    let row = vec![7u8; 24];
    let far_row = vec![8u8; 24];
    let batch = HashMap::from([
        (1.into(), vec![(link(1, 0, 24), row)]),
        (2_000.into(), vec![(link(2_000, 0, 24), far_row)]),
    ]);
    space.save_batch_data(batch).await.unwrap();

    // Reclaim ~2000 pages: unbounded, the resulting empty_links_list
    // serializes to roughly twice the info slot and overwrites page 1.
    let page_ids = (1..=2_000u32).map(Into::into).collect::<Vec<_>>();
    space.reclaim_data_pages(page_ids).await.unwrap();

    let budget = PAGE_SIZE - GENERAL_HEADER_SIZE;
    let serialized = space.info.inner.as_bytes().as_ref().len();
    assert!(
        serialized <= budget,
        "info page must stay inside page 0's slot: {serialized} > {budget}"
    );
    let kept = space.info.inner.empty_links_list.len();
    assert!(kept > 0, "bounding must keep as many ranges as fit");
    assert!(kept < 2_000, "the overflow condition was not constructed");

    // Page 1 must be untouched by the info persist.
    let header = parse_general_header_by_index(&mut space.data_file, 1).await.unwrap();
    assert_eq!(header.page_type, PageType::Data);
    drop(space);

    // And the table still reopens.
    let space = TestSpaceData::from_table_files_path(&dir, 1).await.unwrap();
    assert_eq!(space.info.inner.empty_links_list.len(), kept);

    drop(space);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn creating_a_page_past_the_next_id_advances_to_the_links_page() {
    let dir = test_dir("page-jump");
    let mut space = TestSpaceData::from_table_files_path(&dir, 1).await.unwrap();

    // The link's page is more than one past the current last page. The old
    // bare increment left last_page_id at 1, so this very page would be seen
    // as "new" again by the next write and re-created zero-filled.
    let row = [5u8; 16];
    space.save_data(link(3, 0, 16), &row).await.unwrap();
    assert_eq!(space.last_page_id, 3);
    assert_eq!(space.current_data_length, 16);

    space.save_data(link(3, 16, 16), &row).await.unwrap();
    assert_eq!(space.last_page_id, 3);
    assert_eq!(space.current_data_length, 32);

    drop(space);
    let space = TestSpaceData::from_table_files_path(&dir, 1).await.unwrap();
    assert_eq!(space.last_page_id, 3);
    assert_eq!(space.current_data_length, 32);

    drop(space);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn a_data_file_ending_on_an_exact_page_boundary_reopens() {
    let dir = test_dir("exact-multiple");
    let mut space = TestSpaceData::from_table_files_path(&dir, 1).await.unwrap();

    // Fill page 1 completely: the file then ends exactly on a page boundary
    // (2 * PAGE_SIZE), the case where the old floor division computed a last
    // page id one past EOF and reopening failed on the header read.
    let full_page = vec![3u8; INNER_PAGE_SIZE];
    let batch = HashMap::from([(1.into(), vec![(link(1, 0, INNER_PAGE_SIZE as u32), full_page)])]);
    space.save_batch_data(batch).await.unwrap();
    drop(space);

    let file_length = std::fs::metadata(format!("{dir}/.wt.data")).unwrap().len();
    assert_eq!(
        file_length % PAGE_SIZE as u64,
        0,
        "fixture must end exactly on a page boundary"
    );

    let space = TestSpaceData::from_table_files_path(&dir, 1).await.unwrap();
    assert_eq!(space.last_page_id, 1);
    assert_eq!(space.current_data_length, INNER_PAGE_SIZE as u32);

    drop(space);
    std::fs::remove_dir_all(&dir).unwrap();
}
