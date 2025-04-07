use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use data_bucket::INNER_PAGE_SIZE;
use tokio::fs::OpenOptions;
use worktable::prelude::IndexTableOfContents;

#[tokio::test]
async fn test_index_table_of_contents_read_from_space_index_unsized() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index_unsized/process_create_node.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(2));
    let toc = IndexTableOfContents::<String, { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    assert_eq!(
        toc.get(&"Something from someone".to_string()),
        Some(2.into())
    )
}

#[tokio::test]
async fn test_index_table_of_contents_read_from_space_index_unsized_with_two_nodes() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index_unsized/process_create_second_node.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(3));
    let toc = IndexTableOfContents::<String, { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    assert_eq!(
        toc.get(&"Something from someone".to_string()),
        Some(2.into())
    );
    assert_eq!(
        toc.get(&"Someone from somewhere".to_string()),
        Some(3.into())
    )
}
