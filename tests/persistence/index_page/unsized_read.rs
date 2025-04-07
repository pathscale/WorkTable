use data_bucket::{parse_page, IndexPage, UnsizedIndexPage, INNER_PAGE_SIZE};
use tokio::fs::OpenOptions;

#[tokio::test]
async fn test_index_page_read_after_create_node_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index_unsized/process_create_node.wt.idx")
        .await
        .unwrap();

    let page = parse_page::<
        UnsizedIndexPage<String, { INNER_PAGE_SIZE as u32 }>,
        { INNER_PAGE_SIZE as u32 },
    >(&mut file, 2)
    .await
    .unwrap();

    assert_eq!(page.inner.node_id, "Something from someone".to_string());
}

#[tokio::test]
async fn test_index_pages_read_after_creation_of_second_node_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index_unsized/process_create_second_node.wt.idx")
        .await
        .unwrap();

    let page = parse_page::<
        UnsizedIndexPage<String, { INNER_PAGE_SIZE as u32 }>,
        { INNER_PAGE_SIZE as u32 },
    >(&mut file, 2)
    .await
    .unwrap();

    assert_eq!(page.inner.node_id, "Something from someone".to_string());

    let page = parse_page::<
        UnsizedIndexPage<String, { INNER_PAGE_SIZE as u32 }>,
        { INNER_PAGE_SIZE as u32 },
    >(&mut file, 3)
    .await
    .unwrap();

    assert_eq!(page.inner.node_id, "Someone from somewhere".to_string());
}

#[tokio::test]
async fn test_index_pages_read_after_remove_node_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index_unsized/process_remove_node.wt.idx")
        .await
        .unwrap();

    let page = parse_page::<
        UnsizedIndexPage<String, { INNER_PAGE_SIZE as u32 }>,
        { INNER_PAGE_SIZE as u32 },
    >(&mut file, 2)
    .await
    .unwrap();

    assert_eq!(page.inner.node_id, "Something from someone".to_string());
}
