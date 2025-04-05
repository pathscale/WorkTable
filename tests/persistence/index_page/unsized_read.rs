use data_bucket::{parse_page, UnsizedIndexPage, INNER_PAGE_SIZE};
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
