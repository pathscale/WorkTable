use data_bucket::INNER_PAGE_SIZE;
use std::fs::{File, OpenOptions};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use worktable::prelude::IndexTableOfContents;

#[test]
fn test_index_table_of_contents_read() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/persist_index_table_of_contents.wt.idx")
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(1));
    let mut toc = IndexTableOfContents::<u8, { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .unwrap();

    assert_eq!(toc.get(&13), Some(1.into()))
}

#[test]
fn test_index_table_of_contents_read_from_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_create_node.wt.idx")
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(2));
    let mut toc = IndexTableOfContents::<u8, { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .unwrap();

    assert_eq!(toc.get(&5), Some(2.into()))
}

#[test]
fn test_index_table_of_contents_read_from_space_index_with_updated_node_id() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_insert_at_with_node_id_update.wt.idx")
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(2));
    let mut toc = IndexTableOfContents::<u8, { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .unwrap();

    assert_eq!(toc.get(&7), Some(2.into()))
}
