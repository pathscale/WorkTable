use data_bucket::page::IndexValue;
use data_bucket::{parse_page, GeneralPage, Link, NewIndexPage, INNER_PAGE_SIZE};
use rkyv::{Archive, Serialize};
use serde::Deserialize;
use std::fs::{File, OpenOptions};

#[test]
fn test_index_page_read_after_create_node_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_create_node.wt.idx")
        .unwrap();

    let page = parse_page::<NewIndexPage<u8>, { INNER_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();
    assert_eq!(page.inner.node_id, 5);
    assert_eq!(page.inner.values_count, 1);
    assert_eq!(page.inner.slots.get(0).unwrap(), &0);
    assert_eq!(page.inner.index_values.get(0).unwrap().key, 5);
    assert_eq!(page.inner.index_values.get(0).unwrap().link.length, 24);
}

#[test]
fn test_index_page_read_after_insert_at_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_insert_at.wt.idx")
        .unwrap();

    let page = parse_page::<NewIndexPage<u8>, { INNER_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();
    assert_eq!(page.inner.node_id, 5);
    assert_eq!(page.inner.values_count, 2);
    assert_eq!(page.inner.slots.get(0).unwrap(), &1);
    assert_eq!(page.inner.index_values.get(1).unwrap().key, 3);
    assert_eq!(page.inner.index_values.get(1).unwrap().link.length, 48);
    assert_eq!(page.inner.index_values.get(1).unwrap().link.offset, 24);
    assert_eq!(page.inner.slots.get(1).unwrap(), &0);
    assert_eq!(page.inner.index_values.get(0).unwrap().key, 5);
    assert_eq!(page.inner.index_values.get(0).unwrap().link.length, 24);
}

#[test]
fn test_index_page_read_after_insert_at_with_node_id_update_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_insert_at_with_node_id_update.wt.idx")
        .unwrap();

    let page = parse_page::<NewIndexPage<u8>, { INNER_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();
    assert_eq!(page.inner.node_id, 7);
    assert_eq!(page.inner.values_count, 2);
    assert_eq!(page.inner.slots.get(0).unwrap(), &0);
    assert_eq!(page.inner.index_values.get(0).unwrap().key, 5);
    assert_eq!(page.inner.index_values.get(0).unwrap().link.length, 24);
    assert_eq!(page.inner.slots.get(1).unwrap(), &1);
    assert_eq!(page.inner.index_values.get(1).unwrap().key, 7);
    assert_eq!(page.inner.index_values.get(1).unwrap().link.length, 48);
    assert_eq!(page.inner.index_values.get(1).unwrap().link.offset, 24);
}

#[test]
fn test_index_page_read_after_remove_at_node_id_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_remove_at_node_id.wt.idx")
        .unwrap();

    let page = parse_page::<NewIndexPage<u8>, { INNER_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();
    assert_eq!(page.inner.node_id, 3);
    assert_eq!(page.inner.values_count, 0);
    assert_eq!(page.inner.slots.get(0).unwrap(), &1);
    assert_eq!(page.inner.index_values.get(1).unwrap().key, 3);
    assert_eq!(page.inner.index_values.get(1).unwrap().link.length, 48);
    assert_eq!(page.inner.index_values.get(1).unwrap().link.offset, 24);
}
