use data_bucket::{parse_page, IndexPage, INNER_PAGE_SIZE};
use std::fs::OpenOptions;

#[test]
fn test_index_page_read_in_space() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/test_persist/primary.wt.idx")
        .unwrap();

    let page = parse_page::<IndexPage<u64>, { INNER_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();
    assert_eq!(page.inner.node_id, 99);
    assert_eq!(page.inner.current_index, 99);
    assert_eq!(page.inner.current_length, 99);
}

#[test]
fn test_index_page_read_after_create_node_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_create_node.wt.idx")
        .unwrap();

    let page = parse_page::<IndexPage<u32>, { INNER_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();
    assert_eq!(page.inner.node_id, 5);
    assert_eq!(page.inner.current_index, 1);
    assert_eq!(page.inner.current_length, 1);
    assert_eq!(page.inner.slots.first().unwrap(), &0);
    assert_eq!(page.inner.index_values.first().unwrap().key, 5);
    assert_eq!(page.inner.index_values.first().unwrap().link.length, 24);
}

#[test]
fn test_index_page_read_after_insert_at_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_insert_at.wt.idx")
        .unwrap();

    let page = parse_page::<IndexPage<u32>, { INNER_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();
    assert_eq!(page.inner.node_id, 5);
    assert_eq!(page.inner.current_index, 2);
    assert_eq!(page.inner.current_length, 2);
    assert_eq!(page.inner.slots.first().unwrap(), &1);
    assert_eq!(page.inner.index_values.get(1).unwrap().key, 3);
    assert_eq!(page.inner.index_values.get(1).unwrap().link.length, 48);
    assert_eq!(page.inner.index_values.get(1).unwrap().link.offset, 24);
    assert_eq!(page.inner.slots.get(1).unwrap(), &0);
    assert_eq!(page.inner.index_values.first().unwrap().key, 5);
    assert_eq!(page.inner.index_values.first().unwrap().link.length, 24);
}

#[test]
fn test_index_page_read_after_insert_at_with_node_id_update_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_insert_at_with_node_id_update.wt.idx")
        .unwrap();

    let page = parse_page::<IndexPage<u32>, { INNER_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();
    assert_eq!(page.inner.node_id, 7);
    assert_eq!(page.inner.current_index, 2);
    assert_eq!(page.inner.current_length, 2);
    assert_eq!(page.inner.slots.first().unwrap(), &0);
    assert_eq!(page.inner.index_values.first().unwrap().key, 5);
    assert_eq!(page.inner.index_values.first().unwrap().link.length, 24);
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

    let page = parse_page::<IndexPage<u32>, { INNER_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();
    assert_eq!(page.inner.node_id, 3);
    assert_eq!(page.inner.current_index, 0);
    assert_eq!(page.inner.current_length, 1);
    assert_eq!(page.inner.slots.first().unwrap(), &1);
    assert_eq!(page.inner.index_values.get(1).unwrap().key, 3);
    assert_eq!(page.inner.index_values.get(1).unwrap().link.length, 48);
    assert_eq!(page.inner.index_values.get(1).unwrap().link.offset, 24);
}

#[test]
fn test_index_page_read_after_insert_at_removed_place_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_insert_at_removed_place.wt.idx")
        .unwrap();

    let page = parse_page::<IndexPage<u32>, { INNER_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();
    assert_eq!(page.inner.node_id, 7);
    assert_eq!(page.inner.current_index, 3);
    assert_eq!(page.inner.current_length, 3);
    assert_eq!(page.inner.slots.first().unwrap(), &1);
    assert_eq!(page.inner.index_values.get(1).unwrap().key, 3);
    assert_eq!(page.inner.index_values.get(1).unwrap().link.length, 48);
    assert_eq!(page.inner.index_values.get(1).unwrap().link.offset, 24);
    assert_eq!(page.inner.slots.get(1).unwrap(), &0);
    assert_eq!(page.inner.index_values.first().unwrap().key, 6);
    assert_eq!(page.inner.index_values.first().unwrap().link.length, 24);
    assert_eq!(page.inner.index_values.first().unwrap().link.offset, 0);
    assert_eq!(page.inner.slots.get(2).unwrap(), &2);
    assert_eq!(page.inner.index_values.get(2).unwrap().key, 7);
    assert_eq!(page.inner.index_values.get(2).unwrap().link.length, 24);
    assert_eq!(page.inner.index_values.get(2).unwrap().link.offset, 72);
}

#[test]
fn test_index_pages_read_after_creation_of_second_node_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_create_second_node.wt.idx")
        .unwrap();

    let page = parse_page::<IndexPage<u32>, { INNER_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();
    assert_eq!(page.inner.node_id, 5);
    assert_eq!(page.inner.current_index, 1);
    assert_eq!(page.inner.current_length, 1);
    assert_eq!(page.inner.slots.first().unwrap(), &0);
    assert_eq!(page.inner.index_values.first().unwrap().key, 5);
    assert_eq!(page.inner.index_values.first().unwrap().link.length, 24);

    let page = parse_page::<IndexPage<u32>, { INNER_PAGE_SIZE as u32 }>(&mut file, 3).unwrap();
    assert_eq!(page.inner.node_id, 15);
    assert_eq!(page.inner.current_index, 1);
    assert_eq!(page.inner.current_length, 1);
    assert_eq!(page.inner.slots.first().unwrap(), &0);
    assert_eq!(page.inner.index_values.first().unwrap().key, 15);
    assert_eq!(page.inner.index_values.first().unwrap().link.length, 24);
}

#[test]
fn test_index_pages_read_after_creation_of_node_after_remove_node_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_create_node_after_remove.wt.idx")
        .unwrap();

    let page = parse_page::<IndexPage<u32>, { INNER_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();
    assert_eq!(page.inner.node_id, 10);
    assert_eq!(page.inner.current_index, 1);
    assert_eq!(page.inner.current_length, 1);
    assert_eq!(page.inner.slots.first().unwrap(), &0);
    assert_eq!(page.inner.index_values.first().unwrap().key, 10);
    assert_eq!(page.inner.index_values.first().unwrap().link.length, 24);

    let page = parse_page::<IndexPage<u32>, { INNER_PAGE_SIZE as u32 }>(&mut file, 3).unwrap();
    assert_eq!(page.inner.node_id, 15);
    assert_eq!(page.inner.current_index, 1);
    assert_eq!(page.inner.current_length, 1);
    assert_eq!(page.inner.slots.first().unwrap(), &0);
    assert_eq!(page.inner.index_values.first().unwrap().key, 15);
    assert_eq!(page.inner.index_values.first().unwrap().link.length, 24);
}

#[test]
fn test_index_pages_read_full_page() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_insert_at_big_amount.wt.idx")
        .unwrap();

    let page = parse_page::<IndexPage<u32>, { INNER_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();
    assert_eq!(page.inner.node_id, 1000);
    assert_eq!(page.inner.current_index, 907);
    assert_eq!(page.inner.current_length, 907);
    assert_eq!(page.inner.size, page.inner.current_index);
}

#[test]
fn test_index_pages_read_after_node_split() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_split_node.wt.idx")
        .unwrap();

    let page = parse_page::<IndexPage<u32>, { INNER_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();
    assert_eq!(page.inner.node_id, 457);
    assert_eq!(page.inner.current_index, 1);
    assert_eq!(page.inner.current_length, 453);

    let page = parse_page::<IndexPage<u32>, { INNER_PAGE_SIZE as u32 }>(&mut file, 3).unwrap();
    assert_eq!(page.inner.node_id, 1000);
    assert_eq!(page.inner.current_index, 454);
    assert_eq!(page.inner.current_length, 454);
}
