use std::fs::{copy, File, OpenOptions};

use data_bucket::{Link, INNER_PAGE_SIZE};
use indexset::cdc::change::ChangeEvent;
use indexset::Pair;
use worktable::prelude::SpaceIndex;

use crate::{check_if_files_are_same, remove_file_if_exists};

#[test]
fn test_space_index_process_create_node() {
    remove_file_if_exists("tests/data/space_index/process_create_node.wt.idx".to_string());

    let file = File::create("tests/data/space_index/process_create_node.wt.idx").unwrap();
    let mut space_index =
        SpaceIndex::<u8, { INNER_PAGE_SIZE as u32 }>::new(file, 0.into()).unwrap();

    space_index
        .process_change_event(ChangeEvent::CreateNode {
            max_value: Pair {
                key: 5,
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
        })
        .unwrap();

    assert!(check_if_files_are_same(
        "tests/data/space_index/process_create_node.wt.idx".to_string(),
        "tests/data/expected/space_index/process_create_node.wt.idx".to_string()
    ))
}

#[test]
fn test_space_index_process_insert_at() {
    remove_file_if_exists("tests/data/space_index/process_insert_at.wt.idx".to_string());
    copy(
        "tests/data/expected/space_index/process_create_node.wt.idx",
        "tests/data/space_index/process_insert_at.wt.idx",
    )
    .unwrap();

    let file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/space_index/process_insert_at.wt.idx")
        .unwrap();
    let mut space_index =
        SpaceIndex::<u8, { INNER_PAGE_SIZE as u32 }>::new(file, 0.into()).unwrap();

    space_index
        .process_change_event(ChangeEvent::InsertAt {
            max_value: Pair {
                key: 5,
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            value: Pair {
                key: 3,
                value: Link {
                    page_id: 0.into(),
                    offset: 24,
                    length: 48,
                },
            },
            index: 0,
        })
        .unwrap();

    assert!(check_if_files_are_same(
        "tests/data/space_index/process_insert_at.wt.idx".to_string(),
        "tests/data/expected/space_index/process_insert_at.wt.idx".to_string()
    ))
}

#[test]
fn test_space_index_process_insert_at_with_node_id_update() {
    remove_file_if_exists(
        "tests/data/space_index/process_insert_at_with_node_id_update.wt.idx".to_string(),
    );
    copy(
        "tests/data/expected/space_index/process_create_node.wt.idx",
        "tests/data/space_index/process_insert_at_with_node_id_update.wt.idx",
    )
    .unwrap();

    let file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/space_index/process_insert_at_with_node_id_update.wt.idx")
        .unwrap();
    let mut space_index =
        SpaceIndex::<u8, { INNER_PAGE_SIZE as u32 }>::new(file, 0.into()).unwrap();

    space_index
        .process_change_event(ChangeEvent::InsertAt {
            max_value: Pair {
                key: 5,
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            value: Pair {
                key: 7,
                value: Link {
                    page_id: 0.into(),
                    offset: 24,
                    length: 48,
                },
            },
            index: 1,
        })
        .unwrap();

    assert!(check_if_files_are_same(
        "tests/data/space_index/process_insert_at_with_node_id_update.wt.idx".to_string(),
        "tests/data/expected/space_index/process_insert_at_with_node_id_update.wt.idx".to_string()
    ))
}

#[test]
fn test_space_index_process_remove_at() {
    remove_file_if_exists("tests/data/space_index/process_remove_at.wt.idx".to_string());
    copy(
        "tests/data/expected/space_index/process_insert_at.wt.idx",
        "tests/data/space_index/process_remove_at.wt.idx",
    )
    .unwrap();

    let file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/space_index/process_remove_at.wt.idx")
        .unwrap();
    let mut space_index =
        SpaceIndex::<u8, { INNER_PAGE_SIZE as u32 }>::new(file, 0.into()).unwrap();

    space_index
        .process_change_event(ChangeEvent::RemoveAt {
            max_value: Pair {
                key: 5,
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            value: Pair {
                key: 3,
                value: Link {
                    page_id: 0.into(),
                    offset: 24,
                    length: 48,
                },
            },
            index: 0,
        })
        .unwrap();

    assert!(check_if_files_are_same(
        "tests/data/space_index/process_remove_at.wt.idx".to_string(),
        "tests/data/expected/space_index/process_create_node.wt.idx".to_string()
    ))
}
