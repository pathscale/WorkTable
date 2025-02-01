use crate::{check_if_files_are_same, remove_file_if_exists};
use data_bucket::{Link, INNER_PAGE_SIZE};
use indexset::cdc::change::ChangeEvent;
use indexset::concurrent::map::BTreeMap;
use indexset::core::pair::Pair;
use std::fs::{copy, File, OpenOptions};
use worktable::prelude::SpaceIndex;

#[test]
fn test_indexset_node_creation() {
    remove_file_if_exists("tests/data/space_index/indexset/process_create_node.wt.idx".to_string());

    let file = File::create("tests/data/space_index/indexset/process_create_node.wt.idx").unwrap();
    let mut space_index =
        SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(file, 0.into()).unwrap();
    let indexset = BTreeMap::<u32, Link>::new();
    let (_, cdc) = indexset.insert_cdc(
        5,
        Link {
            page_id: 0.into(),
            offset: 0,
            length: 24,
        },
    );
    for event in cdc {
        space_index.process_change_event(event).unwrap();
    }

    assert!(check_if_files_are_same(
        "tests/data/space_index/indexset/process_create_node.wt.idx".to_string(),
        "tests/data/expected/space_index/process_create_node.wt.idx".to_string()
    ))
}

#[test]
fn test_space_index_process_insert_at() {
    remove_file_if_exists("tests/data/space_index/indexset/process_insert_at.wt.idx".to_string());
    copy(
        "tests/data/expected/space_index/process_create_node.wt.idx",
        "tests/data/space_index/indexset/process_insert_at.wt.idx",
    )
    .unwrap();

    let file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/space_index/indexset/process_insert_at.wt.idx")
        .unwrap();
    let mut space_index =
        SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(file, 0.into()).unwrap();
    let indexset = space_index.parse_indexset().unwrap();
    let (_, cdc) = indexset.insert_cdc(
        3,
        Link {
            page_id: 0.into(),
            offset: 24,
            length: 48,
        },
    );
    for event in cdc {
        space_index.process_change_event(event).unwrap();
    }

    assert!(check_if_files_are_same(
        "tests/data/space_index/indexset/process_insert_at.wt.idx".to_string(),
        "tests/data/expected/space_index/process_insert_at.wt.idx".to_string()
    ))
}
