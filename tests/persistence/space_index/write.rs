use crate::{check_if_files_are_same, remove_file_if_exists};
use data_bucket::{Link, INNER_PAGE_SIZE};
use indexset::cdc::change::ChangeEvent;
use indexset::Pair;
use std::fs::File;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use worktable::prelude::{IndexTableOfContents, SpaceIndex};

#[test]
fn test_persist_space_index() {
    remove_file_if_exists("tests/data/persist_space_index.wt.idx".to_string());

    let file = File::create("tests/data/persist_space_index.wt.idx").unwrap();
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
        "tests/data/persist_space_index.wt.idx".to_string(),
        "tests/data/expected/persist_space_index.wt.idx".to_string()
    ))
}
