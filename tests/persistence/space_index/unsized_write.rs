use std::fs::copy;

use data_bucket::{Link, INNER_PAGE_SIZE};
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use worktable::prelude::{SpaceIndexOps, SpaceIndexUnsized};

use crate::{check_if_files_are_same, remove_file_if_exists};

mod run_first {
    use super::*;

    #[tokio::test]
    async fn test_space_index_process_create_node() {
        remove_file_if_exists(
            "tests/data/space_index_unsized/process_create_node.wt.idx".to_string(),
        )
        .await;

        let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index_unsized/process_create_node.wt.idx",
            0.into(),
        )
        .await
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::CreateNode {
                max_value: Pair {
                    key: "Something from someone".to_string(),
                    value: Link {
                        page_id: 0.into(),
                        offset: 0,
                        length: 24,
                    },
                },
            })
            .await
            .unwrap();

        assert!(check_if_files_are_same(
            "tests/data/space_index_unsized/process_create_node.wt.idx".to_string(),
            "tests/data/expected/space_index_unsized/process_create_node.wt.idx".to_string()
        ))
    }

    #[tokio::test]
    async fn test_space_index_process_create_second_node() {
        remove_file_if_exists(
            "tests/data/space_index_unsized/process_create_second_node.wt.idx".to_string(),
        )
        .await;
        copy(
            "tests/data/expected/space_index_unsized/process_create_node.wt.idx",
            "tests/data/space_index_unsized/process_create_second_node.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index_unsized/process_create_second_node.wt.idx",
            0.into(),
        )
        .await
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::CreateNode {
                max_value: Pair {
                    key: "Someone from somewhere".to_string(),
                    value: Link {
                        page_id: 1.into(),
                        offset: 24,
                        length: 32,
                    },
                },
            })
            .await
            .unwrap();

        assert!(check_if_files_are_same(
            "tests/data/space_index_unsized/process_create_second_node.wt.idx".to_string(),
            "tests/data/expected/space_index_unsized/process_create_second_node.wt.idx".to_string()
        ))
    }

    #[tokio::test]
    async fn test_space_index_process_remove_node() {
        remove_file_if_exists(
            "tests/data/space_index_unsized/process_remove_node.wt.idx".to_string(),
        )
        .await;
        copy(
            "tests/data/expected/space_index_unsized/process_create_second_node.wt.idx",
            "tests/data/space_index_unsized/process_remove_node.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index_unsized/process_remove_node.wt.idx",
            0.into(),
        )
        .await
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::RemoveNode {
                max_value: Pair {
                    key: "Something from someone".to_string(),
                    value: Link {
                        page_id: 0.into(),
                        offset: 0,
                        length: 24,
                    },
                },
            })
            .await
            .unwrap();

        assert!(check_if_files_are_same(
            "tests/data/space_index_unsized/process_remove_node.wt.idx".to_string(),
            "tests/data/expected/space_index_unsized/process_remove_node.wt.idx".to_string()
        ))
    }

    #[tokio::test]
    async fn test_space_index_process_insert_at() {
        remove_file_if_exists(
            "tests/data/space_index_unsized/process_insert_at.wt.idx".to_string(),
        )
        .await;
        copy(
            "tests/data/expected/space_index_unsized/process_create_node.wt.idx",
            "tests/data/space_index_unsized/process_insert_at.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index_unsized/process_insert_at.wt.idx",
            0.into(),
        )
        .await
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::InsertAt {
                max_value: Pair {
                    key: "Something from someone".to_string(),
                    value: Link {
                        page_id: 0.into(),
                        offset: 0,
                        length: 24,
                    },
                },
                value: Pair {
                    key: "Something else".to_string(),
                    value: Link {
                        page_id: 0.into(),
                        offset: 24,
                        length: 48,
                    },
                },
                index: 0,
            })
            .await
            .unwrap();

        assert!(check_if_files_are_same(
            "tests/data/space_index_unsized/process_insert_at.wt.idx".to_string(),
            "tests/data/expected/space_index_unsized/process_insert_at.wt.idx".to_string()
        ))
    }
}
