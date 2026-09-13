use data_bucket::DEFAULT_PAGE_STRIDE;
use std::fs::copy;

use data_bucket::{INNER_PAGE_SIZE, Link};
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use worktable::prelude::{SpaceIndexOps, SpaceIndexUnsized};

use crate::{check_if_files_are_same, remove_file_if_exists};

mod run_first {
    use super::*;

    #[tokio::test]
    async fn test_space_index_process_create_node() {
        remove_file_if_exists("tests/data/space_index_unsized/process_create_node.wt.idx".to_string()).await;

        let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }, DEFAULT_PAGE_STRIDE>::new(
            "tests/data/space_index_unsized/process_create_node.wt.idx",
            0.into(),
            1,
        )
        .await
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::CreateNode {
                event_id: 0.into(),
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
        remove_file_if_exists("tests/data/space_index_unsized/process_create_second_node.wt.idx".to_string()).await;
        copy(
            "tests/data/expected/space_index_unsized/process_create_node.wt.idx",
            "tests/data/space_index_unsized/process_create_second_node.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }, DEFAULT_PAGE_STRIDE>::new(
            "tests/data/space_index_unsized/process_create_second_node.wt.idx",
            0.into(),
            1,
        )
        .await
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::CreateNode {
                event_id: 0.into(),
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
        remove_file_if_exists("tests/data/space_index_unsized/process_remove_node.wt.idx".to_string()).await;
        copy(
            "tests/data/expected/space_index_unsized/process_create_second_node.wt.idx",
            "tests/data/space_index_unsized/process_remove_node.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }, DEFAULT_PAGE_STRIDE>::new(
            "tests/data/space_index_unsized/process_remove_node.wt.idx",
            0.into(),
            1,
        )
        .await
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::RemoveNode {
                event_id: 0.into(),
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
        remove_file_if_exists("tests/data/space_index_unsized/process_insert_at.wt.idx".to_string()).await;
        copy(
            "tests/data/expected/space_index_unsized/process_create_node.wt.idx",
            "tests/data/space_index_unsized/process_insert_at.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }, DEFAULT_PAGE_STRIDE>::new(
            "tests/data/space_index_unsized/process_insert_at.wt.idx",
            0.into(),
            1,
        )
        .await
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::InsertAt {
                event_id: 0.into(),
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

    #[tokio::test]
    async fn test_space_index_process_insert_at_big_amount() {
        remove_file_if_exists("tests/data/space_index_unsized/process_insert_at_big_amount.wt.idx".to_string()).await;
        copy(
            "tests/data/expected/space_index_unsized/process_create_node.wt.idx",
            "tests/data/space_index_unsized/process_insert_at_big_amount.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }, DEFAULT_PAGE_STRIDE>::new(
            "tests/data/space_index_unsized/process_insert_at_big_amount.wt.idx",
            0.into(),
            1,
        )
        .await
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::InsertAt {
                event_id: 0.into(),
                max_value: Pair {
                    key: "Something from someone".to_string(),
                    value: Link {
                        page_id: 0.into(),
                        offset: 0,
                        length: 24,
                    },
                },
                value: Pair {
                    key: "Something from someone _100".to_string(),
                    value: Link {
                        page_id: 0.into(),
                        offset: 24,
                        length: 24,
                    },
                },
                index: 1,
            })
            .await
            .unwrap();

        for i in (1..100).rev() {
            space_index
                .process_change_event(ChangeEvent::InsertAt {
                    event_id: 0.into(),
                    max_value: Pair {
                        key: "Something from someone _100".to_string(),
                        value: Link {
                            page_id: 0.into(),
                            offset: 24,
                            length: 24,
                        },
                    },
                    value: Pair {
                        key: format!("Something from someone {i}"),
                        value: Link {
                            page_id: 0.into(),
                            offset: i * 24,
                            length: 24,
                        },
                    },
                    index: 1,
                })
                .await
                .unwrap();
        }

        assert!(check_if_files_are_same(
            "tests/data/space_index_unsized/process_insert_at_big_amount.wt.idx".to_string(),
            "tests/data/expected/space_index_unsized/process_insert_at_big_amount.wt.idx".to_string()
        ))
    }
}

#[tokio::test]
async fn test_space_index_process_remove_at() {
    remove_file_if_exists("tests/data/space_index_unsized/process_remove_at.wt.idx".to_string()).await;
    copy(
        "tests/data/expected/space_index_unsized/process_insert_at.wt.idx",
        "tests/data/space_index_unsized/process_remove_at.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }, DEFAULT_PAGE_STRIDE>::new(
        "tests/data/space_index_unsized/process_remove_at.wt.idx",
        0.into(),
        1,
    )
    .await
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::RemoveAt {
            event_id: 0.into(),
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
        "tests/data/space_index_unsized/process_remove_at.wt.idx".to_string(),
        "tests/data/expected/space_index_unsized/process_remove_at.wt.idx".to_string()
    ))
}

#[tokio::test]
async fn test_space_index_process_remove_at_node_id() {
    remove_file_if_exists("tests/data/space_index_unsized/process_remove_at_node_id.wt.idx".to_string()).await;
    copy(
        "tests/data/expected/space_index_unsized/process_insert_at.wt.idx",
        "tests/data/space_index_unsized/process_remove_at_node_id.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }, DEFAULT_PAGE_STRIDE>::new(
        "tests/data/space_index_unsized/process_remove_at_node_id.wt.idx",
        0.into(),
        1,
    )
    .await
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::RemoveAt {
            event_id: 0.into(),
            max_value: Pair {
                key: "Something from someone".to_string(),
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            value: Pair {
                key: "Something from someone".to_string(),
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            index: 1,
        })
        .await
        .unwrap();

    assert!(check_if_files_are_same(
        "tests/data/space_index_unsized/process_remove_at_node_id.wt.idx".to_string(),
        "tests/data/expected/space_index_unsized/process_remove_at_node_id.wt.idx".to_string()
    ))
}

#[tokio::test]
async fn test_space_index_process_insert_at_with_node_id_update() {
    remove_file_if_exists("tests/data/space_index_unsized/process_insert_at_with_node_id_update.wt.idx".to_string())
        .await;
    copy(
        "tests/data/expected/space_index_unsized/process_create_node.wt.idx",
        "tests/data/space_index_unsized/process_insert_at_with_node_id_update.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }, DEFAULT_PAGE_STRIDE>::new(
        "tests/data/space_index_unsized/process_insert_at_with_node_id_update.wt.idx",
        0.into(),
        1,
    )
    .await
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::InsertAt {
            event_id: 0.into(),
            max_value: Pair {
                key: "Something from someone".to_string(),
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            value: Pair {
                key: "Something from someone 1".to_string(),
                value: Link {
                    page_id: 0.into(),
                    offset: 24,
                    length: 48,
                },
            },
            index: 1,
        })
        .await
        .unwrap();

    assert!(check_if_files_are_same(
        "tests/data/space_index_unsized/process_insert_at_with_node_id_update.wt.idx".to_string(),
        "tests/data/expected/space_index_unsized/process_insert_at_with_node_id_update.wt.idx".to_string()
    ))
}

#[tokio::test]
async fn test_space_index_process_insert_at_removed_place() {
    remove_file_if_exists("tests/data/space_index_unsized/process_insert_at_removed_place.wt.idx".to_string()).await;
    copy(
        "tests/data/expected/space_index_unsized/process_insert_at.wt.idx",
        "tests/data/space_index_unsized/process_insert_at_removed_place.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }, DEFAULT_PAGE_STRIDE>::new(
        "tests/data/space_index_unsized/process_insert_at_removed_place.wt.idx",
        0.into(),
        1,
    )
    .await
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::InsertAt {
            event_id: 0.into(),
            max_value: Pair {
                key: "Something from someone".to_string(),
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            value: Pair {
                key: "Something from someone 1".to_string(),
                value: Link {
                    page_id: 0.into(),
                    offset: 72,
                    length: 24,
                },
            },
            index: 2,
        })
        .await
        .unwrap();
    space_index
        .process_change_event(ChangeEvent::RemoveAt {
            event_id: 0.into(),
            max_value: Pair {
                key: "Something from someone 1".to_string(),
                value: Link {
                    page_id: 0.into(),
                    offset: 72,
                    length: 24,
                },
            },
            value: Pair {
                key: "Something from someone".to_string(),
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            index: 1,
        })
        .await
        .unwrap();
    space_index
        .process_change_event(ChangeEvent::InsertAt {
            event_id: 0.into(),
            max_value: Pair {
                key: "Something from someone 1".to_string(),
                value: Link {
                    page_id: 0.into(),
                    offset: 72,
                    length: 24,
                },
            },
            value: Pair {
                key: "Something from someone 0".to_string(),
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            index: 1,
        })
        .await
        .unwrap();

    assert!(check_if_files_are_same(
        "tests/data/space_index_unsized/process_insert_at_removed_place.wt.idx".to_string(),
        "tests/data/expected/space_index_unsized/process_insert_at_removed_place.wt.idx".to_string()
    ))
}

#[tokio::test]
async fn test_space_index_process_create_node_after_remove() {
    remove_file_if_exists("tests/data/space_index_unsized/process_create_node_after_remove.wt.idx".to_string()).await;
    copy(
        "tests/data/expected/space_index_unsized/process_remove_node.wt.idx",
        "tests/data/space_index_unsized/process_create_node_after_remove.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }, DEFAULT_PAGE_STRIDE>::new(
        "tests/data/space_index_unsized/process_create_node_after_remove.wt.idx",
        0.into(),
        1,
    )
    .await
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::CreateNode {
            event_id: 0.into(),
            max_value: Pair {
                key: "Something else".to_string(),
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
        "tests/data/space_index_unsized/process_create_node_after_remove.wt.idx".to_string(),
        "tests/data/expected/space_index_unsized/process_create_node_after_remove.wt.idx".to_string()
    ))
}

#[tokio::test]
async fn test_space_index_process_split_node() {
    remove_file_if_exists("tests/data/space_index_unsized/process_split_node.wt.idx".to_string()).await;
    copy(
        "tests/data/expected/space_index_unsized/process_insert_at_big_amount.wt.idx",
        "tests/data/space_index_unsized/process_split_node.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }, DEFAULT_PAGE_STRIDE>::new(
        "tests/data/space_index_unsized/process_split_node.wt.idx",
        0.into(),
        1,
    )
    .await
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::SplitNode {
            event_id: 0.into(),
            max_value: Pair {
                key: "Something from someone _100".to_string(),
                value: Link {
                    page_id: 0.into(),
                    offset: 24,
                    length: 24,
                },
            },
            split_index: 53,
        })
        .await
        .unwrap();

    assert!(check_if_files_are_same(
        "tests/data/space_index_unsized/process_split_node.wt.idx".to_string(),
        "tests/data/expected/space_index_unsized/process_split_node.wt.idx".to_string()
    ))
}

#[tokio::test]
async fn max_removed_at_batch_boundary_does_not_orphan_the_next_unsized_event() {
    let path = "tests/data/space_index_unsized/cross_batch_max.wt.idx";
    remove_file_if_exists(path.to_string()).await;

    let mut space_index =
        SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }, DEFAULT_PAGE_STRIDE>::new(path, 0.into(), 1)
            .await
            .unwrap();
    let pair = |key: &str, offset: u32| Pair {
        key: key.to_owned(),
        value: Link {
            page_id: 0.into(),
            offset,
            length: 24,
        },
    };

    space_index
        .process_change_event_batch(vec![
            ChangeEvent::CreateNode {
                event_id: 0.into(),
                max_value: pair("30", 30),
            },
            ChangeEvent::InsertAt {
                event_id: 0.into(),
                max_value: pair("30", 30),
                value: pair("10", 10),
                index: 0,
            },
            ChangeEvent::InsertAt {
                event_id: 0.into(),
                max_value: pair("30", 30),
                value: pair("20", 20),
                index: 1,
            },
            ChangeEvent::RemoveAt {
                event_id: 0.into(),
                max_value: pair("30", 30),
                value: pair("30", 30),
                index: 2,
            },
        ])
        .await
        .unwrap();

    space_index
        .process_change_event_batch(vec![ChangeEvent::InsertAt {
            event_id: 0.into(),
            max_value: pair("30", 30),
            value: pair("25", 25),
            index: 2,
        }])
        .await
        .unwrap();

    let restored = space_index.parse_indexset().await.unwrap();
    for key in ["10", "20", "25"] {
        assert!(restored.contains_key(key), "key {key} must survive cross-batch replay");
    }
    assert!(!restored.contains_key("30"));
}
