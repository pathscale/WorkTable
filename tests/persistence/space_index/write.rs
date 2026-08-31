use std::fs::copy;

use data_bucket::{INNER_PAGE_SIZE, Link};
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use worktable::prelude::{SpaceIndex, SpaceIndexOps};

use crate::{check_if_files_are_same, remove_file_if_exists};

mod run_first {
    use super::*;

    #[tokio::test]
    async fn test_space_index_process_create_node() {
        remove_file_if_exists("tests/data/space_index/process_create_node.wt.idx".to_string()).await;

        let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index/process_create_node.wt.idx",
            0.into(),
            1,
        )
        .await
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::CreateNode {
                event_id: 0.into(),
                max_value: Pair {
                    key: 5,
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
            "tests/data/space_index/process_create_node.wt.idx".to_string(),
            "tests/data/expected/space_index/process_create_node.wt.idx".to_string()
        ))
    }

    #[tokio::test]
    async fn test_space_index_process_create_second_node() {
        remove_file_if_exists("tests/data/space_index/process_create_second_node.wt.idx".to_string()).await;
        copy(
            "tests/data/expected/space_index/process_create_node.wt.idx",
            "tests/data/space_index/process_create_second_node.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index/process_create_second_node.wt.idx",
            0.into(),
            1,
        )
        .await
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::CreateNode {
                event_id: 0.into(),
                max_value: Pair {
                    key: 15,
                    value: Link {
                        page_id: 1.into(),
                        offset: 0,
                        length: 24,
                    },
                },
            })
            .await
            .unwrap();

        assert!(check_if_files_are_same(
            "tests/data/space_index/process_create_second_node.wt.idx".to_string(),
            "tests/data/expected/space_index/process_create_second_node.wt.idx".to_string()
        ))
    }

    #[tokio::test]
    async fn test_space_index_process_insert_at() {
        remove_file_if_exists("tests/data/space_index/process_insert_at.wt.idx".to_string()).await;
        copy(
            "tests/data/expected/space_index/process_create_node.wt.idx",
            "tests/data/space_index/process_insert_at.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index/process_insert_at.wt.idx",
            0.into(),
            1,
        )
        .await
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::InsertAt {
                event_id: 0.into(),
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
            .await
            .unwrap();

        assert!(check_if_files_are_same(
            "tests/data/space_index/process_insert_at.wt.idx".to_string(),
            "tests/data/expected/space_index/process_insert_at.wt.idx".to_string()
        ))
    }

    #[tokio::test]
    async fn test_space_index_process_insert_at_big_amount() {
        remove_file_if_exists("tests/data/space_index/process_insert_at_big_amount.wt.idx".to_string()).await;
        copy(
            "tests/data/expected/space_index/process_create_node.wt.idx",
            "tests/data/space_index/process_insert_at_big_amount.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index/process_insert_at_big_amount.wt.idx",
            0.into(),
            1,
        )
        .await
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::InsertAt {
                event_id: 0.into(),
                max_value: Pair {
                    key: 5,
                    value: Link {
                        page_id: 0.into(),
                        offset: 0,
                        length: 24,
                    },
                },
                value: Pair {
                    key: 1000,
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

        for i in (6..909).rev() {
            space_index
                .process_change_event(ChangeEvent::InsertAt {
                    event_id: 0.into(),
                    max_value: Pair {
                        key: 1000,
                        value: Link {
                            page_id: 0.into(),
                            offset: 24,
                            length: 24,
                        },
                    },
                    value: Pair {
                        key: i,
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
            "tests/data/space_index/process_insert_at_big_amount.wt.idx".to_string(),
            "tests/data/expected/space_index/process_insert_at_big_amount.wt.idx".to_string()
        ))
    }

    #[tokio::test]
    async fn test_space_index_process_remove_node() {
        remove_file_if_exists("tests/data/space_index/process_remove_node.wt.idx".to_string()).await;
        copy(
            "tests/data/expected/space_index/process_create_second_node.wt.idx",
            "tests/data/space_index/process_remove_node.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index/process_remove_node.wt.idx",
            0.into(),
            1,
        )
        .await
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::RemoveNode {
                event_id: 0.into(),
                max_value: Pair {
                    key: 5,
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
            "tests/data/space_index/process_remove_node.wt.idx".to_string(),
            "tests/data/expected/space_index/process_remove_node.wt.idx".to_string()
        ))
    }
}

#[tokio::test]
async fn test_space_index_process_insert_at_with_node_id_update() {
    remove_file_if_exists("tests/data/space_index/process_insert_at_with_node_id_update.wt.idx".to_string()).await;
    copy(
        "tests/data/expected/space_index/process_create_node.wt.idx",
        "tests/data/space_index/process_insert_at_with_node_id_update.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
        "tests/data/space_index/process_insert_at_with_node_id_update.wt.idx",
        0.into(),
        1,
    )
    .await
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::InsertAt {
            event_id: 0.into(),
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
        .await
        .unwrap();

    assert!(check_if_files_are_same(
        "tests/data/space_index/process_insert_at_with_node_id_update.wt.idx".to_string(),
        "tests/data/expected/space_index/process_insert_at_with_node_id_update.wt.idx".to_string()
    ))
}

#[tokio::test]
async fn test_space_index_process_remove_at() {
    remove_file_if_exists("tests/data/space_index/process_remove_at.wt.idx".to_string()).await;
    copy(
        "tests/data/expected/space_index/process_insert_at.wt.idx",
        "tests/data/space_index/process_remove_at.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
        "tests/data/space_index/process_remove_at.wt.idx",
        0.into(),
        1,
    )
    .await
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::RemoveAt {
            event_id: 0.into(),
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
        .await
        .unwrap();

    assert!(check_if_files_are_same(
        "tests/data/space_index/process_remove_at.wt.idx".to_string(),
        "tests/data/expected/space_index/process_create_node.wt.idx".to_string()
    ))
}

#[tokio::test]
async fn test_space_index_process_remove_at_node_id() {
    remove_file_if_exists("tests/data/space_index/process_remove_at_node_id.wt.idx".to_string()).await;
    copy(
        "tests/data/expected/space_index/process_insert_at.wt.idx",
        "tests/data/space_index/process_remove_at_node_id.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
        "tests/data/space_index/process_remove_at_node_id.wt.idx",
        0.into(),
        1,
    )
    .await
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::RemoveAt {
            event_id: 0.into(),
            max_value: Pair {
                key: 5,
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            value: Pair {
                key: 5,
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
        "tests/data/space_index/process_remove_at_node_id.wt.idx".to_string(),
        "tests/data/expected/space_index/process_remove_at_node_id.wt.idx".to_string()
    ))
}

#[tokio::test]
async fn test_space_index_process_insert_at_removed_place() {
    remove_file_if_exists("tests/data/space_index/process_insert_at_removed_place.wt.idx".to_string()).await;
    copy(
        "tests/data/expected/space_index/process_insert_at.wt.idx",
        "tests/data/space_index/process_insert_at_removed_place.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
        "tests/data/space_index/process_insert_at_removed_place.wt.idx",
        0.into(),
        1,
    )
    .await
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::InsertAt {
            event_id: 0.into(),
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
                key: 7,
                value: Link {
                    page_id: 0.into(),
                    offset: 72,
                    length: 24,
                },
            },
            value: Pair {
                key: 5,
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
                key: 7,
                value: Link {
                    page_id: 0.into(),
                    offset: 72,
                    length: 24,
                },
            },
            value: Pair {
                key: 6,
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
        "tests/data/space_index/process_insert_at_removed_place.wt.idx".to_string(),
        "tests/data/expected/space_index/process_insert_at_removed_place.wt.idx".to_string()
    ))
}

#[tokio::test]
async fn test_space_index_process_create_node_after_remove() {
    remove_file_if_exists("tests/data/space_index/process_create_node_after_remove.wt.idx".to_string()).await;
    copy(
        "tests/data/expected/space_index/process_remove_node.wt.idx",
        "tests/data/space_index/process_create_node_after_remove.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
        "tests/data/space_index/process_create_node_after_remove.wt.idx",
        0.into(),
        1,
    )
    .await
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::CreateNode {
            event_id: 0.into(),
            max_value: Pair {
                key: 10,
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
        "tests/data/space_index/process_create_node_after_remove.wt.idx".to_string(),
        "tests/data/expected/space_index/process_create_node_after_remove.wt.idx".to_string()
    ))
}

#[tokio::test]
async fn test_space_index_process_split_node() {
    remove_file_if_exists("tests/data/space_index/process_split_node.wt.idx".to_string()).await;
    copy(
        "tests/data/expected/space_index/process_insert_at_big_amount.wt.idx",
        "tests/data/space_index/process_split_node.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
        "tests/data/space_index/process_split_node.wt.idx",
        0.into(),
        1,
    )
    .await
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::SplitNode {
            event_id: 0.into(),
            max_value: Pair {
                key: 1000,
                value: Link {
                    page_id: 0.into(),
                    offset: 24,
                    length: 24,
                },
            },
            split_index: 453,
        })
        .await
        .unwrap();

    assert!(check_if_files_are_same(
        "tests/data/space_index/process_split_node.wt.idx".to_string(),
        "tests/data/expected/space_index/process_split_node.wt.idx".to_string()
    ))
}

/// A split re-keys the left page and registers the right page under the
/// pre-split maximum; a later remove of that maximum re-keys the right page
/// too. Events generated before those re-keys still name historical maxima.
/// The sized batch path used to panic on the table-of-contents miss; with the
/// PageAliases port it resolves the historical identity to the same page.
#[tokio::test]
async fn batch_split_then_max_remove_then_historical_identity_insert_applies() {
    remove_file_if_exists("tests/data/space_index/batch_alias.wt.idx".to_string()).await;

    let mut space_index =
        SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new("tests/data/space_index/batch_alias.wt.idx", 0.into(), 1)
            .await
            .unwrap();

    fn link(offset: u32) -> Link {
        Link {
            page_id: 0.into(),
            offset,
            length: 24,
        }
    }
    fn pair(key: u32) -> Pair<u32, Link> {
        Pair {
            key,
            value: link(key),
        }
    }

    let events = vec![
        ChangeEvent::CreateNode {
            event_id: 0.into(),
            max_value: pair(30),
        },
        ChangeEvent::InsertAt {
            event_id: 0.into(),
            max_value: pair(30),
            value: pair(10),
            index: 0,
        },
        ChangeEvent::InsertAt {
            event_id: 0.into(),
            max_value: pair(30),
            value: pair(20),
            index: 1,
        },
        // Left page keeps [10]; the right page [20, 30] stays keyed by the
        // pre-split maximum 30.
        ChangeEvent::SplitNode {
            event_id: 0.into(),
            max_value: pair(30),
            split_index: 1,
        },
        // Removing the right page's maximum re-keys it to 20 mid-batch...
        ChangeEvent::RemoveAt {
            event_id: 0.into(),
            max_value: pair(30),
            value: pair(30),
            index: 1,
        },
        // ...while this event still names the historical maximum 30.
        ChangeEvent::InsertAt {
            event_id: 0.into(),
            max_value: pair(30),
            value: pair(25),
            index: 1,
        },
    ];

    space_index.process_change_event_batch(events).await.unwrap();

    let restored = space_index.parse_indexset().await.unwrap();
    for key in [10u32, 20, 25] {
        assert!(restored.contains_key(&key), "key {key} must survive the batch replay");
    }
    assert!(
        !restored.contains_key(&30),
        "the removed maximum must not reappear after replay"
    );
}

/// End-to-end equivalence: one batch carrying a real CDC stream with node
/// splits, maximum removals, and re-inserts must replay into exactly the
/// source index. This is the sized twin of the unsized alias machinery's
/// production scenario.
#[tokio::test]
async fn batch_replay_of_real_cdc_stream_with_splits_matches_the_source() {
    remove_file_if_exists("tests/data/space_index/batch_cdc_replay.wt.idx".to_string()).await;

    let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
        "tests/data/space_index/batch_cdc_replay.wt.idx",
        0.into(),
        1,
    )
    .await
    .unwrap();

    let source = space_index.parse_indexset().await.unwrap();
    let mut events = Vec::new();
    let link = |offset: u32| Link {
        page_id: 0.into(),
        offset,
        length: 24,
    };

    // Enough keys to split nodes several times inside one batch.
    for key in 0..3_000u32 {
        let (_, cdc) = source.insert_cdc(key, link(key));
        events.extend(cdc);
    }
    // Remove maxima so node identities are re-keyed mid-batch.
    for key in (2_980..3_000u32).rev() {
        let (_, cdc) = source.remove_cdc(&key);
        events.extend(cdc);
    }
    // And keep inserting past the removed maxima.
    for key in 3_000..3_020u32 {
        let (_, cdc) = source.insert_cdc(key, link(key));
        events.extend(cdc);
    }

    space_index.process_change_event_batch(events).await.unwrap();

    let restored = space_index.parse_indexset().await.unwrap();
    assert_eq!(restored.len(), source.len());
    for key in 0..2_980u32 {
        assert!(restored.contains_key(&key), "key {key} lost in batch replay");
    }
    for key in 2_980..3_000u32 {
        assert!(!restored.contains_key(&key), "removed key {key} reappeared");
    }
    for key in 3_000..3_020u32 {
        assert!(restored.contains_key(&key), "key {key} lost in batch replay");
    }
}
