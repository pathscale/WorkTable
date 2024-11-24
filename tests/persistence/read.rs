use std::fs::File;

use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: Test,
    persist: true,
    columns: {
        id: u128 primary_key,
        another: u64,
    },
    indexes: {
        another_idx: another,
    },
);

// fn test_read () {
//     // this call will read space file from `tests/db`. It will be `tests/db/test.wt`
//     // TODO: How to config this? Maybe we will need to have DATABASE_CONFIG env
//     let space = TestSpace::read();
//     let table = space.into_table();
//
//     // Check tables data
// }

#[test]
fn test_info_parse() {
    let mut file = File::open("tests/data/expected/test_persist.wt").unwrap();
    let info = parse_info(&mut file).unwrap();

    assert_eq!(info.header.space_id, 0.into());
    assert_eq!(info.header.page_id, 0.into());
    assert_eq!(info.header.previous_id, 0.into());
    assert_eq!(info.header.next_id, 1.into());
    assert_eq!(info.header.page_type, PageType::SpaceInfo);
    assert_eq!(info.header.data_length, 100);

    assert_eq!(info.inner.id, 0.into());
    assert_eq!(info.inner.page_count, 2);
    assert_eq!(info.inner.name, "Test");
    assert_eq!(info.inner.primary_key_intervals, vec![Interval(1, 1)]);
    assert!(info
        .inner
        .secondary_index_intervals
        .contains_key("another_idx"));
    assert_eq!(
        info.inner.secondary_index_intervals.get("another_idx"),
        Some(&vec![Interval(2, 2)])
    );
    assert_eq!(info.inner.data_intervals, vec![Interval(3, 3)]);
}

#[test]
fn test_index_parse() {
    let mut file = File::open("tests/data/expected/test_persist.wt").unwrap();
    let index = parse_index::<u128, { TEST_PAGE_SIZE as u32 }>(&mut file, 1).unwrap();

    assert_eq!(index.header.space_id, 0.into());
    assert_eq!(index.header.page_id, 1.into());
    assert_eq!(index.header.previous_id, 0.into());
    assert_eq!(index.header.next_id, 2.into());
    assert_eq!(index.header.page_type, PageType::Index);
    assert_eq!(index.header.data_length, 3176);

    let mut key = 1;
    let length = 48;
    let mut offset = 0;
    let page_id = 0.into();

    for val in index.inner.index_values {
        assert_eq!(val.key, key);
        assert_eq!(
            val.link,
            Link {
                page_id,
                offset,
                length,
            }
        );

        key += 1;
        offset += length;
    }
}
