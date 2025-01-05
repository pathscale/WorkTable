use data_bucket::Link;
use worktable::prelude::SpaceDataOps;

use crate::persistence::{get_empty_test_wt, TestPersistRow};

#[test]
fn test_file_write() {
    let table = get_empty_test_wt();
    let row = TestPersistRow {
        another: u64::MAX - 1,
        id: u128::MAX - 1,
    };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row).unwrap();
    let mut space = table.get_space().unwrap();

    space
        .data
        .save_data(
            Link {
                page_id: 1.into(),
                length: 32,
                offset: 0,
            },
            &*bytes,
        )
        .unwrap();
}
