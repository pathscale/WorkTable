use crate::persistence::{TestPersistRow, TestPersistSpace, TEST_PERSIST_PAGE_SIZE};
use data_bucket::Link;
use std::fs::OpenOptions;
use worktable::prelude::SpaceData;

#[test]
fn test_file_write() {
    let row = TestPersistRow {
        another: u64::MAX - 1,
        id: u128::MAX - 1,
    };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row).unwrap();
    let mut space = TestPersistSpace::<{ TEST_PERSIST_PAGE_SIZE }> {
        file: OpenOptions::new()
            .write(true)
            .open("tests/data/test_persist.wt")
            .unwrap(),
    };

    space
        .save_data(
            Link {
                page_id: 3.into(),
                length: 32,
                offset: 0,
            },
            &*bytes,
        )
        .unwrap();
}
