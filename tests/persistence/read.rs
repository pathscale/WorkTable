use std::fs::File;
use worktable::worktable;
use worktable::prelude::*;


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
    let info = TestSpace::<TEST_PAGE_SIZE>::parse_info(&mut file).unwrap();

    print!("{:?}", info)
}