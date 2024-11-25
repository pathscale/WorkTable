use crate::check_if_files_are_same;
use crate::persistence::{get_test_wt, TestSpace};

#[test]
fn test_persist() {
    let table = get_test_wt();
    let mut space: TestSpace = table.into_space();
    // this call will save space file to `tests/db`. It will be `tests/data/test.wt`
    // TODO: How to config this? Maybe we will need to have DATABASE_CONFIG env
    space.persist().unwrap();

    assert!(check_if_files_are_same(
        "tests/data/test.wt".to_string(),
        "tests/data/expected/test_persist.wt".to_string()
    ))
}
