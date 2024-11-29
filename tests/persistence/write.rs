use crate::{check_if_files_are_same, remove_file_if_exists};

use crate::persistence::{get_test_wt, get_test_wt_without_secondary_indexes};

#[test]
fn test_persist() {
    remove_file_if_exists("tests/data/test.wt".to_string());

    let table = get_test_wt();
    table.persist().unwrap();

    assert!(check_if_files_are_same(
        "tests/data/test.wt".to_string(),
        "tests/data/expected/test.wt".to_string()
    ))
}

#[test]
fn test_persist_without_secondary_indexes() {
    remove_file_if_exists("tests/data/testwithoutsecondaryindexes.wt".to_string());

    let table = get_test_wt_without_secondary_indexes();
    table.persist().unwrap();

    assert!(check_if_files_are_same(
        "tests/data/testwithoutsecondaryindexes.wt".to_string(),
        "tests/data/expected_no_secondary_indexes/testwithoutsecondaryindexes.wt".to_string()
    ))
}