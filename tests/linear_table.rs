use worktable::{LinearInsertError, LinearTable};

#[derive(
    Clone,
    Debug,
    PartialEq,
    worktable::prelude::rkyv::Archive,
    worktable::prelude::rkyv::Serialize,
    worktable::prelude::rkyv::Deserialize,
)]
#[rkyv(crate = worktable::prelude::rkyv)]
struct SavedRow {
    line: u32,
}

#[test]
fn push_preserves_duplicates_and_exposes_the_exact_contiguous_order() {
    let mut table = LinearTable::with_capacity(3);
    assert_eq!(table.push(20, "later"), 0);
    assert_eq!(table.push(10, "first"), 1);
    assert_eq!(table.push(20, "duplicate"), 2);

    assert_eq!(table.rows(), &[(20, "later"), (10, "first"), (20, "duplicate")]);
    assert_eq!(table.as_ref(), table.rows());
}

#[test]
fn a_frozen_sorted_slice_supports_predecessor_search_without_an_index() {
    let table = LinearTable::from(vec![(0_u64, 10_u32), (16, 11), (32, 12)]);
    let rows = table.rows();
    let after = rows.partition_point(|(offset, _)| *offset <= 24);

    assert_eq!(after, 2);
    assert_eq!(rows.get(after - 1), Some(&(16, 11)));
}

#[test]
fn insert_is_the_explicit_unique_alternative_to_duplicate_preserving_push() {
    let mut table = LinearTable::new();
    assert_eq!(table.insert(7, "first"), Ok(0));
    assert_eq!(table.insert(7, "second"), Err(LinearInsertError::DuplicateKey(7)));
    assert_eq!(table.rows(), &[(7, "first")]);
}

#[test]
fn vec_pages_round_trip_duplicates_and_append_in_insertion_order() {
    let first = LinearTable::from(vec![(7_u64, SavedRow { line: 10 }), (7_u64, SavedRow { line: 11 })]);
    let second = LinearTable::from(vec![(9_u64, SavedRow { line: 12 })]);

    let mut pages = first.unload().expect("rows fit");
    pages.extend_from_slice(&second.unload_appending(0).expect("rows fit"));
    let loaded = LinearTable::<u64, SavedRow>::load(&pages).expect("valid pages");

    assert_eq!(
        loaded,
        LinearTable::from(vec![
            (7, SavedRow { line: 10 }),
            (7, SavedRow { line: 11 }),
            (9, SavedRow { line: 12 }),
        ])
    );
}
