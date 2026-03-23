use psc_nanoid::alphabet::Base64UrlAlphabet;
use psc_nanoid::{Nanoid, packed_nanoid_type};
use worktable::prelude::*;
use worktable::worktable;

// Create a type alias for packed nanoid (21 chars, Base64Url alphabet -> 16 bytes)
type PackedNanoid21 = packed_nanoid_type!(21, Base64UrlAlphabet);

worktable!(
    name: Test,
    columns: {
        id: PackedNanoid21 primary_key,
        another: i64,
    }
);

#[tokio::test]
async fn insert_and_select() {
    let table = TestWorkTable::default();

    let nanoid: Nanoid<21, Base64UrlAlphabet> = Nanoid::new();
    let packed = PackedNanoid21::pack(&nanoid).unwrap();

    let row = TestRow {
        id: packed,
        another: 42,
    };

    let pk = table.insert(row.clone()).unwrap();
    let selected_row = table.select(pk).unwrap();

    assert_eq!(selected_row, row);

    let unpacked = selected_row.id.unpack().unwrap();
    assert_eq!(unpacked, nanoid);
}

#[tokio::test]
async fn select_nonexistent() {
    let table = TestWorkTable::default();

    let fake_packed = PackedNanoid21::pack(&Nanoid::new()).unwrap();
    assert!(table.select(fake_packed).is_none());
}
