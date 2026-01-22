use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: Test,
    columns: {
        id: u64 primary_key,
        test: u64 primary_key,
        another: i64,
    }
);

#[tokio::test]
async fn insert() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: 1,
        test: 1,
        another: 1,
    };
    let pk = table.insert(row.clone()).unwrap();
    let selected_row = table.select(pk).await.unwrap();

    assert_eq!(selected_row, row);
    assert!(table.select((1, 0)).await.is_none())
}
