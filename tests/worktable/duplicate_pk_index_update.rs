use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: DuplicatePrimaryIndex,
    columns: {
        id: u64 primary_key,
        value: u64,
    },
    indexes: {
        id_idx: id unique,
    },
    queries: {
        update: {
            ValueById(value) by id,
        }
    }
);

/// An explicit index on the primary-key field has routing precedence in the
/// generated update query. Its hidden method accepts the raw indexed type, so
/// typed selector dispatch must not wrap that key as the table primary key.
#[tokio::test]
async fn update_by_primary_key_duplicated_as_unique_index_uses_raw_key() {
    let table = DuplicatePrimaryIndexWorkTable::default();
    table
        .insert(DuplicatePrimaryIndexRow { id: 7, value: 1 })
        .await
        .unwrap();

    table
        .update_by_id(7, DuplicatePrimaryIndexColumns::VALUE, 9)
        .await
        .unwrap();

    assert_eq!(table.select(7).unwrap().value, 9);
}
