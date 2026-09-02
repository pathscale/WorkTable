use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: BorrowedStringKey,
    columns: {
        id: String primary_key,
        value: u64,
    },
    queries: {
        update: {
            BorrowedValueById(value) by id,
        }
        in_place: {
            BorrowedValueById(value) by id,
        }
    }
);

worktable!(
    name: BorrowedTupleKey,
    columns: {
        tenant: String primary_key,
        record: String primary_key,
        value: u64,
    },
);

#[tokio::test]
async fn string_primary_key_accepts_borrowed_forms() {
    let table = BorrowedStringKeyWorkTable::default();
    let id = "tenant".to_owned();
    let row = BorrowedStringKeyRow {
        id: id.clone(),
        value: 7,
    };
    table.insert(row.clone()).await.unwrap();

    assert_eq!(table.select(&id), Some(row.clone()));
    assert_eq!(table.select(id.as_str()), Some(row.clone()));

    let generated = BorrowedStringKeyPrimaryKey::from(&id);
    assert_eq!(table.select(&generated), Some(row));

    table
        .update_borrowed_value_by_id(BorrowedValueByIdQuery { value: 8 }, &id)
        .await
        .unwrap();
    table
        .update_borrowed_value_by_id_in_place(|value| *value += 1, &id)
        .await
        .unwrap();
    assert_eq!(table.select(&id).unwrap().value, 9);
    assert_eq!(
        table
            .select_by_pk_range(id.as_str()..=id.as_str())
            .execute()
            .unwrap()
            .len(),
        1
    );

    table.delete(&id).await.unwrap();
    assert!(table.select(&id).is_none());
}

#[tokio::test]
async fn tuple_primary_key_accepts_a_borrowed_tuple() {
    let table = BorrowedTupleKeyWorkTable::default();
    let key = ("tenant".to_owned(), "record".to_owned());
    let row = BorrowedTupleKeyRow {
        tenant: key.0.clone(),
        record: key.1.clone(),
        value: 11,
    };
    table.insert(row.clone()).await.unwrap();

    assert_eq!(table.select(&key), Some(row));
    table.delete(&key).await.unwrap();
    assert!(table.select(&key).is_none());
}
