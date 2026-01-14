use uuid::Uuid;

use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: Test,
    columns: {
        id: u64 primary_key autoincrement,
        test: u64 optional,
        another: u64,
        exchange: i32,
    },
    indexes: {
        another_idx: another unique,
        exchnage_idx: exchange,
    },
    queries: {
        update: {
            TestById(test) by id,
            TestByAnother(test) by another,
            TestByExchange(test) by exchange,
        }
    }
);

#[tokio::test]
async fn update() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row.clone()).unwrap();
    let new_row = TestRow {
        id: pk.clone().into(),
        test: Some(1),
        another: 1,
        exchange: 1,
    };
    table.update(new_row.clone()).await.unwrap();
    let selected_row = table.select(pk).unwrap();
    assert_eq!(selected_row, new_row);
}

#[tokio::test]
async fn update_by_another() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row.clone()).unwrap();
    table
        .update_test_by_another(TestByAnotherQuery { test: Some(1) }, 1)
        .await
        .unwrap();
    let selected_row = table.select(pk).unwrap();
    assert_eq!(selected_row.test, Some(1));
}

#[tokio::test]
async fn update_by_exchange() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row.clone()).unwrap();
    table
        .update_test_by_exchange(TestByExchangeQuery { test: Some(1) }, 1)
        .await
        .unwrap();
    let selected_row = table.select(pk).unwrap();
    assert_eq!(selected_row.test, Some(1));
}

#[tokio::test]
async fn update_none_to_some() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row.clone()).unwrap();
    assert_eq!(table.select(pk.clone()).unwrap().test, None);

    table
        .update_test_by_id(TestByIdQuery { test: Some(42) }, pk.clone())
        .await
        .unwrap();

    let selected_row = table.select(pk).unwrap();
    assert_eq!(selected_row.test, Some(42));
}

#[tokio::test]
async fn update_some_to_none() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: Some(100),
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row.clone()).unwrap();
    assert_eq!(table.select(pk.clone()).unwrap().test, Some(100));

    table
        .update_test_by_id(TestByIdQuery { test: None }, pk.clone())
        .await
        .unwrap();

    let selected_row = table.select(pk).unwrap();
    assert_eq!(selected_row.test, None);
}

#[tokio::test]
async fn update_multiple_values() {
    let table = TestWorkTable::default();

    let row1 = TestRow {
        id: table.get_next_pk().into(),
        test: Some(10),
        another: 1,
        exchange: 1,
    };
    let pk1 = table.insert(row1).unwrap();

    let row2 = TestRow {
        id: table.get_next_pk().into(),
        test: Some(20),
        another: 2,
        exchange: 2,
    };
    let pk2 = table.insert(row2).unwrap();

    table
        .update_test_by_id(TestByIdQuery { test: Some(30) }, pk1.clone())
        .await
        .unwrap();

    assert_eq!(table.select(pk1).unwrap().test, Some(30));
    assert_eq!(table.select(pk2).unwrap().test, Some(20));
}

worktable! (
    name: TestCustom,
    columns: {
        id: u64 primary_key autoincrement,
        test: Uuid optional,
        another: u64,
        exchange: i32,
    },
    indexes: {
        another_idx: another unique,
        exchnage_idx: exchange,
    },
    queries: {
        update: {
            CustomTestById(test) by id,
            CustomTestByAnother(test) by another,
            CustomTestByExchange(test) by exchange,
        }
    }
);

#[tokio::test]
async fn custom_update() {
    let table = TestCustomWorkTable::default();
    let row = TestCustomRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row.clone()).unwrap();
    let test_uuid = Uuid::new_v4();
    let new_row = TestCustomRow {
        id: pk.clone().into(),
        test: Some(test_uuid),
        another: 1,
        exchange: 1,
    };
    table.update(new_row.clone()).await.unwrap();
    let selected_row = table.select(pk).unwrap();
    assert_eq!(selected_row, new_row);
}

#[tokio::test]
async fn custom_update_by_another() {
    let table = TestCustomWorkTable::default();
    let row = TestCustomRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row.clone()).unwrap();
    let test_uuid = Uuid::new_v4();
    table
        .update_custom_test_by_another(
            CustomTestByAnotherQuery {
                test: Some(test_uuid),
            },
            1,
        )
        .await
        .unwrap();
    let selected_row = table.select(pk).unwrap();
    assert_eq!(selected_row.test, Some(test_uuid));
}

#[tokio::test]
async fn custom_update_by_exchange() {
    let table = TestCustomWorkTable::default();
    let row = TestCustomRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row.clone()).unwrap();
    let test_uuid = Uuid::new_v4();
    table
        .update_custom_test_by_exchange(
            CustomTestByExchangeQuery {
                test: Some(test_uuid),
            },
            1,
        )
        .await
        .unwrap();
    let selected_row = table.select(pk).unwrap();
    assert_eq!(selected_row.test, Some(test_uuid));
}

#[tokio::test]
async fn custom_update_none_to_some() {
    let table = TestCustomWorkTable::default();
    let row = TestCustomRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row.clone()).unwrap();
    assert_eq!(table.select(pk.clone()).unwrap().test, None);

    let test_uuid = Uuid::new_v4();
    table
        .update_custom_test_by_id(
            CustomTestByIdQuery {
                test: Some(test_uuid),
            },
            pk.clone(),
        )
        .await
        .unwrap();

    let selected_row = table.select(pk).unwrap();
    assert_eq!(selected_row.test, Some(test_uuid));
}

#[tokio::test]
async fn custom_update_some_to_none() {
    let table = TestCustomWorkTable::default();
    let test_uuid = Uuid::new_v4();
    let row = TestCustomRow {
        id: table.get_next_pk().into(),
        test: Some(test_uuid),
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row.clone()).unwrap();
    assert_eq!(table.select(pk.clone()).unwrap().test, Some(test_uuid));

    table
        .update_custom_test_by_id(CustomTestByIdQuery { test: None }, pk.clone())
        .await
        .unwrap();

    let selected_row = table.select(pk).unwrap();
    assert_eq!(selected_row.test, None);
}

#[tokio::test]
async fn custom_update_multiple_uuids() {
    let table = TestCustomWorkTable::default();
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    let row1 = TestCustomRow {
        id: table.get_next_pk().into(),
        test: Some(uuid1),
        another: 1,
        exchange: 1,
    };
    let pk1 = table.insert(row1).unwrap();

    let row2 = TestCustomRow {
        id: table.get_next_pk().into(),
        test: Some(uuid2),
        another: 2,
        exchange: 2,
    };
    let pk2 = table.insert(row2).unwrap();

    let uuid3 = Uuid::new_v4();
    table
        .update_custom_test_by_id(CustomTestByIdQuery { test: Some(uuid3) }, pk1.clone())
        .await
        .unwrap();

    assert_eq!(table.select(pk1).unwrap().test, Some(uuid3));
    assert_eq!(table.select(pk2).unwrap().test, Some(uuid2));
}

worktable! (
    name: TestIndex,
    columns: {
        id: u64 primary_key autoincrement,
        test: Uuid optional,
        another: u64,
        exchange: i32,
    },
    indexes: {
        another_idx: another unique,
        test_idx: test,
        exchnage_idx: exchange,
    },
    queries: {
        update: {
            IndexTestById(test) by id,
            IndexTestByAnother(test) by another,
            IndexTestByExchange(test) by exchange,
        }
    }
);

#[tokio::test]
async fn indexed_insert_and_select_by_uuid_some() {
    let table = TestIndexWorkTable::default();
    let test_uuid = Uuid::new_v4();

    let row = TestIndexRow {
        id: table.get_next_pk().into(),
        test: Some(test_uuid),
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row.clone()).unwrap();

    // Select by the indexed UUID field with Some value
    let result = table.select_by_test(Some(test_uuid)).execute().unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].id, pk.0);
}

#[tokio::test]
async fn indexed_select_by_uuid_none() {
    let table = TestIndexWorkTable::default();

    let row = TestIndexRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row).unwrap();

    // Select by None in the indexed field
    let result = table.select_by_test(None).execute().unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].id, pk.0);
}

#[tokio::test]
async fn indexed_multiple_rows_same_uuid() {
    let table = TestIndexWorkTable::default();
    let test_uuid = Uuid::new_v4();

    let row1 = TestIndexRow {
        id: table.get_next_pk().into(),
        test: Some(test_uuid),
        another: 1,
        exchange: 1,
    };
    let pk1 = table.insert(row1).unwrap();

    let row2 = TestIndexRow {
        id: table.get_next_pk().into(),
        test: Some(test_uuid),
        another: 2,
        exchange: 2,
    };
    let pk2 = table.insert(row2).unwrap();

    let row3 = TestIndexRow {
        id: table.get_next_pk().into(),
        test: Some(test_uuid),
        another: 3,
        exchange: 3,
    };
    let pk3 = table.insert(row3).unwrap();

    // Should find all three rows with the same UUID
    let result = table.select_by_test(Some(test_uuid)).execute().unwrap();
    assert_eq!(result.len(), 3);
    assert!(result.iter().any(|r| r.id == pk1.0));
    assert!(result.iter().any(|r| r.id == pk2.0));
    assert!(result.iter().any(|r| r.id == pk3.0));
}

#[tokio::test]
async fn indexed_multiple_rows_none() {
    let table = TestIndexWorkTable::default();

    let row1 = TestIndexRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 1,
        exchange: 1,
    };
    let pk1 = table.insert(row1).unwrap();

    let row2 = TestIndexRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 2,
        exchange: 2,
    };
    let pk2 = table.insert(row2).unwrap();

    let row3 = TestIndexRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 3,
        exchange: 3,
    };
    let pk3 = table.insert(row3).unwrap();

    // Should find all three rows with None
    let result = table.select_by_test(None).execute().unwrap();
    assert_eq!(result.len(), 3);
    assert!(result.iter().any(|r| r.id == pk1.0));
    assert!(result.iter().any(|r| r.id == pk2.0));
    assert!(result.iter().any(|r| r.id == pk3.0));
}

#[tokio::test]
async fn indexed_update_indexed_field() {
    let table = TestIndexWorkTable::default();
    let uuid1 = Uuid::new_v4();

    let row = TestIndexRow {
        id: table.get_next_pk().into(),
        test: Some(uuid1),
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row).unwrap();

    // Verify initial UUID is indexed
    let result = table.select_by_test(Some(uuid1)).execute().unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].id, pk.0);

    // Update to a new UUID
    let uuid2 = Uuid::new_v4();
    table
        .update_index_test_by_id(IndexTestByIdQuery { test: Some(uuid2) }, pk.clone())
        .await
        .unwrap();

    // Old UUID should not be found
    let result_old = table.select_by_test(Some(uuid1)).execute().unwrap();
    assert_eq!(result_old.len(), 0);

    // New UUID should be found
    let result_new = table.select_by_test(Some(uuid2)).execute().unwrap();
    assert_eq!(result_new.len(), 1);
    assert_eq!(result_new[0].id, pk.0);
}

#[tokio::test]
async fn indexed_update_from_some_to_none() {
    let table = TestIndexWorkTable::default();
    let test_uuid = Uuid::new_v4();

    let row = TestIndexRow {
        id: table.get_next_pk().into(),
        test: Some(test_uuid),
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row).unwrap();

    // Verify initial UUID is indexed
    let result = table.select_by_test(Some(test_uuid)).execute().unwrap();
    assert_eq!(result.len(), 1);

    // Update to None
    table
        .update_index_test_by_id(IndexTestByIdQuery { test: None }, pk.clone())
        .await
        .unwrap();

    // Old UUID should not be found
    let result_old = table.select_by_test(Some(test_uuid)).execute().unwrap();
    assert_eq!(result_old.len(), 0);

    // Should be findable by None
    let result_none = table.select_by_test(None).execute().unwrap();
    assert_eq!(result_none.len(), 1);
    assert_eq!(result_none[0].id, pk.0);
}

#[tokio::test]
async fn indexed_update_from_none_to_some() {
    let table = TestIndexWorkTable::default();

    let row = TestIndexRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row).unwrap();

    // Verify None is indexed
    let result = table.select_by_test(None).execute().unwrap();
    assert_eq!(result.len(), 1);

    // Update to Some UUID
    let test_uuid = Uuid::new_v4();
    table
        .update_index_test_by_id(
            IndexTestByIdQuery {
                test: Some(test_uuid),
            },
            pk.clone(),
        )
        .await
        .unwrap();

    // None count should decrease
    let result_none = table.select_by_test(None).execute().unwrap();
    assert_eq!(result_none.len(), 0);

    // New UUID should be findable
    let result_uuid = table.select_by_test(Some(test_uuid)).execute().unwrap();
    assert_eq!(result_uuid.len(), 1);
    assert_eq!(result_uuid[0].id, pk.0);
}

#[tokio::test]
async fn indexed_update_via_another_index() {
    let table = TestIndexWorkTable::default();
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    let row = TestIndexRow {
        id: table.get_next_pk().into(),
        test: Some(uuid1),
        another: 999,
        exchange: 1,
    };
    table.insert(row).unwrap();

    // Update via the unique 'another' index
    table
        .update_index_test_by_another(IndexTestByAnotherQuery { test: Some(uuid2) }, 999)
        .await
        .unwrap();

    // Verify both indexes are updated correctly
    let result_uuid = table.select_by_test(Some(uuid2)).execute().unwrap();
    assert_eq!(result_uuid.len(), 1);

    let result_another = table.select_by_another(999).unwrap();
    assert_eq!(result_another.test, Some(uuid2));
}

#[tokio::test]
async fn indexed_update_via_non_unique_index() {
    let table = TestIndexWorkTable::default();
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    let row1 = TestIndexRow {
        id: table.get_next_pk().into(),
        test: Some(uuid1),
        another: 1,
        exchange: 100,
    };
    let pk1 = table.insert(row1).unwrap();

    let row2 = TestIndexRow {
        id: table.get_next_pk().into(),
        test: Some(uuid1),
        another: 2,
        exchange: 100,
    };
    let pk2 = table.insert(row2).unwrap();

    // Update both rows via the non-unique 'exchange' index
    table
        .update_index_test_by_exchange(IndexTestByExchangeQuery { test: Some(uuid2) }, 100)
        .await
        .unwrap();

    // Both rows should be updated
    assert_eq!(table.select(pk1).unwrap().test, Some(uuid2));
    assert_eq!(table.select(pk2).unwrap().test, Some(uuid2));

    // Old UUID should not be found
    let result_old = table.select_by_test(Some(uuid1)).execute().unwrap();
    assert_eq!(result_old.len(), 0);

    // New UUID should find both rows
    let result_new = table.select_by_test(Some(uuid2)).execute().unwrap();
    assert_eq!(result_new.len(), 2);
}

#[tokio::test]
async fn indexed_mixed_none_and_some() {
    let table = TestIndexWorkTable::default();
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    // Insert rows with None and Some UUIDs
    let row1 = TestIndexRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 1,
        exchange: 1,
    };
    table.insert(row1).unwrap();

    let row2 = TestIndexRow {
        id: table.get_next_pk().into(),
        test: Some(uuid1),
        another: 2,
        exchange: 2,
    };
    table.insert(row2).unwrap();

    let row3 = TestIndexRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 3,
        exchange: 3,
    };
    table.insert(row3).unwrap();

    let row4 = TestIndexRow {
        id: table.get_next_pk().into(),
        test: Some(uuid2),
        another: 4,
        exchange: 4,
    };
    table.insert(row4).unwrap();

    // Verify counts
    let result_none = table.select_by_test(None).execute().unwrap();
    assert_eq!(result_none.len(), 2);

    let result_uuid1 = table.select_by_test(Some(uuid1)).execute().unwrap();
    assert_eq!(result_uuid1.len(), 1);

    let result_uuid2 = table.select_by_test(Some(uuid2)).execute().unwrap();
    assert_eq!(result_uuid2.len(), 1);
}

#[tokio::test]
async fn indexed_delete_row_with_uuid() {
    let table = TestIndexWorkTable::default();
    let test_uuid = Uuid::new_v4();

    let row = TestIndexRow {
        id: table.get_next_pk().into(),
        test: Some(test_uuid),
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row).unwrap();

    // Verify UUID is indexed
    let result = table.select_by_test(Some(test_uuid)).execute().unwrap();
    assert_eq!(result.len(), 1);

    // Delete the row
    table.delete(pk).await.unwrap();

    // UUID should not be found after deletion
    let result = table.select_by_test(Some(test_uuid)).execute().unwrap();
    assert_eq!(result.len(), 0);
}

#[tokio::test]
async fn indexed_delete_row_with_none() {
    let table = TestIndexWorkTable::default();

    let row = TestIndexRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row).unwrap();

    // Verify None is indexed
    let result = table.select_by_test(None).execute().unwrap();
    assert_eq!(result.len(), 1);

    // Delete the row
    table.delete(pk).await.unwrap();

    // None count should decrease
    let result = table.select_by_test(None).execute().unwrap();
    assert_eq!(result.len(), 0);
}

#[tokio::test]
async fn indexed_no_match_for_uuid() {
    let table = TestIndexWorkTable::default();
    let test_uuid = Uuid::new_v4();

    let row = TestIndexRow {
        id: table.get_next_pk().into(),
        test: Some(test_uuid),
        another: 1,
        exchange: 1,
    };
    table.insert(row).unwrap();

    // Search for a different UUID
    let other_uuid = Uuid::new_v4();
    let result = table.select_by_test(Some(other_uuid)).execute().unwrap();
    assert_eq!(result.len(), 0);
}

#[tokio::test]
async fn indexed_no_match_for_none_when_all_have_values() {
    let table = TestIndexWorkTable::default();
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    let row1 = TestIndexRow {
        id: table.get_next_pk().into(),
        test: Some(uuid1),
        another: 1,
        exchange: 1,
    };
    table.insert(row1).unwrap();

    let row2 = TestIndexRow {
        id: table.get_next_pk().into(),
        test: Some(uuid2),
        another: 2,
        exchange: 2,
    };
    table.insert(row2).unwrap();

    // Search for None when all rows have Some values
    let result = table.select_by_test(None).execute().unwrap();
    assert_eq!(result.len(), 0);
}
