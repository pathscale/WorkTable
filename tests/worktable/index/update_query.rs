use crate::worktable::index::{
    Test3UniqueRow, Test3UniqueWorkTable, TwoAttrByThirdQuery, UniqueTwoAttrByThirdQuery,
};

#[tokio::test]
async fn update_two_via_query_unique_indexes() {
    let test_table = Test3UniqueWorkTable::default();

    let attr1_old = "TEST".to_string();
    let attr2_old = 1000;
    let attr3_old = 65000;

    let row = Test3UniqueRow {
        val: 1,
        attr1: attr1_old.clone(),
        attr2: attr2_old,
        attr3: attr3_old,
        id: 0,
    };

    let attr1_new = "1337".to_string();
    let attr2_new = 1337;

    let _ = test_table.insert(row.clone()).unwrap();
    test_table
        .update_unique_two_attr_by_third(
            UniqueTwoAttrByThirdQuery {
                attr1: attr1_new,
                attr2: attr2_new,
            },
            attr3_old,
        )
        .await
        .unwrap();

    // Check old idx removed
    let updated = test_table.select_by_attr1(attr1_old.clone());
    assert_eq!(updated, None);
    let updated = test_table.select_by_attr2(attr2_old);
    assert_eq!(updated, None);
    let updated = test_table.select_by_attr3(attr3_old);
    assert!(updated.is_some());
}
