use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: OrderTest,
    columns: {
        id: u64 primary_key autoincrement,
        field_a: String,
        field_b: i64,
        field_c: u32,
    },
    indexes: {
        idx_c: field_c unique,   // should be 1st field in struct
        idx_a: field_a unique,   // should be 2nd field in struct
        idx_b: field_b unique,   // should be 3rd field in struct
    }
);

#[test]
fn index_struct_field_order() {
    let variant_c = OrderTestAvailableIndexes::IdxC;
    let variant_a = OrderTestAvailableIndexes::IdxA;
    let variant_b = OrderTestAvailableIndexes::IdxB;

    assert_eq!(variant_c as usize, 0);
    assert_eq!(variant_a as usize, 1);
    assert_eq!(variant_b as usize, 2);
}

#[test]
fn available_indexes_enum_order() {
    let variants = vec![
        OrderTestAvailableIndexes::IdxC,
        OrderTestAvailableIndexes::IdxA,
        OrderTestAvailableIndexes::IdxB,
    ];

    assert!(variants[0] < variants[1], "IdxC should be less than IdxA");
    assert!(variants[1] < variants[2], "IdxA should be less than IdxB");
    assert!(variants[0] < variants[2], "IdxC should be less than IdxB");
}

#[tokio::test]
async fn insert_failure_rollback_order() {
    let table = OrderTestWorkTable::default();

    let row1 = OrderTestRow {
        id: 0,
        field_a: "a".to_string(),
        field_b: 1,
        field_c: 1,
    };
    let pk1 = table.insert(row1).unwrap();
    let pk1_val: u64 = pk1.into();
    assert_eq!(pk1_val, 0u64);

    let row2 = OrderTestRow {
        id: 1,
        field_a: "a".to_string(),
        field_b: 2,
        field_c: 2,
    };
    let err = table.insert(row2).unwrap_err();

    let err_str = err.to_string();
    assert!(
        err_str.contains("unique") || err_str.contains("IdxA") || err_str.contains("idx_a"),
        "Error should mention the failed index: {}",
        err_str
    );
}

#[tokio::test]
async fn insert_success_order() {
    let table = OrderTestWorkTable::default();

    let row = OrderTestRow {
        id: 0,
        field_a: "test".to_string(),
        field_b: 42,
        field_c: 100,
    };

    let pk = table.insert(row.clone()).unwrap();
    let pk_val: u64 = pk.into();
    assert_eq!(pk_val, 0u64);

    let by_idx_c = table.select_by_field_c(100);
    assert!(by_idx_c.is_some());
    assert_eq!(by_idx_c.unwrap().field_a, "test");

    let by_idx_a = table.select_by_field_a("test".to_string());
    assert!(by_idx_a.is_some());
    assert_eq!(by_idx_a.unwrap().field_c, 100);

    let by_idx_b = table.select_by_field_b(42);
    assert!(by_idx_b.is_some());
    assert_eq!(by_idx_b.unwrap().field_a, "test");
}
