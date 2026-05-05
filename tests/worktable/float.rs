use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: TestFloat,
    columns: {
        id: u64 primary_key autoincrement,
        test: i64,
        another: f64,
        exchange: String
    },
    indexes: {
        test_idx: test unique,
        exchnage_idx: exchange,
        another_idx: another
    }
);

#[test]
fn select_all_range_float_test() {
    let table = TestFloatWorkTable::default();

    let row1 = TestFloatRow {
        id: table.get_next_pk().into(),
        test: 3,
        another: 100.0,
        exchange: "M".to_string(),
    };
    let row2 = TestFloatRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 200.0,
        exchange: "N".to_string(),
    };
    let row3 = TestFloatRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 300.0,
        exchange: "P".to_string(),
    };

    let _ = table.insert(row1.clone()).unwrap();
    let _ = table.insert(row2.clone()).unwrap();
    let _ = table.insert(row3.clone()).unwrap();

    let all = table
        .select_all()
        .where_by(|row| row.another > 99.0 && row.another < 300.0)
        .execute()
        .unwrap();

    assert_eq!(all.len(), 2);
    assert!(all.contains(&row1));
    assert!(all.contains(&row2))
}

#[test]
fn select_by_another_test() {
    let table = TestFloatWorkTable::default();

    let row1 = TestFloatRow {
        id: table.get_next_pk().into(),
        test: 3,
        another: 100.0,
        exchange: "M".to_string(),
    };
    let row2 = TestFloatRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 100.0,
        exchange: "N".to_string(),
    };
    let row3 = TestFloatRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 200.0,
        exchange: "P".to_string(),
    };

    let _ = table.insert(row1.clone()).unwrap();
    let _ = table.insert(row2.clone()).unwrap();
    let _ = table.insert(row3.clone()).unwrap();

    let where_100 = table.select_by_another(100.0).execute().unwrap();
    assert_eq!(where_100.len(), 2);
    assert!(where_100.contains(&row1));
    assert!(where_100.contains(&row2));
    let where_200 = table.select_by_another(200.0).execute().unwrap();
    assert_eq!(where_200.len(), 1);
    assert!(where_200.contains(&row3));
}

#[test]
fn select_by_another_range_test() {
    let table = TestFloatWorkTable::default();

    let rows: Vec<TestFloatRow> = (0..10)
        .map(|i| TestFloatRow {
            id: table.get_next_pk().into(),
            test: i,
            another: (i * 10) as f64,
            exchange: format!("ex_{}", i),
        })
        .collect();

    for row in &rows {
        table.insert(row.clone()).unwrap();
    }

    let results = table.select_by_another_range(20.0..50.0).execute().unwrap();
    assert_eq!(results.len(), 3);

    let results = table
        .select_by_another_range(20.0..=50.0)
        .execute()
        .unwrap();
    assert_eq!(results.len(), 4);

    let results = table.select_by_another_range(70.0..).execute().unwrap();
    assert_eq!(results.len(), 3);

    let results = table.select_by_another_range(..30.0).execute().unwrap();
    assert_eq!(results.len(), 3);

    let results = table.select_by_another_range(..).execute().unwrap();
    assert_eq!(results.len(), 10);

    let results = table
        .select_by_another_range(0.0..)
        .limit(3)
        .execute()
        .unwrap();
    assert_eq!(results.len(), 3);

    let results = table
        .select_by_another_range(20.0..=50.0)
        .order_on(TestFloatRowFields::Another, Order::Desc)
        .execute()
        .unwrap();
    assert_eq!(results.len(), 4);
    assert_eq!(results.first().unwrap().another, 50.0);
    assert_eq!(results.last().unwrap().another, 20.0);
}
