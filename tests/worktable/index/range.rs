use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: RangeTest,
    columns: {
        id: u64 primary_key autoincrement,
        value: i64,
        name: String,
    },
    indexes: {
        val_idx: value,
    }
);

worktable!(
    name: UniqueRangeTest,
    columns: {
        id: u64 primary_key autoincrement,
        num: u64,
    },
    indexes: {
        num_idx: num unique,
    }
);

worktable!(
    name: PkRangeTest,
    columns: {
        id: u64 primary_key autoincrement,
        data: String,
    }
);

#[tokio::test]
async fn idle_select_builder_does_not_pin_retired_links() {
    let table = UniqueRangeTestWorkTable::default();
    let first = UniqueRangeTestRow {
        id: table.get_next_pk().into(),
        num: 1,
    };
    let first_pk = table.insert(first).unwrap();
    let first_link = table.0.primary_index.pk_map.get_value(&first_pk).unwrap().0;

    // Construct the lazy query but do not consume it. Query configuration is
    // not an active read and must not delay reclamation or slot reuse.
    let idle_query = table.select_all();
    table.delete(first_pk).await.unwrap();

    let second = UniqueRangeTestRow {
        id: table.get_next_pk().into(),
        num: 2,
    };
    let second_pk = table.insert(second).unwrap();
    let second_link = table.0.primary_index.pk_map.get_value(&second_pk).unwrap().0;

    assert_eq!(
        OffsetEqLink::<{ worktable::in_memory::DATA_INNER_LENGTH }>(first_link),
        OffsetEqLink::<{ worktable::in_memory::DATA_INNER_LENGTH }>(second_link)
    );
    drop(idle_query);
}

#[test]
fn range_read_revalidates_each_resolved_row() {
    let table = RangeTestWorkTable::default();
    let inside = RangeTestRow {
        id: table.get_next_pk().into(),
        value: 10,
        name: "inside".to_string(),
    };
    let outside = RangeTestRow {
        id: table.get_next_pk().into(),
        value: 100,
        name: "outside".to_string(),
    };
    table.insert(inside.clone()).unwrap();
    table.insert(outside.clone()).unwrap();

    let outside_link = table
        .0
        .primary_index
        .pk_map
        .get(&RangeTestPrimaryKey(outside.id))
        .map(|entry| entry.get().value.0)
        .unwrap();

    // Model the transient state where an index entry still falls inside the
    // requested range but its completed row version has already moved out.
    TableIndex::insert(&table.0.indexes.val_idx, 15, outside_link);

    let rows = table.select_by_value_range(0..20).execute().unwrap();
    assert_eq!(rows, vec![inside]);
}

#[test]
fn test_range_select_basic() {
    let table = RangeTestWorkTable::default();

    for v in 0..6 {
        table
            .insert(RangeTestRow {
                id: table.get_next_pk().into(),
                value: v * 10,
                name: format!("name_{}", v * 10),
            })
            .unwrap();
    }

    let results = table.select_by_value_range(10..30).execute().unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn test_range_select_inclusive() {
    let table = UniqueRangeTestWorkTable::default();

    let base = 10000u64;
    for n in base..=base + 10 {
        table
            .insert(UniqueRangeTestRow {
                id: table.get_next_pk().into(),
                num: n,
            })
            .unwrap();
    }

    let results = table.select_by_num_range(base..=base + 5).execute().unwrap();
    assert_eq!(results.len(), 6);
}

#[test]
fn test_range_select_open_from() {
    let table = UniqueRangeTestWorkTable::default();

    let base = 20000u64;
    for n in base..=base + 10 {
        table
            .insert(UniqueRangeTestRow {
                id: table.get_next_pk().into(),
                num: n,
            })
            .unwrap();
    }

    let results = table.select_by_num_range(base + 5..).execute().unwrap();
    assert_eq!(results.len(), 6);
}

#[test]
fn test_range_select_open_to() {
    let table = UniqueRangeTestWorkTable::default();

    let base = 30000u64;
    for n in base..=base + 10 {
        table
            .insert(UniqueRangeTestRow {
                id: table.get_next_pk().into(),
                num: n,
            })
            .unwrap();
    }

    let results = table.select_by_num_range(..base + 5).execute().unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn test_range_select_with_limit() {
    let table = UniqueRangeTestWorkTable::default();

    let base = 40000u64;
    for n in base..=base + 20 {
        table
            .insert(UniqueRangeTestRow {
                id: table.get_next_pk().into(),
                num: n,
            })
            .unwrap();
    }

    let results = table.select_by_num_range(base..).limit(5).execute().unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn test_range_select_with_offset() {
    let table = UniqueRangeTestWorkTable::default();

    let base = 50000u64;
    for n in base..=base + 20 {
        table
            .insert(UniqueRangeTestRow {
                id: table.get_next_pk().into(),
                num: n,
            })
            .unwrap();
    }

    let results = table.select_by_num_range(base..=base + 10).offset(3).execute().unwrap();
    assert_eq!(results.len(), 8);
}

#[test]
fn test_range_select_empty_result() {
    let table = UniqueRangeTestWorkTable::default();

    let base = 60000u64;
    for n in base..=base + 10 {
        table
            .insert(UniqueRangeTestRow {
                id: table.get_next_pk().into(),
                num: n,
            })
            .unwrap();
    }

    let results = table.select_by_num_range(100000..200000).execute().unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_range_select_full_range() {
    let table = UniqueRangeTestWorkTable::default();

    let base = 70000u64;
    for n in base..=base + 10 {
        table
            .insert(UniqueRangeTestRow {
                id: table.get_next_pk().into(),
                num: n,
            })
            .unwrap();
    }

    let results = table.select_by_num_range(..).execute().unwrap();
    assert_eq!(results.len(), 11);
}

#[test]
fn test_range_select_non_unique_multiple_per_key() {
    let table = RangeTestWorkTable::default();

    for (v, suffix) in [(10, "a"), (10, "b"), (10, "c"), (20, "d"), (20, "e")] {
        table
            .insert(RangeTestRow {
                id: table.get_next_pk().into(),
                value: v,
                name: format!("item_{}", suffix),
            })
            .unwrap();
    }

    let results = table.select_by_value_range(10..=20).execute().unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn test_range_select_with_order() {
    let table = UniqueRangeTestWorkTable::default();

    let base = 80000u64;
    for n in base..=base + 10 {
        table
            .insert(UniqueRangeTestRow {
                id: table.get_next_pk().into(),
                num: n,
            })
            .unwrap();
    }

    let results = table
        .select_by_num_range(base..=base + 5)
        .order_on(UniqueRangeTestRowFields::Num, Order::Desc)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 6);
    assert_eq!(results.first().unwrap().num, base + 5);
    assert_eq!(results.last().unwrap().num, base);
}

#[test]
fn test_pk_range_select_basic() {
    let table = PkRangeTestWorkTable::default();

    for i in 0..20 {
        table
            .insert(PkRangeTestRow {
                id: table.get_next_pk().into(),
                data: format!("data_{}", i),
            })
            .unwrap();
    }

    let results = table.select_by_pk_range(5..10).execute().unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn test_pk_range_select_inclusive() {
    let table = PkRangeTestWorkTable::default();

    for i in 0..20 {
        table
            .insert(PkRangeTestRow {
                id: table.get_next_pk().into(),
                data: format!("data_{}", i),
            })
            .unwrap();
    }

    let results = table.select_by_pk_range(5..=10).execute().unwrap();
    assert_eq!(results.len(), 6);
}

#[test]
fn test_pk_range_select_open_from() {
    let table = PkRangeTestWorkTable::default();

    for i in 0..20 {
        table
            .insert(PkRangeTestRow {
                id: table.get_next_pk().into(),
                data: format!("data_{}", i),
            })
            .unwrap();
    }

    let results = table.select_by_pk_range(15..).execute().unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn test_pk_range_select_open_to() {
    let table = PkRangeTestWorkTable::default();

    for i in 0..20 {
        table
            .insert(PkRangeTestRow {
                id: table.get_next_pk().into(),
                data: format!("data_{}", i),
            })
            .unwrap();
    }

    let results = table.select_by_pk_range(..5).execute().unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn test_pk_range_select_with_limit() {
    let table = PkRangeTestWorkTable::default();

    for i in 0..50 {
        table
            .insert(PkRangeTestRow {
                id: table.get_next_pk().into(),
                data: format!("data_{}", i),
            })
            .unwrap();
    }

    let results = table.select_by_pk_range(10..).limit(5).execute().unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn test_pk_range_select_with_order() {
    let table = PkRangeTestWorkTable::default();

    for i in 0..20 {
        table
            .insert(PkRangeTestRow {
                id: table.get_next_pk().into(),
                data: format!("data_{}", i),
            })
            .unwrap();
    }

    let results = table
        .select_by_pk_range(5..=10)
        .order_on(PkRangeTestRowFields::Id, Order::Desc)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 6);
    assert_eq!(results.first().unwrap().id, 10);
    assert_eq!(results.last().unwrap().id, 5);
}
