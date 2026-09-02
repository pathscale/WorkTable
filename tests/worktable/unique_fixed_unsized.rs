use worktable::prelude::*;
use worktable::worktable;

// Compile regression: an UNSIZED table (a String column exists) whose
// unique-keyed update touches only fixed-size, non-indexed columns. The
// unique-update generator used to gate its tail expression on `is_sized`
// alone, emitting a fn body with no tail for exactly this schema.
worktable!(
    name: UniqueFixedUnsized,
    columns: {
        id: u64 primary_key,
        code: u64,
        amount: u64,
        note: String,
    },
    indexes: {
        code_idx: code unique,
    },
    queries: {
        update: {
            AmountByCode(amount) by code,
        }
    }
);

#[tokio::test]
async fn unique_keyed_fixed_size_update_on_unsized_row_works() {
    let table = UniqueFixedUnsizedWorkTable::default();
    table
        .insert(UniqueFixedUnsizedRow {
            id: 1,
            code: 10,
            amount: 0,
            note: "unsized part".to_string(),
        })
        .await
        .unwrap();

    table
        .update_amount_by_code(AmountByCodeQuery { amount: 55 }, 10)
        .await
        .unwrap();

    let row = table.select(1).unwrap();
    assert_eq!(row.amount, 55);
    assert_eq!(row.note, "unsized part");
}
