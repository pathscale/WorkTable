# Queries

WorkTable supports declared `update`, `delete`, and `update_in_place` queries.

```rust
worktable!(
    name: Something,
    columns: {
        id: u64 primary_key autoincrement,
        name: String
        amount: u64,
        some_value: i64,
    },
    indexes: {
        value_idx: value unique,
        name_idx: name,
        some_value_idx: some_value,
    },
    // Queries declaration section.
    queries: {
        // `update` queries
        update: {
            AmountById(amount) by id,
        },
        // `delete` queries
        delete: {
            ByName() by name,
        },
        update_in_place: {
            SomeValueById(some_value) by id,
        }
    }
);
```

### `update` queries

An `update` changes only the declared field set. The lookup column is part of
the method name and the generated, table-scoped selector is an explicit
argument:

```rust
table
    .update_by_id(pk, SomethingColumns::AMOUNT, 250)
    .await?;
```

For one field, the last argument is that field's Rust type. A declaration over
several fields exposes one selector such as `SomethingColumns::NAME_AND_AMOUNT`
and takes the generated query struct, preserving the declaration's atomic field
set. Selector dispatch is sealed, statically typed, and allocation-free.

### `update_in_place` queries

`update_in_place` queries allow you to update a declared field set
without need to select it before query. It is useful for counters, as example, because with
internal mutation queries locking logic user's don't need to add explicit locks over `WorkTable`
object. So you can safely use `update_in_place` queries in multiple threads simultaneously.

!!! For now only `by {pk_field}` queries are supported !!!

To declare an `update_in_place` query, add an `update_in_place` section to `queries`. Its definition is
the same shape as `update`: `{YourQueryNameCamelCase}({fields_you_want_to_update}) by {by_field_name}`.
For example:

```
update_in_place: {
    SomeValueById(some_value) by id,
    AmountAndSomeValueById(amount, some_value) by id,
}
```

It enables `update_in_place_by_id` for the generated
`SomethingColumns::SOME_VALUE` selector. The method takes the lookup value, the
selector, and a closure over the mutable archived field value.
For a multi-column declaration the selector preserves the exact atomic field
set and the closure receives a tuple, for example
`update_in_place_by_id(id, SomethingColumns::AMOUNT_AND_SOME_VALUE,
|(amount, some_value)| ...)`. Both fields are edited under one row lock and
persisted as one mutation.

```rust
#[tokio::main]
async fn main() -> eyre::Result<()> {
    // Table creation.
    let table = SomethingWorkTable::default();
    let row = SomethingRow {
        // Autoincrement primary key generation.
        id: table.get_next_pk().into(),
        name: "SomeName".to_string(),
        amount: 100,
        some_value: 0,
    };
    let pk = table.insert(row)?;
    // This will lead to `some_value` field update by adding 100 to it value.
    table
        .update_in_place_by_id(pk.0, SomethingColumns::SOME_VALUE, |some_value| *some_value += 100)
        .await?;
    let row = table.select(pk)?;
    assert_eq!(row.some_value, 100);

    Ok(())
}
```

You can find tests that cover `update_in_place` queries [here](../tests/worktable/in_place.rs).

### `delete` queries

`TODO`
