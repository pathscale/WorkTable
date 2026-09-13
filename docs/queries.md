# Queries

WorkTable supports declared `update_partial`, `delete`, and `update_partial_in_place` queries.

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
        // `update_partial` queries
        update_partial: {
            AmountById(amount) by id,
        },
        // `delete` queries
        delete: {
            ByName() by name,
        },
        update_partial_in_place: {
            SomeValueById(some_value) by id,
        }
    }
);
```

### `update_partial` queries

`TODO`

### `update_partial_in_place` queries

`update_partial_in_place` queries allow you to update a field's value
without need to select it before query. It is useful for counters, as example, because with
internal mutation queries locking logic user's don't need to add explicit locks over `WorkTable`
object. So you can safely use `update_partial_in_place` queries in multiple threads simultaneously.

!!! For now only `by {pk_field}` queries are supported !!!

To declare an `update_partial_in_place` query, add an `update_partial_in_place` section to `queries`. Its definition is
the same shape as `update_partial`: `{YourQueryNameCamelCase}({fields_you_want_to_update}) by {by_field_name}`.
For example:

```
update_partial_in_place: {
    SomeValueById(some_value) by id,
}
```

It will generate `update_partial_in_place_some_value_by_id` method for `WorkTable` object (name generation logic is same
as for other queries). It will have two arguments: your `by` field value and closure, where you can use mutable
field value itself.

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
        .update_partial_in_place_some_value_by_id(|some_value| *some_value += 100, pk.0)
        .await?;
    let row = table.select(pk)?;
    assert_eq!(row.some_value, 100);

    Ok(())
}
```

You can find tests that cover `update_partial_in_place` queries [here](../tests/worktable/in_place.rs).

### `delete` queries

`TODO`
