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

A `delete` query removes every row matching the lookup column:

```rust
delete: {
    ById() by id,
    ByName() by name,
}
```

When the lookup column is the single primary key (and has no secondary index), the generated `delete_by_<pk>` method delegates to `self.delete(pk).await` and treats `WorkTableError::NotFound` as `Ok(())`. When the lookup column has a unique or non-unique secondary index, `delete_by_<col>` looks up matching links through that index and deletes each matching row.

### Lookup column rule (`by <column>`)

Every declared `update`, `delete`, or `update_in_place` query must name a lookup column that can be resolved without a hidden full-table scan:

- `update` and `delete` require `by <col>` to be either the single primary key or a column covered by `indexes:`.
- `update_in_place` requires `by <col>` to be the single primary key.

Declaring a query `by` a column that is neither the primary key nor indexed is rejected at compile time with:

```text
{query} is declared `by {col}`, but `{col}` is neither the primary key nor indexed. Add an index on `{col}`, or write the scan explicitly with select_all().where_by(...).
```

Unsupported index or key combinations (such as `update_in_place` on a secondary index, or any declared mutation `by` one column of a composite primary key) are rejected at compile time with:

```text
{kind} queries do not yet support `by {col}` ({reason}).
```

When a scan is intended on an unindexed column, write the scan at the call site with `select_all().where_by(...)` and then call `update` or `delete` per matched primary key so the linear scan is visible in the caller's code.

### Compile-time validation rules

The macro and `worktable_dsl::check` also reject the following shapes at compile time, spanning the offending token:

1. **Primary-key columns in paged `update` queries.** On a paged table (in-memory or persisted), an `update` query whose column list includes any primary-key column (including one member of a composite primary key, regardless of its `by` column) is rejected because paged `update` rewrites the row without re-keying the primary index (`vec: true` tables re-key properly and remain allowed):

   ```text
   update query `{name}` cannot change primary key column `{col}`; delete the row and insert it with the new key
   ```

2. **Duplicate secondary indexes on one column.** Declaring two `indexes:` entries over the same column is rejected because only one `select_by_<col>` method is generated per column:

   ```text
   column `{col}` is already indexed by `{first}`; declare one index per column
   ```

3. **Empty column list on `update` or `update_in_place`.** Every `update` and `update_in_place` query must name at least one column to mutate:

   ```text
   update query `{name}` declares no columns to update
   ```

4. **Columns on a `delete` query.** A `delete` query removes the entire row and takes no column list:

   ```text
   delete query `{name}` takes no columns; write it as `{name}() by {by}`
   ```

