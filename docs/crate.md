# WorkTable

WorkTable is a typed, macro-generated embedded table for Rust. It provides
primary and secondary indexes, generated CRUD/query methods, optional local or
S3-backed persistence, and per-table concurrency. It is not a SQL database and
does not provide multi-table transactions or multi-process access.

## In-memory quick start

```rust
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: User,
    columns: {
        id: u64 primary_key,
        email: String,
    },
    indexes: {
        email_idx: email unique,
    },
);

let table = UserWorkTable::default();
let row = UserRow {
    id: 1,
    email: "person@example.com".to_owned(),
};
table.insert(row.clone()).unwrap();
assert_eq!(table.select(1), Some(row.clone()));
assert_eq!(table.select_by_email("person@example.com".to_owned()), Some(row));
```

String and tuple primary keys accept borrowed forms, so callers do not need to
write an explicit clone merely to perform a lookup or delete.

```rust
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: Account,
    columns: {
        tenant: String primary_key,
        account: String primary_key,
        enabled: bool,
    },
);

let table = AccountWorkTable::default();
let key = ("tenant-a".to_owned(), "account-1".to_owned());
let row = AccountRow {
    tenant: key.0.clone(),
    account: key.1.clone(),
    enabled: true,
};
table.insert(row.clone()).unwrap();
assert_eq!(table.select(&key), Some(row));
```

## Persistence contract

Persistence is an optional background write path. `insert`, `update`, and
`delete` returning means the in-memory mutation was accepted and its
persistence operation was queued; it does not mean the operation reached stable
storage. `wait_for_ops()` means the persistence engine completed the queued
operations. `close()` stops intake, drains, and joins the engine task. Neither
call currently issues an fsync or stable-storage guarantee, and neither makes
an in-place batch atomic against process death or power loss.

A graceful persistence failure is terminal and is returned by later mutations,
`wait_for_ops()`, and `close()`. Abrupt termination can lose acknowledged rows,
leave a torn file, or produce bytes that pass structural validation but do not
represent a row that was written. Persistence is therefore best-effort in the
1.0 beta line; applications that require crash durability need an external
snapshot/rebuild strategy.

Persisted `SpaceInfo` records the generated row schema, primary-key fields, and
secondary-index types. Existing legacy stores whose metadata is completely
empty remain readable but cannot be schema-validated, and loading them does not
rewrite the file. A non-empty schema mismatch is rejected before row loading.

`PersistedWorkTable::load()` is always the strict production path: it validates
primary/data integrity and agreement with every secondary index before returning a
table. An offline recovery program may explicitly use
`load_with(engine, LoadMode::Recovery)` on a private scratch copy to read
individually validated rows through a surviving index and copy them into a new table.
Recovery mode is not an in-place repair, must not serve traffic, and the rebuilt
destination must pass a normal strict load before publication. See the repository's
`docs/persistence-durability.md` for the complete procedure.

Persisted vacuum compacts the live in-memory layout and keeps persisted indexes
consistent with moved rows. It does not truncate `.wt.data`. Generated
persisted tables expose `persisted_data_file_size_bytes()` so operators can
observe physical growth and schedule replacement/offline compaction.

## Column grammar

The 1.0 grammar keeps column modifiers inline:

```text
id: u64 primary_key autoincrement,
tenant: String primary_key,
nickname: String optional,
id: u64 primary_key using congee,
```

A separate `attributes` section is intentionally not part of the 1.0 grammar.
The macro rejects it with a migration-oriented diagnostic rather than silently
accepting a second spelling immediately before the DSL is frozen.

## Concurrency boundary

Generated reads use immutable row publication and a read grace period. Point
and range lookups return complete owned row versions, and retired links/pages
are not reused while a reader could still resolve them. This is not MVCC: a
range scan is not a snapshot, and disjoint archived-page writes currently share
a table-wide writer barrier. Direct use of low-level page mutation APIs is
outside the generated safe-API guarantee.

See the repository README and `docs/versioned-row-publication.md` for backend,
query, and implementation details.
