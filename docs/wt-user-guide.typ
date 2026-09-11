#set document(title: "WorkTable User Guide", author: "PathScale")
#set page(paper: "a4", margin: (x: 2.2cm, y: 2.4cm), numbering: "1")
#set text(font: ("Helvetica", "Arial"), size: 10pt)
#set par(justify: true, leading: 0.62em)
#show heading: set block(above: 1.4em, below: 0.7em)
#show heading.where(level: 1): set text(size: 17pt, weight: "bold")
#show heading.where(level: 2): set text(size: 12.5pt, weight: "bold")
#show heading.where(level: 3): set text(size: 10.5pt, weight: "bold")
#show raw.where(block: true): it => block(
  fill: rgb("#f4f4f2"), inset: 9pt, radius: 3pt, width: 100%, breakable: false, text(size: 8.5pt, it),
)
#show raw.where(block: false): it => box(fill: rgb("#f0f0ee"), inset: (x: 2.5pt, y: 0pt), outset: (y: 2.5pt), radius: 2pt, text(size: 9pt, it))
#show link: set text(fill: rgb("#1a4f8a"))
#let note(title, body) = block(
  fill: rgb("#fbf6e8"), stroke: (left: 2.5pt + rgb("#c8a13a")), inset: 9pt, radius: 2pt, width: 100%,
  [*#title.* #body],
)

#align(center)[
  #text(size: 26pt, weight: "bold")[WorkTable]
  #v(-0.4em)
  #text(size: 11pt, style: "italic")[Absolutely not a database.]
  #v(0.6em)
  #text(size: 9.5pt)[A user's guide to the `worktable!` macro, its queries, its indexes and its
  persistence tier. Written against 1.9.0-alpha1.]
]
#v(1.2em)

= What this is

Embedded table storage for Rust. Declare a table with a macro, get a typed struct back:
a primary key, secondary indexes, generated queries. Rows live in memory as paged,
zero-copy records. Persisting them to local disk or S3 is opt-in.

#note("What it is not")[No transaction journal, no fsync per batch. A mutation
returning means the change was accepted and queued, not that it is on stable storage.
See #link(<persistence>)[Persistence].]

= Getting started

```sh
cargo add worktable@=1.9.0-alpha1
```

Until this alpha is published, depend on the reviewed checkout with
`worktable = { path = "../WorkTable" }`. A plain `cargo add worktable` selects the
published release and may not include the APIs described here.

```rust
use worktable::prelude::*;
use worktable::worktable;
```

Everything the macro emits resolves through `worktable::prelude`, so that one import is
the whole setup.

= Examples <examples>

Every clause the macro accepts appears below, labelled where it is used.

== 1. The smallest table

```rust
worktable! (
    name: Order,              // required, and must come first. CamelCase.
    columns: {
        id: u64 primary_key,  // this table has a single-column primary key
        total: u64,
    },
);

let table = OrderWorkTable::default();
table.insert(OrderRow { id: 1, total: 500 }).await?;   // errors if the key exists
table.upsert(OrderRow { id: 1, total: 600 }).await?;   // overwrites instead
let row = table.select(1).expect("just inserted");
```

`name: Order` generates `OrderWorkTable`, `OrderRow`, `OrderPrimaryKey`, and for a
persisted table `OrderPersistenceEngine`.

== 2. Column clauses

```rust
worktable! (
    name: Account,
    columns: {
        id: u64 primary_key autoincrement,  // the table assigns keys
        email: String,                      // any sized type
        nickname: String optional,          // becomes Option<String>
        balance: i64,
    },
);
```

Clause order inside a column is fixed by the grammar and is not the order you might
guess: `<name>: <Type> [primary_key [autoincrement|custom]] [optional] [columnar(..)]
[using <backend>]`. The generator binds to `primary_key`, and `optional` follows both.

```rust
let id = table.insert(AccountRow {
    id: table.get_next_pk().into(),
    email: "a@b.c".to_string(),
    nickname: None,            // optional column
    balance: 0,
}).await?;
```

`custom` replaces `autoincrement` when you generate keys yourself and still want the
table to track the high-water mark.

== 3. Composite primary keys

```rust
worktable! (
    name: Quote,
    columns: {
        exchange: u32 primary_key,   // both columns carry primary_key
        symbol: u32 primary_key,     // one generator is shared between them
        price: f64,
    },
);

let row = table.select((1_u32, 42_u32).into()).expect("present");
```

A composite key keeps `worktables_index` even though the default is `arctic`, because
arctic cannot represent a tuple key.

== 4. Secondary indexes

```rust
worktable! (
    name: Customer,
    columns: {
        id: u64 primary_key,
        email: String,
        country: u16,
    },
    indexes: {
        // <name>: <column> [unique] [using <backend>]
        email_idx: email unique using worktables_index,  // one row back
        country_idx: country using arctic,               // many rows back
    },
);

let one = table.select_by_email("a@b.c".to_string());     // Option<Row>
let many = table.select_by_country(44).execute()?;        // Vec<Row>
```

`using` is optional and defaults to `arctic`. An index over an
optional column must say `using worktables_index`; Arctic supports `String` keys,
but does not support optional keys.

== 5. Declared queries

```rust
worktable! (
    name: Invoice,
    columns: {
        id: u64 primary_key,
        amount: u64,
        state: u8,
    },
    queries: {
        update: {
            AmountById(amount) by id,   // <Name>(<columns>) by <key>
        },
        delete: {
            ById() by id,               // empty parens: names no columns
        },
        in_place: {
            StateById(state) by id,     // only `by <primary key>` is supported
        },
    },
);
```

CamelCase declared, snake_case generated:

```rust
table.update_amount_by_id(AmountByIdQuery { amount: 900 }, 1).await?;  // name + "Query"
table.delete_by_id(1).await?;
table.update_state_by_id_in_place(|state| *state = 2.into(), 1).await?;
```

`update` reads, changes and writes. `in_place` mutates without selecting first and locks
internally, so it is safe from several threads without the caller holding anything.

== 6. Selects you do not declare

Generated from the columns and indexes, so none of these appear in the macro:

```rust
table.select(id)                                  // primary key
table.select_by_email("a@b.c".to_string())        // unique index
table.select_by_country(44).execute()?            // non-unique index
table.select_by_pk_range(10..=20).execute()?      // range over the primary key
table.select_by_country_range(40..=50).execute()?  // range over an indexed column
table.select_all().execute()?
table.select_all()
     .order_on(InvoiceRowFields::Amount, Order::Desc)   // generated field enum
     .limit(10)
     .execute()?
```

== 7. Columnar fields and indexes

```rust
worktable! (
    name: Reading,
    columns: {
        id: u64 primary_key,          // must NOT say columnar: implicit already
        host_id: u64 columnar(chunk_rows(2), compression(none)),
        timestamp: i64 columnar,      // bare form, not the same as columnar(..)
        payload: String,              // row-wise only
    },
    columnar_indexes: {
        host_time: {                  // <name>: { cluster_by: [..] }
            cluster_by: [host_id, timestamp],   // every field must be columnar
        },
    },
    config: {
        columnar_slot_id: ColumnSlotId16,   // slot width, default ColumnSlotId32
        columnar_chunk_rows: 4096,          // default chunk size, default 65536
    },
);
```

A `columnar` column is stored column-wise as well as row-wise, so a scan over that field
reads only that field's bytes.

== 8. Page size and row derives

```rust
worktable! (
    name: Small,
    columns: { id: u64 primary_key, v: u64 },
    config: {
        page_size: 4096,             // 512 minimum, 65535 max under arctic
        row_derives: Clone, Debug,   // bare identifiers, NOT [Clone, Debug]
    },                               // row_derives must be written last
);
```

`row_derives` reads identifiers until it meets another config key, which is why it goes
last. The `config` block takes no trailing comma after its closing brace.

== 9. Partitioned tables

```rust
worktable! (
    name: Book,
    persist: false,
    partition_by: symbol_id: u16,   // <name>: <unsigned type>, stored per partition
    partition_max_size: u8,         // required: rows per partition, as an index width
    columns: {
        exchange_id: u8 primary_key,
        bid: f64,
        ask: f64,
    },
);
```

The partition key is stored once per partition rather than once per row, and no query
can name it.

`partition_max_size` is required whenever `partition_by` is present, and it is a *type*
rather than a count, because it is an index width. It is how the declaration says how
many rows one partition holds:

#table(
  columns: (auto, auto, 1fr),
  stroke: 0.4pt + rgb("#cccccc"),
  inset: 6pt,
  [*width*], [*rows per partition*], [*shape*],
  [`bool`], [2], [dense],
  [`u8`], [256], [dense],
  [`u16`], [65,536], [dense],
  [`u32`, `u64`], [unbounded in practice], [a full table per partition],
)

There is no `unbounded` keyword: the widths run out of smallness, so `u64` is the escape
and generates exactly what a partitioned table generated before this key existed.

It is required rather than defaulted because without it the declaration says nothing
about the shape being generated. A reader seeing `exchange_id: u8 primary_key` in a
partitioned table reads "big table with a suspiciously tiny key", when the truth is
"twenty thousand little tables, each of which only needs a byte". Two declarations
differing by 28 KB a partition would otherwise look identical.

A count is not accepted in its place. A count is not an index width, it is not a power
of two, and it duplicates a constant that lives in the caller's code and will drift.

=== A narrow primary key off a partition is linted

`u8` or `bool` as the primary key of a table with no `partition_by` means a table that
can never hold more than 256 or 2 rows. That is occasionally what someone means and
usually a key that was meant to be wider, so it warns rather than failing:

```text
warning: use of deprecated constant `_::NARROW_PRIMARY_KEY`: `id: u8` is the
primary key of an unpartitioned table, so this table can never hold more than
256 rows...
```

Beside `partition_by` it is silent, because there it is correct: the routing key does the
spreading and the inner key only separates the rows inside one partition. A narrow key is
what makes the dense shape below possible.

To keep it, put `#[allow(deprecated)]` on the module holding the declaration. The warning
is a deprecation because a procedural macro cannot emit a warning any other way.

=== What a dense width actually generates

`bool`, `u8` and `u16` generate `<Name>DenseTable` as the partition payload instead of
the full table. It addresses rows by *position*: the primary key is the row's index, so
there is no primary index, no pages, no links, no free list, no lock map and no CDC. A
lookup is a bounds check and a load.

Measured on one declaration at two widths, 200 partitions of 23 rows each, counting what
the allocator was asked for:

#table(
  columns: (1fr, auto),
  stroke: 0.4pt + rgb("#cccccc"),
  inset: 6pt,
  [*shape*], [*bytes per partition*],
  [full table, empty], [28,404],
  [*dense, empty*], [*108*],
  [full table, 23 rows of an 88-byte row], [32,900],
  [*dense, same*], [*3,180*],
)

Read the empty row. The saving is fixed apparatus allocated when a partition is created,
so it is roughly 28 KB per partition whatever the rows weigh; the ratio falls for wider
rows only because the rows themselves grow. At 2,000 symbols that is about 56 MB.

The width is a *bound, not a reservation*. The row vector grows to the highest key used,
so a `u16` partition holding three rows holds three slots, and an empty one allocates
nothing at all.

Every method takes `&self`, because `partition_or_create` hands out an `Arc`. There is a
generated `update_<column>` per column, which edits one field in place rather than
cloning the row out and back. Writes serialise per partition rather than per cell: the
full table needs cell-level locking because its writes are async and a query can hold a
column across an await, and nothing here is async.

A dense width is refused, by name, for a primary key that is not a single unsigned
column, for a width the key cannot count to (`u16` beside a `u8` key declares 65,536 rows
into a partition that holds 256), and for `persist: true`, which it has no engine to
honour.

`queries:` works. An `update` or `delete` keyed by the primary key generates the same
method name and takes the same `<Name>Query` struct as the paged table, so the call reads
the same; it is not `async` and does not return `WorkTableError`, so a call cannot move
between the shapes by accident. A query keyed by any other column is refused, because a
dense partition has no secondary index and scanning instead would turn a keyed operation
into a linear one without saying so. `in_place` is refused as a synonym: every update
here is already in place.

Note that `memory_by_key` and `memory_total` cannot see any of this. They report
`used_bytes`, which is rows plus indexes and excludes the fixed floor by definition, so
both shapes measure the same through them.

=== A partition here is a whole table, which is a choice

WorkTable's partitioning is Postgres-shaped: a partition is a complete table with its own
storage, index and locks. That is a real decision with a real cost rather than an
implementation detail, and `docs/partition-models.md` compares it against PostgreSQL,
Kafka, ClickHouse, Cassandra, HBase and Snowflake, with each claim checked against those
systems' current documentation.

One thing from it belongs here. The isolation is stronger than Postgres's, because there
is no shared lock manager to contend on: a partition is an independent generated table
behind its own handle. What it is *not* is free, which is what `partition_max_size`
exists to let you decline.

== 9b. `vec: true`, a table with no pages

```rust
worktable! (
    name: Lookup,
    vec: true,                      // positional: after `version`, before `persist`
    columns: {
        id: u64 primary_key,
        value: u64,
    },
);
```

The rows live in one contiguous `Vec` with an index of positions into it. It pays for
none of the paging, archived rows, lock map, change-data-capture or async surface a paged
table carries.

It is a key rather than a second macro. `worktable_vec!` existed for a day, emitted
`<Name>VecRow` and `<Name>VecTable`, and is deleted: one macro means one `<Name>Row` and
one `<Name>WorkTable` whatever the storage is.

=== The two are deliberately not interchangeable

Moving a declaration between them breaks every call site, which is the safety property
rather than an omission. A swap that changed a table's concurrency and durability
guarantees while everything still compiled is the hazard worth having:

#table(
  columns: (auto, 1fr, 1fr),
  stroke: 0.4pt + rgb("#cccccc"),
  inset: 6pt,
  [], [*paged*], [*`vec: true`*],
  [`insert`], [`async fn(&self, Row) -> Result<Pk, WorkTableError>`], [`fn(&mut self, Row) -> Result<(), Row>`],
  [`upsert`], [`async fn(&self, Row) -> Result<(), WorkTableError>`], [`fn(&mut self, Row)`],
  [`delete`], [`async fn(&self, Pk) -> Result<(), WorkTableError>`], [`fn(&mut self, &Pk) -> Option<Row>`],
  [`select`], [`fn(&self, Pk) -> Option<Row>`, cloned], [`fn(&self, &Pk) -> Option<&Row>`, borrowed],
)

A missing `.await`, `&self` against `&mut self`, an owned row against a borrowed one: the
compiler rejects the swap four different ways.

=== What it refuses, and why

`persist`, `runtime`, `config` and columnar fields are each refused with an
error naming what to use instead, rather than being accepted and ignored.
`partition_by` is *not* refused: see section 9, where partitioning is what makes the
`Vec` shape correct.

Declared `queries` are supported. They use equality on a primary or secondary index,
including `fxhash`, and run synchronously through `&mut self`. An update declaration
such as `StateById(state) by id` emits
`update_state_by_id(StateByIdQuery { state: 7 }, &id) -> usize`; a delete declaration
`ByOwner() by owner` emits `delete_by_owner(&owner) -> usize`. The return value counts
affected rows. `in_place: { Status(state) by id }` emits
`update_status_in_place(|state| *state = 42, &id) -> usize` and accepts one column.
These methods belong to the table, not mutable wrappers on the shared partition set.

Vec edits validate a cloned candidate before replacing a row. A primary or unique
secondary-key collision panics with that row and its indexes unchanged; a panicking
edit closure also leaves the stored row unchanged. Replacing an existing row through
`upsert` checks unique secondary keys first. Multi-row queries apply one row at a time
and are not transactions: earlier successful edits remain if a later edit fails.
Cloning owned fields is part of this mutation cost, including Vec `in_place` queries.

=== Bytes and back: `unload` and `load`

There is no persistence engine, no background task and no flush. When you want the rows
as bytes you ask for them:

```rust
let pages: Vec<u8> = table.unload()?;        // 16 KiB self-describing pages
let table = LookupWorkTable::load(&pages)?;  // and back
```

Each 16 KiB page has a 28-byte header, an archived row batch and a 12-byte trailer:
row count at byte 16,372, row-type fingerprint at 16,376 and CRC-32 at 16,380.
The CRC covers the header, archive, padding, count and fingerprint. Page type 4
identifies archived rows; the space id is zero. Ordinary persisted tables use a
different page type and directory, so these containers cannot be interchanged.
The reader checks page links, detects incomplete chains and rebuilds indexes from rows.
The fingerprint hashes Rust's type name; it catches obvious foreign row types, but is
neither a complete schema hash nor stable across compiler versions. Renaming a type
can invalidate a snapshot; changing fields under the same name still requires an
explicit data cutover. The codec is `worktable::vec_hydrate`.

For an append-only table, save the number of live rows already written and append only
new rows. `first` counts live rows in insertion order, skipping ghosts:

```rust
let first = table.len();
let mut bytes = table.unload()?;
// Insert new rows, without updating or deleting earlier rows.
let pages_before = u32::try_from(bytes.len() / worktable::vec_hydrate::PAGE_SIZE)?;
bytes.extend_from_slice(&table.unload_appending(first, pages_before)?);
```

`unload_appending` reports oversized rows and page-number overflow. The previous
terminal page stays unchanged. Independent `unload()` segments can also be concatenated.
At load, the first accepted primary or unique key wins; an appended duplicate cannot
replace a row. Updates, deletes or invalidated cursors require a full snapshot.


=== Picking an index backend

`using` selects the physical index, and four of the five choices are ordered trees:

#table(
  columns: (auto, 1fr, auto),
  stroke: 0.4pt + rgb("#cccccc"),
  inset: 6pt,
  [*clause*], [*stores*], [*ranges*],
  [absent, or `using arctic`], [`ArcticIndex`, the default], [yes],
  [`using worktables_index`], [WTI's `IndexMap`, leaf width tunable at the call site], [yes],
  [`using indexset`], [a plain `BTreeMap`], [yes],
  [`using congee`], [`CongeeIndex`], [yes],
  [`using fxhash`], [`FxHashMap`], [*no*],
)

`fxhash` is the odd one and is worth what it costs to explain. Measured on a million
rows against the default (`perf-benchmarks/benchmarks/fx-index.rs`): *build 4.9x, lookup
4.0x*. Nothing else in the backend list moves a number that far.

What you give up is order. A table `using fxhash` has no `range` and no `range_by_`
methods at all — not a method that panics, not one that returns insertion order and calls
it key order; the methods are simply not generated, so asking for one is a compile error
at your call site.

It is accepted on `vec: true` and *refused on a paged table*, with an error saying so. Two
reasons, neither negotiable: a paged table generates `select_by_<column>_range` for every
index, and a persisted index's on-disk form is sorted pages, rebuilt with `attach_nodes`
on load. A hash map has neither an order to walk nor a page form to write.

`with_capacity` reserves an `fxhash` index along with the row vector, and that is most of
the build win: without it the same table managed 2.4x rather than 4.9x. It reserves
nothing for the tree backends, because there is nothing to gain — making allocation
completely free measures at *0.92x* for Arctic, below one, since it changes where nodes
land and sequential order is worse for a tree walked in key order.
=== Ranges

The ordered backends expose primary-key ranges. `fxhash` has no range API:

```rust
for row in table.range(100..200) { .. }          // by primary key, in key order
for row in table.range(..).rev() { .. }          // backwards
for row in table.range_by_code(&10..&20) { .. }  // by a unique secondary index
```

This is not a sorted vector. The keys come out in order and the rows they name are
wherever insertion put them, so a long range is a walk of random accesses into the row
vector rather than a sequential read. `range_by_` is emitted for unique secondary indexes
only; a non-unique one holds a posting list per key and has no single row to yield.

=== Deleting, and the ghosts it leaves

`delete` empties one slot and removes its index entries. It avoids shifting all later
rows; index removal still has the selected backend's cost:

```rust
table.delete(&7);                 // no vector-wide shift; returns the removed row
table.ghost_count();              // 1
table.slots();                    // unchanged
table.compact();                  // reclaims the slot, renumbers the indexes
```

It used to close the hole with `Vec::remove`, which meant moving every row above it *and*
rewriting every index entry above it. At a million rows that cost 21 milliseconds per
delete, so two hundred deletes took four seconds.

What you pay instead is a slot that stays allocated until you ask for it back. That is the
paged table's ghost-and-vacuum model applied to a vector, and the same judgement applies:
`ghost_count` and `slots` are there so a caller decides when compaction is worth its cost.
`compact` keeps the row vector's capacity for reuse; `shrink_to_fit` is separate, because a
table that compacts in order to keep inserting wants the capacity it already has.

`select_all` returns an iterator rather than a `&[Row]` for this reason: with a hole in it
the live rows are not a contiguous slice, and handing one back would mean paying the
compaction the design exists to defer.

=== Sizing it

`with_capacity`, `capacity` and `reserve` size the row vector. `with_capacity` also
reserves the primary FxHash index when selected. Tree indexes do not reserve nodes
through this callsite. `with_capacity_and_node_size` combines row reserve with WTI
leaf width on tables that use WTI. Measure build and lookup separately before choosing
capacity or leaf width.

== 10. Choosing a runtime

The table declaration selects the default executor for owned async selects and the
backend identity required by named profiles. Ordinary borrowed mutations execute
where their caller polls them. Table locks remain portable; persistence uses a private
I/O pool, and engine background work follows the process runtime setting.

```rust
runtimes! { scheduled: nagoya(shared_slot), wide: nagoya(spread), }
worktable! {
    name: Orders,
    runtime: nagoya(shared_slot),
    columns: { id: u64 primary_key, total: u64 },
    queries: {
        update runtime scheduled: { TotalById(total) by id },
        in_place runtime scheduled: { TotalById(total) by id },
    }
}
let table = Arc::new(OrdersWorkTable::default());
table.insert(OrdersRow { id: 1, total: 10 }).await?;
table.update_total_by_id(TotalByIdQuery { total: 20 }, 1u64).await?;
table.update_total_by_id_in_place(|total| *total = 21.into(), 1u64).await?;
let rows = table.select_all()
    .order_on(OrdersRowFields::Total, Order::Desc)
    .limit(100).runtime(wide).execute_async().await?;
```

Omitting the declaration defaults to Nagoya locality. A profile must match the declared backend family; Nagoya profiles may select a different flavor at the callsite. Tokio requires the `tokio-runtime` feature and an
entered Tokio runtime. `WT_DEFAULT_RUNTIME` overrides Nagoya flavors process-wide;
`WT_RUNTIME_WORKERS` sets pool size on first use. Keep these fixed when comparing runs.

`execute()` stays synchronous. With an explicit `.runtime(profile)`, it returns
`RuntimeRequiresAsync` instead of silently ignoring the profile. `execute_async()`
uses the table default when no profile was supplied. It materializes borrowed iterators
and `where_by` predicates on the caller before returning its future; range filters,
sorting, offset and limit execute on the worker over those owned rows. The full input
is materialized even for a small limit. This boundary releases borrowed table guards
and permits predicates that borrow local state, but adds allocation and dispatch cost.
It does not parallelize a scan or split sorting across workers.

Runtime-annotated update, delete and in-place sections generate methods on
`Arc<Table>`. Pass owned keys and `Send + 'static` closures; the cloned table handle
keeps storage alive. Unannotated methods retain their borrowed receivers and arguments.
Dropping a pending dispatch cancels it at the next suspension. Synchronous work already
running can finish; cancellation is not transaction rollback. Nested async dispatch
progresses even on one worker. Avoid blocking joins from a pool worker.

Vec tables remain synchronous and reject runtime annotations. Without default features,
explicit hosted profiles are unavailable and `execute_async()` runs its owned plan inline.
The existing dependency closure still needs std; this is not a freestanding-target claim.

== 11. A persisted table, end to end

```rust
worktable! (
    name: Ledger,
    version: 2,           // optional, defaults to 1. Must precede persist.
    persist: true,        // generates LedgerPersistenceEngine
    columns: {
        id: u64 primary_key,
        amount: i64,
    },
);

let config = DiskConfig::new_with_table_name(
    dir,
    LedgerWorkTable::name_snake_case(),
    LedgerWorkTable::version(),
);
let engine = LedgerPersistenceEngine::new(config).await?;
let table = LedgerWorkTable::load(engine).await?;   // replays what is on disk

table.upsert(LedgerRow { id: 1, amount: 42 }).await?;   // queued, not durable

table.close().await?;   // the only thing that proves the queue drained
```

== 12. Everything at once

The prefix is ordered. Everything after `partition_max_size` is free-order.

```rust
worktable! (
    name: Kitchen,                  // 1, required
    version: 3,                     // 2, optional
    // vec: true,                   // 3, optional, and excludes `persist`
    persist: false,                 // 4, optional
    partition_by: shard: u16,       // 5, optional
    partition_max_size: u64,        // 6, required with `partition_by`
    runtime: nagoya(locality),      // free-order from here down
    columns: {
        id: u64 primary_key autoincrement,
        nickname: String optional,
        bucket: u32 columnar,
        score: i64,
    },
    indexes: {
        nickname_idx: nickname unique using worktables_index,
        score_idx: score,
    },
    columnar_indexes: {
        by_bucket: { cluster_by: [bucket] },
    },
    queries: {
        update: { ScoreById(score) by id },
        delete: { ById() by id },
        in_place: { ScoreById(score) by id },
    },
    config: {
        page_size: 4096,
        columnar_chunk_rows: 4096,
        row_derives: Clone, Debug,
    },
);
```

#note("Writing `version` or `persist` late")[The prefix keys are positional and the
error says so rather than reporting an unexpected token. `version` after `columns` is
refused; so is `persist` or `partition_by`.]

= Index backends

`using` names the physical structure. They differ in what they can express, not only in
speed.

#table(
  columns: (auto, 1fr),
  stroke: 0.4pt + rgb("#cccccc"),
  inset: 6pt,
  [*Backend*], [*When it fits*],
  [`worktables_index`], [The general ordered backend, including composite and optional keys.],
  [`indexset`], [Vanilla IndexSet, selectable explicitly while keeping the same disk representation.],
  [`arctic`], [*The default.* Supported integer keys and `String`; packs a row link into a single `u64`. Page stride must fit its 16-bit offset and length fields.],
  [`congee`], [Fixed-width integer keys. Refuses `String` and other variable-width types.],
)

Rules:

- Omitting `using` gives `arctic`. A composite primary key keeps `worktables_index`,
  because arctic cannot represent a tuple key.
- Congee must state `persist` explicitly. Its persistence uses native checkpoint and WAL
  adapters rather than the shared page format.
- Arctic supports `String`, but not optional keys. `nickname_idx: nickname unique`
  over a `String optional` is rejected, and the message names the type rather than the
  omission. Say `using worktables_index`.
- Arctic caps page size at 65535: it packs a link into 64 bits with 16-bit offset and
  length fields. The macro refuses the combination.

= Building without default features

Set `default-features = false` on the WorkTable dependency for the in-memory
API and generated calls with `no_std` and `alloc`. An allocator and supported
Unix or Windows OS services are required. Locks, entropy and the change-event
clock may use libc or Windows APIs without linking Rust's standard library.

Hosted persistence, background vacuum, runtime thread creation and the
`worktable_dsl` parser re-export require `std`. Embedded schema strings and
compile-time macro parsing remain available without it: proc macros run on the
build host. `tokio-runtime`, `vanilla-index`, `s3-support`, `perf_measurements` and `wti-superslice-search` enable `std`.

Point reads retain the fixed page directory. The no-std fallback page-list
snapshot clones an Arc under a short lock and releases the lock before visiting
rows. Standard builds retain ArcSwap. Change-event identifiers retain UUID v7
ordering, using OS time and a shared context when std is disabled.

CI removes Rust std from the target sysroot and compiles both the library and
an isolated consumer. A positive core/alloc control and a failing std control
verify the test environment. Host proc macros retain their normal sysroot.

```sh
sh scripts/check-no-std.sh -p worktable --lib --no-default-features
sh scripts/check-no-std.sh --manifest-path tests/nostd-consumer/Cargo.toml
cargo test --manifest-path tests/nostd-consumer/Cargo.toml
```

The consumer runs generated insertion, selection, scanning and deletion,
concurrent growth, and change-event identifier checks. Its tests supply a host
allocator and executor while WorkTable remains built without std.

= Page size

A page has two sizes and they are not interchangeable. The *stride* is what one page
occupies on disk, header included, and every file offset is computed from it. The
*payload size* is the stride less the 28-byte header. Persisted row pages also
reserve space for a live-row directory and checksum. Their row allocator budget
is smaller and depends on the minimum archived row size. Index and metadata
pages use the full payload budget.

Set it in the `config` block:

```rust
worktable! (
    name: Small,
    columns: { id: u64 primary_key, v: u64 },
    config: { page_size: 4096 }
);
```

#note("Persisted tables have a floor, not a fixed size")[`page_size` works for a
persisted table, and the only rule is a 512-byte minimum: a page on disk carries a
28-byte header, so anything much smaller is mostly header. An Arctic-backed table is
also capped at 65535. In-memory tables have neither limit.

It was refused outright until recently, because the seeks computed offsets from a
hardcoded constant while the table threaded the configured one. Every location that
decides a page size, and the three silent bugs found while making them agree, are in
`docs/page-size.md`.]

= Columnar rules

Syntax is in #link(<examples>)[Example 7]. The constraints:

- Every field in `cluster_by` must itself declare `columnar`.
- A primary-key column must not declare `columnar`. It participates in columnar identity
  implicitly, and declaring it again generates duplicate scan methods.
- `columnar_indexes` requires at least one `columnar` field.
- A columnar index must not take the name of a columnar field, which would generate two
  scan methods with one name.
- `columnar_slot_id` and `columnar_chunk_rows` live in `config` because they apply to the
  table. Defaults are `ColumnSlotId32` and 65,536.

= Persistence <persistence>

Persistence is implemented, not planned. Add `persist: true` and load the table through
an engine.

```rust
let config = DiskConfig::new_with_table_name(dir, OrderWorkTable::name_snake_case(), OrderWorkTable::version());
let engine = OrderPersistenceEngine::new(config).await?;
let table = OrderWorkTable::load(engine).await?;
```

S3 layers on top of the disk engine rather than replacing it:
`S3SyncDiskPersistenceEngine` wraps a `DiskPersistenceEngine` and syncs it. Enable the
`s3-support` feature.

== The durability contract

#table(
  columns: (auto, 1fr),
  stroke: 0.4pt + rgb("#cccccc"),
  inset: 6pt,
  [*Boundary*], [*What you actually get*],
  [A mutation returns], [The in-memory change was accepted and its persistence operation was queued.],
  [`wait_for_ops()` returns], [The engine completed the queued operations. No fsync, no stable-storage guarantee.],
  [`close()` returns], [Intake stopped, the queue drained, the engine task joined. Still no fsync guarantee.],
  [Process crash or `SIGKILL`], [Acknowledged rows may be lost and the file may be torn.],
  [Power loss], [No atomic-batch or stable-storage guarantee.],
)

Call `close()` on orderly shutdown. `wait_for_ops()` is not a shutdown boundary: it does
not stop another task queueing more work, so it means nothing without writer quiescence.

Persistence failure is terminal. An event gap, queue-analysis error, batch-apply error or
engine-task failure fails the table, and the original error goes to waiters, to `close()`
and to later mutations.

== Loading a torn store

A normal load audits archived rows and both primary and secondary index consistency
before exposing the table, and refuses torn state with `PersistenceLoadError` rather
than opening plausible-but-invented rows.

`LoadMode::Recovery` exists for offline tools only. It copies individually validated
rows through a surviving index into a clean table, which must then pass a normal strict
load before anyone reads it. It is not an in-place repair and must never serve live
traffic.

== Vacuum

Persisted vacuum compacts the in-memory layout and keeps disk indexes consistent with
moved rows. It does not truncate `.wt.data`. Watch physical growth with
`persisted_data_file_size_bytes().await` and decide when to snapshot and rebuild.

= The filesystem

One module, `worktable::prelude::fsx`, naming no async runtime. The file type is
`std::fs::File` behind `AllowStdIo`: blocking semantics, `futures-io` traits.

Measured, not assumed. `tokio::fs` ran scattered updates at 12,316 rows per second
against 74,728 on `std::fs`, a factor of 6.1, with bulk insert within noise. A scattered
update is many small IOs and `tokio::fs` pays a thread-pool round trip for each.

#note("Where to put the work")[The calls block, so a persistence engine should own a
thread rather than share a worker pool. They were never waiting on the disk anyway: 89
voluntary context switches across 25,000 inserts.]

= Choosing a runtime <runtime>

Syntax is in #link(<examples>)[Example 10]. The parenthesised name is a *flavor*: a set
of scheduler tunings, not a different scheduler. Each selected flavor owns a separate
process-lifetime pool. Reusing a flavor reuses its pool; selecting several starts several
pools. Idle spinning from those pools can interfere with measurements. Compare flavors
in separate processes and report CPU next to throughput and latency.

#table(
  columns: (auto, 1fr),
  stroke: 0.4pt + rgb("#cccccc"),
  inset: 6pt,
  [*Flavor*], [*What it changes*],
  [`shared_slot`], [Keeps the local slot and first displaced inbox job private; shares further displaced work while that inbox is occupied.],
  [`locality`], [The default. Keeps wakes local with four short spin rounds before parking. Displaced work enters a private inbox, then a local queue that can promote work to peers.],
  [`spread`], [Sends every wake through the shared injector instead of keeping it local.],
  [`throughput`], [`spread`, taking a larger batch from the injector at a time.],
  [`wide_injector`], [`spread`, taking a larger batch still.],
  [`low_latency`], [`locality` with a longer idle spin budget before parking.],
)

#note("Measure before changing policy")[`locality` is the release baseline, using four rounds of 128 spin hints before parking.
Sparse-burst CPU measurements are part of this choice, not only peak throughput.
The earlier YCSB figures and the WorkTable workloads in `perf-benchmarks/runtime-flavours`
are different experiments. They do not establish a universally fastest flavor. Worker
count, update mix, task wake behavior and CPU consumption all matter. Keep the workload
and worker count with any quoted result.]

= Concurrency

Indexes are lock-free with change-data-capture; a row-level `LockMap` gives ordered
access when you want it. Reads always use immutable row-version publication, including
under `default-features = false`: turning off a feature must never expose a safe API that
races deserialization against page-byte mutation.

Point lookups use a strict backend-specific visibility contract. WorkTablesIndex pins the
structural mapping until its node is locked, so hits and misses are both definitive.

= Feature flags worth knowing

#table(
  columns: (auto, 1fr),
  stroke: 0.4pt + rgb("#cccccc"),
  inset: 6pt,
  [*Feature*], [*Effect*],
  [`std`], [On by default. Off, hosted persistence and runtime pools are excluded. The dependency closure still uses std; isolated consumer checks guard this supported configuration.],
  [`s3-support`], [The S3 sync engine, and the HTTP stack under it.],
  [`logical-index-persistence`], [Moves unique structural CDC work off the mutation path into the background worker. The page format is unchanged either way.],
  [`wti-predictable-search`], [On by default. The branch-based node search, which avoids a measured regression on sequential numeric keys.],
)

The three alternative search policies (`wti-hybrid-search`, `wti-std-search`,
`wti-superslice-search`) are compile-time gates. Enable one, and only one, for an
unambiguous build. If feature unification turns on several, WorkTablesIndex applies a
documented precedence rather than refusing the graph.

= Reference coverage

The callsite reference below covers operations beyond the declaration examples. The
executable `examples/guide_check.rs` demonstrates the public table and maintenance APIs;
the tests named there cover persistence, dense storage and columnar identity boundaries.

= Rust callsite reference

These are existing Rust APIs, not additional grammar. A declaration chooses a storage
shape and therefore an API contract. Do not transfer a call between shapes by removing
an `await` or changing a borrowed key until the ownership and return type are understood.

== Paged table operations

`default()` creates an in-memory table. `insert(row).await` rejects duplicate keys and
returns the primary key. `upsert(row).await` inserts or replaces. `select(key)` returns an
owned row in `Option`, while `select_all()` and non-unique-index selects return builders.
Use `execute()` to materialize those builders. Unique secondary-index selects return an
`Option<Row>`. Primary-key and secondary-index range methods require an ordered backend.

`insert_many(Vec<Row>).await` validates and publishes the batch atomically to readers;
`BatchInsertError` identifies the rejected row/index. Persisted success means the batch
was queued, not committed to stable storage. `delete_many(Vec<Key>).await` and
`delete_range(range).await` return deleted keys and may report a `BatchDeleteError` with
partial progress. Range deletion walks the keys present at that walk; it does not promise
to delete concurrent future inserts into the range. `reinsert(old, new).await` is the
explicit row-replacement operation; ordinary updates should use `upsert` or declared
queries so secondary indexes stay synchronized.

With `autoincrement`, get a key from `get_next_pk()`, convert it into the row field, then
insert. `reserve_pks(count)` reserves a disjoint range for a bulk producer. Reserved keys
can be unused; allocation is not publication. `custom` lets the application supply its
generator under the generated primary-key trait contract. `name()`,
`name_snake_case()` and generated schema metadata identify a table. Persisted tables
also expose `version()` and `pk_gen_state()`.

`row_count()` and `count()` report live rows. `used_bytes()` reports accounted row and
index storage; it is not allocator RSS. `system_info()` provides per-index and table
information. `iter_with(callback)` passes each owned row to a callback returning
`Result<(), WorkTableError>`. `iter_with_async(callback).await` accepts a callback
returning a future with the same result type. Both stop on the first error.

== Select builders and runtime overrides

Chain `limit(n)`, `offset(n)`, `order_on(Fields::field, Order::Asc)` or `Order::Desc`, and
`range_on(Fields::field, bounds)` before `execute()`. Generated field and range enums are
table-specific. A limit alone does not establish an order. Filtering and ordering can
require more work than the returned row count suggests.

`runtime(profile).execute_async().await` dispatches the owned plan to a matching
profile declared by `runtimes!`. Example 10 covers materialization, Arc mutation
receivers and cancellation. `execute_async()` without a profile selects the table's
default executor. Runtime initialization reads `WT_DEFAULT_RUNTIME` and
`WT_RUNTIME_WORKERS` once. Changing environment variables afterwards does not rebuild
an already-created pool. `Runtime`, `NagoyaRt`, optional `TokioRt`, flavor marker types,
`run_on`, `run_profile` and `executor_for_flavor` are the lower-level integration surface.
They do not make storage durable or turn synchronous file access into nonblocking I/O.

Paged custom updates require a single primary key or an indexed predicate. Paged
in-place queries require the single primary key and cannot mutate primary or secondary
indexed columns. Unsupported predicates are rejected during validation rather than
panicking in code generation. Vec query methods have their own synchronous contract.

== Vec table operations

`with_capacity(n)` reserves row storage and, for `fxhash`, its primary hash index.
`with_node_size(n)` selects a WorkTablesIndex leaf width where that backend is used;
it is not a reserve. `insert`, `upsert`, `update` and `delete` require `&mut self`, are
synchronous, and return the shape-specific result documented by the generated method.
Lookups borrow rows; concurrent readers can share an immutable table, but mutation needs
exclusive access. An external lock changes the measured concurrency contract.

`new/default`, `capacity`, `reserve`, `len`, `is_empty`, `select`, `iter`, `select_all`,
`into_rows`, `range`, generated secondary-index lookups/ranges, `slots`, `ghost_count`,
`compact` and `shrink_to_fit` expose the live and physical layout.
`insert` returns `Result<(), Row>` with the rejected row; `upsert` returns `()`.
`update(&key, edit)` returns whether a row was found. `delete(&key)` returns the removed
row in `Option`; its destructor runs when the caller drops that row. It is not merely a
bit flip for rows owning heap allocations. `compact()` moves
survivors and repairs index positions. Measure deletion separately from compaction and
whole-table drop. Hash-indexed access paths do not provide ordered ranges.

`unload()`, `unload_appending(first, pages_before)` and `load(bytes)` use the page codec
described above. This is a caller-
managed snapshot, with validation errors such as `RowTooLarge`, `NotAnArchive` and
`LoadError` and append `UnloadError`; it is not the paged persistence worker.
`vec_hydrate::{to_pages, to_pages_at, from_pages}`
and `Codec` are the lower-level codec surface. The proposed persisted dirty-bit/sidecar
design in `vec-persistence-design.md` is *not* a shipped Vec durability API.

== Dense partitions and partition ownership

Dense tables expose `new/default`, `insert`, `upsert`, `select(&key)`, `contains(&key)`,
`update`, `delete(&key)`, `select_all`, `row_count/len`, `is_empty`, `slots` and
`used_bytes`. Declared primary-key updates/deletes and generated scalar setters operate
synchronously. Capacity is bounded by the declared width and a failure returns
`DenseError`; dense storage does not silently fall back to paged storage or persistence.

Generated partition sets expose `partition(key)` for an owned `Arc`,
`partition_ref(key)` for a guarded borrowed reference, and `pinned().get(key)` for
several lookups under one read epoch. Keep guards short: long-lived pins defer reclaim.
`partition_or_create` applies where a default constructor exists;
`partition_or_insert_with` accepts a factory. `keys`, `iter`, `contains`, `len` and
`is_empty` inspect the directory. A removed partition remains usable through an already-
owned `Arc`; directory removal is not revocation of those handles.

`remove(key)` retires a directory entry. `collect()` performs bounded reclamation and
can execute destructors on its caller. `gc(&mut self)` requires exclusive access for
collection. `retired_len`, `retired_bytes`, `memory_by_key`, `memory_total` and
`rows_by_key` distinguish live directories from retired storage. The low-level
`PartitionSet<T>` adds `get_or_create`, `for_each`, `for_each_retired` and memory-stat
methods for integrations without a generated partition wrapper. Do not benchmark only
the directory unlink and call that the total destruction cost.

== Columnar callsites and identity

For the `Reading` declaration in Example 7:

```rust
let values = table.columnar_scan_host_id()?;
let refs = table.columnar_select_host_time(7, 1000)?;
let projected = table.columnar_project_timestamp(&refs)?;
let ordered_refs = table.columnar_scan_host_time()?;
```

Field scans return `(ColumnarRowRef, value)` pairs. Exact clustered-index selects and
clustered scans return row references; projection reads only the requested column.
`ColumnarRowRef::primary_key()` exposes the authoritative identity. Its slot,
generation and table incarnation prevent retained references from addressing a different
row after slot reuse or loading another table. Rebuilding a replica preserves references
to surviving rows. Invalidated references are omitted by projection.
They are not serializable durable IDs and not primary-key sort order.

`columnar_slots_in_use`, `columnar_slots_high_water` and `columnar_is_dirty` expose the
replica's state; `rebuild_columnar()` reconstructs it from authoritative rows. Normal
columnar reads ensure the replica is current. The slot width bounds capacity: 8 bits
cannot represent a 20,000-row table. Reuse and failure behavior are tested in
`tests/worktable/columnar.rs`. `ColumnarColumn`, `ClusteredColumnarIndex`,
`ColumnSlotId8/16/32/64` and `ColumnCompression` are lower-level building blocks.
Only implemented compression policies are accepted; the declaration is not a promise of
an unimplemented codec.

== Vacuum policy, scheduling and observability

```rust
let vacuum = table.vacuum_with_pacing(VacuumPacing {
    batch_pages: 64,
    backoff: std::time::Duration::from_millis(2),
    max_backoff: std::time::Duration::from_millis(128),
    quiet_samples: 3,
});
let before = vacuum.analyze_fragmentation();
let stats = vacuum.vacuum().await?;
let counters = vacuum.diagnostics();
```

`vacuum()` uses the default policy: 8 source pages, 2 ms initial backoff, 128 ms maximum
and three quiet observations. Positive `batch_pages` waits for quiet mutation activity
before the first and subsequent batches. Zero requests an unpaced sweep. The wait can
defer all useful sweeping under sustained writes. Completion after the foreground stops
does not establish reclamation while it was running. A successful sweep may free zero
pages because free space was reused before sweeping.

`arm_wake(bytes)` sets the reclaimable-space wake threshold; zero disables it.
`wait_until_worth_running().await` waits for that threshold. `diagnostics()` reports
cumulative requests, batches, examined/reclaimed pages and completions. Fragmentation
metadata describes the free-space registry: its `total_pages` is the number represented
there, not necessarily every allocated page. Empty registries require special care when
forming ratios.

`VacuumManager::new/with_config`, `register`, `diagnostic_snapshot` and
`run_vacuum_task` manage registered sweeps. Its task lifetime must be handled explicitly.
The concrete `EmptyDataVacuum` additionally offers `with_gate`, `gate` and
`with_persistence`; a `VacuumGate` can pause/resume work at batch boundaries and expose
stand-down counts. Generated callsites return `Arc<dyn WorkTableVacuum>`; choose their
policy at construction using `vacuum_with_pacing`, not a mutation of the trait object.

== Persistence, recovery, S3 and versioned schemas

`PersistenceEngine::new(config)` and generated `load(engine)` open a table.
`load_with(engine, LoadMode::Recovery)` is the explicit offline recovery boundary;
normal loads use strict validation. `wait_for_ops`, `close`,
`persisted_data_file_size_bytes` and the error contracts are described above. Stop writers
before waiting for a drain; consume the table through `close()` when shutting down.
For an `Arc<Table>`, release all other owners and use `Arc::try_unwrap` first.

Under `s3-support`, `s3_sync_persistence!(TableName)` generates an S3-backed engine alias.
`S3DiskConfig` combines `DiskConfig` with `S3Config` fields `bucket_name`, `endpoint`,
`access_key`, `secret_key`, optional `region` and optional `prefix`. Supply credentials
from application configuration. Local disk remains the working copy. After each completed
disk operation, the engine hashes fixed 4 MiB regions, uploads only content-addressed
chunks absent from the preceding generation, then replaces one checksummed table manifest.
That manifest is the remote commit point for the data file and all index files together.
A failed manifest write leaves the preceding complete generation visible.

Startup validates the manifest, chunk lengths, BLAKE3 hashes and complete file lengths in
a sibling staging directory. Only a complete table is renamed over the local working copy.
A committed manifest that is corrupt or incomplete is a startup error; the engine does not
continue from possibly stale local data. An old whole-file S3 layout is restored when no
manifest exists and migrates on its next successful mutation. Immutable chunks that fall
out of the current manifest are retained because deleting them could race a restore that
already read the prior generation; reclaim them only with an offline or lease-aware tool.

The optimization removes repeated network payload, including the historical whole-table
upload after a small mutation. It still reads and hashes the local table files; dirty-range
reporting is a future compatible optimization for that local work. The HTTP implementation
is blocking `ureq`, so it does not require a Tokio socket reactor. S3 does not add local
`fsync`, multi-process writer coordination, or power-loss atomicity to the disk engine.

*The v3 format cutover is a storage migration.* Ordinary persisted tables now
write format 3, with a page-local directory that records every live row and a
checksum covering the payload and directory. Version 2 stores are refused
without being modified. For stores that can be regenerated, stop the application,
explicitly remove the old store, deploy the new binary and rebuild its data.
Retained data needs an explicit conversion using the old reader. Changing the
table's `version:` declaration alone does not convert disk bytes. An old binary
cannot reopen a new store. Vec snapshots use a separate codec and cannot be
opened as ordinary WorkTable space files.

`worktable_version!` and `migration_engine!` describe explicit versioned conversions;
see `docs/migration.md` and the executable `tests/migration` fixtures for each required
trait and transformation. They do not automatically infer data migration from a changed
schema. For this 1.9 alpha, a planned rebuild/data wipe is supported by the release plan;
do not infer cross-version file compatibility from a successful same-version reopen.
`worktable::worktable_dsl` exposes parsing, checking and canonical schema emission for
tools; the TypeScript emitter is tested against that Rust source of truth.

== Fixed-capacity atomic rows

`AtomicKeyTable<V>::with_capacity(n)` is a separate Rust type for counter-like rows,
not another macro grammar. `upsert(usize)` claims or finds a slot and returns `Option<&V>`;
`select`, `iter`, `len`, `is_empty` and `capacity` inspect it. `V` provides interior
mutability. There is no removal, resizing or multi-field snapshot. Two atomic fields
can be observed from different logical updates; pack mutually consistent values into one
atomic or choose a locked table. This type requires a 64-bit target.

== Feature and capability boundaries

The Cargo feature surface includes `std`, `vanilla-index`, `tokio-runtime`,
`s3-support`, `logical-index-persistence`, `versioned-row-publication` and the four
`wti-*-search` choices. `versioned-row-publication` is a compatibility no-op: safe row
publication is mandatory. `vanilla-index` makes upstream indexset available.
`tokio-runtime` enables that backend; it is independent of merely accepting a runtime
name in schema metadata. No-std support must be checked through a downstream consumer,
not just by disabling features on this crate while another dependency re-enables them.

Use `cargo tree -e features` to inspect the resolved graph. Search features are additive
and have precedence; disabling defaults on one dependency does not cancel another
dependency's defaults. The release review found exactly this error in the original
four-way benchmark. Performance claims require the measured graph and an appropriate
workload, not only a compile-time feature label.

`perf_measurements` enables operation instrumentation; measure its overhead separately
when enabling it in an application. `runtime-backends` is an empty compatibility feature;
runtime selection is available through the existing declaration and Rust callsites.

= Where to look next

#table(
  columns: (auto, 1fr),
  stroke: 0.4pt + rgb("#cccccc"),
  inset: 6pt,
  [*Document*], [*Covers*],
  [`docs/persistence-durability.md`], [The durability contract in full, and the snapshot-restore procedure.],
  [`docs/index-backend-dsl-proposal.md`], [The `using` syntax and the capability matrix per backend.],
  [`docs/page-size.md`], [Every location across the four crates that decides a page size.],
  [`docs/queries.md`], [The generated query surface and the custom query grammar.],
  [`docs/migration.md`], [Moving a store between formats.],
  [`docs/known-issues.md`], [What is known to be wrong right now.],
  [`docs/small-tables.md`], [Where an index stops paying for itself, what a partition costs, and what reserving capacity is and is not worth.],
  [`docs/partition-models.md`], [How WorkTable's partitioning compares with Postgres, Kafka, ClickHouse and the rest, and what the cost buys.],
)
