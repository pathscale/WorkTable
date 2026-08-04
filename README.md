# WorkTable

*Absolutely not a database.*

Embedded table storage for Rust. Declare a table with the `worktable!` macro and get a
typed struct with a primary key, secondary indexes and generated queries. Data is held in
memory as paged, zero-copy records; persistence to local disk or S3 is opt-in.

If you have used .NET's `DataTable`, this will feel familiar: a typed in-memory table
with a primary key and indexes. The differences are that the type is generated for you
from a macro, and that persisting it is one feature flag away.

## Install

```sh
cargo add worktable@1.0.0-beta.2
```

## What you get

| | |
|---|---|
| **Extreme low latency** | Queries can return at nanosecond scale. |
| **Embedded optimized** | Very low overhead, built for resource-sensitive environments. |
| **Typed tables from a macro** | `worktable!` generates the table, row and primary-key types. No hand-written boilerplate per table. |
| **Primary and secondary indexes** | Autoincrement or supplied primary keys; unique and non-unique secondary indexes, each adding a `select_by_<column>` method. |
| **Per-index physical selection** | An optional `using` clause can statically select WorkTablesIndex, vanilla IndexSet, Congee, or Arctic where their capabilities fit. See [the backend guide](docs/index-backend-dsl-proposal.md). |
| **Generated queries** | `select`, `insert`, `upsert`, `update`, `delete` and a `select_all` query builder on every table, plus the custom update/delete queries you declare. |
| **Paged in-memory storage** | Records live in `DataPages` with a free list for reuse. `rkyv` gives zero-copy access to archived rows. |
| **Concurrency** | Lock-free concurrent indexes with change-data-capture, plus a row-level `LockMap` for ordered access. |
| **Optional persistence** | `PersistedWorkTable` writes to local disk; the `s3-support` feature syncs that to S3. Both opt-in, so a purely in-memory table pays for neither. |
| **Schema migration** | `worktable_version!` and `migration_engine!` version a table's schema and generate migrations between versions. See [docs/migration.md](docs/migration.md). |
| **Memory accounting** | `MemStat` reports actual memory held. |

## Persistence

Persistence is implemented, not planned. `PersistedWorkTable` and `PersistenceConfig` are
exported from the crate root; the prelude carries `DiskPersistenceEngine`,
`ReadOnlyPersistenceEngine`, the space and table-of-contents types, and the operation-log
types (`InsertOperation`, `UpdateOperation`, `DeleteOperation`, `AcknowledgeOperation`).

S3 support layers *on top of* the disk engine rather than replacing it.
`S3SyncDiskPersistenceEngine` wraps a `DiskPersistenceEngine` and syncs it.

```toml
[dependencies]
worktable = { version = "=1.0.0-beta.2", features = ["s3-support"] }   # S3 sync, optional
```

Persisted indexes default to WorkTablesIndex. Vanilla IndexSet can be selected explicitly with `using indexset` while retaining the existing disk/S3 representation. Congee and Arctic are explicitly memory-only and require `persist: false`. The full syntax and capability matrix are documented in [Per-index backends with `using`](docs/index-backend-dsl-proposal.md).

### Persistence lifecycle

Persisted tables expose fallible draining and graceful shutdown:

```rust
table.wait_for_ops().await?; // drain currently queued operations
table.close().await?;        // stop intake, drain, and join the engine task
```

An unrecoverable event gap, queue-analysis error, batch-apply error, or engine-task
failure moves persistence into a terminal failed state. The original error is
returned to waiters, graceful close, and later mutation attempts; later durable
operations are not applied after that failure. Dropping a busy table remains a
last-resort diagnostic path, so applications should call `close()` during orderly
shutdown.

WorkTablesIndex uses its predictable branch-based node search by default in WorkTable. This avoids a measured regression for sequential numeric-key workloads. Alternative search policies remain compile-time feature gates: disable WorkTable's default features and enable one of `wti-hybrid-search`, `wti-std-search`, or `wti-superslice-search` (plus any other features such as `s3-support`). Prefer one search feature for an unambiguous build. If Cargo feature unification enables several, WorkTablesIndex applies the documented deterministic precedence rather than rejecting the graph.

## Concurrent read/write publication

Generated reads always use immutable row-version publication. This is also true
for `default-features = false` builds: disabling a Cargo feature must not expose
a safe API that can race deserialization against page-byte mutation. The former
`versioned-row-publication` feature name remains accepted as a compatibility
no-op for existing manifests.

Generated point lookups use a strict backend-specific visibility contract by
default. WorkTablesIndex 0.0.5 keeps the structural mapping pinned until its
selected node is locked, making both hits and misses definitive; contended
lookups release the structural guard before waiting and retry the mapping.
Congee and Arctic use their native concurrent point lookups. The explicit
vanilla `using indexset` backend remains experimental and is excluded from the
stable concurrent-read contract because upstream IndexSet does not expose an
equivalent validation primitive. This index-visibility contract is independent
of row publication.

Generated reads acquire an immutable owned row version instead
of borrowing the mutable archived page image. Writers replace a per-row version
only after a complete page mutation, insert visibility is an atomic lifecycle
transition after every index is installed, and deleted or relocated links are
not reused until readers that could have captured them have drained. Page bytes
remain the persistence image and are internally serialized; range queries are
still non-snapshot reads. Archived-page mutations currently take one table-wide
writer barrier, so even disjoint writes can serialize and latency-sensitive
deployments should validate contention throughput and tail latency. The
protocol intentionally trades memory, an atomic
read-side grace-period counter, and publication bookkeeping for this stronger
concurrent-read contract. See
[`docs/versioned-row-publication.md`](docs/versioned-row-publication.md) for the
protocol and its scope.

## Relationship to `data_bucket`

WorkTable is built on [`data_bucket`](https://crates.io/crates/data_bucket), which
provides the page and link primitives its data layout uses: `PageId`, `Link`,
`INNER_PAGE_SIZE` and `SizeMeasurable`. That is a foundation rather than a swappable
backend, and those types appear throughout the in-memory paging, the indexes, the memory
accounting and the on-disk format alike.

WorkTable re-exports it (`pub use data_bucket;`) and pins an exact version. **Take it
through that re-export rather than depending on it separately.** A second copy in your
graph gives you two incompatible sets of the same types, and the resulting error names two
different `data_bucket` paths while looking like something else entirely.

## When to use something else

- **You need SQL or a query planner.** This is a typed table with generated accessors,
  not a query engine.
- **You need multi-process access.** Storage is embedded in your process.
- **You need ACID transactions spanning tables.** Operations are per-table.
- **You want a proven general-purpose embedded store.** There may be better options.
  Check your exact requirements first.

Reach for WorkTable when you want *typed, indexed, macro-generated* tables in-process,
with persistence as an option rather than an assumption.

## Usage

`WorkTable` can be used just in user's code with `worktable!` macro. It will generate table structs and other related
structs that will be used for table logic.

```rust
worktable!(
    name: Test,
    columns: {
        id: u64 primary_key autoincrement,
        test: i64,
        another: u64 optional,
        exchange: String
    },
    indexes: {
        test_idx: test unique,
        exchnage_idx: exchange,
    }
    queries: {
        update: {
            AnotherByExchange(another) by exchange,
            AnotherByTest(another) by test,
            AnotherById(another) by id,
        },
        delete: {
            ByAnother() by another,
            ByExchange() by exchange,
            ByTest() by test,
        }
    }
);
```

## Declaration parts

### `name` declaration

`name` field is used to define table's name, and is a prefix for generated objects. For example declaration
above will generate struct `TestWorkTable`, so table struct will always have name as `<name>WorkTable`.

```rust
let table = TestWorkTable::default ();
let name = table.name();
assert_eq!(name, "Test");
```

### `columns` declaration

`columns` field is used to define table's row schema. Default usage is `<column_name>: <type>`. But also there are some
flags that can be applied to columns as `<column_name>: <type> <flags>*`.

Flags list:

- `primary_key` flag and related to it.
- `optional` flag.

#### `primary_key` flag declaration

If user want to mark column as primary key `primary_key` flag is used. This flag can be used on multiple columns at a
time. Primary key generation is also supported. For some basic types `autoincrement` is supported. Also `custom`
generation is available. In this case user must provide his own implementation.

```rust
#[derive(
    Archive,
    Debug,
    Default,
    Deserialize,
    Clone,
    Eq,
    From,
    PartialOrd,
    PartialEq,
    Ord,
    Serialize,
    SizeMeasure,
)]
#[rkyv(compare(PartialEq), derive(Debug))]
struct CustomId(u64);

#[derive(Debug, Default)]
pub struct Generator(AtomicU64);

impl PrimaryKeyGenerator<TestPrimaryKey> for Generator {
    fn next(&self) -> TestPrimaryKey {
        let res = self.0.fetch_add(1, Ordering::Relaxed);
        if res >= 10 {
            self.0.store(0, Ordering::Relaxed);
        }
        CustomId::from(res).into()
    }
}

impl TablePrimaryKey for TestPrimaryKey {
    type Generator = Generator;
}

worktable!(
  name: Test,
  columns: {
    id: CustomId primary_key custom,
    test: u64
  }
);
```

For primary key newtype is generated for declared type:

```rust
// Generated code
#[derive(
    Clone,
    rkyv::Archive,
    Debug,
    rkyv::Deserialize,
    rkyv::Serialize,
    From,
    Eq,
    Into,
    PartialEq,
    PartialOrd,
    Ord
)]
pub struct TestPrimaryKey(u64);
```

#### `optional` flag declaration

If column field is `Option<T>`, `optional` flag can be used like it was done in declaration.

```rust
another: u64 optional,
```

#### Row type generation

For described column row type struct is generated:

```rust
// Generated code
#[derive(
    rkyv::Archive,
    Debug,
    rkyv::Deserialize,
    Clone,
    rkyv::Serialize,
    PartialEq
)]
#[rkyv(derive(Debug))]
#[repr(C)]
pub struct TestRow {
    pub id: u64,
    pub test: i64,
    pub another: Option<u64>,
    pub exchange: String,
}
 ```

This struct is used in `WorkTable` interface and will be used by users.

### `indexes` declaration

`indexes` field is used to define table's index schema. Default usage is `<index_name>: <column_name> <unique>?`.

Index allows faster access to data by some field. Adding `indexes` field adds methods to the generated `WorkTable`. This
method for now is `select_by_<indexed_column_name>`. It will be described below.

### Default implemented `queries`

There are some default query implementations that are available for all `WorkTable`'s:

- `select(&self, pk: <Name>PrimaryKey) -> Option<<Name>Row>`;
- `insert(&self, row: <Name>Row) -> Result<<Name>PrimaryKey, WorkTableError>`;
- `upsert(&self, row: <Name>Row) -> Result<(), WorkTableError>`;
- `update(&self, row: <Name>Row) -> Result<(), WorkTableError>`;
- `delete(&self, pk: <Name>PrimaryKey) -> Result<(), WorkTableError>`;
- `select_all<'a>(&'a self) -> SelectQueryBuilder<'a, <Name>Row, Self>`;

### `queries` declaration

`indexes` field is used to define table's queries schema. Queries are used to update/select/delete data.

```
queries: {
    update: {
        AnotherByExchange(another) by exchange,
        AnotherByTest(another) by test,
        AnotherById(another) by id,
    },
    delete: {
        ByAnother() by another,
        ByExchange() by exchange,
        ByTest() by test,
    }
}
```

Default query declaration is `<QueryName>(<column_name>*) by <column_name>`. It is same for update/select/delete.

For each query `<QueryName>Query` and `<QueryName>By` structs are generated. They will be used by user to call the
query.

#### `update` query declaration

`update` queries are used to update row's data partially. Default generated `update` allows only full update of the row.
But if user's logic needs some simultaneous update of row parts from different code parts. `update` logic supports
smart lock logic that allows simultaneous update of not overlapping row fields.

#### `select_all` query declaration

`select_all` queries are used to select row's data. select_all query returns Result<SelectQueryBuilder> accepts next params

 ```rust
 .where_by(std::ops::Range, "column"), Returns exact range of a column, works only with Number types, 
                                       e.g. .where_by(0..10u64, "test") exclusive or for inclusive .where_by(0..=10u64, "test"), default i32; Supports multiple chain 
 .order_by(Order::Desc||Order::Asc, "column"), Returns rows sorted by column,  e.g .order_by(Order::Desc, "test"); Supports multiple chain
 .offset(usize), Skips first N records, e.g .offset(5) - 
 .limit(usize), Takes first N records, e.g .limit(5) 
 

 The all params could be chained, for example - my_table.select_all()
                                                              .where_by(10..=30i32, "test")
                                                              .where_by(0..=35u64, "test2")
                                                              .order_by(Order::Desc, "test")
                                                              .order_by(Order::Asc, "test2")
                                                              .limit(10)
                                                              .offset(5)
                                                              .execute()

 ```

 `select_by_index_filed` the same as select_all, just iterates by non unique index, for unique index returns `Option<TestRow>`



## WorkTable internals structure

```rust 
worktable
    pub struct WorkTable   -- The main container that holds all data and manages its structure.

Fields

    data: DataPages<Row, DATA_LENGTH>       // stores data as pages (DataPages)
    pk_map: IndexType                       // primary index ensuring the uniqueness of records
    indexes: SecondaryIndexes               // secondary indexes for efficient searches across other columns
    pk_gen: PkGen                           // Primary Key Generator 
    lock_map: LockMap                       // from indexset crate, supports data ordering with LockMap
    table_name: &'static str                // table name (e.g., Test, which generates TestWorkTable and TestRow
    pk_phantom: PhantomData<PrimaryKey>     // a helper field for type management

Implementations 

   pub fn default() -- creates default WorkTable

```

```rust
worktable::in_memory
    pub struct DataPages  -- A container for managing data pages

Fields (/*private*/)

    pages: RwLock<Vec<Arc<Data<...>>>>,  // an array of pages (Data) that hold the records
    empty_links: Stack<Link>,            // a stack for storing links to deleted records
    row_count: AtomicU64,                // a counter for the current number of records
    last_page_id: AtomicU32,             // identifier for last page 
    current_page_id: AtomicU32,          // identifier for current page

Implementations

   pub fn new() -> Self
   pub fn from_data(vec: Vec<Arc<Data<<Row as StorableRow>::WrappedRow, DATA_LENGTH>>>,) -> Self
   pub fn insert(&self, row: Row) -> Result<Link, ExecutionError>   
   pub fn select(&self, link: Link) -> Result<Row, ExecutionError>
   pub fn with_ref<Op, Res>(&self, link: Link, op: Op,) -> Result<Res, ExecutionError>
   pub unsafe fn with_mut_ref<Op, Res>(&self, link: Link, op: Op,) -> Result<Res, ExecutionError>
   pub unsafe fn update<const N: usize>(&self, row: Row, link: Link,) -> Result<Link, ExecutionError>
   pub fn delete(&self, link: Link) -> Result<(), ExecutionError>
   pub fn get_bytes(&self) -> Vec<([u8; DATA_LENGTH], u32)>
   pub fn get_page_count(&self) -> usize
   pub fn get_empty_links(&self) -> Vec<Link>
   pub fn with_empty_links(self, links: Vec<Link>) -> Self
```

```rust 
in-memory::data
    pub struct Data  -- Data itself 

Fields
    pub free_offset: AtomicU32,                        // the offset to the first free byte 
    (/* private */)   
    id: PageId,                                        // the identifier of the page
    inner_data: UnsafeCell<AlignedBytes<DATA_LENGTH>>, // a byte array where rows are stored 
    _phantom: PhantomData<Row>,                        // a helper field for type management

Implementations 

   pub fn new(id: PageId) -> Self 
   pub fn from_data_page(page: GeneralPage<DataPage<DATA_LENGTH>>) -> Self 
   pub fn set_page_id(&mut self, id: PageId) 
   pub fn save_row(&self, row: &Row) -> Result<Link, ExecutionError
   pub unsafe fn save_row_by_link(&self, row: &Row, link: Link) -> Result<Link, ExecutionError
   pub unsafe fn get_mut_row_ref
   pub fn get_row_ref(&self, link: Link) -> Result<&<Row as Archive>::Archived, ExecutionError
   pub fn get_row(&self, link: Link) -> Result<Row, ExecutionError
   pub fn get_bytes(&self) -> [u8; DATA_LENGTH] 



```

```rust 
enum WorkTableError
    NotFound,
    AlreadyExists,
    SerializeError,
    PagesError(in_memory::PagesExecutionError),
```


## Examples 

Check out - [Examples](./examples)
