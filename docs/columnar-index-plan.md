# Per-field columnar storage and columnar secondary indexes

**Status:** Design proposal.

**Scope:** Add optional column-oriented copies of individual WorkTable fields,
then optionally build columnar secondary indexes over those copies. Existing
row storage remains authoritative. There is no table-level `layout` setting.

## Core model

The design has three independent layers:

1. **Authoritative row storage.** Every declared field remains in the existing
   `DataPages<Row>` representation.
2. **Per-field columnar storage.** A field marked `columnar(...)` is duplicated
   into a typed column store addressed by a stable logical row ID.
3. **Optional columnar indexes.** A declaration in `columnar_indexes` creates
   a search/clustering structure over already-columnar fields and returns
   logical row IDs.

Conventional indexes remain a fourth, independent choice. A field may have any
combination of row storage, a columnar copy, a conventional index, and a
columnar index.

```text
                         authoritative
                              row
                               |
              +----------------+----------------+
              |                                 |
       conventional index                 column stores
        value -> row Link              ColumnRowId -> value
                                                |
                                      columnar indexes
                                  key tuple -> ColumnRowIds
```

The important correction from the earlier proposal is that a columnar index
does **not** own another covering copy of every projected field. Base column
stores exist once per marked field. Multiple columnar indexes share them and
store their own key/order metadata plus logical row IDs.

## Proposed syntax

```rust
worktable!(
    name: HistoricalCpu,
    persist: true,

    columns: {
        id: u128 primary_key,

        host_id: u64 columnar(
            chunk_rows(65_536),
            compression(auto),
        ),

        timestamp: i64 columnar(
            chunk_rows(65_536),
            compression(delta),
        ),

        metrics: [i64; 10] columnar(
            chunk_rows(65_536),
            compression(auto),
        ),

        region: String columnar(
            compression(dictionary),
        ),

        // Authoritative row storage only.
        notes: String,
    },

    // Existing conventional value -> row-link indexes.
    indexes: {
        by_host: host_id,
        by_timestamp: timestamp using arctic,
    },

    // Search/clustering structures over existing column stores.
    columnar_indexes: {
        host_time: {
            columns: [host_id, timestamp],
            cluster_by: [host_id, timestamp],
        },

        region_time: {
            columns: [region, timestamp],
            cluster_by: [region, timestamp],
        },

        // A different physical ordering over the same base columns.
        time_host: {
            columns: [timestamp, host_id],
            cluster_by: [timestamp, host_id],
        },
    },
);
```

Attributes follow the field type like the existing `primary_key`, `optional`,
and `using` attributes. There is no comma between the type and `columnar(...)`.

## What a non-indexed columnar field means

`metrics` is columnar but is not part of a conventional or columnar index:

```rust
metrics: [i64; 10] columnar(
    chunk_rows(65_536),
    compression(auto),
),
```

WorkTable maintains a dense typed `metrics` column addressed by
`ColumnRowId`. It can be:

- scanned sequentially;
- filtered with a full column scan;
- projected after another column or index selects row IDs;
- aggregated without materializing complete rows;
- persisted/compressed independently in a later phase.

It does **not** have a structure that locates row IDs from a metric value.
Filtering it requires examining the relevant values unless another predicate
first narrows the row-ID selection.

Examples:

```rust
// Full scan of one non-indexed column.
table
    .columnar()
    .metrics()
    .scan_batches(|batch| {
        aggregate(batch.values(), batch.selection());
    })?;

// The host_time index selects RowIds; metrics is retrieved from its base
// column store without materializing HistoricalCpuRow.
table
    .columnar_index()
    .host_time()
    .host_id_eq(42)
    .timestamp_range(start..end)
    .project(|columns| columns.metrics())
    .scan_batches(|batch| {
        aggregate(batch.metrics(), batch.selection());
    })?;
```

`notes` has no column store. Retrieving it requires an explicit authoritative
row gather.

## Representation matrix

For the example above:

| Field | Row storage | Column store | Conventional index | Columnar index |
|---|---:|---:|---:|---:|
| `id` | yes | implicit identity | primary | implicit identity |
| `host_id` | yes | yes | `by_host` | `host_time`, `time_host` |
| `timestamp` | yes | yes | `by_timestamp` | all three |
| `metrics` | yes | yes | no | no |
| `region` | yes | yes | no | `region_time` |
| `notes` | yes | no | no | no |

The physical mapping is:

```text
DataPages
    row Link L -> { id, host_id, timestamp, metrics, region, notes }

Stable identity
    primary key id -> ColumnRowId 7

Base column stores
    host_id[ColumnRowId 7]   -> 42
    timestamp[ColumnRowId 7] -> 1_720_000_000
    metrics[ColumnRowId 7]   -> [ ... ]
    region[ColumnRowId 7]    -> "ap-southeast"

Conventional index
    by_host: 42 -> row Link L

Columnar index
    host_time: (42, 1_720_000_000) -> ColumnRowId 7
```

The conventional index resolves a mutable physical row location. The
columnar index resolves a stable logical identity used by every base column
store.

## Stable `ColumnRowId`

Columnar storage cannot use `data_bucket::Link` as its identity. Unsized row
updates and vacuum may change the physical row link while preserving the same
logical row.

Each table with at least one columnar field therefore generates:

```rust
#[repr(transparent)]
struct HistoricalCpuColumnRowId(u64);

struct HistoricalCpuColumnDirectory {
    next_id: AtomicU64,
    by_primary_key: IndexMap<HistoricalCpuPrimaryKey, HistoricalCpuColumnRowId>,
    row_state: Vec<ColumnRowState>,
}
```

The contract is:

- insert allocates one `ColumnRowId` after the primary key is accepted;
- every marked field stores its value under that ID;
- update retains the ID;
- delete invalidates the ID;
- upsert reuses the ID when the row already exists;
- row vacuum does nothing to column stores or columnar indexes;
- IDs are not immediately reused, avoiding ABA hazards;
- a later columnar compactor may relocate physical values while preserving the
  logical ID through store-specific directories.

The primary key is implicit metadata for every columnar table. It need not be
marked `columnar(...)` to participate in identity and row gathering.

## Base column stores

The generator creates one typed store for each marked field:

```rust
struct HistoricalCpuColumnStores {
    directory: HistoricalCpuColumnDirectory,
    host_id: PrimitiveColumn<u64>,
    timestamp: PrimitiveColumn<i64>,
    metrics: FixedArrayColumn<i64, 10>,
    region: StringColumn,
}
```

Each store maps `ColumnRowId` to its current value. The first implementation
may calculate the physical chunk and slot directly from a dense ID. Later
compaction may add an indirection directory while preserving the API.

### Physical encodings

- integers/floats: contiguous aligned values;
- `bool`: packed bits or bytes in the first implementation;
- `Option<T>`: value storage plus a validity bitmap;
- fixed arrays: flattened values or fixed-size-list storage;
- `String`: offsets plus a contiguous byte buffer for sealed chunks;
- arbitrary structs: initially rejected unless they implement a future
  `ColumnEncode` trait.

### `chunk_rows`

`chunk_rows` is permitted on the field because the field owns its column
store. Different stores may eventually choose different chunk sizes while
remaining joinable through `ColumnRowId`.

For the first optimized batch path:

- equal chunk sizes permit direct positional alignment across fields;
- different chunk sizes remain correct through RowId-based gathers;
- the macro may warn when fields commonly queried together use different
  chunk sizes;
- the default should be shared and deterministic;
- a columnar index should record the chunk geometry of every referenced
  column so its query executor can choose aligned or gathered access.

Correctness must not depend on equal chunk sizes. Performance may.

### `compression`

Compression belongs to the field because each type and value distribution may
need a different encoding.

Proposed initial policies:

```rust
compression(none)
compression(auto)
compression(delta)
compression(rle)
compression(dictionary)
```

Mutable chunks should remain in a write-friendly representation. Compression
is applied when a chunk is sealed. `auto` chooses from supported encodings
using deterministic chunk statistics and records the choice in the chunk
header. Unsupported combinations fail at macro expansion or sealing rather
than silently changing semantics.

Arrow should not be the mandatory internal representation. An optional Arrow
export can be layered over typed chunks later without imposing Arrow's binary
and compile-time cost on ordinary WorkTable consumers.

## Columnar secondary indexes

A columnar index contains search/order metadata and logical row IDs. It does
not own the projected base columns.

Conceptually:

```rust
struct HostTimeColumnarIndex {
    mutable: RwLock<HostTimeDelta>,
    sealed: ArcSwap<Vec<Arc<HostTimeSegment>>>,
}

struct HostTimeSegment {
    host_id: EncodedKeys<u64>,
    timestamp: EncodedKeys<i64>,
    row_ids: Vec<HistoricalCpuColumnRowId>,
    validity: Bitmap,
    host_id_min: u64,
    host_id_max: u64,
    timestamp_min: i64,
    timestamp_max: i64,
}
```

The key values may be duplicated inside index segments because the index must
search and prune without randomly loading every base value. Non-key projected
fields are not duplicated; they are retrieved from their base column stores by
selected RowId.

### `columns`

`columns` declares which columnar fields participate in index predicates and
metadata:

```rust
columns: [host_id, timestamp]
```

The macro should require each referenced field to have `columnar(...)`. This
keeps every storage cost visible in the field declaration.

### `cluster_by`

`cluster_by` declares the physical ordering of one index's segments:

```rust
cluster_by: [host_id, timestamp]
```

It belongs to the index rather than a field because clustering is a
multi-column access-path property. Base column stores remain in canonical
`ColumnRowId` order.

Two columnar indexes can therefore share the same base columns but keep
different RowId permutations:

```text
host_time -> RowIds ordered by (host_id, timestamp)
time_host -> RowIds ordered by (timestamp, host_id)
```

`cluster_by` does not by itself promise that every result is globally sorted.
Late inserts and multiple sealed segments may overlap. A globally ordered query
must merge ordered segment streams or perform an explicit final sort.

### Mutable and sealed index segments

The intended index structure is a small LSM-like projection:

1. inserts and changed key versions enter a mutable delta;
2. at a configured size, the delta is sealed;
3. sealing sorts key tuples and RowIds by `cluster_by`;
4. sealed segments are immutable and published through `Arc`;
5. per-segment min/max metadata prunes irrelevant segments;
6. updates invalidate the old key entry and append the new one for the same
   RowId;
7. deletes invalidate the current key entry;
8. background compaction merges segments and removes stale entries.

Low-cardinality bitmap or dictionary indexes can be added as physical
encodings without changing the macro syntax.

## Generated representation

A generated table retains its row engine and adds private column components:

```rust
pub struct HistoricalCpuWorkTable(
    WorkTable</* existing generated types */>,
    HistoricalCpuColumnStores,
    HistoricalCpuColumnarIndexes,
);

struct HistoricalCpuColumnarIndexes {
    host_time: HostTimeColumnarIndex,
    region_time: RegionTimeColumnarIndex,
    time_host: TimeHostColumnarIndex,
}
```

This avoids generalizing `DataPages` or introducing a table-level layout. A
schema without any `columnar(...)` fields generates neither extra field and
retains existing hot paths.

## Mutation integration

Every affected base column and columnar index must be updated before the
existing per-key mutation gate is released:

```text
generated mutation
  -> acquire per-key mutation gate
  -> update authoritative row and conventional indexes
  -> update affected base column stores
  -> update affected columnar indexes
  -> publish the new column version/state
  -> release mutation gate
  -> return
```

Updating columns after the current `WorkTable::insert` or update method has
released its mutation guard is unsafe. Two same-key operations could publish
their column changes in reverse order.

The core mutation pipeline needs an internal generated observer or hook:

```rust
trait ColumnarMutationSink<Row> {
    fn insert(&self, row: &Row);
    fn update(&self, old: &Row, new: &Row, changed: ChangedColumns);
    fn delete(&self, row: &Row);
}
```

`()` implements the trait as a no-op so ordinary tables compile to the current
behavior. The generator already knows which fields an update changes and can
route work statically:

- update `notes`: no column work;
- update `metrics`: update only the base `metrics` column;
- update `host_id`: update its base column plus `host_time` and `time_host`;
- update `timestamp`: update its base column plus all three columnar indexes.

### Operation matrix

- **Insert:** accept the authoritative row, allocate a RowId, stage every
  marked field, insert columnar index keys, then publish before releasing the
  mutation gate.
- **Checked-insert rollback:** no visible RowId, column value, or columnar key
  may survive a primary/unique-index conflict.
- **Fixed-width update:** update marked fields at the existing RowId and only
  rebuild affected columnar keys.
- **Unsized reinsert:** retain the RowId; the changed row Link is irrelevant.
- **Update by secondary index:** route every locked and revalidated row through
  the same observer.
- **Upsert:** reuse the RowId on the update path and allocate on insert.
- **Delete:** invalidate the RowId, its field values, and every columnar key.
- **Row vacuum:** no column action.
- **Column compaction:** may relocate encoded values but preserves RowId.
- **Columnar-index compaction:** may rewrite key segments but preserves RowId.

## Concurrent read consistency

WorkTable does not currently promise snapshot range scans, and columnar scans
should not introduce that claim. However, one projected result must not combine
field values from different committed versions of the same RowId.

The correctness-first implementation may acquire referenced chunk read locks
in a deterministic `(column_id, chunk_id)` order. Writers acquire affected
chunk locks in the same order. Disjoint single-column writes remain concurrent;
multi-column readers see a coherent set.

The optimized implementation can replace long-held read locks with generated
per-RowId publication metadata:

```rust
struct ColumnRowState {
    generation: AtomicU64,
    active_writers: AtomicU32,
    live: AtomicBool,
}
```

Readers validate generation and active-writer state before and after gathering
the selected fields, retrying when an overlapping update occurred. Writers to
disjoint fields may proceed concurrently while a reader retries rather than
observing a torn projection.

The exact memory-ordering proof and retry bound must be documented and tested
before replacing the lock-based path.

## Query API

The current `SelectQueryBuilder` consumes complete owned rows. Reusing it
unchanged would materialize rows before filtering and erase much of the
columnar benefit. The generator needs two related APIs.

### Direct column scans

```rust
table
    .columnar()
    .metrics()
    .scan_batches(|batch| {
        aggregate(batch.values(), batch.selection());
    })?;
```

A non-indexed column may still be filtered; the operation is a vectorized/full
column scan rather than an indexed lookup:

```rust
table
    .columnar()
    .timestamp()
    .filter_range(start..end)
    .scan_batches(|batch| consume(batch.values(), batch.selection()))?;
```

### Columnar-index scans and projection

```rust
table
    .columnar_index()
    .host_time()
    .host_id_eq(42)
    .timestamp_range(start..end)
    .project(|columns| (columns.timestamp(), columns.metrics()))
    .scan_batches(|batch| {
        aggregate(batch.timestamp(), batch.metrics(), batch.selection());
    })?;
```

Execution is:

1. prune and search `host_time` segments;
2. produce selected `ColumnRowId`s;
3. gather `timestamp` and `metrics` from their base column stores;
4. return typed batches and a selection vector;
5. never materialize `HistoricalCpuRow`.

If the query requests `notes`, which is not columnar, the expensive fallback
must be explicit:

```rust
let rows = query.collect_rows()?;
```

That maps selected RowIds to primary keys/current row Links and gathers
authoritative rows from `DataPages`.

An arbitrary `where_by(|row| ...)` closure remains row-oriented because the
macro cannot reliably analyze and push down arbitrary Rust code. Generated
aggregations and group-by operations can be layered on typed batch access
later.

## Persistence

Authoritative row storage remains the recovery boundary, enabling a staged
implementation.

### Stage one: rebuild on load

- persisted row/index formats remain unchanged;
- load scans authoritative rows and reconstructs every marked column plus its
  columnar indexes;
- a missing or corrupt derived column structure cannot make the table
  unloadable;
- S3 behavior remains unchanged;
- startup cost is measured explicitly.

### Stage two: derived column checkpoints

Persist base columns independently:

```text
<table>.<field>.wt.col
```

Each file records:

- magic and format version;
- table/schema version;
- field identity and physical type;
- chunk geometry;
- compression policy and actual encoding per chunk;
- RowId range and validity metadata;
- checksums and mutation high-water mark.

Persist columnar index structures separately:

```text
<table>.<columnar-index>.wt.cidx
```

Each index file records:

- referenced field identities;
- clustering definition;
- encoded key segments and RowIds;
- segment statistics and validity metadata;
- checksums and mutation high-water mark.

Logical insert/update/delete events can use the existing persistence worker.
If a derived file fails validation or trails authoritative data, WorkTable
discards/replays/rebuilds it rather than treating it as the source of truth.

S3 may initially omit `.wt.col` and `.wt.cidx` files and rebuild locally.
Uploading them is a startup optimization rather than a correctness
requirement.

## Covered column access versus row gathering

The engine should expose and benchmark:

1. **Direct column scan:** sequentially process one marked field.
2. **Indexed column projection:** select RowIds with a columnar index and
   retrieve other marked fields.
3. **Non-columnar row gather:** select RowIds, then fetch complete rows from
   `DataPages`.

Modern NVMe reduces mechanical seek cost, but locality can still affect page
cache misses, memory bandwidth, CPU cache lines, read amplification,
decompression, and mapping/system-call overhead. Warm and cold measurements
should decide whether random row gathering is material for WorkTable's actual
layout.

Different `chunk_rows` values also create a measurable aligned-versus-gathered
column access tradeoff. The design remains correct in both modes.

## Code-generation changes

### Field model and parser

Extend the existing parsed field model with:

```rust
struct ColumnarFieldConfig {
    chunk_rows: Option<usize>,
    compression: Compression,
}
```

Likely entry points:

- `codegen/src/common/model/column.rs`;
- `codegen/src/common/parser/columns.rs`;
- `codegen/src/common/parser/attribute.rs`.

### Columnar-index model and parser

Add:

```rust
struct ColumnarIndex {
    name: Ident,
    columns: Vec<Ident>,
    cluster_by: Vec<Ident>,
}
```

Likely entry points:

- `codegen/src/worktable/mod.rs`;
- `codegen/src/common/model/columnar_index.rs`;
- `codegen/src/common/parser/columnar_indexes.rs`.

### Validation

Reject:

- an unknown field in `columnar_indexes`;
- an indexed field without `columnar(...)`;
- an empty columnar index;
- a `cluster_by` field absent from `columns`;
- duplicate fields or index names;
- `chunk_rows(0)` or unreasonable sizes;
- compression unsupported for the field type;
- unsupported column field types with an actionable diagnostic;
- `columnar(...)` on composite field syntax that cannot be encoded yet.

### Generated runtime modules

Add generated definitions for:

- stable RowId and directory;
- each typed base column store;
- column mutation observer;
- column batch views and scan builders;
- each columnar index and predicate builder;
- persistence/rebuild adapters in later phases.

## Implementation sequence

### 1. Grammar and compile-time model

- parse `columnar(...)` field attributes;
- parse `columnar_indexes`;
- add validation and compile-fail tests;
- generate no runtime changes yet.

### 2. Stable RowId and fixed-width base columns

- generate the primary-key-to-RowId directory;
- implement primitive/fixed-array column chunks;
- implement direct column scans;
- support insert/delete with no columnar indexes;
- prove ordinary schemas remain unchanged.

### 3. Full mutation coverage

- invoke generated column hooks within the per-key mutation gate;
- cover fixed updates, unsized reinserts, upserts, multi-row updates, deletes,
  unique-index rollback, and cancellation;
- add `ChangedColumns` routing;
- make row vacuum a verified no-op for columns.

### 4. First columnar index

- implement mutable key tuples plus RowIds;
- implement sealing, `cluster_by`, min/max metadata, and range/equality lookup;
- update/invalidate keys on mutations;
- add compaction fixtures.

### 5. Typed multi-column projection

- produce RowId selections from direct scans and indexes;
- gather other base columns;
- implement aligned and RowId-gathered paths;
- add explicit `collect_rows()` fallback.

### 6. Correctness campaign

Run identical randomized mutation traces through authoritative rows, base
columns, and columnar indexes. Compare quiesced results across WTI, Congee, and
Arctic primary-index configurations.

### 7. TSBS integration

Run row and columnar modes over identical data/query files. Measure:

- load throughput;
- query p50/p95/p99;
- direct column scan rate;
- columnar-index selectivity and segment pruning;
- RowId gather cost;
- row gather cost, warm and cold;
- allocations, RSS, and bytes per row;
- insert/update/delete amplification;
- compression ratio and decode cost;
- equal versus different field chunk sizes.

### 8. Rebuild-on-load persistence

Allow columnar declarations on persisted tables while retaining row storage as
the only durable source of truth.

### 9. Native derived persistence

Add `.wt.col` and `.wt.cidx` checkpoints, logical redo, corruption fallback,
compaction, and optional S3 transfer.

### 10. Optimization

- immutable column chunk publication;
- lock-free or validated batch reads;
- dictionary/bitmap key encodings;
- background index-segment merging;
- prefetching and vectorized kernels;
- optional Arrow export;
- generated aggregates and group-by operations.

## Required tests

- schemas without `columnar(...)` generate no changed runtime path;
- every marked field equals its authoritative row field after all mutation
  classes;
- a non-indexed column can be scanned and filtered correctly;
- columnar index selections equal authoritative predicates;
- multiple differently clustered indexes share base columns correctly;
- unique-index rollback leaves no RowId, column value, or columnar key ghost;
- same-key concurrent mutations cannot publish out of order;
- disjoint field updates retain their intended concurrency;
- multi-column projections never combine incompatible versions;
- scans do not return duplicate live RowIds;
- unsized reinsertion and row vacuum preserve column identity;
- column and index compaction do not lose or duplicate values;
- reload rebuild is logically equivalent;
- nullable, string, fixed-array, compression, and chunk-boundary cases are
  covered;
- aligned, RowId-gathered, and authoritative-row-gathered modes are measured
  separately.

## Smallest defensible MVP

The first benchmarkable release should provide:

- per-field `columnar(...)` syntax;
- `chunk_rows(...)` and `compression(none)` initially, with `auto` accepted
  only after it has a deterministic implementation;
- stable `ColumnRowId` identity;
- primitive and fixed-array in-memory column stores;
- synchronous insert/update/delete maintenance;
- direct scans of non-indexed columns;
- one two-column `columnar_indexes` implementation with `cluster_by`;
- projection of a third non-indexed column by selected RowIds;
- explicit complete-row gathering;
- randomized equivalence tests;
- one TSBS row-versus-columnar comparison.

It should not initially claim:

- a table-level columnar layout;
- native column/index persistence;
- automatic compression selection;
- global scan ordering;
- arbitrary user-defined column encodings;
- automatic analysis of Rust closures;
- snapshot isolation;
- Arrow as the internal representation.
