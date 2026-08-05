# WorkTables Columnar Storage and Hybrid Row/Column Access

## Status

This document is a design investigation, not documentation of an implemented feature.
The syntax, APIs, storage structures, and roadmap below are proposals.

## Executive summary

WorkTables can support row-oriented and column-oriented access to the same logical
table. The most practical design is not to replace the current row store. It is to
keep the row representation authoritative and add opt-in, table-specific columnar
projections for analytical access.

The recommended first implementation is:

- Keep the existing row store as the source of truth.
- Give every logical row a stable `RowId` that is independent of its physical page
  location.
- Define columnar projections in the WorkTables DSL.
- Materialize projections as immutable, Arrow-compatible columnar batches or row
  groups.
- Initially rebuild projections manually.
- Later maintain them asynchronously from a logical table-change stream.
- Store recent changes in a small mutable delta and periodically compact that delta
  into immutable columnar row groups.
- Treat persisted projections as rebuildable caches until recovery and atomic
  publication semantics are mature.

This can provide most of the practical benefits of both layouts. It does not provide
them for free: the costs are additional memory, write amplification, compaction,
recovery complexity, and potentially stale columnar reads.

The architecture is feasible and has precedents. SQL Server supports a nonclustered
columnstore alongside a row store, Oracle Database In-Memory maintains row and column
formats concurrently, and the fractured-mirrors research describes maintaining
different physical representations of the same logical data.

## What WorkTables has today

WorkTables currently owns row data through `DataPages<Row>` and maintains separate
primary and secondary index structures. Rows are serialized as complete values into
byte-oriented pages. Secondary indexes map indexed values to one or more physical row
links.

Relevant implementation areas include:

- [`src/table/mod.rs`](src/table/mod.rs)
- [`src/in_memory/data.rs`](src/in_memory/data.rs)
- [`src/index/primary_index.rs`](src/index/primary_index.rs)
- [`codegen/src/generators/in_memory/index/mod.rs`](codegen/src/generators/in_memory/index/mod.rs)
- [`codegen/src/generators/in_memory/queries/update.rs`](codegen/src/generators/in_memory/queries/update.rs)
- [`src/persistence/operation/operation.rs`](src/persistence/operation/operation.rs)

This is a good foundation for point reads, inserts, updates, deletes, and indexed row
retrieval. It is not naturally optimized for queries that scan a few fields across
many rows. Such a query still traverses row records and pays for row decoding and
unneeded fields.

## Proposed DSL

Columnar layout should be an opt-in physical projection on a particular table:

```rust
worktable!(
    name: Trade,
    version: 1,
    persist: true,

    columns: {
        id: u64 primary_key autoincrement,
        account_id: u64,
        timestamp: u64,
        symbol: String,
        price: f64,
        quantity: u64,
        notes: String,
    },

    storage: row,

    indexes: {
        account_idx: account_id,
        timestamp_idx: timestamp,
    },

    projections: {
        analytics: columnar {
            columns: [
                id,
                account_id,
                timestamp,
                symbol,
                price,
                quantity,
            ],
            order_by: [timestamp],
            row_group_rows: 65_536,
            maintenance: async { max_lag_ms: 100 },
            persistence: rebuild,
        },
    },
);
```

This says that:

- the row store remains authoritative;
- `analytics` is a named physical representation;
- only selected columns are mirrored;
- columnar rows are grouped and optionally ordered for analytical access;
- projection updates may lag row commits by up to the configured target;
- the projection may be reconstructed from the row store and logical change log.

The initial, safer form should be manual:

```rust
projections: {
    analytics: columnar {
        columns: [timestamp, account_id, price, quantity],
        row_group_rows: 65_536,
        maintenance: manual,
        persistence: rebuild,
    },
},
```

The application would explicitly build or refresh it:

```rust
table.analytics().rebuild()?;
let batch = table.analytics().scan()?;
```

Once maintenance and visibility semantics are established, WorkTables could expose:

```rust
maintenance: manual
maintenance: async { max_lag_ms: 100 }
maintenance: sync
```

`sync` should not be offered until a row mutation, index mutation, projection change,
and persistence record can be published atomically.

## Generated query API

A projection-specific API makes physical intent explicit:

```rust
let result = table
    .analytics()
    .filter(col!(timestamp).ge(start))
    .select((col!(account_id), col!(price), col!(quantity)))
    .scan()?;
```

An eventual unified planner could select the representation explicitly:

```rust
let result = table
    .scan()
    .using(TradeProjection::Analytics)
    .filter(col!(timestamp).ge(start))
    .select((col!(account_id), col!(price)))
    .collect()?;
```

Automatic row-versus-column routing is possible later, but it should not be the first
interface. Explicit routing makes performance and freshness behavior understandable.

## Table-specific or global?

Projection definitions should be table-specific. A Cargo feature can enable the
columnar machinery globally, but it should not silently create a second layout for
every table.

Different tables have different economics:

- transaction and session tables may be write-heavy and lookup-oriented;
- event and measurement tables may be append-heavy and scan-heavy;
- some tables need only three projected fields out of twenty;
- some tables contain values that do not have an efficient columnar representation;
- freshness requirements vary by table and projection.

Making projection layout part of the table schema also gives code generation enough
information to validate types and generate typed scanners.

## Bidirectional row identity

A bidirectional mapping is useful, but it does not itself create the best of both
worlds. It supplies stable identity and navigation between physical representations.

The central identifier should be a stable logical `RowId`, not a physical page link:

```text
PrimaryKey <-> RowId
                  |---> current RowLink
                  `---> ColumnLocator
```

For example:

```rust
struct ColumnLocator {
    generation: u64,
    row_group: u32,
    offset: u32,
}
```

Physical row links can change when pages are compacted, values move, slots are reused,
or storage is reconstructed. A stable `RowId` survives those operations.

This directory supports hybrid execution:

1. Scan projected columns and produce matching `RowId` values.
2. Resolve those identifiers to current row locations.
3. Fetch complete rows only for the surviving matches.

It also supports the opposite path: a point lookup can find a row and then locate its
projected value if necessary.

The mapping cannot remove the costs of maintaining two physical copies. It solves
identity and lookup; it does not solve consistency, memory use, or write amplification.

## Storage architecture

### Authoritative row store

The existing row store continues to handle:

- inserts, updates, and deletes;
- point reads and complete-row retrieval;
- primary and secondary indexes;
- authoritative persistence and recovery.

### Immutable columnar base

The analytical representation should consist of immutable row groups. Each projected
field is held as a contiguous column with validity information and, where useful,
encoding or compression metadata.

An Arrow-compatible representation is attractive because it provides:

- a defined in-memory columnar layout;
- validity bitmaps and common primitive representations;
- an ecosystem of vectorized kernels;
- a direct interoperability path for Python, Polars, PyArrow, DuckDB, and DataFusion;
- zero-copy or low-copy export where WorkTables buffers satisfy Arrow's ownership,
  alignment, and lifetime requirements.

Arrow compatibility does not require making Arrow the authoritative WorkTables
storage engine. WorkTables can own immutable buffers shaped like Arrow arrays and
export them through the Arrow C Data Interface or Rust Arrow crates.

### Mutable delta

In-place mutation of compressed columnar arrays is expensive. Recent inserts, updates,
and deletes should instead accumulate in a small mutable delta:

```text
columnar query = immutable base + delta updates - tombstones
```

The delta can contain:

- new projected rows;
- the latest projected values for updated `RowId`s;
- deleted `RowId` tombstones;
- a monotonically increasing logical sequence number.

A background merge periodically builds new immutable row groups and atomically swaps
the projection generation. Readers retain an `Arc` to the old generation until they
finish.

This is similar in spirit to delta/main designs and lineage-based update systems. It
keeps foreground mutations cheap while preserving efficient scans after compaction.

## Logical change stream

The existing persistence and index change records are structural: they describe row
bytes, physical links, and index operations. A columnar projection should not be driven
directly by those physical records.

It needs a logical table-change stream, for example:

```rust
enum ProjectionChange {
    Insert {
        sequence: u64,
        row_id: RowId,
        projected_values: ProjectedTrade,
    },
    Update {
        sequence: u64,
        row_id: RowId,
        changed_fields: TradeFieldMask,
        projected_values: ProjectedTradePatch,
    },
    Delete {
        sequence: u64,
        row_id: RowId,
    },
}
```

Important properties are:

- logical identity rather than a physical page address;
- a sequence or commit epoch for ordering and visibility;
- enough projected data to avoid rereading unstable physical locations;
- a changed-field mask so updates to unprojected fields require no columnar work;
- idempotent replay where possible.

This stream can feed in-memory projections, persistence, replication, or future change
data capture, but its semantics should be defined independently from any one consumer.

## Consistency models

### Manual

The application explicitly rebuilds or refreshes the projection. This has the simplest
correctness model and is suitable for an initial implementation and benchmarks.

### Asynchronous

A committed row mutation appends a compact logical projection change. A background
worker applies changes to the delta and compacts them later:

```text
row mutation
    -> row/index update
    -> append logical change
    -> commit

background worker
    -> apply changes to delta
    -> build columnar row groups
    -> atomically publish a new projection generation
```

Columnar reads report their applied sequence or freshness point. The API can allow the
caller to wait for a required sequence when necessary.

### Synchronous

Immediate consistency requires all affected state to become visible together:

```text
row mutation
    -> row indexes
    -> columnar delta
    -> persistence record
    -> atomic publication
    -> commit
```

This is the most fragile option. Partial failure, rollback, process crashes, reader
snapshots, and lock ordering all require a real commit protocol. Implementing it before
WorkTables has atomic multi-structure publication would risk inconsistent mirrors.

## Write amplification: is mirroring worth it?

Often, yes. Major database systems deliberately offer parallel row and column
representations because the savings from repeated analytical scans can outweigh the
maintenance cost.

The trade is better described as:

> Additional write, memory, recovery, and consistency cost in exchange for much
> cheaper analytical reads without sacrificing fast transactional row access.

### The cost is not necessarily exactly 2x

A mirrored mutation may involve:

1. Writing the authoritative row.
2. Updating ordinary indexes.
3. Appending a logical projection change.
4. Encoding that change into columnar storage.
5. Periodically compacting columnar row groups.

Total lifecycle CPU and byte writes can therefore exceed 2x, especially when projected
values are updated repeatedly before compaction.

However, asynchronous maintenance means the foreground transaction need only append a
small logical change. Columnar encoding, sorting, compression, and compaction can happen
later and in batches. Foreground write latency can therefore increase by much less than
2x even though the system eventually performs more total work.

Physical storage is also not necessarily doubled. A projection can contain only the
analytically useful columns, and columnar values may compress much better than complete
rows. Consequently:

- logical information is duplicated;
- physical storage may be substantially less than another full row copy;
- total background CPU and write I/O may exceed 2x;
- foreground latency can remain relatively small with asynchronous maintenance.

### When the benefit offsets the cost

Mirroring is attractive when the same data supports point operations and substantial
scans:

- writes use primary keys or selective secondary indexes;
- analytical queries consume only a subset of columns;
- queries filter or aggregate thousands to millions of rows;
- the table is read much more often than it is mutated;
- slightly stale analytical reads are acceptable;
- the data remains in the table long enough to amortize projection construction;
- projected columns are updated less often than unprojected columns.

For a twenty-column table where an aggregation needs only `price` and `quantity`, a row
scan may fetch and decode complete records. A columnar scan reads two tightly packed
arrays and can process them in vectorized batches. One scan that becomes an order of
magnitude cheaper can pay for many projection updates.

A useful break-even model is:

```text
analytical read savings * analytical query frequency
    >
projection maintenance cost * mutation frequency
    + additional memory/storage cost
    + compaction and recovery cost
```

The decisive ratio is not simply reads divided by writes. It is the bytes and CPU saved
by analytical queries compared with the bytes and CPU spent maintaining the projected
fields.

### When it does not pay

Mirroring is likely a net loss when:

- the table is write-heavy and rarely scanned;
- most reads retrieve complete rows;
- the table is small enough that row scans are already cheap;
- every analytical read must observe the latest committed mutation immediately;
- updates frequently change most projected columns;
- rows contain many variable-length or unsupported values;
- memory capacity is more important than analytical latency;
- projections are created speculatively without a measured query workload.

A lookup-oriented session table is a poor candidate. An append-heavy event or trade
table queried repeatedly by time range and aggregate is a strong candidate.

## Covering indexes as an intermediate step

WorkTables could first add included fields to secondary indexes:

```rust
indexes: {
    timestamp_idx: timestamp include [account_id, price, quantity],
},
```

This can answer some queries without fetching complete rows and would require less new
machinery than a complete column store. It is useful for selective, index-ordered
queries.

It is not equivalent to columnar storage:

- values remain organized around index entries rather than contiguous vectors;
- broad scans still traverse index structures;
- it does not naturally produce Arrow arrays;
- compression and SIMD execution opportunities are weaker;
- every covered index independently duplicates its included values.

Covering indexes are a good milestone, not the final analytical representation.

## Alternative: PAX-style pages

A PAX-style layout stores one logical copy of a page but organizes fields into
mini-columns within that page:

```rust
storage: pax {
    page_rows: 1_024,
}
```

This can improve cache behavior for scans without maintaining a complete second copy.
It is an interesting compromise, but it changes the authoritative storage engine and
therefore touches row serialization, updates, page allocation, persistence, and
recovery. It may also provide less scan efficiency than large immutable columnar
batches.

PAX should be evaluated experimentally after the projection path is benchmarked. It is
not the easiest first implementation.

## Pure columnar tables

A future table could declare columnar storage as authoritative:

```rust
worktable!(
    name: Measurement,
    version: 1,
    persist: true,

    columns: {
        timestamp: u64,
        sensor_id: u32,
        value: f64,
    },

    storage: columnar {
        row_group_rows: 65_536,
        order_by: [timestamp],
        compression: auto,
    },
);
```

That is effectively a new storage engine. Point updates, deletes, row reconstruction,
primary-key enforcement, persistence, and compaction would all need columnar-native
implementations. It should not be the first target.

## Index backend syntax should remain separate

The proposed per-index `using` syntax in
[`docs/index-backend-dsl-proposal.md`](docs/index-backend-dsl-proposal.md) selects the
physical map implementation for an index. A table projection is a separate concept.

Keeping the DSL namespaces distinct avoids ambiguity:

```rust
indexes: {
    account_idx: account_id using hash,
    timestamp_idx: timestamp using btree,
},

projections: {
    analytics: columnar {
        columns: [timestamp, account_id, price, quantity],
        maintenance: async { max_lag_ms: 100 },
    },
},
```

An index maps keys to row identities or locations. A projection is another physical
representation of selected table values.

## Type-system requirements

The macro must reject or explicitly encode fields that cannot be represented safely.
A trait boundary might look like:

```rust
trait ColumnarType {
    type Array;

    fn append_to(&self, builder: &mut Self::Array);
    fn logical_type() -> ColumnarLogicalType;
}
```

Straightforward initial types include:

- integers and floating-point values;
- booleans;
- fixed-width identifiers;
- timestamps with defined units;
- nullable forms of supported primitives;
- UTF-8 strings and bytes with offset buffers.

Early limitations are likely to include:

- nested or recursive Rust structures;
- arbitrary generic fields;
- borrowed data and complex lifetimes;
- user-defined serialization without a declared logical type;
- unstable enum layouts;
- very large variable-length values;
- application types whose equality, ordering, or null semantics do not map cleanly.

Arrow interoperability adds further requirements around buffer alignment, ownership,
validity bitmaps, offsets, and lifetime guarantees.

## Persistence and recovery

### First stage: rebuildable projections

The row store and its normal persistence remain authoritative. On startup, WorkTables
rebuilds a projection from rows or from a durable logical change stream. This avoids
making projection file compatibility part of the first release.

### Later stage: persistent projection cache

A persistent projection file might use a separate suffix such as `.wt.col` and include:

- table schema fingerprint;
- projection definition fingerprint;
- source commit epoch or sequence;
- row-group directory;
- checksums;
- encoding and format version;
- enough metadata to detect incomplete publication.

Recovery can load the last valid projection generation and replay later logical
changes. If validation fails, WorkTables discards the cache and rebuilds it from the
authoritative row data.

Persisted projections should remain disposable caches until their transaction and
recovery guarantees equal those of the row store.

## Snapshot and query semantics

Hybrid queries introduce an important visibility rule. A columnar filter cannot safely
produce `RowId`s from one logical snapshot and fetch current rows from another without
defining how updates and deletes are handled.

The query must pin or record:

- the row-store snapshot or commit epoch;
- the projection generation and applied sequence;
- the delta visibility boundary;
- the lifetime of `RowId` mappings used during the query.

At minimum, an asynchronous projection result should expose its freshness:

```rust
struct ProjectionSnapshot {
    generation: u64,
    applied_sequence: u64,
}
```

Applications that need read-your-writes behavior can wait until the projection reaches
a required sequence or fall back to the row path.

## Suggested implementation roadmap

### Phase 0: benchmark and validate the premise

- Choose two or three representative tables and query shapes.
- Measure row scan bandwidth, decoding time, cache misses, and full-row lookup cost.
- Build a standalone Arrow `RecordBatch` from a WorkTable snapshot.
- Compare row scans, covering-index scans, and Arrow/vectorized scans.
- Measure projection build cost and memory consumption.
- Define the minimum improvement that justifies the feature.

Estimated investigation: four to six weeks for a credible prototype and benchmark
matrix, depending on existing benchmark infrastructure.

### Phase 1: static manual projection

- Extend the parser with a `projections` section.
- Validate projected fields and supported types.
- Generate a projection row/builder type.
- Build immutable Arrow-compatible batches from a table snapshot.
- Expose explicit typed scans.
- Keep projections in memory and rebuild them manually.

This proves the DSL, type mapping, and analytical benefit without adding write-path
correctness risks.

### Phase 2: stable identity and logical changes

- Introduce stable `RowId` values.
- Add primary-key-to-`RowId`, `RowId`-to-row-link, and projection locator directories.
- Define commit sequences and a logical table mutation stream.
- Implement asynchronous application into a mutable projection delta.
- Expose projection freshness and wait-for-sequence behavior.

### Phase 3: compaction and generations

- Build immutable row groups from base plus delta.
- Track tombstones and latest updates.
- Publish generations atomically.
- Retain old generations for active readers.
- Add memory budgets, backpressure, and compaction metrics.

### Phase 4: rebuildable persisted-table support

- Rebuild projections after authoritative recovery.
- Validate restart behavior, schema changes, deletes, and interrupted rebuilds.
- Keep projection data non-authoritative.

### Phase 5: persistent projection cache

- Define a versioned projection file format.
- Add fingerprints, epochs, checksums, and atomic generation publication.
- Load a valid cached generation and replay later changes.
- Fall back safely to a rebuild.

### Phase 6: advanced options

- Cost-based automatic row/column routing.
- Synchronous projections where justified.
- PAX or other one-copy layouts.
- Pure authoritative columnar tables.
- Dictionary encoding, compression policies, and row-group statistics.
- Multiple projections for distinct orderings or workloads.

## Measurements required before committing to the design

The prototype should report at least:

- insert and update latency at p50, p95, and p99;
- mutation throughput with no projection, manual projection, and asynchronous
  projection;
- bytes written per logical insert/update/delete;
- memory per logical row for the row store, directories, base, and delta;
- full-table and selective scan throughput;
- aggregation throughput for one, two, and several columns;
- projection lag under sustained writes;
- compaction CPU, duration, and temporary memory peak;
- restart and rebuild duration;
- performance when projected columns are updated repeatedly;
- hybrid filter-then-row-fetch performance at different selectivities.

Without this matrix, a hybrid store risks adding considerable complexity for a
workload that may be better served by covering indexes or Arrow export on demand.

## Principal limitations and fragility

1. **Memory and storage duplication.** Selected values, row identity, validity data,
   and projection metadata exist in another representation.
2. **Write amplification.** Every projected mutation creates additional work, and
   compaction rewrites data again.
3. **Consistency complexity.** Row data, indexes, deltas, projections, and persistence
   need defined publication and recovery boundaries.
4. **Staleness.** Asynchronous projections may not immediately reflect committed
   mutations.
5. **Snapshot complexity.** Hybrid queries must not mix incompatible projection and row
   versions.
6. **Delete and update overhead.** Immutable columnar groups require tombstones and
   overlays until compaction.
7. **Compaction spikes.** Merges consume CPU, memory, and bandwidth and require
   backpressure.
8. **Type restrictions.** Not every Rust field has an efficient or stable columnar
   representation.
9. **Schema evolution.** Adding, removing, or changing projected fields may require a
   complete rebuild.
10. **Recovery surface area.** A persisted mirror introduces generation, checksum,
    replay, and partial-publication failure cases.
11. **Optimizer complexity.** Automatic selection between row, index, covering index,
    and projection paths requires statistics and a cost model.
12. **API lifetime hazards.** Zero-copy Arrow or Python export must prevent WorkTables
    buffers from moving or being freed while consumers still hold them.

## Recommendation

WorkTables should pursue columnar access as an optional analytical projection, not as
an immediate replacement for the current row store.

The first deliverable should be a manual, in-memory, Arrow-compatible projection over
an authoritative row table. It should be narrow enough to answer three questions with
measurements:

1. How much faster are representative scans and aggregations?
2. How much memory does the projection require?
3. How expensive is building and refreshing it?

If the results justify the feature, the next architectural investment should be stable
`RowId` identity and a logical commit-ordered change stream. Asynchronous delta
maintenance and generation-based compaction should follow. Synchronous mirroring and
authoritative columnar tables should remain later projects.

The approach can deliver genuinely useful parallel row and column access. Its success
depends on keeping the feature selective and explicit, measuring the target workloads,
and accepting bounded staleness before attempting fully synchronous dual-format
transactions.

## References

- [Fractured Mirrors: A Natural Asymmetric Architecture for Commercial Database Systems](https://www.vldb.org/conf/2002/S12P03.pdf)
- [PAX: A Cache-Conscious Data Organization](https://www.pdl.cmu.edu/PDL-FTP/Database/pax_abs.shtml)
- [SQL Server columnstore indexes overview](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/columnstore-indexes-overview?view=sql-server-ver17)
- [SQL Server `CREATE COLUMNSTORE INDEX`](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-columnstore-index-transact-sql?view=sql-server-ver17)
- [Oracle Database In-Memory Guide](https://docs.oracle.com/cd/E96517_01/inmem/database-memory-guide.pdf)
- [Apache Arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html)
- [PostgreSQL index-only scans and covering indexes](https://www.postgresql.org/docs/current/indexes-index-only-scans.html)
- [SAP HANA delta merge](https://help.sap.com/docs/SAP_HANA_PLATFORM/6b94445c94ae495c83a19646e7c3fd56/bd9ac728bb57101482b2ebfe243dcd7a.html)
- [L-Store: A Real-time OLTP and OLAP System](https://expolab.org/papers/l-store.pdf)
