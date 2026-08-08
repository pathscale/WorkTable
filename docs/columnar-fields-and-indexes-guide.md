# Columnar WorkTables

## Add analytical access without giving up the row

WorkTable's columnar support adds purpose-built column replicas and clustered
columnar indexes to the same macro that defines the authoritative row table.
Applications keep their primary keys, row-oriented point operations, generated
queries, persistence behavior, and lock model. Selected fields also become
available through chunked column scans, ordered lookup, and projection APIs.

The result is a practical hybrid: one schema can serve the point-oriented path
and the analytical path without introducing a second hand-maintained data
model.

> The primary key remains authoritative and load-bearing. An immutable sort ID
> is compact supplemental metadata for column position, stable ordering, and
> cross-column alignment. It never replaces the primary key.

## Complete example

```rust
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: HistoricalCpu,
    persist: true,

    columns: {
        id: u128 primary_key,

        host_id: u64 columnar(
            chunk_rows(65_536),
            compression(auto)
        ),

        captured_at_ns: u64 columnar(
            chunk_rows(65_536),
            compression(delta)
        ),

        cpu_percent: f32 columnar(
            chunk_rows(65_536),
            compression(auto)
        ),

        status: String columnar(
            chunk_rows(16_384),
            compression(dictionary)
        ),

        // Ordinary row-only data remains ordinary.
        diagnostic_blob: Vec<u8>,
    },

    columnar_indexes: {
        // Optional. ImmutableSortId32 is the default.
        immutable_sort_id: ImmutableSortId16,

        host_time: {
            columns: [host_id, captured_at_ns, cpu_percent],
            cluster_by: [host_id, captured_at_ns],
        },
    },
);
```

This declaration creates four column replicas. `host_time` provides exact
lookup and deterministic clustered traversal by `(host_id, captured_at_ns)`.
`status` demonstrates a non-indexed columnar field: it can be scanned and
projected even though it is not part of any `columnar_indexes` entry.
`diagnostic_blob` remains row-only.

## The three independent choices

### 1. Which fields are columnar?

Attach `columnar(...)` directly to each field that needs a scan or projection
path:

```rust
cpu_percent: f32 columnar(
    chunk_rows(65_536),
    compression(auto)
),
```

This does not turn the entire table into a column store. It creates a derived
column replica for that field while the row store remains the source of truth.
Different fields may use different chunk sizes.

### 2. Which access paths are clustered?

Declare a named entry in `columnar_indexes`:

```rust
host_time: {
    columns: [host_id, captured_at_ns, cpu_percent],
    cluster_by: [host_id, captured_at_ns],
},
```

`cluster_by` defines the ordered key. `columns` documents the fields served by
the access path and is validated against the base column declarations.
Conventional WorkTable indexes may coexist with columnar indexes.

### 3. How wide is the immutable sort ID?

The optional table-wide setting appears at the top of `columnar_indexes`:

```rust
columnar_indexes: {
    immutable_sort_id: ImmutableSortId16,
    // named indexes follow
},
```

Omit the setting to use `ImmutableSortId32`.

| Type | Simultaneously assigned positions | Typical use |
| --- | ---: | --- |
| `ImmutableSortId8` | 256 | Tiny bounded/embedded tables and tests |
| `ImmutableSortId16` | 65,536 | Bounded caches, desktop data, compact windows |
| `ImmutableSortId32` | 4,294,967,296 | Default; general-purpose workloads |
| `ImmutableSortId64` | 18,446,744,073,709,551,616 | Explicit very-large logical range |

The configured width is a capacity contract. The user is responsible for
choosing a type large enough for the maximum number of simultaneously assigned
columnar rows. WorkTable does not guess, widen the type, or silently migrate
the table.

When the selected range is exhausted, insertion returns:

```rust
WorkTableError::ImmutableSortIdExhausted(bits)
```

The failed insert is rolled back. Existing rows remain valid. WorkTable never
wraps, truncates, replaces another row, or panics because the configured range
was exceeded.

## Generated API

The example generates methods shaped like these:

```rust
// Exact clustered-key lookup.
let rows = table.columnar_select_host_time(host_id, captured_at_ns)?;

// Clustered traversal in (host_id, captured_at_ns) order.
let ordered_rows = table.columnar_scan_host_time()?;

// Project one field for a retained result set.
let cpu = table.columnar_project_cpu_percent(&rows)?;

// Scan an indexed or non-indexed column directly.
let statuses = table.columnar_scan_status()?;
```

Columnar selection returns owned row references rather than physical links:

```rust
ColumnarRowRef {
    primary_key: HistoricalCpuPrimaryKey { id: 938_271_u128 },
    immutable_sort_id: ImmutableSortId16(41_207),
}
```

Every projection returns the row reference alongside its value. Results can be
retained after the internal read lock is released:

```rust
for (row_ref, cpu_percent) in table.columnar_project_cpu_percent(&rows)? {
    println!("{:?}: {cpu_percent}", row_ref.primary_key);
}
```

Projection revalidates the pair. If a deleted row's compact slot has since been
reused, a stale `{ primary_key, immutable_sort_id }` cannot alias the new row.

## Insert, update, delete, and vacuum

The generated mutation path maintains the derived columnar state together with
the existing indexes:

```text
authoritative primary key -> authoritative WorkTable row/link
                          -> immutable sort ID directory
                          -> per-field column chunks
                          -> zero or more clustered columnar indexes
```

- Insert allocates a compact sort ID and writes the selected column replicas.
- Update retains the ID for the same primary key and refreshes affected values
  and clustered keys.
- Delete removes column values and clustered entries, then makes the bounded
  integer slot reusable.
- Vacuum may move the physical row link without changing either the primary key
  or immutable sort ID.
- In-place archived-field changes mark the derived replica dirty; the next
  columnar access rebuilds it from authoritative rows.

The immutable sort ID is stable for one row's lifetime. It is not a mutable
rank in the current clustered order. The clustered index key determines order;
the ID supplies a stable tie-break and column-alignment token.

## Persistence and recovery

The first implementation does not change WorkTable's on-disk format. Row data
and existing persistent indexes retain their current formats. Columnar state is
derived, omitted from `PersistIndex`, and rebuilt from authoritative rows after
load.

That boundary provides three useful properties:

- existing persisted tables remain format-compatible;
- the primary key and row store remain the recovery authority;
- native column checkpoints can be evaluated with benchmarks before adding a
  new durable format.

The declared compression policy is currently retained as metadata. Mutable
chunks are stored unencoded, so `auto`, `delta`, `rle`, and `dictionary` are not
yet performance or space-saving claims. Actual codecs belong on sealed,
immutable chunks where updates do not repeatedly recompress hot buffers.

## Practical patterns

### Bounded HFT window

Use a 16-bit sort ID when the application enforces a hard live-window limit at
or below 65,536 rows. Keep order/event IDs as the authoritative primary key and
cluster analytical access by instrument and timestamp.

```rust
columns: {
    event_id: u128 primary_key,
    instrument: u32 columnar(chunk_rows(16_384), compression(auto)),
    timestamp_ns: u64 columnar(chunk_rows(16_384), compression(delta)),
    price_ticks: i64 columnar(chunk_rows(16_384), compression(delta)),
},
columnar_indexes: {
    immutable_sort_id: ImmutableSortId16,
    instrument_time: {
        columns: [instrument, timestamp_ns, price_ticks],
        cluster_by: [instrument, timestamp_ns],
    },
},
```

The application must enforce the bound. If bursts can exceed it, retain the
32-bit default.

### SaaS telemetry

Use the 32-bit default, cluster by tenant and time, and keep large diagnostic
payloads row-only. Add a non-indexed columnar status or duration field when the
common operation is scanning or projecting it after another index identifies
the rows.

### Desktop or embedded catalog

Choose 8 or 16 bits only when the domain itself supplies a strict upper bound.
The smaller token reduces directory and clustered-index footprint; it does not
change the primary-key representation or its behavior elsewhere in WorkTable.

## What this design deliberately avoids

- No table-wide `layout: columnar` switch.
- No replacement or weakening of the primary key.
- No implicit 64-bit row identifier when a bounded workload needs less.
- No claim that configured compression is already applied.
- No new columnar disk format in the initial compatibility-first slice.
- No silent overflow or automatic type widening.

## Current and planned surface

Available in the initial implementation:

- per-field chunked column replicas;
- a shared configurable immutable sort-ID width;
- exact clustered lookup and ordered scan;
- single-field scan and projection;
- insert/update/delete/reinsert/vacuum maintenance;
- safe stale-reference validation;
- lazy rebuild after persistent load.

Natural follow-up work:

- range predicates and batched multi-column projection;
- incremental archived in-place updates instead of dirty full rebuilds;
- null bitmaps and fixed-width vector kernels;
- sealed-chunk compression;
- native column checkpoints, manifests, and recovery watermarks;
- a measured cost model for row, conventional-index, clustered-columnar, and
  base-column scan paths.

WorkTable's columnar direction is additive by design: applications opt fields
and access paths into analytical storage while the proven primary-key and row
machinery continues to carry correctness.
