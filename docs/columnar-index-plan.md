# Columnar fields and indexes

Status: initial implementation in `feat/columnar-fields-indexes`.

## Syntax

Columnar storage is a property of an individual field. It is not a table
layout, and row storage remains authoritative.

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
        temperature: i64 columnar(
            chunk_rows(32_768),
            compression(auto),
        ),
        label: String,
    },
    columnar_indexes: {
        host_time: {
            columns: [host_id, timestamp, temperature],
            cluster_by: [host_id, timestamp],
        },
    },
);
```

`columnar(...)` creates a base column replica. A field does not need to be in a
`columnar_indexes` declaration to benefit from sequential scan or projection.
For example, `temperature` can be projected after `host_time` produces logical
row IDs, while a columnar `status` field that appears in no index can still be
scanned directly.

`columnar_indexes` declares ordering and lookup metadata over existing base
columns. `cluster_by` belongs here because it describes index order, not field
storage order. Conventional WorkTable indexes are unchanged and may coexist
with columnar indexes.

## Implemented model

The initial implementation adds:

- per-field `columnar(chunk_rows(...), compression(...))` parsing and
  validation;
- a `columnar_indexes` section with `columns` and `cluster_by` validation;
- a stable `ColumnRowId`, independent of physical `data_bucket::Link` values;
- separately chunked vectors for every columnar field;
- a shared primary-key-to-row-ID directory;
- ordered clustered metadata backed by a `BTreeMap` and row-ID sets;
- generated exact-lookup, ordered-index-scan, field-scan, and projection APIs;
- maintenance for inserts, updates, in-place updates, deletes, reinserts, and
  vacuum link changes;
- derived-state rebuild after persisted/read-only load without changing the
  existing WorkTable disk format.

For the example above, generated APIs include:

```rust
let ids = table.columnar_select_host_time(host_id, timestamp);
let temperatures = table.columnar_project_temperature(&ids);
let primary_keys = table.columnar_resolve_primary_keys(&ids);
let all_temperatures = table.columnar_scan_temperature();
let clustered_ids = table.columnar_scan_host_time();
```

Field scans and projections return owned values in this first API. That keeps
locks out of the public return type and gives callers a coherent batch they can
retain independently of later mutations.

## Stable identity and mutation flow

The row store remains the source of truth:

```text
primary key -> WorkTable row/link
            -> ColumnRowId directory
            -> per-field chunks
            -> zero or more clustered columnar indexes
```

A vacuum may change the WorkTable link without changing `ColumnRowId`. An
update with the same primary key also retains the row ID. Delete removes the
directory entry, field slots, and clustered entries; IDs are not reused during
the process lifetime.

Generated mutation paths use the existing per-key mutation gate. Direct row
insert/reinsert/delete hooks update columnar state under its own lock. Update
paths that mutate archived fields in place mark the replica dirty; the next
columnar access rebuilds it from authoritative rows while preserving IDs for
surviving primary keys. This is deliberately a correctness-first design. The
dirty rebuild can later become an incremental difference application once its
concurrency invariants and benchmark benefit are established.

## Chunk alignment

Each field owns its `chunk_rows` setting. Different values remain correct
because all access is joined by `ColumnRowId`; equal values provide an aligned
fast path for multi-column vector work. The runtime does not require aligned
physical chunks.

## Persistence

This change does not introduce a columnar on-disk format. The row store and
existing indexes retain their current formats. Generated columnar state is
marked as derived and skipped by `PersistIndex`; a loaded table rebuilds it
from authoritative rows on first columnar access.

That choice keeps this PR format-compatible and lets benchmarks answer whether
native column checkpoints are worth their complexity. A later format can add
sealed immutable chunks, manifests, checksums, and recovery watermarks without
changing the DSL or logical row identity.

## Compression boundary

The DSL accepts `none`, `auto`, `delta`, `rle`, and `dictionary`, and generated
columns retain the requested policy as metadata. Mutable chunks are currently
stored unencoded: `auto` resolves to no encoding, and the explicit codecs are
not yet applied. `ColumnCompression::is_encoded()` therefore returns `false`.

This is intentional rather than a compression claim. Encoding belongs on
sealed/immutable chunks so point updates do not repeatedly rewrite compressed
buffers. Codec implementation and per-type validation are follow-up work and
must be benchmarked independently.

## Current concurrency boundary

Columnar state is derived and protected by a table-local read/write lock.
Ordinary row reads do not touch it, so declaring a columnar field does not add a
lock to the existing select path. Columnar reads clone a result batch while
holding the replica read lock. Mutations update or dirty the replica only after
the authoritative row operation succeeds.

Before calling this production-ready for HFT workloads, benchmarks must cover:

- row-operation throughput with no columnar access;
- insert/update/delete overhead with columnar fields and indexes;
- exact lookup and ordered scan throughput;
- p50/p95/p99 latency under mixed readers and writers;
- dirty-rebuild latency after in-place updates;
- memory amplification by field type, chunk size, and index cardinality.

## Next implementation slices

1. Add range predicates and generated projection batches that fetch several
   fields in one lock acquisition.
2. Replace dirty full rebuilds with typed incremental mutations for archived
   in-place updates.
3. Add null bitmaps and specialized fixed-width chunk kernels.
4. Seal cold chunks and implement actual delta/RLE/dictionary codecs.
5. Benchmark row-store random projection against native column checkpoints,
   then add a disk format only if the result justifies it.
6. Add a cost model that chooses conventional index lookup, clustered
   columnar lookup, or base-column scan.
