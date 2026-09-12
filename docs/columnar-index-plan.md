# Columnar side-index implementation plan

Status: phase-one implementation in `feat/columnar-fields-indexes`. The complete user and reviewer
guide is [`columnar-fields-and-indexes-guide-v3.md`](columnar-fields-and-indexes-guide-v3.md).

## Scope boundary

WorkTable has three distinct storage flavors:

1. **Tabular** — the existing authoritative row engine.
2. **Tabular + columnar side indexes** — the scope of this branch. Selected field values and
   clustered keys are duplicated into derived structures for cheaper columnar-flavored access.
3. **Columnar** — an authoritative vector layout, vectorized execution, sealed/encoded segments,
   and native columnar persistence. This is not implemented by this branch.

The phase-one feature must not be marketed or documented as the third flavor.

## Accepted DSL

```rust
worktable!(
    name: HistoricalCpu,
    persist: true,
    columns: {
        id: u128 primary_key,
        host_id: u64 columnar,
        captured_at_ns: u64 columnar,
        temperature: i64 columnar(chunk_rows(32_768), compression(none)),
        status: String columnar,
        diagnostic_blob: DiagnosticBlob,
    },
    columnar_indexes: {
        host_time: {
            cluster_by: [host_id, captured_at_ns],
        },
    },
    config: {
        columnar_slot_id: ColumnSlotId32,
        columnar_chunk_rows: 65_536,
    },
);
```

- Bare `columnar` uses defaults.
- `compression(none)` is the only accepted policy until a codec exists.
- `cluster_by` orders index metadata, not the side-field vectors.
- `columns:` inside a columnar index has been removed as semantically redundant.
- Slot width is a table setting, independent of whether the table declares any clustered side
  index.

## Identity and capacity

`ColumnSlotId8|16|32|64` uses its complete unsigned range for side-index slot positions. It neither
replaces the primary key nor represents sort rank.

An opaque `ColumnarRowRef` carries:

```text
primary key + slot + separate u64 generation + table incarnation
```

Delete increments the generation before slot reuse. Generation never wraps; an exhausted slot is
retired. Table incarnation invalidates retained refs across a new/reopened instance. The ref is not
serializable and exposes only the authoritative primary key.

The schema author is responsible for selecting a width that covers maximum simultaneously live
side-indexed rows. Exceeding it returns `WorkTableError::ColumnSlotIdExhausted(bits)` and rolls back
the insert. The implementation never widens, truncates, wraps, or evicts automatically.

## Implemented side structures

Generated tables maintain under one table-local `RwLock`:

- primary-key → `(ColumnSlotId, generation)` directory;
- reusable slot set and generation vector;
- process/table incarnation;
- chunked `Vec<Vec<Option<T>>>` for each opted-in field;
- primary-key side column for ref validation; and
- a `BTreeMap<composite key, BTreeSet<slot>>` for each `columnar_indexes` entry.

Insert/update/delete/reinsert hooks maintain these after the authoritative row mutation. A vacuum
link change does not change the slot. In-place paths without a typed delta mark the side indexes
dirty; `rebuild_columnar()` lets applications pay the whole-table rebuild cost deliberately.

Persisted tables skip these derived fields in their existing index disk format and rebuild them
from authoritative rows after load. This branch adds no on-disk format.

## Current generated operations

```rust
table.columnar_select_host_time(host_id, captured_at_ns)?;
table.columnar_scan_host_time()?;
table.columnar_scan_status()?;
table.columnar_project_temperature(&row_refs)?;
table.columnar_is_dirty();
table.rebuild_columnar()?;
table.columnar_slots_in_use();
table.columnar_slots_high_water();
```

They return owned `Vec` collections. Full-key equality is the only clustered predicate in phase
one.

## Phase-one correctness gates

- Same-primary-key delete/reinsert into the same slot must invalidate the old ref.
- A ref from another table incarnation must fail validation.
- Slot exhaustion must roll back the authoritative mutation.
- All four slot widths must enforce their numeric range without wrapping.
- Mutation paths must update or dirty side indexes before returning.
- A dirty rebuild must preserve live slot/generation mappings.
- Macro validation must reject primary-key `columnar`, unknown/non-columnar cluster keys, duplicate
  keys/names/config, inert compression, non-nesting chunks, `columns:`, and reserved `include:`.
- Persisted load must reconstruct side indexes without changing the current disk format.

## Performance gates

Before an HFT-facing claim or default:

- compare tabular baseline against fields-only and fields-plus-clustered side indexes;
- measure row select, insert, update, delete, churn, exact lookup, scan, and gather;
- report p50/p95/p99, allocations, memory, and code size;
- run 1→core-count concurrency with correctness counters;
- measure first-reader and explicit dirty rebuild costs; and
- repeat across supported WorkTablesIndex, congee-wt, and arctic-wt `Using` configurations.

## Follow-up within the side-index flavor

1. Namespaced builders and prefix/range predicates.
2. Bounded `scan_batches` and one-lock multi-field projection.
3. Per-chunk dirty tracking and typed in-place deltas.
4. Validity bitmaps, fixed-width kernels, and variable-width offset buffers.
5. Optional sealed side-index snapshots if benchmarks justify persistence.

## Separate full-columnar flavor

SAP HANA's unified-table record lifecycle is useful prior art for a future third flavor: an
uncompressed row write delta, a column delta, a compressed main, asynchronous merge, old/new
snapshot coexistence, and vector/block execution. That is a separate architecture and performance
contract. It must not arrive by quietly changing what `columnar` side indexes mean.

See the v3 guide for the detailed comparison and citation.
