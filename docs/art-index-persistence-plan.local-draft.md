# Plan: persistence for the ART index backends (Congee, Arctic)

**Status:** proposal. No implementation yet.
**Depends on:** [`index-persistence-architecture.md`](index-persistence-architecture.md) for why the
current design forces this problem, and [`index-backend-dsl-proposal.md`](index-backend-dsl-proposal.md)
for the `using` syntax that introduced the backends.
**Verified against:** `congee =0.4.1`, `arctic-map =0.1.4`, branch `feat/versioned-row-publication`.

## The finding that decides the approach

The DSL proposal assumes a persisted ART would need "WorkTable-compatible structural events that can
reconstruct the exact ART topology". I checked whether either crate can emit them.

| | Public API | Node-level introspection |
|---|---|---|
| Congee 0.4.1 | `get`, `insert`, `remove`, `range`, `keys`, `compute_or_insert`, `compute_if_present`, `compare_exchange`, `stats` | None. `stats()` returns aggregate `NodeStats`, not topology |
| Arctic 0.1.4 | `get`, `insert`, `upsert`, `update`, `remove`, `range`, `prefix`, `all`, `entries(Order)`, `*_with` variants | None |

Both are lock-free with their own reclamation (Congee epoch-based, Arctic SMR-based). Neither
exposes a mutation callback at node granularity, and neither could without breaking its concurrency
model, because a split is not a linearization point an observer can safely be handed.

**Structural CDC for ARTs would require forking both crates and maintaining those forks against a
lock-free ART's internals indefinitely.** That is not a viable commitment.

The good news is that it is also unnecessary. Per the architecture doc: the index is derived state,
so persistence is a load-time optimization, and **order fidelity is sufficient** where topology
fidelity was assumed. Both crates already expose ordered iteration (`entries(Order::Ascend)`,
`range`), so producing a sorted run is close to free. ARTs additionally have **no insertion-order
sensitivity** on bulk load: insert cost is a function of key length, not of tree shape, so replaying
a sorted run costs the same as replaying a shuffled one and needs no rebalancing. That is a better
bulk-load story than the B-tree has.

## Approach: logical checkpoint plus write-ahead log

Persist ART indexes as **a sorted checkpoint plus a logical mutation log**, as a *second* protocol
running alongside the existing structural-CDC protocol. The existing B-tree path is not touched.

```
                    ┌─ StructuralCdc  ──> IndexPage + TOC        (WorkTablesIndex, IndexSet)
generated index  ───┤
                    └─ Snapshot+Log   ──> sorted run + WAL       (Congee, Arctic)
```

Protocol selection is static, from `IndexBackend` in codegen, consistent with the PR's no-runtime-
dispatch goal.

## Phases

### Phase 0: prerequisites (valuable independent of ARTs)

| Item | Why |
|---|---|
| **Rebuild index from data pages** as a recovery path | This is the foundation. It makes the index file an optimization rather than a source of truth, which is what licenses a simpler format. It is also the only honest answer to "the log tail was torn". Rows already contain the PK and every indexed column, so this is a data-page scan plus inserts |
| **Index-format version stamp** in the file header | Needed for the B-tree path too (its format is currently hostage to upstream split policy). Needed here to distinguish protocol 1 from protocol 2 |
| **Native `range` in the ART adapters** | `CongeeIndex::range_values` and `ArcticIndex::range_values` currently full-scan and materialize (`src/index/congee.rs:210`, `src/index/arctic.rs:147`). Both crates have native ordered `range`. Fixing this is a prerequisite for cheap checkpointing and independently fixes a serious query-path regression |

Phase 0 is the bulk of the risk and most of the value. Do not skip it to get to Phase 1 faster.

### Phase 1: capability split in the persistence layer

Replace the implicit "every persisted index emits `ChangeEvent`" assumption with two explicit
capabilities:

```rust
trait StructuralCdcIndex { /* existing: Vec<ChangeEvent<Pair<T, Link>>> */ }

trait SnapshotIndex {
    /// Ordered scan for checkpointing. Streaming, not materializing.
    fn ordered_entries(&self) -> impl Iterator<Item = (K, Link)> + '_;
    /// Bulk load from a sorted run. ARTs are order-insensitive, so this is a plain loop.
    fn bulk_load(sorted: impl Iterator<Item = (K, Link)>) -> Self;
}
```

Today `impl_memory_only_cdc!` (`src/index/table_index/cdc.rs:96`) makes Congee and Arctic return
`Vec::new()` for every event, which is what silently makes them unpersistable. Replace that with a
real `SnapshotIndex` implementation and have codegen refuse to pair a `SnapshotIndex` backend with
the structural-CDC engine path.

Codegen change is small: `validate_index_backends` (`codegen/src/worktable/mod.rs:57`) currently
rejects `is_memory_only()` backends under `persist: true`. That check becomes "reject only if the
backend implements neither protocol".

### Phase 2: the on-disk format

**Checkpoint.** A sorted run of `(key, Link)` pairs, page-framed for I/O but with no node semantics.
Optionally a sparse directory (every Nth key to page id) if partial loads are ever wanted; not
required for the load-everything case. Critically, this format is **structure-independent**: the same
checkpoint loads into Congee, Arctic, IndexSet, or WorkTablesIndex.

**Log.** Append-only logical records:

```
{ seq: u64, op: Insert { key, link } | Remove { key } | Relocate { key, from, to }, crc32 }
```

`Relocate` matters: vacuum moves rows between pages and swings index entries
(`update_index_after_move` in `src/table/vacuum/vacuum.rs`). A logical log must record that, or
recovery reconstructs links to vacated slots.

`seq` must come from a **monotonic counter, not a UUID**. The existing batch path orders by
`OperationId`, which is a UUIDv7 and therefore orders randomly within a millisecond (see review
finding C6). Do not inherit that here.

**Recovery.** Load checkpoint (linear bulk-load), replay log tail from `checkpoint_seq + 1`, stop at
the first record that fails its CRC and discard the rest. If the checkpoint itself fails validation,
fall through to Phase 0's rebuild-from-data-pages.

**Compaction.** When the log exceeds a threshold, write a new checkpoint from an ordered scan to a
temp file, fsync, rename over, then truncate the log. Standard, and the rename gives atomicity
without a journal.

### Phase 3: engine wiring

`SpaceIndex::process_change_event_batch` (`src/persistence/space/index/mod.rs:396`) gets a sibling
that consumes logical records and appends them. The generated persistence type selects which one per
index, so a table can mix: a WorkTablesIndex primary key on the structural path and an Arctic
secondary on the log path, in the same file.

Note this is strictly simpler than the structural path. There is no table of contents to re-key, no
page splitting, no `panic!("page should be available in table of contents")` failure mode.

### Phase 4: crash consistency and testing

- Ordering: data pages durable before the index log record that references them, so a torn tail
  never produces an index entry pointing at bytes that were never written. The reverse (data written,
  index record lost) is safe because it degrades to a missing index entry, which the Phase 0 rebuild
  repairs.
- Test matrix: kill at every phase boundary (mid-checkpoint, mid-rename, mid-log-append,
  mid-compaction) and assert the index either matches the data pages exactly or fails validation
  cleanly into rebuild. The existing suite has no crash-injection harness, so this is new
  infrastructure.

## Effort and risk

| Phase | Size | Risk | Independent value |
|---|---|---|---|
| 0 | Large | Medium | High. Needed for the B-tree path too |
| 1 | Small | Low | Clarifies a currently implicit contract |
| 2 | Medium | Medium | The format is the durable commitment; get review before coding |
| 3 | Small | Low | None on its own |
| 4 | Medium | High | Crash-injection harness benefits everything |

## The cheap fallback, if this is too much for the beta

The "accelerator" shape already sketched in the DSL proposal's persistence follow-up section:
**WorkTablesIndex remains authoritative and persisted; the ART is an in-memory accelerator rebuilt on
load.** Zero new format, zero new recovery semantics, ships in a fraction of the time.

Costs, stated honestly: roughly double the memory for that access path, a write to both structures on
every mutation, and no write-path benefit from the ART at all. It only pays off if the ART's read
advantage exceeds the doubled write cost for the workload in question, which is plausible for
read-heavy point lookups (the DSL proposal's own measurements say Arctic is strongest exactly there)
and implausible otherwise.

This is a reasonable answer for `1.0.0-beta` if the goal is to unblock `using arctic` with
`persist: true` quickly. It is not a reasonable permanent answer, because it means the ART never
participates in durability and the memory cost is permanent.

## Open questions

1. Is the checkpoint format worth sharing with the B-tree backends as a migration target, or should
   protocol 2 stay ART-only? Sharing it is the path toward retiring the mirroring design entirely.
2. What is the acceptable recovery-time budget? It determines checkpoint frequency and therefore log
   size, and it is the number that decides whether Phase 0's rebuild is an acceptable primary path
   rather than a backstop.
3. Do secondary ART indexes need the same protocol as an ART primary key, or can secondaries be
   rebuilt from data pages on every load and skip persistence entirely? Rebuilding only secondaries
   may be cheap enough to halve the scope.
4. Does the `versioned-row-publication` retirement scheme interact with `Relocate` records? Vacuum
   already retires publications; the log must not replay a relocation whose source slot has been
   reused.
