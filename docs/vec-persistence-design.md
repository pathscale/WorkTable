# Persisting a Vec table: what was decided, measured, and left undone

A design conversation on 2026-09-11, recorded because most of it is conclusions
that took measurements to reach and would otherwise be re-derived wrongly.

Nothing here is built. The measurements are real and live in
`perf-benchmarks`; the design is a proposal with its open questions named.

## The problem

A checkpoint costs O(table) when the change is O(1). `unload()` writes every
row, so recording ten changed rows in a million-row table costs **110 ms**.

Every design below is an answer to one question: *what is the unit of change?*

## What is already true, and surprised us

**Dense partitions are already mutable through the router.** Every generated
method takes `&self` — `insert`, `upsert`, `update`, `delete`, the per-column
setters — because `DenseRows` is an `RwLock<Vec<Option<T>>>` inside. A
background flush can hold a read lock while writers work. This was believed to
be a blocker and is not one.

**`vec: true` partitions are build-then-freeze.** The router hands out `Arc<T>`
and every mutation wants `&mut self`, so a partition is populated and then given
away. That is the real gap, and the answer is to use a dense partition rather
than to add interior mutability: a `vec: true` table with a lock inside *is* a
dense table, keyed by a hash index instead of by position.

**Update is already a clobber on both Vec shapes**, in place, no ghost, no
appended version. Ghosts come only from `delete`. See
`docs/update-and-delete-semantics.md`. A workload that updates and never deletes
— an order book over a fixed set of exchanges — produces no ghosts at all.

**A fixed-width row's pages are byte-stable under update.** Asserted, not
assumed: changing every value in 5,000 rows leaves the page count and every page
header identical, while a `String` column growing from 1 byte to 64 grows the
file. Checkable from the declaration, and it is what decides whether a page can
be written back in place.

**`vec: true` cannot know which page a row lands on until it serialises**, because
`rows_per_page` searches rather than computes. So a dirty-*page* bitmap has
nothing to set on that shape. Per-page persistence needs `Link { page_id, offset,
length }`, and Links are the paged table.

## The measurements

`perf-benchmarks/benchmarks/persisted-bit.rs`, 1,000,000 rows:

| dirty rows | write | vs full | flip |
|---:|---:|---:|---:|
| 1,000,000 (today) | 110,359 us | 1.00x | — |
| 10 | **4 us** | **28,480x** | 0.0 us |
| 1,000 (0.1%) | 111 us | 993x | 0.4 us |
| 10,000 (1%) | 1,101 us | 100x | 4.5 us |

The flip never exceeds 0.5% of the write it accompanies. Marking the whole table
is 3 us — a memset over words, not a walk over rows. The bitset costs **0.312%**
of the table; as a `bool` per slot it would be 8x that.

Segment count is free on load: 1,000 segments restore in 131,846 us against one
blob's 131,308, because `load` walks pages and rebuilds the index either way. It
costs bytes — each segment rounds to a whole 16 KiB page, so a thousand waste 2%.

Append already works: two `unload()`s concatenated load as one table
(`two_unloads_concatenate_into_one_table`).

## The design, in order of preference

**1. The partition is the unit.** If a table is partitioned and its partitions
are small — 2,000 symbols of 23 rows is one page each — then a checkpoint
rewrites the dirty partitions and needs one dirty bit *per partition*, held by
the router, which was already in the call path. No per-row bit, no sidecar, no
segments, no last-wins, no tombstones. One file per partition, atomic rename,
recovery that cannot be subtly wrong, and nothing accumulates.

This is the recommended shape and it needs the least new machinery.

**2. The row is the unit.** For one large unpartitioned `vec: true` table, the
per-row dirty bit above. It works and it is measured, but it brings segments,
which bring last-wins, tombstones, a recovery rule, and eventually compaction.

**3. The page is the unit.** Dirty-page writeback in place, the classic answer.
Not available on `vec: true` for the addressing reason above; it is what the
paged table already does, and the paged table already has a persistence engine.

## If a dirty bit is built, two rules

**The orderings are opposite.** The writer sets its bit *after* writing the
value; the engine clears the bit *before* reading it. Clone-then-clear has a
lost-update window that silently loses one row forever. Written out in
`docs/update-and-delete-semantics.md`.

**Only a shape with interior mutability can host a background flush.**
`DenseRows` can. Plain `vec: true` cannot — `&mut self` mutations mean the borrow
checker forbids a concurrent reader, so there is no window to look in.

## What this is not

It is not a write-ahead log and carries no per-transaction guarantee; what
survives a crash is everything as of the last checkpoint. What the numbers say
is that the *window* collapses cheaply: at 4 us for ten rows, a process can
checkpoint every millisecond for under 1% of a core.

Known risks, all real:

- **Nothing fsyncs.** `unload` returns bytes; durability is the caller's. A bit
  flipped on `write()` returning rather than on fsync can lie, and a lying bit
  loses that row permanently because nothing will clear it again.
- **Segment count on object storage is a GET per segment.** Load time is flat
  locally and latency is not, so the S3 variant needs bounded segments, which
  means compaction, which means a small LSM. Choose it deliberately or not at all.
- **A torn tail must truncate at the first bad segment**, not skip it. Skipping
  applies a later update over a missing earlier one.

## Queries, since it comes up

A declared query is `Name(columns) by key` — an equality lookup. **A hash index
serves that fine; it is ranges it cannot serve.**

| query shape | `fxhash` | ordered backends |
|---|---|---|
| `by <primary key>` | yes | yes |
| `by <unique secondary>` | yes | yes |
| `by <non-unique secondary>` | yes | yes |
| range, ordered scan | **no** | yes |

The restriction today is blunter than any of that: `queries:` is refused on
`vec: true` for every backend, and the dense table accepts them only
`by <primary key>` because it has no secondary index. Enabling them on
`vec: true` is parity work rather than performance work — a generated
`update_amount_by_id` would be a named wrapper over `update(&key, |row| ..)`,
which already exists and already takes a closure.

Mixed backends already work and are tested: a `fxhash` primary key with `arctic`
secondaries gives a table with no `range` and a working `range_by_seq`
(`a_hash_primary_key_leaves_an_arctic_secondary_ordered`). Capability is per
index, not per table.
