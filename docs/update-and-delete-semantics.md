# What an update and a delete actually do, per table shape

Three table shapes live side by side in this crate and they do not agree about
what an update is or what a delete costs. The differences are deliberate and
none of them are visible from a declaration, so they are written down here.

Established by test and measurement on 2026-09-11, not by reading.

## The short version

| | `vec: true` | dense partition | paged `worktable!` |
|---|---|---|---|
| writers | one (`&mut self`) | many (`&self`, one lock per partition) | many (`&self`, one lock per cell) |
| update | **clobber, in place** | **clobber, in place, per column** | in place if it fits, else `reinsert` to a new link |
| delete | ghost the slot, O(1) | clear the slot | ghost the row, reclaim out of band |
| reclaim | `compact()`, explicit | none needed | `vacuum()`, paced, background |
| row address | position in a `Vec` | position, and the key *is* the position | `Link { page_id, offset, length }` |
| index on disk | not stored, rebuilt on load | none to store | sorted pages, `attach_nodes` |

## Update is a clobber on both Vec shapes, and there is no bit for it

`vec: true` writes the row where it already lives:

```rust
pub fn update(&mut self, key, edit) -> bool { edit(self.row_at_mut(at)); .. }
pub fn upsert(&mut self, row)              { self.rows[at] = Some(row); }
```

The dense partition does the same one column at a time, through
`mem::replace`. Neither appends a new version and neither ghosts the old one.

**There is no clobber bit and there should not be one.** A bit would exist to
*choose* between clobbering and retaining the previous version, and that choice
is meaningless on a single-writer table: nothing can be reading the row while it
changes. The paged table needs the choice because it has concurrent readers, and
that is what `reinsert` is.

So: **ghosts come only from `delete`.** A workload that updates and never
deletes — an order book over a fixed set of exchanges, a counter table, a
config cache — produces no ghosts at all and never needs `compact()`.

## A fixed-width row's bytes do not move

`to_pages` lays rows into 16 KiB pages, and `rows_per_page` **searches** for how
many fit rather than computing it, so page boundaries are a property of the data
rather than of the schema.

For a row whose columns are all fixed width, boundaries are stable: changing
every value in 5,000 rows leaves the page count and every page header
byte-identical (`a_fixed_width_rows_pages_are_byte_stable_under_update`). Row K
stays at byte `K`'s page forever.

Add one `String`, `Vec` or `Option` of either and it stops being true — the same
test grows the file by changing a label from one byte to sixty-four.

This is the property that decides whether a page can be written back in place,
and it is checkable from the declaration: **all-fixed-width columns means a
stable on-disk layout.**

## What a delete costs, and why it changed

`vec: true` used to close the hole a delete left: `Vec::remove` moved every row
above it, then every index entry above it was rewritten. At a million rows that
was **21 milliseconds per delete**. It now ghosts the slot and moves nothing,
which is **0.497 us** — 23,706x — and `compact()` does the expensive half once,
when asked. Charging a whole compaction to the 200 deletes that caused it still
leaves the new path 108x cheaper (`perf-benchmarks/benchmarks/vec-ghost-and-range.rs`).

The cost is a slot that stays allocated: `Option<Row>` is a word per slot for a
row whose fields all use their whole range, and free for a row carrying any
spare bit pattern — one `bool` is enough. A walk over a half-ghosted table costs
exactly 2.00x per live row, and compaction gives it back.

## Where the row lives, which is what limits everything else

The paged table holds a `Link { page_id, offset, length }`. That is why `vacuum`
can relocate a row and repair the index, and why the paged table can have a
persistence engine at all.

`vec: true` holds a position into a `Vec`. A position means nothing on disk, and
because `rows_per_page` packs variably the table **cannot know which page a row
will land on until it serialises**. So a dirty-page bitmap has nothing to set on
this shape: per-page persistence needs Links, and Links are the paged table.

What `vec: true` can address is the row, because a writer is holding the slot
when it writes. And what a *partitioned* `vec: true` can address is the
partition, because the router was in the call path.

## Consequences worth knowing before designing on top of this

- A partitioned `vec: true` table is **build-then-freeze** today. The router
  hands out `Arc<T>` and every mutation wants `&mut self`, so a partition is
  populated and then given away. There is no mutable-partition API.
- `unload()` writes the whole table, so a flush is a clobber of the file. Two
  unloads concatenated do load as one table
  (`two_unloads_concatenate_into_one_table`), so append is possible — but `load`
  keeps the **first** of a duplicate key, so a later segment cannot supersede an
  earlier row.
- Nothing here fsyncs. `unload` returns bytes; durability is entirely the
  caller's, which matters to anything that wants to record what is persisted.

## If a dirty bit is ever added, the two orderings are opposite

Recorded before anything is built, because it is the detail that decides whether
a background flush loses writes, and it is easy to write backwards.

A flush that clones the row and *then* marks it clean has a lost-update window:

```
engine  clone row        -> gets A
writer  update to B, set dirty
engine  clear dirty
```

B is in memory, A is on disk, and the bit says clean, so nothing will ever write
B again. Silent, permanent, one row.

The safe order is **clear before read on the engine, set after write on the
writer** — the two are reversed relative to each other:

```
writer:  write the value, THEN set dirty
engine:  clear dirty,     THEN read the value
```

Every interleaving of those either persists the new value or leaves the bit
dirty for the next cycle. The cost is that a row may be written twice; the
guarantee is that none is skipped. This is ordinary clear-then-read dirty
tracking, and it is written here because the obvious order is the wrong one.

**Which shape can host a background flush at all:** `DenseRows` is an
`RwLock<Vec<Option<T>>>` with `update(&self, ..)`, so an engine can hold a read
lock while writers work. Plain `vec: true` cannot — every mutation is
`&mut self`, so the borrow checker forbids a concurrent reader and there is no
window to look in. A sidecar on that shape means putting the table under a lock,
which gives up the property the shape exists for.
