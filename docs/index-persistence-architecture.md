# Index persistence architecture: why the on-disk index mirrors the in-memory index

**Status:** descriptive plus assessment. Documents an existing design and argues it is over-committed.
**Scope:** the persisted index path only. Data page persistence and the row-publication work
([`versioned-row-publication.md`](versioned-row-publication.md)) are out of scope.
**Verified against:** branch `feat/versioned-row-publication`, `data_bucket =0.4.1`,
`WorkTablesIndex (indexset) =0.0.1`. PR #187 moves these to `=0.5.1` and `=0.0.4` without changing
the mechanism described here.

## Why this document exists

The mirroring design is load-bearing for three separate decisions in the codebase, and its rationale
is written down nowhere:

- [`index-backend-dsl-proposal.md`](index-backend-dsl-proposal.md) states the *constraint*
  ("WorkTable persistence currently relies on structural CDC from WorkTablesIndex", "logical
  operation logging is not a drop-in substitute") and derives a policy from it (Congee and Arctic
  are permitted only with `persist: false`), but never says why structural CDC is the contract.
- [`reviews/2026-07-27-perf-concurrency-unsafe.md`](reviews/2026-07-27-perf-concurrency-unsafe.md)
  SEV-9 finds the full table-of-contents rewrite per event and proposes dirty-page tracking. That
  fixes a symptom without naming the cause.
- The four `wti-*-search` feature flags exist because in-node search became a measurable cost. The
  reason nodes are wide enough for that to matter is in this document.

Anyone deciding whether to invent ART structural CDC, or to change the node size, or to add a
backend, needs this written down first.

## What the design is

**One in-memory B-tree node is one on-disk page, by construction.** The macro sizes the in-memory
node to whatever fits in one page:

```rust
// codegen/src/generators/in_memory/index/mod.rs
IndexMap::with_maximum_node_size(get_index_page_size_from_data_length::<#t>(#const_name))
```

and `data_bucket`'s `get_index_page_size_from_data_length` (`src/page/index/page.rs:26`) computes
that as

```
(INNER_PAGE_SIZE - node_id - size - current_index - current_length - 2 * vec_header)
    / (slot_size + index_value_size)
```

With `PAGE_SIZE = 4096 * 4` and `GENERAL_HEADER_SIZE = 28`, `INNER_PAGE_SIZE` is 16356. For a `u64`
key, `IndexValue<u64>` is the key plus a `Link` (`LINK_LENGTH = 12`) and each slot is a `u16`, so
the denominator is on the order of 22 bytes and **a node holds roughly 700 pairs**. Check the
arithmetic against your own key type before quoting it; the shape is what matters, not the exact
figure.

**The page directory is keyed by content, not by identity.** `IndexTableOfContents` maps
`(node_max_key, node_max_link) -> PageId`. When a node's maximum changes, the directory is re-keyed
via `update_key` (`src/persistence/space/index/mod.rs:425-431`).

**CDC events map one-to-one onto page operations** in `SpaceIndex::process_change_event_batch`
(`src/persistence/space/index/mod.rs:396-505`):

| `indexset` event | On-disk effect |
|---|---|
| `InsertAt` / `RemoveAt` | Patch slots inside one `IndexPage`; re-key the TOC if the node max moved |
| `CreateNode` | Allocate a page, insert into the TOC, synthesize an `InsertAt` to seed it (`:444`) |
| `SplitNode { split_index }` | `page.split(split_index)`, allocate a page, re-key and insert into the TOC |
| `RemoveNode` | Remove the TOC entry |

`IndexPage::apply_change_event` (`data_bucket/src/page/index/page_cdc_impl.rs:35`) explicitly
refuses `SplitNode`, `CreateNode` and `RemoveNode`; those are handled a level up because they are
page-allocation events, not intra-page edits.

**Load does not rebuild.** Persisted pages are attached directly as nodes
(`codegen/src/persist_table/generator/space_file/mod.rs`), so restore is a sequential page scan
rather than re-insertion.

## Why it exists

Two genuine wins, and they are the reason to keep taking this seriously rather than dismissing it:

1. **Bounded write amplification per mutation.** An insert dirties exactly one index page. The
   in-memory structure *is* the paging plan, so there is no second on-disk B-tree with its own split
   policy to keep in sync and no class of bugs where the two structures disagree.
2. **Restart without an index rebuild.** `attach_node` is close to a memcpy. Reconstructing by
   re-insertion is `O(n log n)` with allocator churn and rebalancing; on a large index that
   difference is the entire restart budget.

Both are real. Neither is free.

## The load-bearing assumption

**The index is derived state.** Data pages hold complete rows, and rows contain the primary key and
every indexed column. Any index in this system is fully reconstructible from the data pages alone.

That reframes what persistence is for. Persisting the index is a **load-time optimization**, not a
durability requirement. The question is therefore not "how do we faithfully mirror the topology" but
"what is the cheapest artifact that lets us skip the rebuild". Topology fidelity is strictly more
than that requires. **Order fidelity is sufficient**: a sorted run of `(key, link)` bulk-loads a
B-tree in linear time, and bulk-loads an ART in linear time as well.

Nothing in the current codebase treats the index file as reconstructible. There is no code path that
rebuilds from data pages when the index file fails validation.

## What the extra fidelity costs

### 1. Node geometry is set by disk, not by cache

Node capacity is a function of `INNER_PAGE_SIZE`. A cache-friendly B-tree node for 8-byte keys is
typically 8 to 32 keys, sized to one to four cache lines. The mirroring forces roughly 700. That is
more than an order of magnitude wider than the in-memory optimum.

The four competing in-node search implementations behind `wti-predictable-search`,
`wti-std-search`, `wti-hybrid-search` and `wti-superslice-search` are the evidence. Those flags exist
because searching inside a node became a measurable cost, and it became a measurable cost because
the disk format chose the node size. For a latency-sensitive read path this is the wrong master.

### 2. Monotonic keys are the worst case for a content-keyed directory

Because the TOC key is the node's maximum value, an insert at the right edge changes it and triggers
`update_key`. With an autoincrement primary key, which is the dominant pattern for this workload,
that is **every** insert.

Compounding it, `IndexTableOfContents::persist`
(`src/persistence/space/index/table_of_contents.rs:146`) unconditionally rewrites **every** TOC page
regardless of what changed. SEV-9 in the perf review measures this as `O(P·E)` page writes and
proposes dirty-page tracking, which is the right immediate fix. The deeper point is that a directory
keyed on mutable content will always churn under append-heavy workloads; dirty tracking reduces the
constant, it does not remove the coupling.

### 3. The on-disk format is owned by a third-party crate's split policy

`SplitNode { split_index }` replays `indexset`'s internal split decision onto the page. The file
layout is therefore a function of the upstream node capacity *and* the upstream rebalancing
heuristic.

PR #187's WorkTablesIndex to vanilla-IndexSet reload switch works only because
`get_index_page_size_from_data_length` forces both to identical node capacity. Nothing stamps the
split policy into the file, and nothing detects a mismatch. A change to either crate's rebalancing
that keeps the same version number presents as data corruption, not as a version error.

If we keep this design, the file header must carry an explicit index-format version covering node
capacity and split policy, bumped independently of the crate version.

### 4. No journal, and structural invariants that a partial write can break

`process_change_event_batch` persists the TOC, then the pages, then flushes once
(`src/persistence/space/index/mod.rs:502-505`). There is no enforced data-before-metadata ordering
and no replay log, so a crash mid-flush can leave the directory referencing pages whose contents
were never written.

The asymmetry matters more than the ordering bug. A torn *logical* key-to-link run is repairable by
rescanning data pages. A torn *structural* page directory is not, and the two `panic!("page should be
available in table of contents")` sites the perf review flags (SEV, `:398` and `:465`) are how that
corruption surfaces today: a panic inside a spawned task whose `JoinHandle` was dropped, so writes
keep appearing to succeed and shutdown hangs.

## This is what blocks ART persistence

The `persist: false` requirement on Congee and Arctic is **not a property of adaptive radix trees**.
ARTs persist perfectly well. It is a property of a persistence layer that only speaks
indexset-B-tree-node.

The DSL proposal's conclusion, that a persisted ART would require "WorkTable-compatible structural
events that can reconstruct the exact ART topology", is the correct consequence of the current
design. It is also the clearest argument against it: the design demands `O(n)` durable event models
and recovery semantics for `n` backends, each pinned to a different upstream crate's internals. That
does not scale, and per-index physical selection is exactly the feature that makes it not scale.

## Alternative: sorted runs plus a write-ahead log

Checkpoint the index as concatenated sorted `(key, link)` runs, which is what a full-node dump
already is once the node framing is removed. Append mutations to a log. Compact in the background.

Gains:

- Load is a linear bulk-load into any ordered structure, so one file feeds WorkTablesIndex, vanilla
  IndexSet, or Arctic. Backend choice stops being a persistence decision.
- Node size becomes a cache decision, which is likely to retire the `wti-*-search` flag zoo.
- Writes become sequential appends rather than random in-place page patches. For write-heavy
  workloads that is better I/O, not merely different I/O.
- Recovery is log replay against a checkpoint, with full rebuild from data pages as a real backstop.
- Congee and Arctic become persistable with zero new event types.
- The format stops being hostage to upstream split policy.

Honest costs:

- `attach_node` is close to a memcpy; bulk-load still constructs nodes. On a very large index that
  restart difference is real and should be measured before committing.
- Bounded per-operation write amplification is traded for background compaction bursts, which need
  their own scheduling and back-pressure story.
- Point-in-time durability now depends on log fsync policy rather than on page writes, which is a
  different (more standard, but new) set of guarantees to specify and test.

## Recommendation

The current design optimizes for restart speed on a large index under many small random updates. It
is defensible for that workload. It is a poor fit for the direction PR #187 is pushing, which is
backend pluggability and cache-optimal in-memory reads, and it is being paid for by every user in
node width whether or not they persist anything.

Suggested sequencing:

1. **Now, independent of any redesign:** dirty-page tracking in `IndexTableOfContents::persist`
   (perf review SEV-9); an explicit index-format version in the file header covering node capacity
   and split policy; observe the spawned engine task's `JoinHandle` so TOC panics are not silent.
2. **Before 1.0:** implement rebuild-from-data-pages as a recovery fallback. It is worth having on
   its own merits, and it is the prerequisite for treating the index file as an optimization rather
   than as a source of truth.
3. **Decide explicitly:** either commit to mirroring and accept that backends must supply structural
   CDC (in which case Congee and Arctic stay memory-only permanently, and the DSL should say so
   rather than framing it as a first-version limitation), or move to sorted runs plus WAL and let
   backend selection become orthogonal to persistence. The current state, where the DSL proposal
   describes a "persistence follow-up" that would require per-backend structural CDC, is the
   expensive middle.

## Open questions

1. What is the actual measured restart time for `attach_node` versus a linear bulk-load at
   representative index sizes? This is the one number that decides item 3 above, and nobody has it.
2. Is node capacity ever intentionally decoupled from page size today, or is
   `get_index_page_size_from_data_length` the only path? If a `page_size` config override exists it
   changes the node width silently, which is worth documenting.
3. Does any current deployment depend on the on-disk index surviving a `WorkTablesIndex` upgrade? If
   not, the format-version work in item 1 is cheap insurance. If yes, it is urgent.
4. Would a sorted-run checkpoint be byte-compatible enough with the current full-node dump
   (`get_peristed_primary_key_with_toc`) to support a one-way migration, or does it need a converter?
