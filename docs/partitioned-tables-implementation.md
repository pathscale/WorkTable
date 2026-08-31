# `partition_by`: implementation plan

**Status:** 31 August 2026. Written after two consumers answered the one-pager.
Companions: [`partition-by-one-pager.md`](partition-by-one-pager.md),
[`partitioned-tables-proposal.md`](partitioned-tables-proposal.md),
[`partitioned-tables-worked-example.md`](partitioned-tables-worked-example.md).

---

## 1. The two consumers want different objects

| | trading backend | AgentCode |
| --- | --- | --- |
| key | symbol | file revision (BLAKE3) |
| partitions | ~500 | 10^3 to 10^5, times 8 tables |
| mode | A, dense assigned | B, sparse derived |
| routing latency | matters, per tick | irrelevant, once per query |
| all resident | yes | **impossible** |
| eviction | not needed | **mandatory** |
| per-partition persistence | not needed today | mandatory |
| partition group | no | yes, 8 tables as a unit |

The overlap is one feature: a generated router. Everything else diverges, and
one of the divergences is load bearing rather than cosmetic. Building the union
in one step would produce something neither consumer wants first.

## 2. The measurement that gates AgentCode, answered

Section 10.6 item 1 asked for fixed overhead per partition. Measured on
WorkTable 1.0.0-beta.11, `SymbolPosting` shape, one unique index, counting
allocator:

| | bytes per instance |
| --- | --- |
| empty, default page size | 32,696 |
| empty, `page_size: 1024` | 17,336 |
| holding 20 rows | 101,591 |

Two things follow immediately.

**Page size is not the knob.** Halving it removes 15 KB, not 31. Roughly half
the empty cost is index structures, free list and lock map, and those do not
shrink with the page.

**Twenty rows cost 101 KB**, about 5 KB of overhead per row for rows holding a
`u64`, a `u128` and two short strings. That amplification, not the empty cost,
is what drives the totals.

All-resident, at eight tables per key:

| file revisions | instances | resident |
| --- | --- | --- |
| 1,000 | 8,000 | 0.8 GB |
| 20,000 | 160,000 | 16.3 GB |
| 100,000 | 800,000 | 81.3 GB |

**So the file axis is not viable if every partition is resident.** But that was
never the proposal. Under AgentCode's own 16,000 fact-weight budget at roughly
20 facts per partition, about 800 partitions are live at once:

| configuration | resident |
| --- | --- |
| ~800 live partitions, 8 tables | **650 MB** |
| repository axis fallback, 64 keys, 8 tables | 52 MB |

**The conclusion is not "the file axis is dead". It is "eviction is load
bearing".** At 650 MB the file axis works; at 81 GB it does not; the only
difference is the residency policy. That promotes requirements 10.3.1 and
10.3.2 from features to preconditions: without them the axis cannot ship, and
with them it fits comfortably.

The trading case needs none of this. 500 partitions of a 23-row order book
measured 68,258 bytes each, 34 MB total, all resident, forever.

## 3. The second gating measurement is worse than the first

Section 10.6 item 2 asked about read fan-out. Construction alone, before any
disk I/O, measured 25.7 us for a 20-row partition and 1.3 to 6.2 us for an
empty one:

| cold fan-out | construct only |
| --- | --- |
| 10 partitions, 8 tables | 2.1 ms |
| 100 partitions, 8 tables | 20.6 ms |
| 1,000 partitions, 8 tables | 205.8 ms |

A term present in 100 files, none resident, costs 20 ms of pure construction
before a single byte is read from disk or a single index probed. Against a
current 161 ms publish path that may be acceptable; against an interactive
query it is not.

This says something specific about the design: **cheap partition construction
matters more than fast routing.** Requirement 10.3.5 is the one to optimise,
and 25.7 us is the number to beat. It also argues for materialising a partition
group lazily per table rather than eagerly for all eight, since a symbol query
needs three of the eight.

## 4. Staged plan

Each stage is independently useful and independently shippable. The ordering is
by risk retired per unit of work, not by consumer.

### Stage 0: measure before building. Partly done, and it moved the plan.

Run with `cargo run --release --example partition_overhead_probe`. Bytes are
live heap from a counting global allocator, 1,000 instances per variant.

**Where the 30,920 byte floor goes.**

| variant | bytes | vs floor |
| --- | --- | --- |
| minimal, no secondary index | 30,920 | |
| `page_size: 8192` | 22,728 | -8,192 |
| `page_size: 1024` | 15,560 | -15,360 |
| `page_size: 256` | 14,792 | -16,128 |
| twelve columns instead of two | 30,920 | +0 |
| one secondary index | 32,696 | +1,776 |
| two secondary indexes | 34,360 | +3,440 |
| String column and String index | 32,696 | +1,776 |

Three results, none of them what was assumed.

1. **The data page is 16,384 bytes of it and shrinks linearly.** `page_size` is
   a real knob after all, worth roughly half the floor.
2. **Columns are free.** Twelve columns cost exactly what two do.
3. **An irreducible 14,536 bytes remains** after the page is minimised. That is
   the primary index, free list, lock map and CDC state, and no configuration
   touches it. It is the true floor per instance.

**Index backend is the largest single variable, by a wide margin.**

| backend | cost over no index |
| --- | --- |
| congee | +112 |
| worktables_index | +1,776 |
| arctic | +17,264 |

Arctic costs **154 times** what congee does per instance, consistently at any
page size. That matters directly: congee accepts `u8`, `u16`, `u32`, `u64` and
`usize`; arctic accepts `u16`, `u32`, `u64` and `u128`. A `u128` key forces
arctic. AgentCode's posting hash is `u128`, so narrowing it to `u64` would move
those indexes to congee and save 17 KB per instance, 138 KB per partition
across their eight tables.

Best in-memory configuration measured: congee plus `page_size: 1024`, at
**15,672 bytes**, against 48,296 for arctic at the default page. **3.1x, from
configuration alone, with no change to WorkTable.**

**Persistence is where the plan breaks.** Measured separately, 200 instances,
each with its own directory, arctic index, `page_size: 1024`, real I/O:

| | value |
| --- | --- |
| bytes per persisted instance | **110,912** |
| construction time per instance | **6.1 ms** |

The memory number is bad and the time number is disqualifying. At eight tables
per partition:

| resident partitions | memory | time to materialise one partition |
| --- | --- | --- |
| 800 | 710 MB | 49 ms |
| 10,000 | 8.9 GB | 49 ms |
| 100,000 | 88.7 GB | 49 ms |

**Materialising one partition group costs 49 ms.** A read fanning out over 100
cold file revisions costs 4.9 seconds. That is not a slow path to optimise,
it is a different design.

So requirement 10.3.2, lazy load per partition, collides with requirement
10.3.1, bounded residency: eviction is what makes the memory fit, and lazy
reload is what makes eviction affordable, but reload costs 49 ms per partition
group. The two cannot both hold at the file-revision axis.

**What this implies, before any code is written.**

- Fine-grained partitions cannot each own a persistence space. Either
  partitions are coarse enough that 49 ms of cold start is paid rarely, or
  persistence is shared across partitions and only the in-memory routing is
  partitioned.
- The repository axis, 64 keys, costs 64 x 49 ms = 3.1 s of cold start once at
  startup and 52 MB resident. That is comfortable, and it is AgentCode's own
  stated fallback.
- The trading case is unaffected: 500 in-memory partitions of a 23-row table,
  34 MB, no persistence, all resident forever.

**The 6.1 ms, decomposed.** `cargo run --release --example
partition_persist_breakdown_probe`, 200 instances, one directory each:

| phase | ms each | share |
| --- | --- | --- |
| `create_dir_all` | 0.146 | 2.5% |
| `DiskConfig::new_with_table_name` | 0.001 | 0.0% |
| **`PersistenceEngine::new`** | **5.666** | **95.3%** |
| `PersistedWorkTable::new` | 0.133 | 2.2% |
| total | 5.948 | |

Only 3 files and 279 bytes per partition reach disk, so this is not I/O
volume. Cost drifts mildly as the parent directory fills, 5.798 ms at 200
partitions to 6.332 ms at 300, but that is a rounding error next to the engine
term.

**This closes the question and the answer is negative.** Restructuring the
directory layout, a shared parent with per-partition files inside, would remove
2.5 percent. The cost is inside engine construction, so a fine-grained
persisted partition axis stays unaffordable until that path itself changes.
The remaining options are unchanged: coarsen the axis so cold start is rare, or
share one persistence space across partitions and partition only in memory.

**Still to measure:**

1. **The K sweep to 10^5,** for construction time under allocator and
   filesystem pressure rather than in isolation.
2. **Read fan-out**, AgentCode's second gating question, which now has a
   pessimistic prior from the 49 ms figure.
3. **What inside `PersistenceEngine::new` costs 5.7 ms**, if anyone wants the
   file axis badly enough to attack it.

### Stage 1: the resident router. Implemented.

```rust
worktable!(
    name: Price,
    partition_by: symbol_id: u16,
    columns: { exchange_id: u8 primary_key, bid: f64, ask: f64 }
);
```

generates `PricePartitions` with `partition`, `partition_or_create`,
`partition_or_insert_with`, `contains`, `remove`, `collect`, `gc`,
`retired_len`, `keys`,
`iter`, `len`, `is_empty`,
`memory_by_key`, `memory_total` and `rows_by_key`, all typed on `u16` rather
than a raw `u64`.

**Where the code lives.** Storage is `worktable::partition::PartitionSet<T>` in
the library; codegen emits only a typed facade. That was deliberate: one
`worktable!` already expands to roughly 1,940 lines and 84 KB, and a router
generated inline would be paid for by every partitioned table. A table without
`partition_by` generates nothing extra at all.

**Measured routing cost, from `benches/cases/partition_routing.rs`.** The
0.73 ns figure quoted below is a bare `Vec` index, not the public API, and
review was right to reject it as a justification. What callers actually execute,
M4 Max, release, criterion `--quick`:

| call | 1 thread | 8 threads, same key |
| --- | --- | --- |
| cached handle, no routing | 0.64 ns | n/a |
| `contains` | 0.77 ns | n/a |
| `partition_ref` | 0.71 ns | **0.79 ns** |
| `partition` plus handle drop | 3.77 ns | **488.9 ns** |

`partition` revives an `Arc`, so it pays an atomic increment and, on drop, an
atomic decrement. Both hit the same strong count, so readers routing to one hot
symbol serialise on that cache line: 3.65 ns at one thread, 26.8 at two, 125.4
at four, 488.9 at eight. That is a 134x degradation, and it is the traffic
partitioning exists to remove.

`partition_ref` returns a borrow, touches no refcount, and is flat across the
same sweep. **Use it on the tick path.** `partition` is for handles that must
outlive the borrow or move to another thread.

Numbers are `--quick`, not core-pinned, and averages rather than percentiles, so
they are directional. The shape is not in doubt: one call scales, the other
inverts.

**Storage.** A segmented vector: a fixed spine of 64 chunk pointers, each chunk
1,024 slots, chunks allocated on demand and never moved. Readers index straight
in with no lock and no reference counting on the spine; creation takes a mutex,
which is the right trade because routing is hot and creation is rare.
Segmented lookup measured within 0.2 ns of a flat `Vec`, so no bound is
declared and `partitions: N` was dropped entirely rather than kept as a limit.
Ceiling is 65,536 partitions, and an out-of-range key is refused rather than
wrapped.

**A slot is an `AtomicPtr`, and removal retires rather than frees.** The first
cut stored `Option<Arc<T>>` in a slot and relied on the creation mutex. That is
wrong twice over. Readers never take that mutex, so a slot written by one
thread while another reads it is a data race by the memory model, benign on
aarch64 only by luck. Worse, `remove` dropped the reference the slot held, so a
reader that had read the pointer but not yet incremented the strong count could
have the allocation freed under it: a genuine use-after-free, not a formality.

The fix keeps the read path lock-free. A slot holds one owned strong reference
as a raw pointer, and `remove` moves that reference to a retire queue instead
of dropping it, deferring an epoch marker in the set's own domain. Every read
of a slot pointer happens under an epoch pin (thread-local, no shared
read-modify-write), so the marker executes only after every reader that could
have loaded the pointer has finished. `collect` then frees the expired prefix
through `&self`: reclamation works through the production `Arc`-shared router,
and `remove`/`get_or_create` collect opportunistically, so a delist-and-relist
loop no longer accumulates removed tables. `gc(&mut self)` remains as the
exhaustive variant for callers that do hold exclusive access. `retired_len`
and `retired_bytes` now report partitions still inside their grace period
rather than a list that only exclusive access could drain.

**The key is restricted to `u8`, `u16`, `u32`, `u64`, `usize`.** A `String` key
is rejected at macro expansion with a message pointing at a registry table,
because hashing a string costs more than every other part of the lookup
combined.

**How the storage is verified.** Native tests alone cannot establish this code.
A racy pointer-sized store is atomic in practice on aarch64, so the
use-after-free above passed all 725 tests. Three tools, each answering a
different question:

```bash
cargo test                                                          # behaviour
cargo +nightly miri test --lib partition                            # UB
RUSTFLAGS="--cfg wt_loom" cargo test --release --lib partition::loom_tests
cargo mutants --file src/partition/mod.rs                           # test quality
```

Miri reports undefined behaviour on schedules it happens to run, and caught the
original race at once. Loom explores *every* interleaving of the atomics and the
mutex, which is what makes the `Ordering` choices a checked claim rather than an
assertion; the models fail if either the release store or the acquire load on a
slot is weakened. The cfg is `wt_loom` rather than the conventional `loom`
because `RUSTFLAGS` reaches every crate in the graph and tokio gates
`tokio::fs` on `cfg(not(loom))` for its own loom builds.

Two traps worth writing down. Loom only tracks accesses it owns, so a model
whose payload is an ordinary `Arc<u64>` proves nothing about publication and
stays green when the store is weakened to `Relaxed`; the payload has to sit
behind `loom::cell::UnsafeCell`. And Miri interprets rather than executes, so
the concurrency tests carry `cfg(miri)` shapes: what matters there is the
interleavings explored, not the iteration count.

Mutation testing found nine surviving mutants on the first run, all real gaps:
nothing asserted `is_empty` was ever false, the `MemStat` reporting was not
covered at all, and `Display for PartitionError` was never read. `mem_stat`
charges `size_of::<T>()` per partition on top of the payload, because
`MemStat for Arc<T>` counts what the allocation actually holds. All 59 viable
mutants are now caught.

**`memory_by_key` reports used bytes, not resident bytes.** Row bytes plus
secondary index bytes. It excludes the table's fixed floor (14,536 B
irreducible, 30,920 B with the default page), reserved-but-unused page
capacity, the router spine, and `Arc` overhead. Do not size a process from it.

It also counts only live partitions, so a total *falls* after a `remove`
whose grace period has not expired yet. `retired_bytes` reports what removal
has not freed yet, and should be read next to it.

**Creation takes one global lock, and `make` runs under it.** So constructing a
partition blocks every other creation and removal for its duration: 25.7 µs for
an in-memory instance, 6.1 ms for a persisted one. A burst of first-touch
routing serialises behind it.

Not fixed, and sharding the lock per chunk would not help the case that
matters: a chunk holds 1,024 keys, so ~500 symbols all land in chunk 0 and
contend on the same shard regardless. A real fix is per-slot construction
state. Until then, create every routable partition before serving
latency-sensitive traffic and keep `partition_or_create` off the tick path.

**Memory reporting uses `system_info()`, not `MemStat`.** The generated table
does not implement `MemStat`; `system_info()` carries `memory_usage_bytes`,
`idx_size` and `row_count`, which is what a residency budget would weigh on
anyway.

**Semantics, now covered by tests rather than asserted in prose:** the primary
key is unique per partition, `autoincrement` counts per partition, and a
`unique` secondary index is unique per partition. The integration test inserts
the same key and the same unique-index value into two partitions and checks
both survive independently.

Sixteen tests: eight on `PartitionSet` including two concurrency tests, eight
on the generated facade including one that races eight threads over 128 keys.

Ships the trading case completely. Ships nothing for AgentCode, who need
stages 2 and 3.

### Stage 2: per-partition persistence

`persist: true` with `partition_by` gives one space per key, plus `load`,
`flush` and `wait_for_ops` across the set. Directory naming is `part-<id>` and
needs no escaping because the key is an integer.

Required by AgentCode, unused by trading. Also a precondition for stage 3: you
cannot evict what you cannot reload.

### Stage 3: residency policy

`with_loader`, `with_weigher`, `with_budget`, `evict_to_budget(Protected(key))`,
`resident_keys`. Lazy materialisation on first touch, drop to durable state on
eviction.

This is the stage that decides whether AgentCode can adopt at all, and section
2 shows why: it is the difference between 650 MB and 81 GB. It is also the
stage with the most design surface, since a loader is fallible and asynchronous
and an eviction can race a reader.

### Stage 4: partition groups

One key routing N tables, admitted, verified, evicted and persisted as a unit.
AgentCode calls this "the half that is actually hard" and they are right. It is
last because it is the least understood and because stages 1 to 3 make it
merely tedious rather than novel.

## 5. Two things that are not this feature and may outrank it

AgentCode named both, and by their own account both matter more to them than
the router does.

1. **A non-unique fixed-width index**, which would move their remaining string
   indexes to Arctic. Independent of partitioning.
2. **A batch insert path.** Persisted inserts measure 10.24 us marginal per row
   against 0.46 us for a non-persisted Arctic insert. That factor of 22 is
   their dominant cost and no amount of partitioning touches it.

If capacity is limited, these should be weighed against stage 1 rather than
queued behind it. Partitioning improves locality; a 22x insert path improves
everything.

## 6. Open questions carried forward

1. **The routing table becomes the new contention point.** AgentCode's read
   path consults a global `name_routes` table on every query to find candidate
   revisions. Partitioning removes contention from the fact tables and
   concentrates it there. Does that table need partitioning too, and by what?
2. **Eviction racing a reader.** A partition handed out and then evicted while
   a query holds it. Reference counting solves it; the interaction with the
   fact-weight budget needs stating.
3. **Does per-partition uniqueness match the scope the uniqueness needs?**
   AgentCode gains 32 bits by assuming the partition boundary is exactly the
   scope. Worth confirming against their duplicate-detection requirements
   rather than inferring it.
4. **Is 25.7 us of construction reducible?** If not, cold fan-out sets a floor
   on interactive query latency that no other optimisation can lift.

---

## 7. AgentCode reply to sections 2, 3 and 6

Thank you for measuring both gates. Two corrections to the model, then the four
open questions.

### 7.1 Correction: five tables partition by file, not eight

Three of the eight are keyed by generation, not by file revision:
`SymbolIndexState`, `TextIndexState` and `DependencyIndexState` each hold one
count row per generation, indexed by `snapshot_hash unique`. They are markers
saying "generation G has N names indexed", so a file revision is meaningless for
them and they must stay global.

That is a 37.5% reduction in every instance count in section 2. The all-resident
column becomes 0.5 GB, 10.2 GB and 50.8 GB, and the live estimate becomes about
**406 MB rather than 650 MB**. It does not change the conclusion, which is that
eviction is load bearing, but it moves the numbers.

It also answers open question 3 directly. See 7.4.

### 7.2 The correction that does change the conclusion: de-aggregation

Section 2 assumes the row count is conserved when the axis changes. It is not.

Today a posting is the aggregation: one row per term for the whole repository,
whose blob holds every match. Partitioning by file revision splits that into one
row per term **per file**. A term appearing in 300 files becomes 300 rows where
it was one. Measured over real Rust repositories, counting distinct lowercase
identifier tokens:

| repository | files | rows today | rows per file, summed | factor |
| --- | ---: | ---: | ---: | ---: |
| agentcode | 18 | 2,928 | 6,806 | 2.32x |
| WorkTable | 272 | 6,798 | 36,701 | 5.39x |
| ps-blitz | 350 | 15,936 | 96,943 | 6.08x |

The factor grows with file count, because common tokens appear in a growing
fraction of files. At monorepo scale it is plausibly 15x to 30x, and that is
unmeasured.

This does not blow the memory budget, because a budget is a budget. It does
something worse: **it shrinks the fraction of the repository the budget covers.**
At 800 files with a 6x factor, the text tables hold roughly 115,000 rows rather
than 19,000, so a 16,000 fact budget covers about 14% of them. Most queries then
land on cold partitions, which is exactly the cost section 3 measured at 25.7 us
each.

So the two gates are not independent. De-aggregation makes gate 2 worse in
proportion to how well gate 1 is solved, and gate 2 was already the worse of the
two.

### 7.3 What that means for the axis

Unchanged: the write side is still the reason to do this, and it is still
O(change) versus O(repository).

Changed: we would not put the **text** tables on the file axis. They are the ones
with high rows per file and high de-aggregation, and they are the ones whose
queries fan out widest, since a prefix query can legitimately match a token
present in every file. Symbol and dependency tables have roughly 18 rows per file
and are queried by a specific name, so both problems are small there.

The honest shape is therefore a mixed axis, not a uniform one, which is worth
knowing before stage 4 designs partition groups around the assumption that a
group is homogeneous.

### 7.4 The four open questions

**1. Does the routing table need partitioning, and by what?** The uncomfortable
answer is that a global `name_routes` table is our current lexeme table wearing a
different hat, so introducing it would reintroduce the structure the change set
out to remove. Its write cost stays O(terms in the changed file), which is fine;
its size is the de-aggregated row count in 7.2, which is not. If it exists, the
partition key is the **first byte of the normalised term**, roughly 256 ways: a
prefix query always arrives with a known first byte, so it routes to exactly one
partition, and ordering inside the partition is preserved so range scans still
work. The skew is real and inherent, since identifiers cluster in lowercase
ASCII, and hashing to fix it would destroy the ordering the range scan depends
on.

**2. Eviction racing a reader.** Already solved in our hand-rolled version, and
the pattern transfers: `hot_partition` hands out an `Arc`, so eviction drops the
map entry while a reader's strong reference keeps the tables alive until it
finishes. What needs stating is the budget semantics, not the safety: the budget
must count **resident** weight and tolerate temporary overshoot while readers
hold evicted partitions, rather than blocking eviction until refcounts drop.

**3. Does per-partition uniqueness match the scope the uniqueness needs?** For
the five file-axis tables, exactly. We need one posting per name per file
revision and one lexeme per term per file revision, so the partition boundary is
the uniqueness scope with nothing left over. For the three index-state tables it
does not match at all, which is 7.1: their uniqueness is per generation. So the
answer is yes for everything that should be partitioned and no for the three
tables that should not be, and the 32 bits we get back are real.

**4. Is 25.7 us reducible?** Not ours to answer, but here is what it decides. With
the de-aggregation factor in 7.2, most partitions a query touches are cold, so
cold construction is not an edge case, it is the normal path. If 25.7 us does not
come down substantially, the file axis is limited to the symbol and dependency
tables described in 7.3, and text search stays on a global index. That is a
smaller and still worthwhile feature, but it is not the one section 2 is costing.

### 7.5 On section 5

Agreed, and more strongly than written. The batch insert path is 22x on our
dominant cost and is independent of every open question above. Partitioning is
gated on two measurements that could still kill it; a batch insert path is gated
on nothing. If capacity is limited it should go first.

### 7.6 Correction to 7.3, which was wrong in a way that matters

7.3 said symbol and dependency tables suit the file axis because they are
"queried by a specific name", and that only text fans out. That is wrong, and
appended rather than edited in place because it may already have been read.

Checked against the code rather than recalled:

- **Symbol prefix search is a range scan, not a point lookup.** `search_symbols`
  with `exact: false` calls `select_by_lexeme_key_range(lower..=upper)` over
  `symbol_lexemes`. It fans out exactly like text prefix search.
- **Dependency reverse lookup is a non-unique scan.** `dependencies(incoming:
  true)` calls `select_by_target_key`. Answering "who depends on X" has to visit
  every file, because the answer is not derivable from X alone.

So the dividing line is not symbol against text. It is:

| access | partitions cleanly |
| --- | --- |
| exact lookup by a key derivable from the query | yes |
| ordered range scan (any prefix search) | no |
| reverse adjacency (`select_by_target_key`) | no |

Which reclassifies the tables as **postings partition, lexemes stay global**:

- `symbol_postings`, `text_postings`, and forward `dependency_facts` are exact
  lookups by a hash the query can compute. File axis.
- `symbol_lexemes` and `text_lexemes` are the ordered routing layer. Global, one
  row per distinct term, which is what makes a prefix range scan a single scan.
- Reverse dependency needs a global index for the same reason.

This is **better news than 7.2 suggested**, and it partly dissolves that
objection. The 2.32x to 6.08x de-aggregation lands in the lexeme layer, which
stays global and holds small rows, rather than being multiplied across hundreds
of partitioned tables. The measured factors stand; where the cost lands moves.

It also simplifies stage 4. If lexemes stay global, a partition group is three
tables of one kind rather than eight of two kinds, and the homogeneity assumption
holds after all.

### 7.7 One question, and it only affects stage 2

Per-partition persistence at 10^3 to 10^5 keys means 10^3 to 10^5 directories,
each with its own files. What is the file descriptor and inode cost per resident
partition, and does a partition hold descriptors open while resident or only
during a flush?

At roughly 800 live partitions times three tables, that is 2,400 partition spaces
at once. If each holds even a few descriptors open, this runs into per-process
limits that a single-table design never encounters. It does not change the design
but it may change the default, and it belongs in stage 2 rather than being
discovered in stage 3.

Nothing else is blocking. Stage 0, stage 1, stage 2 and the batch insert path can
proceed without further input from us.
