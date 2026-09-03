# Vacuum: what is wrong, and what the literature offers

Written 2026-09-03, after a session spent fixing four bugs in this area. It is
a design note, not a plan: nothing here is decided.

## The observation that matters

Four defects were fixed in reclamation this week. Written out, they are one
defect:

| what was reported | what it was |
| --- | --- |
| a page and its inner links handed to two allocators | an index entry resolving to reused storage |
| `value_idx[792]` returning a row holding `2703` | an index entry resolving to reused storage |
| `upsert` returning `PrimaryUpdateTry` | a primary entry resolving to reused storage |
| a page reclaimed with a live row on it | an index entry resolving to reused storage |

They are not four bugs that happened to cluster. They are one property of the
design, surfacing wherever storage moves or is reused.

## The property

**Indexes store physical addresses.** `pk_map` maps a primary key to an
`OffsetEqLink`; `reverse_pk_map` maps back; every secondary index maps a value
to a `Link`, which is `(page_id, offset, length)`.

So a row's identity *is* its location. Two consequences follow, and everything
above is one of them:

1. **Relocation is O(indexes).** Moving one row rewrites the primary entry, the
   reverse entry, and one entry per secondary index, each under that row's
   lock. `move_candidate_if_current` does exactly this, once per row.
2. **Reuse is a race with every reader.** A freed link is a name that can be
   handed to a different row. Any entry still holding that name is now wrong,
   and correctness depends on every removal path having run first.

Vacuum is where both bite at once, because it relocates *and* frees.

## What vacuum costs today

Measured 2026-09-03 with `wt-benchmarks`' `vacuum-stress-worktable`: three
index backends, two fragmentation levels, each run twice — once with vacuum
stopped and once with it running — with interleaved inserts and selects for two
seconds per arm. The delta between the two vacuum arms is the measurement; a
single arm says nothing, which is why every cell is run twice.

Re-run on an idle machine after the vacuum work landed. These are the numbers
to trust; the first run is kept below only to show what contention did to it.

| backend | fragmentation | inserts, vacuum off | vacuum on | delta | p50 off | p50 on | max off | max on |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| wti | 25% | 1,572,963 | 1,390,294 | -11.6% | 1000 ns | 1083 ns | 0.16 ms | 0.15 ms |
| arctic | 25% | 1,766,008 | 1,582,590 | -10.4% | 875 ns | 917 ns | 0.20 ms | 0.28 ms |
| congee | 25% | 1,589,229 | 1,555,932 | -2.1% | 958 ns | 958 ns | 0.17 ms | 0.27 ms |
| wti | 60% | 1,606,719 | 827,927 | **-48.5%** | 1083 ns | 2208 ns | 0.13 ms | 0.12 ms |
| arctic | 60% | 1,824,133 | 897,328 | **-50.8%** | 917 ns | 2041 ns | 0.24 ms | 6.92 ms |
| congee | 60% | 1,818,984 | 869,964 | **-52.2%** | 958 ns | 2084 ns | 0.16 ms | 0.44 ms |

The penalty at 60% is about 50% on all three backends. An earlier reading that
arctic was cheaper there (-24.7%) came from a depressed baseline: its
vacuum-off arm measured 1.23M under contention and 1.82M idle. No backend
handles vacuum better than another.

**Batching the exclusion did not reduce the penalty**, which is the other thing
the clean run settles. At 25% the sweep still costs 2 to 12%, at 60% still
about half. Releasing the lock more often does not reduce the work of moving
rows, and at high fragmentation the work is the cost. That case needs a
work-side bound -- how much one sweep moves, or how much is moved per page
reclaimed -- not more yielding.

Three things fall out of this, and they set the whole design.

**The cost is not a constant.** At 25% fragmentation a sweep is between free
and 10%. At 60% it costs a quarter to half of insert throughput and doubles
median insert latency. Vacuuming early is not merely nicer, it is *cheaper by a
factor of five*, which is an argument for reacting to fragmentation rather than
waiting out an interval that lets it accumulate.

**Retracted: "not vacuuming has a tail".** The first run showed multi-
millisecond worst-case inserts in every vacuum-off arm at 60%, 32 ms on wti and
41 ms on arctic, and this document argued from it. A clean re-run on an idle
machine does not reproduce it: every vacuum-off arm is 0.13 to 0.24 ms. Those
maxima were machine contention. The only multi-millisecond tail in the clean
run is on the vacuum-*on* side, 6.92 ms for arctic at 60%.

The cost curve below still stands and is still the argument for reacting early.
The tail benefit was not real, and nothing should be built on it.

**The mechanism is the exclusion, not the work.** `pop_max` takes the registry
read side with `try_read_owned().ok()?`, so while a sweep holds the write side
every insert wanting reclaimable space is turned away *immediately* and
allocates a fresh page instead. Inserts do not block on vacuum; they lose
free-space reuse for the sweep's entire duration. That is why the p50 doubles
and stays doubled, and it is what the batching below addresses.

The rest of this section is from reading the code.

- `defragment` holds `lock_vacuum()`'s write side for the **whole pass**.
  `pop_max` takes the read side, so for the duration no insert can reuse a
  freed link; inserts append instead. Not a stall, but vacuum and space reuse
  are mutually exclusive.
- Each row move takes that row's full lock and awaits every predecessor
  operation on the key. O(rows moved) lock acquisitions and awaits.
- Each row move swings every secondary index individually.
- Planning re-reads `get_per_page_info()` from scratch each pass and sorts it.
- A pass is not resumable and carries no work budget.

## The shape that fits: non-blocking, reactive, range-detecting

Before reaching for the literature, note that most of this design's machinery
already exists in `EmptyLinkRegistry` and planning does not use it.

### Range detection: the structure is already built and ignored

`index_ord_links` is a `BTreeSet` of free links **ordered by absolute position
and coalesced on insert**: pushing a run of adjacent freed links merges them
into one entry. `length_ord_links` orders the same runs by size.

`get_per_page_info` uses neither. It takes the `op_lock`, iterates every entry
in `page_links_map`, rebuilds a per-page `HashMap` from scratch, and produces
a fragmentation ratio per page. Planning is therefore O(all free links) per
pass and its output is page granular.

Planning from runs instead changes what vacuum does, not just how it decides:

- **Today**: to reclaim a page, move *every* live row on it. The work is
  proportional to how full the page is, and a mostly-full page is skipped as
  poor value even if one row is stranding a large gap.
- **From runs**: find the rows standing *between* two adjacent runs and move
  only those. One move merges two runs into one. The work is proportional to
  the number of stranding rows, which is usually small, and the value is the
  size of the run it creates.

That is a different cost curve, not a tuning change.

### Reactive: the signal is already maintained

The trigger today is a 60 second timer plus a fragmentation ratio. The registry
already keeps `sum_links_len` (free bytes) and `item_count` (free runs), both
updated on every registration and removal and both free to read.

After coalescing, **the ratio between them is the fragmentation measure**. The
same free bytes in one run is a healthy table; in five hundred runs it is a
fragmented one. No scan is needed to know which.

So vacuum can wake when that ratio crosses a threshold rather than on a clock,
and can decline to run at all when `length_ord_links` already holds a run big
enough for the allocations being made. Today it wakes every 60 seconds and
scans regardless.

### Non-blocking: the lock is the whole pass

`defragment` holds `lock_vacuum()`'s write side for its entire duration, and
`pop_max` takes the read side. So for the length of a pass no insert can reuse
a freed link; every insert appends. Vacuum and space reuse are mutually
exclusive, which is close to self-defeating for a pass whose purpose is to make
space reusable.

Working a bounded range at a time and taking the lock per range rather than per
pass removes that. It is the same fix shape as review finding WT-6, which is
about bulk delete holding every stripe it touches for the whole batch.

### What this does not fix

It reduces how much relocation happens and how long anything is held, which is
worth doing on its own. It does not remove the class of bug that has been
recurring, because a relocation still rewrites index entries that hold physical
addresses. Fewer relocations means fewer chances to get it wrong, not a
guarantee. That distinction is what the rest of this note is about.

## The literature, and what each idea would actually change

### Indirection: the one that removes the class

Give a row a stable identity and resolve location through one mapping, so
indexes never hold an address.

- **Bw-tree and LLAMA** (Levandoski, Lomet, Sengupta) use a mapping table from
  logical page id to physical address. Relocation updates one entry; nothing
  that points at the logical id needs touching.
- **Forwarding pointers** in copying garbage collection are the same idea at
  object granularity. **Brooks** (1984) gives every object an indirection word;
  **Shenandoah** used exactly that before moving to load reference barriers.

Applied here: relocation becomes copy bytes, update one mapping entry. Index
swings disappear, per-row locking during vacuum disappears, and the entire
"entry resolving to reused storage" class becomes unrepresentable, because no
entry holds a location to go stale.

The cost is the **RUM tradeoff** (Athanassoulis et al., EDBT 2016): one extra
dereference on every read, on a read path this project has spent real effort
making fast. That is the honest objection and it is why the design is physical
today. It is also the decision worth actually measuring rather than assuming.

### Concurrent evacuation: keep physical addresses, stop stopping

If indirection is too expensive, the collector literature still applies.

- **Baker** (1978) is incremental copying with a read barrier: evacuate a bit
  at a time, and readers that touch a not-yet-moved object do the work.
- **Shenandoah** and **ZGC** relocate concurrently with mutators and bound the
  pause to work proportional to roots rather than to the heap. ZGC's coloured
  pointers and load barriers are the mechanism.

Applied here: a tombstone at the old link pointing to the new one lets readers
that land on it follow, which means index swings no longer have to happen under
each row's lock and can be batched or done lazily. That is a smaller change
than a mapping table and removes the per-row lock acquisition, which is the
cost driver.

### Skipping work: the cheapest win available

- **PostgreSQL's visibility map** keeps a bit per page meaning "nothing here
  needs collecting", so vacuum skips those pages without reading them.
- **HOT updates** avoid touching indexes when no indexed column changed.

Applied here: a per-page "no dead rows since last pass" bit turns planning from
O(all pages) into O(pages that changed). No addressing change, no read cost,
and it composes with everything else. The HOT idea is separately interesting
because this codebase's reinsert path swings *every* secondary index even when
only one column changed.

### Scheduling: what a pass should cost

The LSM compaction literature is the closest analogue for the policy question,
since vacuum here is compaction. **Dayan and Idreos** (Monkey, Dostoevsky) frame
the tuning as an explicit tradeoff between write amplification, space
amplification and read cost rather than a fixed policy. The relevant transfer is
not a specific algorithm but the discipline: pick a target for space
amplification, derive the work rate from it, and give the pass a budget it
cannot exceed. Today the policy is a 60 second timer and a fragmentation ratio.

## Three backends, not one

Everything above says "the index" as though there were one. There are three,
they do not have the same shape, and any change here has to hold for all of
them.

| backend | keys | non-unique | value storage |
| --- | --- | --- | --- |
| `worktables_index` (WTI) | any | yes | the fork of `indexset` |
| `arctic` | u16, u32, u64, u128 | yes, via `ArcticMultiIndex` | `ConcurrentMap<K::Raw, Box<V>>`, and `Box<RwLock<LinkSlot<V>>>` for the multi variant |
| `congee` | u8, u16, u32, u64, usize | **no** | `CongeeIndex<K, V>` |

Two things follow, and one of them cuts in favour of indirection.

**Indirection is a value-type change, not a key-type change.** All three store
the `Link` as an opaque `V`. Replacing it with a stable row id touches nothing
about congee's and arctic's key constraints, which is where their restrictions
live. That is cheaper than the note above implied.

**The per-write cost differs and is not small.** Arctic boxes every value and
its multi variant puts each behind an `RwLock`, so an index write is an
allocation and a lock rather than a slot store. A scheme that reduces the
*number* of index writes per relocation therefore pays off more on arctic than
on WTI, and a scheme that adds a read dereference pays differently again.

**Measured this session, so the ranking is not a guess:**

| workload | result |
| --- | --- |
| mixed read/write, 8 threads, all ratios | arctic fastest, congee close, WTI 13 to 15% behind |
| delete grid, all APIs and distributions | congee fastest, WTI 20 to 48% behind |
| AgentCode generation write, in memory | arctic and congee about 20% ahead of WTI |

WTI is last on every axis measured, and it is the default. That is worth
knowing before optimising the layer above it.

**Congee cannot hold a non-unique index at all.** Any design that wants a
secondary mapping from row id to location has to say what happens on a table
whose only backend is congee.

## What I would do, in order

1. **Benchmark it.** There is no vacuum benchmark, so every claim above about
   cost is inference. Foreground insert and select p99.9 *during* a pass, over
   fragmentation levels, is the measurement that decides everything else. This
   is also what the review asked for and what `wt-benchmarks` does not cover.
2. **Plan from runs, not pages.** `index_ord_links` is already a coalesced,
   position-ordered set of free runs and planning ignores it. This is the
   change with the best ratio of value to risk, because the structure exists
   and the current planner is the thing being replaced rather than extended.
3. **Make the trigger reactive.** `item_count` against `sum_links_len` is a
   fragmentation measure that costs nothing to read, so vacuum can wake on
   fragmentation and decline when a large enough run already exists.
4. **Take the lock per range, not per pass**, so vacuum stops excluding the
   space reuse it exists to enable. Same fix shape as WT-6.
5. **Add the page-level skip bit**, or a generational split, so planning is
   proportional to what changed rather than to the table.
6. **Then decide on indirection**, with the numbers from step 1 in hand. It is
   the only option that removes the bug class rather than narrowing it, and it
   is the only one that costs something on every read. That tradeoff should be
   made against a measurement, not against an intuition about which is faster.

## What not to conclude from this

The four bugs are fixed. The design is not unsound; it is exacting, and it has
been getting exacted correctly at some cost in vigilance. The argument for
changing it is that the same class keeps recurring, which is evidence about
where the next one will come from, not evidence that the current one is
broken.
