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

Measured properties, from reading the code rather than from a benchmark,
because **there is no vacuum benchmark**. That is the first gap.

- `defragment` holds `lock_vacuum()`'s write side for the **whole pass**.
  `pop_max` takes the read side, so for the duration no insert can reuse a
  freed link; inserts append instead. Not a stall, but vacuum and space reuse
  are mutually exclusive.
- Each row move takes that row's full lock and awaits every predecessor
  operation on the key. O(rows moved) lock acquisitions and awaits.
- Each row move swings every secondary index individually.
- Planning re-reads `get_per_page_info()` from scratch each pass and sorts it.
- A pass is not resumable and carries no work budget.

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

## What I would do, in order

1. **Benchmark it.** There is no vacuum benchmark, so every claim above about
   cost is inference. Foreground insert and select p99.9 *during* a pass, over
   fragmentation levels, is the measurement that decides everything else. This
   is also what the review asked for and what `wt-benchmarks` does not cover.
2. **Add the page-level skip bit.** Cheapest real win, no addressing change.
3. **Bound the pass.** A work budget and a resumable cursor, so vacuum can
   never hold the registry lock for an unbounded interval. This is also review
   finding WT-6 for bulk delete, and the same fix shape.
4. **Then decide on indirection**, with the numbers from step 1 in hand. It is
   the only option that removes the bug class rather than narrowing it, and it
   is the only one that costs something on every read. That tradeoff should be
   made against a measurement, not against an intuition about which is faster.

## What not to conclude from this

The four bugs are fixed. The design is not unsound; it is exacting, and it has
been getting exacted correctly at some cost in vigilance. The argument for
changing it is that the same class keeps recurring, which is evidence about
where the next one will come from, not evidence that the current one is
broken.
