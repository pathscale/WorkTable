# Known issues

Open defects and accepted limitations, recorded so the next audit starts here instead of
rediscovering them. Source: the 2026-08-31 full audit (WorkTable core plus the
WorkTablesIndex, congee-wt, and arctic-wt backends) and the fix pass that followed it in
1.0.0-beta.13. The 12 September 2026 review updates resolved entries below;
older backend findings retain their stated scope and are not all newly reproduced.

Severity words: "corruption" means wrong or lost data, "outage" means a hang or abort,
"perf" means measurable cost with no wrong answers.

## Persistence engine

- **Fixed in the 2026-09-11 release review: page capacity on reopen.** The metadata
  reader and six generated metadata/index read paths passed the full stride as the
  payload capacity. DataBucket layout validation rejected them before reading any
  data. The readers now use the inner capacity, preserving the existing file format.
  The previous claim that this affected only one call site was wrong: generated
  reads must also be reviewed. `perf-benchmarks/wt-persistence` verifies every row
  and secondary key after reopening at 8, 16 and 32 KiB strides. The existing
  custom-page-size, index-reload and exact-boundary tests cover the same contracts.

- **Reclaim-barrier ordering inversion escalates to a spurious terminal failure.** While a
  `ReclaimPages` message is pending, the worker stops popping the queue, and reclaim only
  runs once the analyzer drains. An operation whose CDC event id precedes events already
  buffered in the analyzer, enqueued after the barrier (event ids are assigned at
  index-mutation time, but push order across writer threads is not globally ordered by id),
  can therefore never drain: the batch defers on the gap 8 times (500 ms each) and then the
  engine fails terminally with "persistence stalled on event gap", though nothing was
  corrupt. Any fix must keep two constraints: a pre-barrier batch must not apply across an
  event-id gap, and post-barrier data writes must not land before the durable free marker.
  Suggested directions: emit `ReclaimPages` behind a sequence fence guaranteeing no lower-id
  mutation can still be enqueued, or at minimum detect the inversion (scan queued
  post-barrier ops for the missing id without applying) and report it accurately.
- **The operation queue is unbounded with no backpressure.** A slow or stalled disk lets
  `apply_operation` accept work forever; memory and durability lag grow without bound.
  Producers also serialize on the lifecycle mutex held across the whole push (the lock is
  what makes the close protocol sound; only the state check needs it).
- **A deferred (gapped) batch sleeps 500 ms per attempt** with nothing popped or applied,
  even though the missing op typically arrives immediately; close() waits it out too.
- **`remove_info_at_pos` is O(n) per removal by contract** (positions are compacted after
  each removal and the caller's reverse-position searches depend on it), so a removal storm
  is O(n squared). An O(n) version requires restructuring `remove_operations_from_events`
  into collect-then-compact. Note for future audits: `validate_events` was reported as
  O(n squared) by the 2026-08-31 audit and is actually amortized O(n); no fix needed.
- **The worker still copies event payloads per batch** in `latest_data_writes`,
  `prepare_indexes_evs`, and `get_indexes_evs` (the `OptimizedVec` clone-on-remove was
  fixed in beta.13; these remain).

## In-memory storage

- **Fixed: table-global page mutation and reader counters.** Pages now have their
  own allocation barrier and exact-cell access guards. Reads pin ps-reclaim
  epochs rather than incrementing one table-global reader counter; reclamation
  no longer requires a simultaneous zero-reader instant. The old per-row
  publication cache was removed. These historical findings do not describe
  the current read path.
- **Fixed in the 2026-09-12 review: released collisions split cell locks.**
  An empty earlier slot could be claimed for a row still locked in a later
  slot. Registration now keeps each active key unique, with a conservative
  displaced-entry counter for the common path. See [cell-lock-registry.md](cell-lock-registry.md)
  for the failing interleaving, invariants and bounded concurrency models.
- **`unsafe impl Sync for Data` is broader than its discipline**: safe `&self` methods
  mutate the page `UnsafeCell` under external page/cell coordination; the low-level
  API does not encode all of those ownership requirements in types. Generated
  table paths and raw Data-page APIs must not be treated as identical safety surfaces.
- **A panicking closure inside `with_mut_ref` leaves the archived page image half-mutated**
  and guards do not poison. The old publication cache no longer exists, but a
  panicking callback can still leave a partial edit; a persisted call that unwinds
  before enqueueing its data operation has no durability guarantee.
- **`mark_page_full` can race a concurrent failing save's `free_offset` rollback**, leaving
  `free_offset` slightly below `DATA_LENGTH` on a non-current page. Capacity pessimism
  only; no double allocation.
- **Fixed for table reload: row count restoration.** Loaded-table hydration calls
  `set_loaded_row_count` after validating live row links. The low-level
  `DataPages::from_data` constructor alone does not infer row boundaries.

## On-disk space layer

Full mechanisms and the pinned data_bucket item list live in
[space-layer-known-issues.md](space-layer-known-issues.md); the summary:

- **There is no fsync/ordering discipline anywhere except the ART checkpoint writer.**
  Ordinary drain is not a power-loss commit or transaction boundary. On power
  loss, writes may vanish or reorder. DataBucket v3 now validates checksums and
  row directories; the old assertion that only ART has checksums is obsolete. This needs
  one durability design decision (write ordering plus sync points), not per-site patches.
- **The old DataBucket 0.5.2 pin is obsolete.** The release graph uses 0.7 with
  checked page bounds and coordinated v3 integrity validation. The WorkTable TOC
  wrapper still permits an oversized entry to occupy a segment in memory, but
  DataBucket rejects an over-budget persist instead of overwriting the next page.
  Early rejection or segment spilling remains a separate API improvement.
- **Perf:** every structural index event rewrites every TOC segment (each re-serialized
  from a cloned BTreeMap); each sized single-event insert performs an on-disk free-slot
  scan (one read syscall per cell); ART compaction runs synchronously inside
  `process_change_event` (multi-ms worker stall every 4 MiB of WAL); each single-row save
  is two seek+write round trips plus a flush, and consuming a free range rewrites the
  entire info page.

## Table core residuals (after the beta.13 reinsert fixes)

- **A reinsert whose second changed unique value collides can transiently expose the row
  via the first changed value**: with two or more unique indexes, the first new entry is
  visible between its insert and the rollback. Readers by primary key or by any
  pre-existing key value are safe. Eliminating it needs `reinsert_row` split into a
  check phase and an apply phase in codegen.
- **`IndexError::NotFound` from `reinsert_row` after a partial multi-index insert would
  dangle the already-inserted new entries** (the variant carries no `inserted_already`
  list, and unwinding by new keys would delete live entries for unchanged values).
  Unreachable today: no index implementation constructs NotFound; the fix is adding the
  list to the variant.
- **`insert_checked_cdc`'s new rollback drops the forward insert's CDC events as a
  cancelling pair**, including any node-split structural event. Only reachable on the
  already-corrupt "link owned by another key" invariant violation, where the previous
  behavior (silent reverse-map rebind) was strictly worse.
- **If vacuum's staged-page cleanup itself fails mid-error-path**, pages staged after the
  failing step still leak (logged via tracing; the original error propagates).

## Row locking

- **Fixed in the 2026-09-12 review: wrapping labels lost predecessors.** Two
  distinct live locks with the same u16 label collapsed in the dependency set.
  Equality and hashing now use the existing shared flag allocation identity.
  Labels remain diagnostic; no counter widening, allocation or API change is needed.
- **`mutation_guard` is an unbounded spin** on an async worker thread; correctness depends
  on the (documented on the guard conversion and mutation APIs) invariant that no holder awaits. 64 stripes
  also collide unrelated keys into one FIFO.
- **`Lock` waker lists grow per `wait()` call and are never pruned**; unlock wakes every
  historical waiter (thundering herd on hot rows).
- **Fixed: explicit guard unlock delegates to Drop.** Cleanup runs once.

## Generated code (accepted semantics and open items)

- **Index-before-data visibility windows are by design**: an update inserts new index keys,
  writes data, then removes old keys, so a reader can briefly find a row that does not
  match the index key it queried. Reachability is never lost, and generated selects
  re-validate predicates; callers must not assume index key equals row state mid-update.
- **`iter_with`/`iter_with_async` fail the whole iteration if a row moves mid-scan**
  (unlike `select_all`'s replacement chasing), and the async variant holds the reclamation
  read guard across user awaits.
- **The read_only generator emits the full mutation and lock machinery** for tables that
  expose no mutating method (code size and compile time only).
- **In-memory CDC generation is entirely behind `if false`** and can drift against the
  persist twin.

## Partition module

Beta.12 fixed the metrics scans and added the `partition_ref` borrow API. Still open:

- **Fixed: shared routers can reclaim retired partitions.** Epoch retirement and
  `collect(&self)` work through an Arc. The remaining inline collection cost is
  described below; the old append-only restriction is obsolete.
- **`collect` runs inline and its batch is bounded by count, not by cost: a routing
  call can pay 3.3 milliseconds.** (perf. Measured 2026-09-11,
  `perf-benchmarks/benchmarks/partition-collect-inline.rs`.) `get_or_create`
  (`src/partition/mod.rs:456`) and `remove` (`:519`) both call `collect`, which frees up to
  `COLLECT_BATCH_LIMIT = 64` retired partitions on the calling thread. The code moves that
  work off the growth *lock*, and its comment says so, but not off the *thread*.

  A quiet router never sees it: `remove` queues a clone, defers the grace marker, then
  calls `collect`, and with no reader pinned the grace has already expired, so `collect`
  drops the queue's reference while the caller still holds the one being returned. The
  teardown lands where the caller drops their own handle.

  With a reader pinned across a run of removals — which `partition_ref` and `pinned` both
  document as delaying reclamation — every marker is held back, `collect` claims nothing,
  the callers drop their handles, and the queue is left holding the last reference to all
  of them. `retired_len` then goes 256, 192, 128, 64, 0 across four consecutive routing
  calls costing 3,340 / 3,879 / 3,268 / 4,379 microseconds against a 1.8 us median.

  Not a backend problem: a congee-indexed payload is worst at 3.3 ms but the cheapest
  payload measured still reaches 2.9 ms, because sixty-four table teardowns is sixty-four
  table teardowns. Two directions, neither chosen: bound the batch by elapsed time rather
  than by count, or hand the drain to a background task and leave the routing path with
  only the queue push.
- **`make()` runs under the global growth mutex**: a slow initializer (or a stage-2
  persisted load) stalls all creations and removals. (Initializer panics no longer poison
  the set: beta.12 moved the lock to parking_lot, which unwinds cleanly.)
- **The per-key memory metrics report used row bytes plus index heap, not residency**:
  the ~14.5 KiB irreducible table floor, reserved page capacity, router chunks, and `Arc`
  overhead are excluded, so capacity planning on these numbers over-packs the process.
  Retired bytes have their own accessor since beta.12; treat the per-key numbers as
  attribution, not RSS.
- **Persisted partitions work per partition but have no set-level orchestration**
  (beta.13 fixed the compile failure): each partition handle has `load`,
  `wait_for_ops`, `close`, and a monitor, but the router has no `load`/`flush`/
  `wait_for_ops` across the set and does not own the `part-<id>` directory naming, so
  every consumer invents both. That is the stage-2 scope. Related API shape:
  `get_or_create` takes a sync `FnOnce() -> T` while constructing a persisted table is
  two awaits, so callers pre-build the table and pay construction even when the
  partition already exists (or must `contains` first); stage 2 wants an async, fallible
  initializer.
- **Measurement correction for stage-2 planning**: a release-build re-measurement from
  the AgentCode consumer (2026-08-31) put opening one persisted partition
  (`PersistenceEngine::new` plus `load`) at 818 microseconds median, versus the earlier
  debug-adjacent 6.1 ms figure whose 95%-in-`PersistenceEngine::new` breakdown drove the
  "fine-grained persisted axis is unaffordable" conclusion. Not like for like, but that
  conclusion should not be relied on without re-measuring. The dominant cost is
  unchanged either way: the like-for-like persisted-vs-memory insert gap is 7.9x
  (7.7 us vs 985 ns per row, same columns and indexes; an earlier 22x figure
  compared different index sets and is superseded), which no partitioning touches.
  A batch insert path is the consumer-ranked top ask against that gap.

## WorkTablesIndex backend

(Additional items fixed or re-documented by the beta.13-era WorkTablesIndex PR are listed
in that repo; the following remain by design or await redesign.)

- **Fixed: concurrent iterators yield owned batches.** The old guard-lifetime
  transmute was removed. Point `Ref` values still hold a node read guard and
  must be dropped before re-entering the map on the same thread.
- **`len()`/`is_empty()`/`capacity()` lock every node**: calling them while holding a live
  point `Ref` on the same thread self-deadlocks; with `remove_range` in the mix a
  three-party variant hangs writers too. Also O(nodes) cost per call.
- **`Operation::commit` is not unwind-safe**: a panic between the index entry removal and
  the reinsert of the halves silently unlinks a whole node (locks do not poison, and the
  ShardedLock poison result is ignored at every call site).
- **Emptied nodes and their skiplist entries can persist as zombies** (stale-key validation
  failures), inflating O(nodes) costs and re-enterable by inserts.
- **The blocking fallback in `lock_node_for_value` holds the structural read guard while
  waiting on a node mutex**: one slow scan consumer stalls every writer globally.
- **`RandomMultiPair`'s `Ord` remains inconsistent by design** (equal-value pairs compare
  Equal regardless of discriminator, breaking transitivity); the discriminator-preserving
  replace fixed the sort-invariant corruption, the comparator shape needs a redesign.
- **Monotonic inserts commit `UpdateMax` under the index-global write lock** (every
  autoincrement insert serializes against all point reads), point reads take a per-node
  mutex, and memory never shrinks (emptied nodes keep capacity; the tower never
  rebalances).

## congee-wt backend

Fixed in pathscale/congee-wt PR #3 (scan validation, seqlock ordering, fallible
KeyTracker, payload tag assertion, doc and refcount fixes). Still open there:

- `stats()` and `to_compact_set()` unwrap read locks and abort under a concurrent writer;
  `to_compact_set()` additionally underflows on empty nodes and truncates i16 offset
  residuals on large trees. Call both only while quiesced.
- Remove never shrinks or merges nodes (memory ratchets under churn); retry loops spin
  without parking; there is no resumable scan cursor, so consumers should key-page.
- **WorkTable follow-up once PR #3 publishes**: drop the global write mutex in
  `src/index/congee.rs` (the 0.4.1 visibility bug it worked around is fixed and the
  reader-side holes are closed) and convert the range adapter from 64-slot doubling
  rescans to key-paging. Congee ranges are end-exclusive; the adapter's arithmetic already
  matches.

## arctic-wt backend

Fixed in pathscale/arctic-wt PR #4 (prefix-match clamp, inverted-range contract,
`V: Send` bound, non-recursive remove wiring). Still open there: `todo!()` in
Node47/Node256 `min`/`max` (currently unreachable); a model-level happens-before gap when
node replacement relays indirect value edges with Relaxed copies (unobservable on
x86-64/ARMv8); pointer packing assumes 48-bit canonical addresses (LA57 and ARM TBI would
break it); scans heap-allocate per visited node and pin SMR reclamation for the whole
iteration; memory is reclaimed only at one-child collapse; the `shuttle` feature does not
compile at 0.1.5, so the concurrency harness runs on a std-thread fallback.
