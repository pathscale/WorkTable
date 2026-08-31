# Known issues

Open defects and accepted limitations, recorded so the next audit starts here instead of
rediscovering them. Source: the 2026-08-31 full audit (WorkTable core plus the
WorkTablesIndex, congee-wt, and arctic-wt backends) and the fix pass that followed it in
1.0.0-beta.13. Every item below was deliberately deferred, with the mechanism written down;
items fixed in beta.13 are not listed.

Severity words: "corruption" means wrong or lost data, "outage" means a hang or abort,
"perf" means measurable cost with no wrong answers.

## Persistence engine

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

- **Every mutation serializes on the table-global `page_access` write lock**, memcpy and
  page bookkeeping included. This is the write-throughput ceiling on multicore;
  per-page locking is the architectural fix.
- **Every read performs a SeqCst RMW on one shared `active_readers` line** (`read_guard`),
  and the hot counters are adjacent with no padding (false sharing). A sharded or epoch
  scheme is the fix.
- **Reclamation requires a global zero-reader instant.** Under sustained overlapping reads
  the instant may never occur: retired links, pages, and publications accumulate without
  bound and deletes stop reclaiming space. When reclamation does trip, the whole backlog
  drains inline inside one arbitrary mutating call (millisecond-class latency spike; the
  code warns at a backlog of 1024). Epoch-based reclamation is the fix for both halves.
- **The publication cache doubles the resident set**: every live row exists as archived
  page bytes and as `Arc<Row>` plus lock plus map slot, and every mutation republish pays a
  full-row deserialize. Design cost, paid per row.
- **`unsafe impl Sync for Data` is broader than its discipline**: safe `&self` methods
  mutate the page `UnsafeCell` relying on callers holding `page_access`; `Arc<Data>` is
  handed to safe code (vacuum), so the soundness boundary lives in convention, not types.
- **A panicking closure inside `with_mut_ref` leaves the archived page image half-mutated**
  while the publication keeps the old row (guards do not poison): memory and disk diverge
  silently until reload. Closures are generated code today; nothing enforces that.
- **`mark_page_full` can race a concurrent failing save's `free_offset` rollback**, leaving
  `free_offset` slightly below `DATA_LENGTH` on a non-current page. Capacity pessimism
  only; no double allocation.
- **`row_count` restarts at 0 on reload** (`DataPages::from_data`), upstream TODO.

## On-disk space layer

Full mechanisms and the pinned data_bucket item list live in
[space-layer-known-issues.md](space-layer-known-issues.md); the summary:

- **There is no fsync/ordering discipline anywhere except the ART checkpoint writer.**
  Every acknowledgement ends at `File::flush()` (tokio buffer to page cache). On power
  loss, any acknowledged write may vanish or reorder against any other; only the ART file
  has checksums, so torn pages surface as rkyv panics or silently wrong links. This needs
  one durability design decision (write ordering plus sync points), not per-site patches.
- **data_bucket 0.5.2 (pinned) carries these classes, all fixed at the source in the
  0.5.3 release PR (pathscale/DataBucket#69)**: u32 offset wraps past 4 GiB in the
  relative page seek and link bound checks, `update_key` size accounting, and unchecked
  over-budget page persists. Once the pin moves to 0.5.3, WorkTable's TableOfContents
  wrapper (src/persistence/space/index/table_of_contents.rs) should adopt the new
  capacity-checked `try_insert`/`try_update_key` and typed overflow errors: its
  size-change re-key workaround can then delegate, and the inherited oversized-entry
  own-page fallback (which can still persist an over-budget segment; the last open item
  in space-layer-known-issues.md) is closed by the checked insert. The small-DATA_LENGTH
  test fixtures that rely on that fallback need regenerating at the same time.
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

- **Lock identity is a wrapping u16 id.** Two distinct in-flight locks 65,536 ids apart
  dedup in a predecessor `HashSet`, silently dropping a real predecessor. Astronomically
  unlikely per row; structurally wrong.
- **`mutation_guard` is an unbounded spin** on an async worker thread; correctness depends
  on the (honored, but unstated at call sites) invariant that no holder awaits. 64 stripes
  also collide unrelated keys into one FIFO.
- **`Lock` waker lists grow per `wait()` call and are never pruned**; unlock wakes every
  historical waiter (thundering herd on hot rows).
- **`LockGuard::unlock` runs the unlock pair twice** (explicitly and again in Drop);
  harmless only because unlock is idempotent.

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

- **`gc(&mut self)` is uncallable through the shared-`Arc` deployment shape**, so removed
  partitions accumulate in the retire list for the process lifetime under key churn.
  Epoch-based retirement is the fix; until then treat shared routers as append-only.
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

- **Iterator lifetimes are transmuted past the node guard**: collected `&T` borrows
  (`iter().collect::<Vec<&T>>()`) dangle once the iterator advances or drops. Reachable
  use-after-free from idiomatic code; needs an API change (owned yields or a lending
  iterator).
- **`len()`/`is_empty()`/`capacity()` lock every node**: calling them while holding a live
  `Iter` or `Ref` on the same thread self-deadlocks; with `remove_range` in the mix a
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
