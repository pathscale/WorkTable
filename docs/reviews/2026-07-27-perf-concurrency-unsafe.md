# WorkTable Review: Performance, Concurrency and `unsafe`

**Date:** 2026-07-27
**Scope:** `src/in_memory/`, `src/lock/`, `src/index/`, `src/table/` (incl. `vacuum/`), `src/persistence/` (engine, task, operation, space), `src/util/`, `codegen/src/generators/**/queries/` and `**/locks.rs`, `benches/`. Read-only pass; no source touched.
**Commit:** `66d8cfc` (working tree dirty: `Cargo.toml`, `codegen/src/persist_index/generator.rs`, `src/lib.rs`, `src/persistence/mod.rs`, `src/persistence/space/index/mod.rs`, `src/persistence/space/mod.rs`, `tests/persistence/duplicate_key_index_reload.rs`, plus untracked `src/persistence/space/index/reconstruct.rs`)
**Reviewer slice:** perf-concurrency-unsafe. Sibling slices cover security, API design, docs and codegen ergonomics.

## Summary

- The row-lock machinery is genuinely well thought through for the *write/write* case, and the recent series (`a8f3081` … `66d8cfc`) closed real races. But the *read* path was never brought into it: `WorkTable::select`, `select_by_<index>`, `select_all`, `iter_with` all read row bytes with no lock at all while `update` mutates those same bytes in place through `UnsafeCell`. That is an unsynchronised data race reachable from 100% safe user code, and for `String`/unsized columns it is a real segfault risk, not a theoretical one. This is finding 01 and it dominates everything else here.
- The `unsafe` surface is small (10 blocks in `src/in_memory/data.rs`, 4 in `pages.rs`, 5 call sites elsewhere) but under-documented and under-enforced: safe functions (`save_row`, `get_row_ref`, `save_raw_row`) form `&mut` references to the whole page array, and `get_row_ref` runs `rkyv::access_unchecked` over bytes that may have come straight off disk. Full inventory in the appendix.
- Vacuum is the weakest concurrency component. Its link-collection scan over `reverse_pk_map` runs completely unsynchronised, `EmptyLinkRegistry::pop_max` accidentally drops its vacuum guard on the same line it takes it, and the gap is papered over with `tokio::time::sleep(100ms)`. A timing constant is not a synchronisation primitive.
- The persistence engine is a detached `tokio::spawn` that nobody joins, and it contains reachable `panic!`/`unwrap()`. A panic there is completely silent and turns every later `wait_for_ops()` into an infinite wait. That is the second-most-important thing to fix.
- Performance: the persistence path is the hotspot, and the cost is structural rather than micro. Every queued operation goes through *two* generated `WorkTable`s (with rkyv serialisation, three secondary indexes and async row locks) purely as queue bookkeeping; every index change event rewrites the *entire* table of contents to disk; several batch helpers are O(n²). The in-memory path is comparatively lean, its main waste being a doubled serialise-and-clone in `insert_cdc` whose result is then thrown away.
- Benchmarks exist and are decent for the in-memory single-op case (5 groups, ~30 benchmarks). They measure **none** of the above: no persisted table, no vacuum, no reload, no concurrent read-vs-write. Details in finding 16.

Top three actions: (1) put reads under the row lock or make rows immutable-with-reinsert; (2) make the persistence engine's failures observable instead of a silent dead task; (3) fix `pop_max`'s dropped guard and delete the 100 ms sleep.

## Findings

### [SEV-1] Row bytes are read and written concurrently with no synchronisation; UB from safe API

- **ID:** `worktable-perf-concurrency-unsafe-01`
- **Severity:** Critical
- **Category:** Correctness (soundness)
- **Confidence:** High
- **Location:** `src/in_memory/data.rs:70`, `src/in_memory/data.rs:118`, `src/in_memory/data.rs:186-198`, `src/table/mod.rs:155-160`, `codegen/src/generators/in_memory/queries/update.rs:111`, `codegen/src/generators/in_memory/table/index_fns.rs:78-80`, `codegen/src/generators/in_memory/table/impls.rs:113-116`
- **What:** `Data` stores rows in an `UnsafeCell<AlignedBytes<N>>` and carries `unsafe impl Sync`. Writers reach the bytes through `with_mut_ref` → `get_mut_row_ref` → `rkyv::access_unchecked_mut(...).unseal_unchecked()`, then `std::mem::swap` the archived fields in place. Readers reach the same bytes through `get_row_ref` → `rkyv::access_unchecked`, and `get_row_ref` is a *safe* function reached from `WorkTable::select` (`src/table/mod.rs:155-160`), which acquires no lock of any kind. The generated `update` does take a row lock (`codegen/.../queries/update.rs:84-92`), so writer/writer is serialised, but nothing on the read path participates.
- **Why it matters:** Two concurrent accesses to the same non-atomic bytes with at least one write and no happens-before edge is a data race: UB by the Rust memory model, and the compiler is free to tear or duplicate the loads. For a `u64` column the practical damage is a torn value. For a `String`/unsized column the archived representation is a *relative pointer plus length*; `mem::swap` writes those two words non-atomically, so a concurrent reader can pair a new pointer with an old length (or vice versa) and `access_unchecked` will happily hand back a `&str` pointing outside the page. `table.select(pk)` racing `table.update(row).await` is the single most obvious thing a user of this crate does. There is no `unsafe` in the user's code.
- **Fix:** Design discussion, not mechanical. Three options, roughly in increasing order of cost:
  1. Make reads take the row lock. `select`/`select_by_*` become `async`. Breaking API change, kills the "near-Vec read performance" claim in `Cargo.toml:11`, but is the smallest correctness delta.
  2. Make in-place update never mutate a *published* row: always `reinsert` (allocate a new link, write, then flip the index pointer). The existing `reinsert` path already does exactly this for unsized rows (`codegen/.../queries/update.rs:57-80`). Readers would then only ever see fully-written bytes. Cost: an allocation per update and more vacuum pressure; benefit: lock-free reads stay lock-free and actually become sound.
  3. Per-row seqlock: a version counter bumped `Release` before and after the write, readers retry on an odd/changed count, with the byte access done through raw pointer reads rather than `&`/`&mut`. Keeps reads cheap but is fiddly to get right and the `access_unchecked` in the retry window still needs to be pointer-based.
  Whichever is chosen, the byte access in `Data` should stop forming `&mut [u8; N]` over the whole page (see finding 02).
- **Effort:** L (option 1 or 2) to XL (option 3, plus proving it).
- **Blast radius:** `src/in_memory/data.rs`, `src/in_memory/pages.rs`, `src/table/mod.rs`, every `select*` generator in `codegen/src/generators/{in_memory,persist,read_only}/`. Option 1 is a breaking API change for every consumer.

### [SEV-2] Persistence engine panics are silent and hang `wait_for_ops` forever

- **ID:** `worktable-perf-concurrency-unsafe-02`
- **Severity:** Critical
- **Category:** Correctness
- **Confidence:** High
- **Location:** `src/persistence/task.rs:445-492` (spawn + `AbortHandle`), `src/persistence/task.rs:507-523` (`wait_for_ops`), panic sites: `src/persistence/space/index/mod.rs:395-403`, `src/persistence/space/index/mod.rs:463-465`, `src/persistence/operation/batch.rs:154`, `src/persistence/space/index/table_of_contents.rs:124`
- **What:** `run_engine` does `tokio::spawn(task).abort_handle()` and keeps only the `AbortHandle`. The `JoinHandle` is dropped, so a panic inside the engine loop is never observed by anyone. The engine body reaches at least four panics: two explicit `panic!("page should be available in table of contents…")` in `SpaceIndex::process_change_event_batch`, an `.unwrap()` on an async delete in `BatchOperation::remove_operations_from_events`, and `panic!("Page with key {old_key:?} not found")` in `IndexTableOfContents::update_key`. Once the task dies, `Queue::push` keeps succeeding, `queue.len()` never returns to zero, and `wait_for_ops()` loops on its 1-second timer forever.
- **Why it matters:** The failure presentation is the worst possible one: writes appear to succeed, nothing is logged, and shutdown hangs. The TOC-missing panic is not hypothetical: the repo has an open reproduction for exactly this class of TOC inconsistency (`e7009a6`, patch `0011`), and `process_change_event_batch` is the path it runs on. Secondary problem: the panic message at `src/persistence/space/index/mod.rs:398` first collects *the entire table of contents* into a `Vec` to format it, so on a large index the panic path allocates proportionally to the index before dying.
- **Fix:** Two changes, both mechanical.
  1. Keep the `JoinHandle`, not just the `AbortHandle`; on the `Drop`/`wait_for_ops` path check `handle.is_finished()` and surface the `JoinError`. Better: wrap the loop body so a returned `Err` sets a terminal `AtomicBool` + stores the report, have `check_wait_triggers` return `true` when the engine is dead, and have `wait_for_ops` return `eyre::Result<()>` so callers learn about it.
  2. Convert the four panics into `eyre::bail!`/`?`. `process_change_event_batch` already returns `eyre::Result<()>`; the two `panic!`s there are the odd ones out among `?`-returning neighbours. Cap the diagnostic to a bounded sample of TOC keys rather than the whole set.
- **Effort:** M
- **Blast radius:** `src/persistence/task.rs`, `src/persistence/space/index/mod.rs`, `src/persistence/operation/batch.rs`. `wait_for_ops` changing to a `Result` is a breaking API change for persisted-table users; the internal changes are not.

### [SEV-3] `EmptyLinkRegistry::pop_max` drops its vacuum guard immediately; the sleep that hides it is not synchronisation

- **ID:** `worktable-perf-concurrency-unsafe-03`
- **Severity:** High
- **Category:** Correctness
- **Confidence:** High
- **Location:** `src/in_memory/empty_link_registry.rs:167-181`, `src/table/vacuum/vacuum.rs:137-140`
- **What:**
  ```rust
  pub fn pop_max(&self) -> Option<Link> {
      if self.vacuum_lock.try_lock().is_err() {
          return None;
      }
      let _g = self.op_lock.lock();
      // ... pops and returns a link, with vacuum_lock NOT held
  ```
  Temporaries created in an `if` condition are dropped at the end of the condition, before the block or the rest of the function runs. The `MutexGuard` returned by `try_lock()` is therefore released on that very line. The vacuum lock is only ever an *advisory instantaneous probe*, never held across the pop. `defragment` compensates with `tokio::time::sleep(Duration::from_millis(100))` and the comment "to avoid some rewrites of ops that used link from empty links registry" (`src/table/vacuum/vacuum.rs:139-140`).
- **Why it matters:** The interleaving `pop_max` probes → guard dropped → vacuum acquires `vacuum_lock` and begins defragmenting → `pop_max` returns a link on a page vacuum is about to move and `free_page`-reset is unprotected. `DataPages::insert` then writes a row into that link (`src/in_memory/pages.rs:106-128`) on a page whose `free_offset` vacuum resets to 0 at `src/table/vacuum/vacuum.rs:209-212`. The row is silently lost or overwritten by the next insert. The 100 ms sleep only covers callers that popped a link *shortly* before vacuum started; a task descheduled between the pop and the write (trivially possible on a loaded runtime, or with a `String` row whose serialisation is slow) blows straight through it.
- **Fix:** Hold the guard. Mechanical:
  ```rust
  let Ok(_vacuum_guard) = self.vacuum_lock.try_lock() else { return None; };
  let _g = self.op_lock.lock();
  ```
  That closes the pop-side window. The *write-after-pop* window is separate and needs the popped link to stay reserved until the insert completes: either have `insert` re-check page validity before writing, or have vacuum drain in-flight pops via a counter rather than a sleep. Once one of those exists, delete the sleep, it is currently a 100 ms unconditional tax on every vacuum run and it advertises a guarantee it does not provide.
- **Effort:** S for the guard, M for removing the sleep properly.
- **Blast radius:** `src/in_memory/empty_link_registry.rs`, `src/table/vacuum/vacuum.rs`, `src/in_memory/pages.rs`. Not a public API change.

### [SEV-4] Vacuum collects the rows to move before taking any lock, and resets pages unconditionally

- **ID:** `worktable-perf-concurrency-unsafe-04`
- **Severity:** High
- **Category:** Correctness
- **Confidence:** Medium-High (the window is clear from the code; a human should confirm with a stress test whether index mutation frequency makes it reachable in practice)
- **Location:** `src/table/vacuum/vacuum.rs:231-258` (unlocked scan), `src/table/vacuum/vacuum.rs:260-286` (per-row lock taken only afterwards), `src/table/vacuum/vacuum.rs:209-212` (`free_page`), `src/index/primary_index.rs:55-63` (non-atomic two-map update)
- **What:** `move_data_from` ranges over `primary_index.reverse_pk_map` to build the `links` list *before* acquiring a single row lock; locks are only taken inside the subsequent move loop. Meanwhile `PrimaryIndex::insert` updates two independent concurrent maps non-atomically:
  ```rust
  let old = self.pk_map.insert(value.clone(), offset_link);
  if let Some(old_link) = old { self.reverse_pk_map.remove(&old_link); }
  self.reverse_pk_map.insert(offset_link, value);
  ```
  Between the `remove` and the `insert` the reverse map contains *neither* link for that row. A vacuum scan crossing that window misses the row entirely. After the move loop, `defragment` calls `self.free_page(page_from)` (`vacuum.rs:171,177`), which does `p.reset()`, `free_offset = 0`, regardless of whether anything was missed, and the page is then handed out for reuse.
- **Why it matters:** A missed row is a *silently deleted* row whose primary index entry still points at the reset page, so a later `select` reads whatever the next insert wrote there. Notably `WorkTable::select` uses `select_non_ghosted`, not `select_non_vacuumed` (`src/table/mod.rs:157`), so the `is_vacuumed` marker that vacuum sets at `vacuum.rs:274-278` is not consulted on the main read path at all, the marker only protects the generated unique-index update loop (`codegen/.../queries/update.rs:727`).
- **Fix:** Two independent pieces.
  - Make the reverse-map update non-lossy: insert the new reverse entry before removing the old one, or hold the row lock across both map mutations in `PrimaryIndex::insert`/`insert_cdc`.
  - Make `free_page` conditional: after the move loop, re-scan `reverse_pk_map` for the page and refuse to reset while any live link remains (log and skip the page for this vacuum run). Given the scan is cheap relative to the moves, this is a good safety net regardless of the fix above.
  - Separately, switch `WorkTable::select` to `select_non_vacuumed` so the marker actually gates the hot read path.
- **Effort:** M
- **Blast radius:** `src/table/vacuum/vacuum.rs`, `src/index/primary_index.rs`, `src/table/mod.rs`. Behavioural change only.

### [SEV-5] `Lock::unlock` uses a `Relaxed` store, so there is no release edge protecting the row bytes

- **ID:** `worktable-perf-concurrency-unsafe-05`
- **Severity:** High
- **Category:** Correctness
- **Confidence:** High (the ordering mismatch is unambiguous; whether it is observable today depends on the target: on x86 TSO it is largely masked, on the aarch64 machines this repo is developed on it is not)
- **Location:** `src/lock/mod.rs:126-136`, `src/lock/mod.rs:159-184`
- **What:**
  ```rust
  pub fn unlock(&self) { self.locked.store(false, Ordering::Relaxed); ... }
  pub fn lock(&self)   { self.locked.store(true,  Ordering::Relaxed); }
  ```
  while `LockWait::poll` reads it with `Ordering::Acquire` in three places. An `Acquire` load only synchronises-with a `Release` (or stronger) store. With a `Relaxed` store there is no happens-before edge between the unlocking writer and the waiter that observes `false`.
- **Why it matters:** The whole point of this lock is to protect row bytes that are written through `UnsafeCell` with no atomics of their own. Writer does `with_mut_ref(...)` → `unlock()`; waiter sees `false` → reads the row. Without the release/acquire pair, the waiter is not guaranteed to see the writer's byte stores at all, and the compiler may hoist loads above the flag check. This is exactly the "atomics with too-weak orderings" case. Note it is masked on x86-64 (stores are release-ish under TSO) which is likely why it has never been observed.
- **Fix:** Mechanical. `unlock()` → `store(false, Ordering::Release)`; `lock()` → `store(true, Ordering::Release)` (or `Relaxed` is defensible for `lock` since it only ever tightens); `is_locked()` → `load(Ordering::Acquire)` for consistency with `LockWait`. Two-line change, no design question.
- **Effort:** S
- **Blast radius:** `src/lock/mod.rs` only.

### [SEV-6] `Lock` identity is a wrapping `u16`, and the wait-set is a `HashSet` keyed on it

- **ID:** `worktable-perf-concurrency-unsafe-06`
- **Severity:** High
- **Category:** Correctness
- **Confidence:** Medium (the mechanism is certain; whether 65 536 acquisitions can overlap one held lock in a real workload needs a human call)
- **Location:** `src/lock/map.rs:14`, `src/lock/map.rs:104-106`, `src/lock/mod.rs:74-92`, `codegen/src/generators/in_memory/queries/locks.rs:81-104`, `src/lock/row_lock.rs:84-91`
- **What:** `LockMap::next_id` is an `AtomicU16::fetch_add`, so ids repeat every 65 536 acquisitions. `Lock`'s `PartialEq` and `Hash` are implemented **solely on `id`** (`src/lock/mod.rs:80-92`). The generated per-column lock function collects the previous locks into a `HashSet<Arc<Lock>>`:
  ```rust
  let mut set = std::collections::HashSet::new();
  if let Some(lock) = &self.#col { set.insert(lock.clone()); }
  ```
  and the caller then `join_all`s a `wait()` over that set.
- **Why it matters:** If two *distinct* in-flight locks in a row's column slots ever collide on id, the `HashSet` silently drops one of them and the acquiring operation never waits for it. Two writers then enter the same row's bytes concurrently, precisely the mutual exclusion the whole design exists to provide. Reaching it needs a lock to be held while ~65 536 other acquisitions complete, which is plausible on a persisted table where an operation can block on disk I/O while unrelated in-memory rows churn. This is an ABA hazard in the classic sense: the id is reused while the old holder is still live.
- **Fix:** Widen `next_id` to `AtomicU64` and `Lock::id` to `u64` (kills the practical wraparound), *and* fix the identity: implement `PartialEq`/`Hash` for `Lock` by pointer (`Arc::as_ptr` / `std::ptr::eq`) rather than by id, since the intent of the `HashSet` is "dedupe the same `Arc`", not "dedupe the same number". Either alone helps; both together make the bug unrepresentable.
- **Effort:** S
- **Blast radius:** `src/lock/mod.rs`, `src/lock/map.rs`, `src/lock/row_lock.rs`, `codegen/src/generators/{in_memory,persist,read_only}/queries/locks.rs`. `Lock::id() -> u16` is public, so widening it is a breaking change for anyone who calls it (nothing in-tree does outside the lock module).

### [SEV-7] Untrusted-file parsing: TOC chain following has no cycle or length bound, and a corrupt first page silently empties the index

- **ID:** `worktable-perf-concurrency-unsafe-07`
- **Severity:** High
- **Category:** Security / Correctness
- **Confidence:** High
- **Location:** `src/persistence/space/index/table_of_contents.rs:150-187`
- **What:**
  ```rust
  let mut index = table_of_contents_pages[0].header.next_id.into();
  while !ind {
      let page = parse_page::<...>(file, index).await?;
      ind = page.header.next_id.is_empty();
      index = page.header.next_id.into();
      table_of_contents_pages.push(page);
  }
  ```
  `next_id` comes from the file. A file whose page 3 has `next_id = 3` (or any cycle) loops forever, pushing a page into a `Vec` each iteration. There is no visited-set, no bound against the file's page count, and no bound against a sane maximum. Separately, the outer `if let Ok(page) = first_page { ... } else { Ok(Self::new(...)) }` swallows *any* parse error on TOC page 1 and returns a brand-new empty table of contents.
- **Why it matters:** Two distinct problems. (a) Unbounded allocation and an infinite `async` loop driven by a length/pointer field in a persisted file. A corrupted or hostile `.wt.idx` file OOMs the process. Since `parse_from_file` is reached from `SpaceIndex::new` → `primary_from_table_files_path`, opening a table is enough. (b) Silently substituting an empty TOC for a corrupt one means a damaged index page turns into "this index has no entries", the table opens, `select_by_<index>` returns nothing, and later CDC events write against an empty TOC. Silent data loss is worse than a refused open.
- **Fix:** Mechanical. Track visited page ids in a `HashSet` and bail with `eyre::bail!("table of contents chain has a cycle at page {index}")` on a repeat; also bound the chain length by `file_length / PAGE_SIZE` (already computed in the caller at `src/persistence/space/index/mod.rs:89-95`, so pass it in). For (b), only fall back to `Self::new` when the read failed because the page does not exist (fresh file); propagate every other error.
- **Effort:** S
- **Blast radius:** `src/persistence/space/index/table_of_contents.rs`, one extra parameter threaded from `src/persistence/space/index/mod.rs`.

### [SEV-8] The persistence queue is unbounded with no backpressure

- **ID:** `worktable-perf-concurrency-unsafe-08`
- **Severity:** High
- **Category:** Performance / Denial of service
- **Confidence:** High
- **Location:** `src/persistence/task.rs:274-301`, `src/persistence/task.rs:412-414`
- **What:** `PersistenceTask::apply_operation` is a synchronous `self.queue.push(op)` into an unbounded `VecDeque`. `len` is tracked but only ever read by `wait_for_ops`; nothing throttles producers. Each queued `Operation::Insert` owns a `Vec<u8>` of the full serialised row plus its primary and secondary change-event vectors.
- **Why it matters:** The engine is a single task that does synchronous-ish disk I/O per event (see finding 09). A writer loop easily outruns it, since an in-memory insert is sub-microsecond while an index event costs several file writes, and the queue then grows without limit until the process is OOM-killed. The queue also holds the *only* copy of not-yet-persisted data, so it cannot simply be dropped. There is no configuration knob for a cap.
- **Fix:** Design decision on the policy, mechanics are easy. Add a high-water mark to `Queue` and make `apply_operation` either (a) `async` and await capacity (`tokio::sync::Semaphore` permits released by the engine after apply), or (b) return `Err(WorkTableError::PersistenceBacklog)` above the mark so the caller can shed load. (a) is the right default for a durable store; it makes `insert` on a persisted table `async`, which the generated persist path already mostly is. Expose the cap on `DiskConfig`.
- **Effort:** M
- **Blast radius:** `src/persistence/task.rs`, `codegen/src/generators/persist/**` (the `#persist_call` sites). Breaking for persisted-table users if `apply_operation` becomes fallible or async.

### [SEV-9] Every index change event rewrites the whole table of contents; TOC lookup is a linear scan

- **ID:** `worktable-perf-concurrency-unsafe-09`
- **Severity:** High
- **Category:** Performance
- **Confidence:** High
- **Location:** `src/persistence/space/index/table_of_contents.rs:133-148` (`persist`), `src/persistence/space/index/table_of_contents.rs:43-51` (`get`), `src/persistence/space/index/table_of_contents.rs:113-126` (`update_key`), call sites `src/persistence/space/index/mod.rs:213,234,246,254,279,505`
- **What:** `IndexTableOfContents::persist` unconditionally writes **every** TOC page:
  ```rust
  for page in &mut self.pages { persist_page(page, file).await?; }
  ```
  and it is called from `process_insert_at`, `process_remove_at`, `process_create_node`, `process_remove_node`, `process_split_node` and once per batch. `get` is a linear scan over all TOC pages that calls `contains` and then `get` on the matching page, two lookups where one would do. `update_key` scans every page on the miss path.
- **Why it matters:** With `P` TOC pages, applying `E` index events costs `O(P·E)` page writes on top of the actual index writes, and `P` grows with the index. On a bulk load `P` grows monotonically, so the cost is quadratic in rows loaded. This is the same shape as the already-known "index-space TOC stall on bulk loads" (`e7009a6`, patch `0011`); patches 0012/0013 fixed the *correctness* half (event-id gap scanning, `save_batch_data` high-water mark) but the full-rewrite cost is still here at `HEAD`. A single-event path (`process_change_event`, `src/persistence/space/index/mod.rs:352-386`) additionally does an explicit `flush()` after the TOC rewrite, so each event is `P` writes plus a flush.
- **Fix:** Mechanical, high payoff.
  1. Track dirty pages: `dirty: HashSet<usize>` on `IndexTableOfContents`, set by `insert`/`remove`/`update_key`/`pop_empty_page_id`, and have `persist` write only dirty pages and clear the set. This alone removes the `O(P)` factor from the common single-key-update case.
  2. Replace the linear `get` with a `HashMap<T, (page_idx, PageId)>` maintained alongside `pages`, rebuilt in `parse_from_file`. `get`/`update_key`/`remove` become `O(1)`.
- **Effort:** M
- **Blast radius:** `src/persistence/space/index/table_of_contents.rs` plus its callers in `src/persistence/space/index/{mod,unsized_}.rs`. Internal only; the on-disk format is unchanged.

### [SEV-10] The persistence queue uses two full generated `WorkTable`s as bookkeeping, at per-operation cost

- **ID:** `worktable-perf-concurrency-unsafe-10`
- **Severity:** High
- **Category:** Performance / Design
- **Confidence:** High
- **Location:** `src/persistence/task.rs:20-34` (`QueueInner` table), `src/persistence/operation/batch.rs:18-42` (`BatchInner` table), `src/persistence/task.rs:100-113`, `src/persistence/task.rs:214-242`
- **What:** Every operation that enters the analyser is inserted as a row into `QueueInnerWorkTable` (5 columns, 3 secondary indexes, rkyv-serialised into a data page). Batch collection then, per operation: `select(id)` it back out, `From`-convert it into a `BatchInnerRow`, insert it into a *second* generated worktable (`BatchInner`, 6 columns, 4 secondary indexes), and `delete_without_lock(id).await` it from the first, then a second pass runs `info_wt.update_pos_by_op_id(q, op_id).await` per operation. `collect_batch_from_op_id` also re-runs `select_by_operation_id(...).execute()?` inside three separate loops, each allocating a `Vec`.
- **Why it matters:** This is the per-write overhead of the persisted path, and it is enormous relative to what it computes (a grouping of operations by data page). Per operation the engine pays: 2 rkyv serialisations into data pages, ~7 secondary-index B-tree insertions, 1 deserialise, 1 async row-lock acquisition for the delete, 1 async indexed update, plus several `Vec` allocations from `.execute()`. The actual bookkeeping is `page_id -> Vec<op_index>` plus an ordered map keyed on `OperationId`. Using the crate's own general-purpose table for this is dogfooding at a cost of roughly two orders of magnitude.
- **Fix:** Replace both inner worktables with plain structures: `BTreeMap<OperationId, usize>` for ordering, `HashMap<PageId, Vec<usize>>` for page grouping, `HashMap<Link, usize>` for the last-op-per-link lookup in `get_batch_data_op`. The `OptimizedVec` that already holds the operations stays as the backing store. This is a self-contained rewrite of `QueueAnalyzer` and `BatchOperation` with no format or API impact, and it would be the single largest write-throughput win available. Worth benchmarking first (see finding 16) so the improvement is measured rather than asserted.
- **Effort:** L
- **Blast radius:** `src/persistence/task.rs`, `src/persistence/operation/batch.rs`. Internal; `QueueInnerRow`/`BatchInnerRow` are `pub` but only used across those two modules.

### [SEV-11] Quadratic helpers on the batch path

- **ID:** `worktable-perf-concurrency-unsafe-11`
- **Severity:** Medium
- **Category:** Performance
- **Confidence:** High
- **Location:** `src/persistence/operation/batch.rs:115-174`, `src/persistence/operation/batch.rs:347-372`, `src/persistence/operation/batch.rs:374-401`, `src/persistence/operation/util.rs:6-26`
- **What:** Four separate O(n²) shapes in the same file:
  - `remove_operations_from_events` runs `self.ops.iter().rev().position(...)` **per invalid event**, which is a scan over all ops with each op scanning all of its own events, followed by `self.ops.remove(idx)` (an O(n) memmove) per hit.
  - `validate_events` loops `validate_events_iteration` (a full backwards scan) until it returns empty, draining at least one element per pass. Worst case one element per pass over the whole vector.
  - `get_indexes_evs` clones both prepared event collections, then does `primary_evs.remove(pos)` in a loop over acknowledge events.
  - `get_batch_data_op` runs a full `select_by_link(...).order_on(...).limit(1).execute()` query **per link** in the batch, each of which sorts a result set, and then copies the row bytes with `data_bytes.to_vec()`.
- **Why it matters:** Batch size is not fixed: `page_limit` starts at `MAX_PAGE_AMOUNT = 16` and grows by 8 every time a batch cannot be collected (`src/persistence/task.rs:258-263`) with no upper bound, so the pathological case is reached exactly when the system is already struggling. `validate` calls `remove_operations_from_events` inside its own retry loop, so the two nest.
- **Fix:** Mechanical.
  - Build `HashMap<change::Id, usize>` (event id → op index) once in `prepare_indexes_evs`; `remove_operations_from_events` then becomes a lookup. Use `swap_remove` or mark-and-`retain` instead of `remove` in a loop.
  - `get_indexes_evs`: collect acknowledge ids into a `HashSet` and `retain` once instead of `binary_search` + `remove` per id.
  - `get_batch_data_op`: fold the last-op-per-link map in one pass over `ops` (see finding 10) rather than one query per link; `Cow`/`Arc<[u8]>` instead of `to_vec()` where the bytes are only read.
  - Cap `page_limit` growth.
- **Effort:** M
- **Blast radius:** `src/persistence/operation/batch.rs`, `src/persistence/operation/util.rs`, `src/persistence/task.rs`.

### [SEV-12] `insert_cdc` serialises the row twice and clones it twice; the result is discarded

- **ID:** `worktable-perf-concurrency-unsafe-12`
- **Severity:** Medium
- **Category:** Performance
- **Confidence:** High
- **Location:** `src/in_memory/pages.rs:165-179`, `src/table/mod.rs:230-233`, `src/table/mod.rs:296-306`
- **What:**
  ```rust
  pub fn insert_cdc(&self, row: Row) -> Result<(Link, Vec<u8>), ExecutionError> {
      let link = self.insert(row.clone())?;                  // serialises once, inside save_row
      let general_row = <Row as StorableRow>::WrappedRow::from_inner(row);
      let bytes = rkyv::to_bytes(&general_row).expect(...).into_vec();   // serialises again
      Ok((link, bytes))
  }
  ```
  and the caller:
  ```rust
  let (link, _) = match self.data.insert_cdc(row.clone()) { ... };   // bytes dropped
  ...
  let bytes = match self.data.select_raw(link) { ... };              // read back from the page
  ```
- **Why it matters:** Per persisted insert this is two `Row` clones (one at `table/mod.rs:230`, one at `pages.rs:173`), two full rkyv serialisations, and one `Vec` copy out of the page. One of the serialisations plus one clone produce a value that is immediately dropped. For a row with a `String` column the clone is a heap allocation of its own. This is the hottest allocation churn on the write path outside the persistence engine. `reinsert_cdc` (`src/table/mod.rs:414-417`, `489-499`) has the identical shape.
- **Why it might be deliberate:** `select_raw` reads the bytes *after* `unghost()` has flipped the ghost flag, so the persisted bytes carry the correct flag while `insert_cdc`'s bytes would not. That justifies discarding them but not producing them.
- **Fix:** Mechanical. Either drop the second serialisation from `pages.rs::insert_cdc` and change its signature to return just `Link` (making it identical to `insert`, at which point delete it), or keep it and have the caller flip the ghost bit in the returned buffer instead of calling `select_raw`. The former is simpler and removes a function. Also take `row: &Row` in `DataPages::insert` so the outer `row.clone()` at `table/mod.rs:230` can go.
- **Effort:** S
- **Blast radius:** `src/in_memory/pages.rs`, `src/table/mod.rs`. `DataPages::insert_cdc` is `pub` but has one in-tree caller.

### [SEV-13] `Data::save_row` never rolls back `free_offset` on failure; wraps silently in release

- **ID:** `worktable-perf-concurrency-unsafe-13`
- **Severity:** Medium
- **Category:** Correctness
- **Confidence:** Medium (the monotonic growth is certain; reaching u32 wraparound needs a specific retry pattern)
- **Location:** `src/in_memory/data.rs:109-116`, `src/in_memory/data.rs:257-264`, `src/in_memory/pages.rs:130-162`
- **What:**
  ```rust
  let offset = self.free_offset.fetch_add(length, Ordering::AcqRel);
  if offset > DATA_LENGTH as u32 - length {
      return Err(ExecutionError::PageIsFull { .. });   // free_offset stays bumped
  }
  ```
  Every failed attempt permanently advances `free_offset` past `DATA_LENGTH`. `DataPages::insert` retries in a loop against whatever page `current_page_id` names, so a page can absorb many failed attempts. There is no `[profile.release]` in `Cargo.toml`, so `overflow-checks` is off in release and the `fetch_add` wraps silently rather than panicking.
- **Why it matters:** `free_space()` uses `saturating_sub` so it reports 0, which is fine, and `mark_page_full` uses `saturating_sub` too. But if `free_offset` ever wraps past `u32::MAX`, the next `offset` lands back near 0 and the bounds check *passes*, so a row is written over live data at the start of the page. Corruption, not UB (the slice index is checked), but silent. Reaching it needs ~2³² / row_length failed attempts on a single page, which the insert loop's `add_next_page` normally prevents; the "reuse an empty page without resetting it" path at `src/in_memory/pages.rs:146-149` is the one that could sustain it (see nits).
- **Fix:** Mechanical: on the failure branch, `self.free_offset.fetch_sub(length, Ordering::AcqRel)` before returning, or switch to a `compare_exchange_weak` loop that only commits a valid offset. The CAS loop is the honest fix and removes the invariant "free_offset may exceed DATA_LENGTH" entirely. Also consider setting `overflow-checks = true` in `[profile.release]` for this crate given how much arithmetic rides on persisted lengths.
- **Effort:** S
- **Blast radius:** `src/in_memory/data.rs`. `free_offset` is a `pub` field read by tests and `mark_page_full`.

### [SEV-14] Busy-spin `continue` with no yield in the generated unique-index update

- **ID:** `worktable-perf-concurrency-unsafe-14`
- **Severity:** Medium
- **Category:** Correctness / Performance
- **Confidence:** High
- **Location:** `codegen/src/generators/in_memory/queries/update.rs:721-733`, `codegen/src/generators/persist/queries/update.rs:678-690`
- **What:**
  ```rust
  let link = loop {
      let link = self.0.indexes.#index.get(#by)...ok_or(WorkTableError::NotFound)?;
      if let Err(e) = self.0.data.select_non_vacuumed(link) {
          if e.is_vacuumed() { continue; }
          return Err(e.into());
      } else { break link; }
  };
  ```
  The `continue` re-runs the loop body with no `.await` anywhere in it, so the enclosing `async fn` never yields to the executor while spinning.
- **Why it matters:** The condition it waits on (`is_vacuumed` cleared, index repointed) is cleared by the *vacuum task*, which runs on the same tokio runtime. On a current-thread runtime this is a hard deadlock: the spinning task owns the only worker and vacuum can never make progress. On a multi-threaded runtime it burns a full core for the duration of the move and blocks that worker from running other tasks. The window is normally short because the row lock is taken before the loop, but "normally short" and "cannot happen" are different guarantees, and the current-thread case is unconditional.
- **Fix:** Mechanical: insert `tokio::task::yield_now().await;` before `continue`, and add a bounded retry count that returns an error rather than spinning indefinitely. Better still, wait on the vacuum's lock rather than polling.
- **Effort:** S
- **Blast radius:** two codegen templates; regenerates into every persisted and in-memory table with a unique-index update query.

### [SEV-15] `PrimaryIndex::insert_checked` leaves the pk map poisoned when the reverse insert fails

- **ID:** `worktable-perf-concurrency-unsafe-15`
- **Severity:** Medium
- **Category:** Correctness
- **Confidence:** Medium (depends on `checked_insert`'s exact semantics in `WorkTablesIndex`, which I did not read)
- **Location:** `src/index/primary_index.rs:66-71`, `src/index/primary_index.rs:98-108`, `src/table/mod.rs:180-183`
- **What:**
  ```rust
  fn insert_checked(&self, value: PrimaryKey, link: Link) -> Option<()> {
      self.pk_map.checked_insert(value.clone(), offset_link)?;
      self.reverse_pk_map.checked_insert(offset_link, value)?;   // no rollback of the line above
      Some(())
  }
  ```
  `WorkTable::insert` treats `None` as `PrimaryAlreadyExists`, deletes the data at `link`, and returns, but the `pk_map` entry inserted by the first line is never removed. Note also that `insert_checked_cdc` is asymmetric: it uses `checked_insert_cdc` for the pk map and a plain unchecked `insert` for the reverse map, so the two functions disagree about what is being checked.
- **Why it matters:** After this path the primary index maps a live primary key to a deleted link. `select(pk)` then reads a link that is on the empty-link free list and may already have been reused by a different row, so it returns *another row's data* under the wrong key. The path is reached when the same `Link` is already present in the reverse map, which is exactly what happens under link reuse from `EmptyLinkRegistry`.
- **Fix:** Mechanical: on the second failure, `self.pk_map.remove(&value)` before returning `None`. Then make `insert_checked_cdc` use the same checked-and-rollback shape so the two cannot drift. Worth a targeted test that forces a reverse-map collision.
- **Effort:** S
- **Blast radius:** `src/index/primary_index.rs`, plus a test.

### [SEV-16] Benchmarks measure only the in-memory single-operation path

- **ID:** `worktable-perf-concurrency-unsafe-16`
- **Severity:** Medium
- **Category:** Performance / Maintainability
- **Confidence:** High
- **Location:** `benches/worktable_benchmarks.rs`, `benches/common/mod.rs:1-66`, `benches/cases/*.rs`
- **What exists:** 5 criterion groups over 4 in-memory tables (`Simple`, `UniqueIndex`, `NonUniqueIndex`, `FullFeatured`): insert, select by pk, select by unique/non-unique index, update, in-place update, delete, upsert (insert and update variants), batch insert, batch select, and one contention group (`update_contention.rs`) that spawns 2/4/8/16/32 concurrent updates on **one** row. Config is `sample_size(300)`, 10 s measurement, 5 s warm-up.
- **What is not measured, in priority order:**
  1. **Anything persisted.** No `DiskPersistenceEngine` benchmark at all. Findings 09, 10 and 11 are all on that path, and none of them would show up as a regression today. This is the biggest gap: the whole batch/analyser/TOC machinery is unmeasured.
  2. **Vacuum.** No benchmark for `defragment` or `analyze_fragmentation`, so the 100 ms sleep (finding 03), the per-row `get_raw_row` `Vec` allocation plus full `select` deserialise plus `Row` clone in `update_index_after_move` (`src/table/vacuum/vacuum.rs:271-282, 304-326`) are all uncosted.
  3. **Reload / open.** `parse_indexset` (`src/persistence/space/index/mod.rs:287-297`) reads every index page serially; startup time on a large table is unknown.
  4. **Concurrent read vs write.** `update_contention` is write-vs-write only. Given finding 01, a select-while-updating benchmark is the one that would actually exercise the interesting path, and would be the natural place to prove out whichever fix is chosen.
  5. **Large-table iteration.** `iter_with`/`select_all` re-seek the pk map per row (`codegen/src/generators/in_memory/table/impls.rs:263-292`: a fresh `pk_map.range(k.clone()..)` and a `k.clone()` for every row). No benchmark covers N large.
  6. Multi-row update by non-unique index, which acquires N row locks up front (`codegen/src/generators/in_memory/queries/update.rs:604-610`).
- **Fix:** Add a `benches/cases/persistence.rs` (tempdir-backed persisted table: insert throughput with `wait_for_ops`, batch apply latency, reload time) and a `benches/cases/vacuum.rs` (fragment then `defragment`, parameterised on page count). Add a read-during-write case to `update_contention.rs`. Note the config is expensive: 5 groups × 10 s + 5 s warm-up already runs several minutes; new groups should use a smaller `sample_size` for the I/O cases.
- **Effort:** M
- **Blast radius:** `benches/` only.

## Appendix: complete `unsafe` inventory

Every `unsafe` block/impl outside `#[cfg(test)]`, the invariant it needs, and whether that invariant is actually enforced.

| Location | What | Invariant required | Enforced? |
|---|---|---|---|
| `src/in_memory/data.rs:70` | `unsafe impl Sync for Data` | All interior mutation of `inner_data` is externally synchronised against all reads | **No**: see finding 01. This impl is what makes the whole crate's concurrency claim, and it is currently unjustified |
| `src/in_memory/data.rs:118` | `&mut *self.inner_data.get()` in **safe** `save_row` | Exclusive access to the entire `[u8; N]` for the duration | **No**: two concurrent `save_row` calls each form a `&mut` to the whole array, which is UB under Stacked/Tree Borrows even though the written sub-ranges are disjoint. Should be a raw-pointer write to `offset..offset+length` |
| `src/in_memory/data.rs:142` | `&mut *…get()` in `unsafe fn save_row_by_link` | Caller ensures no other refs | Partially: callers hold the row lock, but readers do not participate (finding 01) |
| `src/in_memory/data.rs:171` | `&mut *…get()` in `unsafe fn try_save_row_by_link` | Same | Same |
| `src/in_memory/data.rs:186-188` | `&mut *…get()` + `rkyv::access_unchecked_mut` in `unsafe fn get_mut_row_ref` | (a) no other refs; (b) bytes at the link are a valid archived `Row` | (a) partially, (b) **not validated**: trusted because they were written by `save_row`, which is true in memory but not after a reload |
| `src/in_memory/data.rs:196-198` | `&*…get()` + `rkyv::access_unchecked` in **safe** `get_row_ref` | (a) no concurrent writer; (b) valid archive at `offset..offset+length` | **Neither.** (a) is finding 01. (b) matters because pages are built from file bytes via `Data::from_data_page` (`data.rs:83-90`), so `access_unchecked` runs over on-disk data with no `access`/validation. A corrupt file can make rkyv follow an out-of-range relative pointer. Fixing this means using `rkyv::access` (validated) at least on the first read after load, or validating pages at load time |
| `src/in_memory/data.rs:211` | `&mut *…get()` in safe `get_raw_row` | Exclusive access | **No**, and gratuitously so, the function only reads. Should be `&*` |
| `src/in_memory/data.rs:229-242` | `std::ptr::copy` in `unsafe fn move_from_to` | `from`/`to` in bounds of this page, same page, equal lengths | **Partially.** Equal length is checked (`data.rs:225`); the doc comment promises the caller ensures the links are valid, but `ptr::copy` performs no bounds check, so an out-of-range `to.offset` is an out-of-bounds *write*, unlike every neighbouring slice-indexed path which would panic. Note this function has **no production caller** (only the tests at `data.rs:523,547`), dead unsafe API. Delete it, or add explicit bound checks |
| `src/in_memory/data.rs:266` | `&mut *…get()` in **safe** `save_raw_row` | Exclusive access | **No**: same as `save_row` |
| `src/in_memory/data.rs:277` | `&*…get()` in safe `get_bytes` | No concurrent writer | **No**: copies the whole page while writers may be mid-write; used by the persistence snapshot path |
| `src/in_memory/pages.rs:111` | call to `try_save_row_by_link` | Delegates | Inherits the above |
| `src/in_memory/pages.rs:296-300` | `get_mut_row_ref(...).unseal_unchecked()` in `unsafe fn with_mut_ref` | Caller holds the row lock | Callers in `src/table/mod.rs` (`unghost`) do **not** hold a lock; `src/table/vacuum/vacuum.rs:274` and the generated update paths do |
| `src/in_memory/pages.rs:322-325` | `save_row_by_link` in `unsafe fn update` | Documented (`pages.rs:305-310`) | Caller-enforced; no in-tree caller found for `DataPages::update` |
| `src/in_memory/pages.rs:335` | `with_mut_ref(link, delete)` in **safe** `delete` | No other refs to the row | **No**: safe `delete` calls an `unsafe fn` with no lock. Reachable from `WorkTable::insert`'s rollback path (`table/mod.rs:181`) |
| `src/table/mod.rs:196,285,355,420` | `with_mut_ref(link, unghost)` | No other refs | **No**: no lock is held. In practice the row is not yet published in the index at 196/285, which is a real argument; at 355 (`reinsert`) it is a new link, likewise. Worth writing that reasoning into a `// SAFETY:` comment |
| `src/table/vacuum/vacuum.rs:274-278` | `with_mut_ref(link, set_in_vacuum_process)` | No other refs | Row lock held (`vacuum.rs:261`) for writers; readers still race (finding 01) |
| `codegen/…/queries/update.rs:105,469,631,701` and `persist` equivalents | `access_unchecked_mut(&mut bytes[..]).unseal_unchecked()` | `bytes` is a locally-owned buffer just produced by `rkyv::to_bytes` | **Yes**: this is the one clean group. The buffer is exclusively owned and known-valid |
| `codegen/…/queries/update.rs:111,476,640,742`, `in_place.rs:120` | `with_mut_ref` | Row lock held | **Yes** for the update queries; this is the part the recent lock-race series got right |

Nothing uses `mem::transmute`. No `unsafe` in `codegen` output beyond the above. Not one block carries a `// SAFETY:` comment; the four `/// # Safety` doc comments that exist (`data.rs:177-180`, `data.rs:219-223`, `pages.rs:305-310`, and the `#[allow(clippy::missing_safety_doc)]` suppressions at `data.rs:130,148` and `pages.rs:284`) do not say who is responsible for upholding them.

## Cross-cutting recommendations

1. **Decide the reader-concurrency model, then write it down.** Everything in findings 01, 04 and 05 traces back to an unstated assumption about whether reads participate in locking. Pick one (lock reads / copy-on-write rows / seqlock), record it in `docs/` as an invariant with the rationale, and add `// SAFETY:` comments to every block in the inventory that cites it. Right now each `unsafe` block is individually plausible and the composition is unsound. What breaks: option 1 changes the public API; option 2 changes vacuum pressure and link-reuse behaviour; option 3 changes nothing publicly but is the hardest to review.
2. **Make the persistence engine a first-class, observable component.** Keep its `JoinHandle`, give it a terminal-error slot, make `wait_for_ops` fallible, replace the four reachable panics with errors, and bound the queue. This is finding 02 + finding 08 and together they turn "silently stops persisting and hangs on shutdown" into "returns an error". Nothing else in the persistence area is worth optimising until failures are visible. What breaks: `wait_for_ops`/`apply_operation` signatures.
3. **Stop using generated `WorkTable`s for engine bookkeeping** (finding 10), and while in there fix the quadratic helpers (finding 11) and the TOC full-rewrite (finding 09). These three are the persistence write path's cost, in that order. Do 09 first, it is the cheapest and probably the largest single win. What breaks: nothing externally; the on-disk format is untouched.
4. **Give vacuum a real quiescence protocol.** Replace the `try_lock().is_err()` probe with a held guard, replace `sleep(100ms)` with an in-flight-pop counter (or an epoch/RCU-style grace period), and make `free_page` refuse to reset a page that still has live reverse-index entries. That is findings 03 and 04 as one project. What breaks: vacuum may now decline to free some pages, so fragmentation recovery becomes best-effort rather than assumed-total; the tests at `src/table/vacuum/vacuum.rs:815-848` assert `!get_empty_pages().is_empty()` and would need to stay valid.
5. **Add a persistence + vacuum benchmark suite before optimising either** (finding 16). Three of the four biggest recommended changes are performance work on paths with zero coverage. Without a baseline the follow-up agent cannot tell an improvement from a regression, and the AGENTS.md rule "run what you build before reporting it done" cannot be satisfied for these.
6. **Turn on `overflow-checks` in a dedicated profile.** A storage engine whose lengths and offsets come from files should not be doing wrapping arithmetic silently. `free_offset` (finding 13), `sum_links_len.fetch_sub` (nits), `utility.current_length -= 1` and `slots.get(index - 1)` (`src/persistence/space/index/mod.rs:179-186`) all wrap silently in release today. Add a `[profile.release-checked]` used by CI and stress tests even if the shipped release profile stays as-is.

## What I did not cover

- **`data_bucket` and `WorkTablesIndex` (`indexset`) internals.** Both are pinned to exact versions (`=0.4.0`, `=0.0.1`) and I treated them as black boxes. Several conclusions depend on their semantics: `BTreeSet`/`BTreeMultiMap` concurrency guarantees under `EmptyLinkRegistry`, `checked_insert` behaviour (finding 15), `IndexPage::apply_change_event`, and whether `parse_page` validates page headers. A follow-up should read `checked_insert` and `attach_node` at minimum.
- **`src/persistence/space/index/unsized_.rs`** and the unsized/`String` index path generally. It is a parallel implementation of `SpaceIndex` and likely shares findings 09 and 02, but I did not verify block by block.
- **`src/migration/`, `src/mem_stat/`, `src/features/s3_support.rs`, `src/table/system_info.rs`, `paper-bench/`, `performance_measurement/`.** Skipped entirely as out of slice.
- **`codegen/src/generators/read_only/`**: I spot-checked that its `select` shape matches `in_memory` (same unlocked `select_non_ghosted`) but did not review it otherwise.
- **The `perf_measurements` feature.** `#[performance_measurement]` attributes are sprinkled through `data.rs`/`pages.rs`; I did not check what they cost when the feature is off (they should compile out) or whether the instrumentation itself is sound.
- **I did not build, test, or benchmark anything.** No `cargo check`, no `cargo test`, no `cargo bench`: the crate plus workspace is large and the brief discourages expensive builds. Every finding is from reading. The confidence field on each finding reflects that: "High" means the code says so plainly, "Medium" means a human should confirm the runtime path.
- **Miri.** The natural verification for findings 01 and 13 is `cargo +nightly miri test` (with `-Zmiri-tree-borrows` for the `&mut`-aliasing question). I did not run it; it should be the follow-up agent's first move.

## Quick-start for the follow-up agent

Read in this order:

1. `src/in_memory/data.rs` (669 lines, ~half tests): the storage primitive and every interesting `unsafe`. Start at line 70.
2. `src/lock/mod.rs` + `src/lock/map.rs` + `src/lock/row_lock.rs` (~600 lines together): the whole concurrency contract, and short enough to hold in your head.
3. `src/table/mod.rs:139-203`: how `select` and `insert` do *not* use the above.
4. `codegen/src/generators/in_memory/queries/update.rs:83-125`: how `update` *does* use it. Compare with 3; the asymmetry is finding 01.
5. `src/table/vacuum/vacuum.rs:132-302`: `defragment` and `move_data_from`, the least-safe component.
6. `src/persistence/task.rs`: the engine loop, queue and analyser; the recent `wait_for_ops` fix is at lines 303-326 and 494-523.
7. `src/persistence/space/index/table_of_contents.rs`: small, and the source of both a perf finding and a parsing finding.

Commands:

```bash
cargo build && cargo test                 # per AGENTS.md
cargo fmt && cargo clippy --all-targets   # lint failures are build failures here
cargo bench                               # ~several minutes; 300 samples, 10s per bench
cargo +nightly miri test in_memory        # NOT set up; expect to fight the async tests
```

Repo notes that cost me time:

- `.claude/worktrees/gracious-golick-bd563f/` is a **full second copy of the repo**. Every `rg`/`find` doubles unless you exclude it. Use `--glob '!.claude/**'`.
- The working tree is dirty and there is an out-of-repo patch series at `/Users/revenge/code/worktable-patches/` (0001-0015, with a `README.md` mapping them to commits). Check both before reporting a fix as new, patches 0001-0005 are the row-lock races, 0010 is vacuum CDC persistence, 0012/0013 are the TOC/batch fixes, 0014 is the `wait_for_ops` in-flight fix.
- Almost everything user-facing is generated by the `worktable!` macro. When a finding cites `codegen/src/generators/...`, that is the *template*; the behaviour appears in every generated table. There are three near-parallel generator trees (`in_memory`, `persist`, `read_only`) and fixes usually need to land in two or three of them, grep the sibling paths before calling a codegen fix done.
- `#[cfg(test)] mod tests` blocks are large (often more than half the file). Line numbers above are all in production code.

### Nits

- `src/table/vacuum/vacuum.rs:165`: `unreachable!("I hope so")`. A joke in a panic message on a reachable-looking branch; either prove it and say why, or return an error.
- `src/table/vacuum/vacuum.rs:186`: `unreachable!("at least one of two situations should appear to break from while cycle")` in the same `match`.
- `src/table/vacuum/vacuum.rs:243-245`: `if next.page_id != from { continue; }` inside a range that is already bounded to page `from`. Defensive scaffolding for an impossible state.
- `src/in_memory/pages.rs:146-149`: the empty-page reuse branch in `insert` sets `current_page_id` to a popped page **without** calling `page.reset()`, unlike `allocate_new_or_pop_free` (`pages.rs:199-206`) which does. Every insert on that page then fails and burns another empty page.
- `src/in_memory/pages.rs:146-148`: `empty_pages.write()` is held while `pages.write()` is acquired; every other site takes them in the opposite order or one at a time. Nothing currently takes `pages` then `empty_pages`, so there is no cycle today, but the ordering is unstated and one new call site would introduce a deadlock.
- `src/in_memory/pages.rs:148`: `let _pages = self.pages.write();` acquires a write lock purely as a memory fence, then drops it. If a fence is what is wanted, use one.
- `src/in_memory/empty_link_registry.rs:104-120`: `remove_link`/`insert_link` mutate three independent concurrent structures plus an atomic counter with no single lock; `iter()` (`:183`) and `pop_max` read them without `op_lock` consistently. Also `sum_links_len.fetch_sub` underflows silently in release if a link is removed twice, and `get_empty_links_size_bytes` feeds vacuum triggering.
- `src/lock/mod.rs:126-132`: `unlock()` wakes every registered waker **while holding** the `wakers` mutex.
- `src/lock/mod.rs:142-150`: `wait()` pushes an `Arc<AtomicWaker>` that is never removed; the `Vec` grows for the life of the `Lock`. Bounded in practice (locks are per-acquisition) but unbounded in principle if `wait()` is called in a retry loop.
- `src/lock/mod.rs:169-174`: `MAX_SPINS = 12` spin iterations inside `Future::poll`, i.e. on the executor thread. Twelve is harmless; the pattern is worth a comment saying why it is bounded.
- `src/persistence/task.rs:100-113`: `push` does `self.operations.push(value)` and *then* `self.queue_inner_wt.insert(row)?`. If the insert fails the operation is orphaned in the `OptimizedVec` forever and silently dropped from the persistence stream; the caller only logs it (`task.rs:459-463`).
- `src/persistence/task.rs:478`: `sleep(500ms)` per failed batch collection, with `page_limit += 8` growth and no cap (`task.rs:258-263`).
- `src/persistence/task.rs:507-523`: `notify_waiters()` (not `notify_one`) only wakes already-registered waiters, so a notification landing between `check_wait_triggers()` and `notified()` is lost. The 1-second fallback timer bounds it, which is presumably why it is there; a comment would help.
- `src/persistence/space/index/mod.rs:430-453`: `process_create_node` in the batch path calls `self.table_of_contents.insert((max_value.key.clone(), max_value.value), page_id)` **twice** with identical arguments (lines 436-437 and 451-452). Almost certainly a copy-paste survivor; at best wasted work, at worst a duplicate TOC entry that inflates `estimated_size` and survives one `remove`.
- `src/persistence/space/index/mod.rs:506`: `persist_pages_batch(pages.values().cloned().collect(), ...)` clones every dirty page (each an inner-page-sized buffer) when `pages` is dropped immediately after. `into_values().collect()` is free.
- `src/persistence/space/index/mod.rs:183-186`: `utility.slots.get(index - 1)` underflows when `index == 0`; panics either way (debug: subtract overflow, release: `expect` on `None`).
- `src/persistence/space/index/mod.rs:69-80` and `src/persistence/space/data.rs:57-64`: index/table names are derived by `path.split("/")` with `.expect("is not in root...")`. Breaks on Windows separators and panics on a short path.
- `src/persistence/space/mod.rs:81-89`: `open_or_create_file` does `.create(!path.exists())`, a TOCTOU that buys nothing; `.create(true)` is idempotent.
- `src/persistence/engine.rs:168-232`: the three batch sub-operations run concurrently over three files with no ordering and no journal, so a crash mid-batch leaves data and index files inconsistent with no recovery record. The code comments acknowledge cancellation-safety but not crash-atomicity. Out of this slice's scope but worth someone's attention.
- `codegen/src/generators/in_memory/table/impls.rs:263-292`: `iter_with` rebuilds `pk_map.range(k.clone()..)` and clones the key for **every** row, i.e. a fresh tree descent per row instead of holding one iterator.
- `src/util/optimized_vec.rs:47`: `self.empty.pop().unwrap()` immediately after `is_empty()` check; `if let Some(index) = self.empty.pop()` reads better and cannot regress.
