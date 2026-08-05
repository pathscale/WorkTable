# Code Review — PR #39: Persist native Arctic and Congee indexes

Branch: `feat/art-native-persistence` → `master`
Reviewer perspective: senior Rust engineer
Scope: 34 files, +2590 / −185. Core: `src/persistence/space/art_index.rs` (975 LOC),
`src/index/persistent_art.rs`, codegen wiring, concurrency stripes.

## Verdict

**Strong, careful work. Approve with minor changes.** Well-architected persistence
layer with a good format design, honest disclosure of its boundaries, and test
coverage on the cases that matter (torn tail, checksum/header mismatch, concurrent
same-row updates, compaction, exact topology round-trip). The concurrency model was
traced end-to-end and its load-bearing invariant holds. Findings below are refinements
and durability/robustness gaps to document or fix — not blockers.

Grounding: built the crate; ran `index_backends` integration tests (all pass, exit 0);
read the full CDC→persistence path; inspected the fork crates' reconstruction validation.

## What was verified correct (the parts that would have been bugs)

- **Striped concurrency model is sound.** `next_event_id()` (global atomic) is called
  *inside* the per-key stripe lock (`persistent_art.rs:167→177`). Different keys race
  and can enqueue out of event-id order, but the persistence analyzer re-sorts by id
  (`batch.rs:364`) and enforces a gapless gate (`util.rs:44`, `batch.rs:286`) before any
  WAL append, so the WAL is always written in id order. Same-key mutations are serialized
  by the stripe and get ids in mutation order; different-key `Set`/`Remove` records commute
  on replay.
- **Update that changes a unique key (remove old + insert new) is order-independent.**
  The two events land on different keys/stripes, so their ids may interleave either way;
  they commute on replay (distinct tree slots). The analyzer only requires contiguity +
  id-order application, not "remove before insert." Safe.
- **No integer truncation in the codec.** `Link` fields are all `u32`, `PageId` is a `u32`
  newtype, so every `page_id as u32` / `offset` / `length` round-trips losslessly.
  `K::WIDTH ≤ 16`. All `.try_into().unwrap()`s are guarded by preceding length checks.
- **Corrupt-but-checksum-valid files can't cause UB.** WorkTable's decoder validates node
  capacity, prefix length, recursion depth; the fork's `from_topology` *independently*
  re-validates node kinds, slot partitions, and the Node48 free-list
  (`congee-wt/src/topology.rs:175+`, typed `Err`, no panic). Good defense-in-depth against
  a cross-version file.
- **Failed checked inserts consume no event id** (the `?` precedes `next_event_id()`), so
  they cannot punch a permanent gap in the gapless stream. Covered by
  `persistent_art.rs:232`.

## Findings (ranked)

### 1. `rewrite()` compaction is not crash-atomic against the directory entry — medium
`art_index.rs:249-257`. `write_new_file` fsyncs the *temp file* contents, then
`tokio::fs::rename` swaps it in, but the **containing directory is never fsynced** after
the rename. On a crash, POSIX doesn't guarantee the rename is durable; recovery becomes
filesystem-dependent. For a checkpoint-replacement primitive, fsync the parent dir after
rename (or at minimum document it alongside the existing durability disclosures). Low
probability, high blast radius (the index file).

### 2. WAL `append` uses `flush()`, not `sync_data()` — medium (disclosed)
`art_index.rs:240`. tokio `File::flush` only drains the userspace buffer to the OS page
cache — **not** an fsync. An acknowledged mutation (past `wait_for_ops()`) can still be
lost on power loss until the next checkpoint. The PR discloses this, so it's a conscious
tradeoff — but it means the ART "WAL" is no more crash-durable than the rest of WorkTable,
which undercuts the write-ahead-log framing. Make the call-site comment explicit that this
is *not* fsync-durable.

### 3. Same-key ordering guarantee is load-bearing but enforced only by convention — low
`persistent_art.rs`. Correctness hinges on `next_event_id()` being called while the stripe
lock is held. If a refactor moves it out (it looks innocuous — a standalone atomic),
same-key events get ids out of mutation order and the analyzer applies them wrong
(last-writer-wins flips). Add an explicit `// INVARIANT: event id must be allocated under
the stripe lock` at lines 177/193/210, and ideally fold id allocation into a helper that
takes the guard by reference so the coupling is structural, not conventional.

### 4. `next_event_id` restarts at 0 each session; stored WAL `event_id`s are vestigial for recovery — low (document)
`apply_wal` (`art_index.rs:349`) replays purely by file append order and never consults the
persisted `event_id`. Recovery is safe *only because* it ignores stored ids and the
analyzer's `default()`-bypass admits the first post-load batch regardless of starting id.
The `event_id` field is dead weight for recovery and a trap for anyone building
incremental/point-in-time recovery assuming cross-session id continuity. One sentence in
the format doc: "`event_id` is for the in-flight gapless analyzer only; recovery is
positional."

### 5. `should_compact()` runs compaction inline on the persistence task — low
`art_index.rs:450/462` call `self.compact().await` synchronously when the WAL crosses
4 MiB. Compaction re-reads the whole file, rebuilds a temp ART, and rewrites — all on the
single persistence consumer, blocking every other index for that table during the stall.
The PR body's "background persistence task" oversells it: it's inline on the hot path.
Acceptable for beta; flag as a known latency spike or offload behind a spawned task with a
swap-in.

### 6. `.with_extension("wt.idx.art.tmp")` is fragile — nit
`art_index.rs:250`. `with_extension` replaces the final component's extension and produces
a fixed temp name. Works under the single-consumer model, but fragile against index names
with dots. Add a comment or a nonce.

## Nits

- `read_image` reads the entire file into memory (`read_to_end`, line 148) on every
  open/compaction. Fine today; note it as a ceiling for very large indexes.
- The `SpaceArcticIndex` / `SpaceCongeeIndex` impls duplicate ~100 near-identical lines.
  A macro (like the existing `impl_persisted_art_cdc!`) would cut duplication and prevent
  the two backends from drifting.

## Suggested next step

The compaction crash-window (#1) is the finding I'd most want to pressure-test with an
actual fault-injection test before promotion.
