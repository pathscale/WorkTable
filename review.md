# Code review: PR #187 "feat: harden WorkTable beta and select index backends"

**Reviewer perspective:** senior Rust engineer, latency-critical (HFT-grade) storage engine.
**Diff reviewed:** `ff24737` (master) .. `0218d40` (`feat/index-backend-using`), 84 files, +4275 / -536.
**Verdict:** *Request changes.* The direction is right and several fixes here are genuinely good, but there are three defects that I would not merge as-is (one silent regression on the default hot path, one unbounded-growth/starvation bug in the new reclamation scheme, and one algorithmic blow-up in the ART backends), plus a CI configuration that never exercises the default build.

---

## 1. Summary of the change

Three largely independent changes are bundled into one PR:

1. **Index backend selection.** A `using <backend>` clause in the `worktable!` DSL statically selects one of four unique-index implementations (`worktables_index`, `indexset`, `congee`, `arctic`). Implemented via a new `UniqueIndex<K, V>` trait plus generic parameter substitution (`PkNodeType` -> `PkMap`). No dynamic dispatch, which is the right call.
2. **Versioned row publication** (`versioned-row-publication`, `stable-index-read-retry`). Readers get an `Arc<Row>` snapshot instead of deserializing from a concurrently mutated archived page; a counter-based grace period gates link reuse.
3. **Assorted correctness fixes.** Upsert now holds one full-row lock across existence check and mutation; persistence batch writes are coalesced by physical slot; vacuum row moves are made atomic w.r.t. publication; `pop_max` use-after-remove; index-predicate revalidation on point and range reads.

Bundling (1) and (2) makes this diff much harder to reason about than it needs to be. See §8.

---

## 2. What is good

Credit where due, because a lot of this is careful work:

- **`EmptyLinkRegistry::pop_max`** (`src/in_memory/empty_link_registry.rs:173`): copying `*max_length_link` out before `remove_link` is a real use-after-free-of-borrow fix, and the 10k-iteration regression test is the right shape.
- **Insert rollback ordering** (`src/table/mod.rs:220-222`): freeing the data link *after* unindexing rather than before closes a genuine window where a concurrent reader could resolve a stale index entry onto a reused link.
- **Vacuum row move** (`src/table/vacuum/vacuum.rs:269`, `pages.rs:740`): folding `get_raw_row` + `set_in_vacuum_process` + `save_raw_row` into a single `move_row_for_vacuum` under one page barrier is correct and removes a torn intermediate state.
- **Un-ignoring the upsert churn tests** is the single most valuable line in the diff. Two tests that were `#[ignore]`d for a known stall now run.
- **`latest_data_writes` keyed on `(page_id, offset)`** is the right key; keying on the full `Link` (which includes `length`) was a real bug for variable-length rows. The test deliberately reverses vector order, which is good discipline.
- **DSL validation** (`codegen/src/worktable/mod.rs:57-146`) is thorough: memory-only backends require an explicit `persist: false`, non-unique indexes reject non-default backends, key types are checked against each backend's capability, and `persist: maybe` is now an error instead of silently meaning `false`. The negative tests cover all of it.
- CI moves off the archived `actions-rs/*` actions and adds `permissions: contents: read`. Good hygiene.

---

## 3. Blocking findings

### B1. `insert` and `update` clone every row even when the feature is off (default-path regression)

`src/in_memory/pages.rs:314`

```rust
let general_row = <Row as StorableRow>::WrappedRow::from_inner(row.clone());
```

and `src/in_memory/pages.rs:611`:

```rust
let gen_row = <Row as StorableRow>::WrappedRow::from_inner(row.clone());
```

Neither `.clone()` is `#[cfg]`-gated. In the **default** build (`versioned-row-publication` off) the cloned `row` is never used again, so every `insert` and every `update` now pays a full deep `Row` clone that is then dropped. For a row with `String`/`Vec` fields that is a malloc + memcpy + free per operation, on the hottest write path in the engine. `clippy::redundant_clone` is nursery-only, so `-D warnings` did not catch it, and CI runs `--all-features` so the default path is never benchmarked (see B4).

This also means the "ordinary update: -1.063% median latency delta" number in the PR description was measured on a build that has this clone. The real default-path delta is unknown.

**Fix:**

```rust
#[cfg(feature = "versioned-row-publication")]
let general_row = <Row as StorableRow>::WrappedRow::from_inner(row.clone());
#[cfg(not(feature = "versioned-row-publication"))]
let general_row = <Row as StorableRow>::WrappedRow::from_inner(row);
```

and drop the now-unneeded `Row: Clone` bound from the `not(feature)` configuration. In the versioned build, `insert` currently clones **twice** (once at line 314, once at `stage_published_row(link, row.clone())` on line 331/357). The empty-link fast path returns immediately, so that one can be a move; only the retry loop needs the clone.

While you are there: `insert_cdc` (`pages.rs:388`) calls `self.insert(row.clone())` and then re-serializes the same row with `rkyv::to_bytes`. That is a second full serialization of bytes `save_row` already produced. Having `save_row` hand back the serialized buffer would remove one serialize + one clone from every persisted insert. Out of scope for this PR, but it is on the same path.

---

### B2. The read grace period is not a grace period; long-lived iterators stall reclamation forever

`src/in_memory/pages.rs:226`

```rust
fn reclaim_retired(&self) {
    if self.active_readers.load(Ordering::SeqCst) != 0 {
        return;
    }
    let mut retired_links = self.retired_links.lock();
    ...
    if self.active_readers.load(Ordering::SeqCst) != 0 {
        return;
    }
    ...
}
```

Two problems, the second is the serious one.

**(a) Check-then-act, not quiescence.** Nothing prevents a reader from incrementing `active_readers` between the second load and the `published_rows.remove` / `empty_links.push` below it. The counter is a *snapshot*, not an epoch. `SeqCst` does not help: no ordering makes "I observed zero a moment ago" imply "no reader can now hold a link I am about to free." The `read_grace_period_prevents_link_aba` test passes because it is single-threaded and deterministic; it does not exercise the window.

**(b) A single live reader blocks all reclamation, permanently.** This one is structural, not a race. Generated `select_all` / `select_by_pk_range` / `select_by_*_range` capture the `ReadGuard` **into the returned iterator**:

`codegen/src/generators/in_memory/queries/select.rs:32` and `.../table/impls.rs:118`:

```rust
let read_guard = self.0.data.read_guard();
let iter = ... .filter_map(move |link| {
    let _read_guard = &read_guard;
    self.0.data.select_non_ghosted(link.0).ok()
});
SelectQueryBuilder::new(iter)
```

The guard lives as long as the `SelectQueryBuilder`. Any caller that builds a query and holds it (a streaming cursor, a builder stored in a struct, a `.filter(...)` chain awaited across a suspension point) pins `active_readers > 0`. While that is true:

- `retired_links`, `retired_pages` and `retired_publications` grow without bound;
- `empty_links` is never replenished, so `insert` stops reusing freed slots and allocates fresh pages instead;
- the publication map never shrinks, so every deleted row's `Arc<Row>` stays resident.

In a long-lived HFT process with any concurrent scan activity, this is a monotonic memory leak plus free-list starvation. There is no bound, no fallback, no metric, and no log.

**Fix:** use a real QSBR/epoch scheme. `crossbeam-epoch` is already in the dependency graph via `congee`, and `congee::epoch::Guard` is being used directly in `src/index/congee.rs` already. Replace the counter with per-reader epoch pinning and `guard.defer(...)` for retirement; reclamation then happens when all *pinned epochs* have advanced past the retirement epoch, which is both correct and does not stall on a single long reader. If you want to keep the counter for now, at minimum:

1. Bound the retirement queues and emit a `tracing::warn!` when they exceed a threshold, so the failure is observable rather than silent.
2. Add a multi-threaded test that spawns a reader thread which holds a `read_guard` across a `delete` + `insert` and asserts the link was not reused, then holds it for a bounded period and asserts the queues do not grow past a limit.

Also note that `let _read_guard = &read_guard;` inside those closures is load-bearing (edition 2021+ disjoint closure capture means a `move` closure only captures fields it uses; without that line the guard would not be captured and would drop immediately). That is invisible to a future reader doing an "unused variable" cleanup. Add a comment, or capture explicitly with a named struct field.

---

### B3. `iter_values` on the ART backends is O(n) with a full materialization, and generated iteration calls it per row

`src/index/arctic.rs:134`:

```rust
fn iter_values(&self) -> impl DoubleEndedIterator<Item = (K, V)> + '_ {
    let shard = self.inner.all();
    shard.entries(Order::Ascend).map(...).collect::<Vec<_>>().into_iter()
}

fn range_values<'a, R>(&'a self, range: R) -> ... {
    self.iter_values().filter(move |(key, _)| range.contains(key))   // full scan
}
```

`src/index/congee.rs:192` is worse:

```rust
fn iter_values(&self) -> ... {
    let mut values = self.inner.keys().into_iter()      // materialize ALL keys
        .filter_map(|key| { let key = K::from_congee(key); self.get_value(&key).map(...) })  // one tree lookup per key
        .collect::<Vec<_>>();
    values.sort_unstable_by_key(|entry| entry.0);        // sort the whole index
    values.into_iter()
}
```

So a `range_values(k..=k)` on a Congee-backed index is: dump every key, perform *n* point lookups, sort *n* entries, allocate two `Vec`s, then filter down to one row. That is O(n log n) with two heap allocations for a single-key range query.

Now compound it with `codegen/src/generators/in_memory/table/impls.rs:257-270` (`gen_table_iter_inner`), which is used by the generated table iteration helpers:

```rust
let first = self.0.primary_index.pk_map.iter_values().next()...;   // full materialization to take 1
while !ind {
    let mut iter = self.0.primary_index.pk_map.range_values(k.clone()..);   // full materialization, PER ROW
    ...
}
```

On an ART-backed primary key this is **O(n² log n) with an O(n) allocation per step**. A 100k-row table would do 100k full-index dumps and sorts. On WorkTablesIndex/IndexSet this is fine (lazy `range`), which is exactly why it will not show up in the existing benchmark suite: the new `select_by_unique_index_range` bench added in `benches/cases/unique_index.rs:68` runs on the default backend.

**Fix, pick one:**

- *Preferred:* make ordered scans a separate capability. Split `UniqueIndex` into `UniqueIndex` (point ops) and `OrderedUniqueIndex: UniqueIndex` (`iter_values`, `range_values`, ...). Congee and Arctic implement only the former. Then the DSL rejects `using congee` on any index whose generated API includes `select_by_*_range` / `select_all` at compile time, with a clear message. This is the honest contract: these are point-lookup ARTs.
- *Minimum acceptable:* keep the current shape but (i) implement `range_values` natively (Arctic's `entries(Order::Ascend)` can be seeked; Congee 0.4 has `range`), (ii) rewrite `gen_table_iter_inner` to hold one iterator instead of re-ranging per row, and (iii) document the complexity in `docs/index-backend-dsl-proposal.md` and in the rustdoc on `ArcticIndex`/`CongeeIndex`.

Either way, add an ART-backed variant to the range benchmark so a regression is caught.

---

### B4. CI never builds or tests the default configuration

`.github/workflows/rust.yml:26-30`

```yaml
- run: cargo build --workspace --all-targets --all-features --verbose
- run: cargo test  --workspace --all-targets --all-features --verbose
```

`--all-features` turns on `versioned-row-publication` **and** `stable-index-read-retry`. Every `#[cfg(not(feature = "versioned-row-publication"))]` branch introduced by this PR is therefore compiled but never executed in CI. That includes:

- the entire non-versioned `select` path and `select_after_primary_index_miss` (`src/table/mod.rs:174-190`);
- `delete`'s `self.empty_links.push(link)` branch (`pages.rs:637`);
- `mark_page_empty`'s non-versioned branch (`pages.rs:667`);
- every generated non-versioned `select_by_*` (`codegen/.../index_fns.rs:112`).

That is the code path that actually ships to users by default.

`--all-features` also enables all four mutually exclusive `wti-*-search` features at once, which the README explicitly says must not happen ("Enable exactly one `wti-*-search` feature"). Whatever `indexset` does with `custom-binary-search` + `std-binary-search` + `superslice-binary-search` + `wt-slice-binary-search` all set, it is not what any user will run.

**Fix:** run at least three matrix legs: default features, `--features versioned-row-publication`, `--features stable-index-read-retry`. Drop `--all-features` or make the `wti-*` features a single `wti-search = "..."` cfg value. Also add a `compile_error!` guard in `src/lib.rs` for more than one `wti-*-search` being active, since Cargo feature unification will otherwise produce this silently in a user's dependency graph.

The PR body says the exact CI command is `cargo test --locked ...`; the workflow has no `--locked`. Worth aligning.

---

## 4. Correctness and concurrency

### C1. `PublishedRow::load` can return a torn (row, flags) pair

`src/in_memory/publication.rs:33-45`

```rust
pub(super) fn replace(&self, row: Row, flags: u8) {
    *self.row.write() = Arc::new(row);          // 1. row first
    self.flags.store(flags, Ordering::Release); // 2. flags second
}

pub(super) fn load(&self) -> (Arc<Row>, u8) {
    let flags = self.flags.load(Ordering::Acquire);  // 1. flags first
    let row = self.row.read().clone();               // 2. row second
    (row, flags)
}
```

The writer publishes row-then-flags; the reader observes flags-then-row. A reader interleaving between (1) and (2) of `replace` observes the **old flags with the new row**. Concretely, for a delete: the reader reads `flags == 0` (not yet DELETED), then reads the row after the writer has stored the tombstone version. `select_non_ghosted` (`pages.rs:490`) then returns a row that has been deleted.

This is exactly the class of bug the whole feature exists to prevent, and it survives because the row and the flags are two independent synchronization objects.

**Fix:** make the version a single atomically swapped unit.

```rust
struct Version<Row> { row: Row, flags: u8 }

pub(super) struct PublishedRow<Row> {
    version: arc_swap::ArcSwap<Version<Row>>,
}
```

`ArcSwap::load` is a single acquire load plus a debt-slot bump, no `RwLock` at all. That fixes the tearing **and** removes the per-row reader-writer lock from the read path, which is a straight latency win. If you would rather not take an `arc-swap` dependency, put `flags` inside the `RwLock` alongside the `Arc<Row>` and return both from one `read()`.

### C2. Global `RwLock<HashMap>` on every published read and write

`src/in_memory/pages.rs:113` (`published_rows: RwLock<PublicationMap<...>>`), read in `published_slot` (`:184`), written in `publish_wrapped_row` (`:163`) and `reclaim_retired` (`:226`).

In the versioned build, **every** row read takes `published_rows.read()` and **every** row write takes `published_rows.write()`. That is one shared cacheline that every core on the box bounces on for every single operation. `parking_lot`'s read lock is a CAS on a shared word; at HFT rates on a 32-core machine this is the dominant cost and it does not scale, it *anti*-scales.

Worse, `reclaim_retired` takes `published_rows.write()` and it is called on **every** `insert` (`pages.rs:317`), every `delete` (`:634`), every `allocate_new_or_pop_free` (`:432`) and every `retire_published_link` (`:787`). So the common case (no readers active) is: three `Mutex` acquisitions plus one map write lock per insert, before any actual work.

**Fix:**
- Shard the publication map: `[RwLock<PublicationMap>; 64]`, indexed by `absolute_index >> k & 63`. This is a 20-line change and removes the single contention point.
- Do not call `reclaim_retired` on every mutation. Amortize: only attempt reclamation when a retirement queue crosses a threshold (`retired_links.len() >= 256`), tracked with a single relaxed `AtomicUsize` so the common path is one relaxed load and a predicted-not-taken branch.

### C3. `PublicationHasher` is an identity hash that collapses hashbrown's control bytes

`src/in_memory/pages.rs:49-72`

```rust
fn write_u64(&mut self, value: u64) { self.0 = value; }
```

`OffsetEqLink::hash` (`src/util/offset_eq_link.rs:26`) hashes `absolute_index()`, a `u64`, so `write_u64` is the only path taken. hashbrown derives the bucket index from the **low** bits of the hash (fine here, offsets are well distributed) but the 7-bit SIMD control tag from the **high** bits: `h2 = hash >> 57`. `absolute_index()` is `page_id * DATA_LENGTH + offset`, which for any realistic table is far below 2^57, so **every entry gets control tag 0**. Every 16-lane group probe reports a full match and falls through to 16 real key comparisons. You have turned an O(1) lookup with ~1 comparison into an O(1) lookup with ~16, on the hottest read path in the feature.

**Fix:** Fibonacci-mix so the entropy reaches the high bits. Still one multiply, ~3 cycles:

```rust
fn write_u64(&mut self, value: u64) {
    self.0 = value.wrapping_mul(0x9E37_79B9_7F4A_7C15);
}
```

Separately, `write(&mut self, bytes: &[u8])` (`:57`) **assigns** rather than accumulating (`self.0 = hash`, not seeded from `self.0`). If anything ever hashes two fields through this hasher, only the last one contributes. Either seed from `self.0` or `unreachable!()`/`debug_assert!` it, since it is not meant to be reached.

### C4. `select`'s replacement-retry loop is unbounded

`src/table/mod.rs:145-166` and the generated equivalent in `codegen/.../index_fns.rs:86-105`:

```rust
loop {
    let Some(link) = ...get_value(&pk) else { ...; break None };
    if let Ok(row) = self.data.select_non_ghosted(link) { break Some(row); }
    let current_link = ...get_value(&pk);
    if current_link == Some(link) { ...; break None; }
    // else: loop again, forever, with no spin hint and no bound
}
```

When the mapping keeps changing (sustained same-key churn, exactly the workload `upsert_completes_under_extreme_same_key_churn` drives), this spins with no iteration cap, no backoff, and no `spin_loop()` hint on the retry edge. `docs/versioned-row-publication.md:75` acknowledges this ("Point-read replacement retry may starve under perpetual replacement churn") but a read path that can spin indefinitely inside a latency-critical system needs a bound, not a doc note. Cap it (say 64 iterations with exponential `spin_loop` backoff), then fall through to the definitive path or return `None`.

Also, `retry_stable_miss` is a single-shot `bool` consumed with `std::mem::take`, so "bounded stable-miss confirmation" means exactly **one** extra probe separated by one `spin_loop()`. That is a race-narrowing heuristic, not a fix. The PR body is honest that 2 of 120 cells still failed with retry off, but the framing in the README ("need an acknowledged concurrent insert to be immediately visible") oversells what one retry buys. Either state the residual failure probability, or address the underlying `WorkTablesIndex` transient-miss behaviour upstream.

### C5. Trait/inherent method resolution collision in the CDC impls

`src/index/table_index/cdc.rs:82,87,92`

```rust
impl<T, Node, const N: usize> TableIndexCdc<T> for UpstreamIndexMap<T, OffsetEqLink<N>, Node> {
    fn insert_cdc(&self, value: T, link: Link) -> ... {
        let (res, events) = self.insert_cdc(value, OffsetEqLink(link));   // <- which one?
```

This compiles only because Rust prefers the *inherent* `VanillaIndexMap::insert_cdc` over the trait method of the same name. If upstream `indexset` ever renames or removes that inherent method, or moves it behind a trait, this silently becomes **infinite recursion and a stack overflow** rather than a compile error. Same for `remove_cdc` (`:92`) and `checked_insert_cdc` (`:87`).

**Fix:** disambiguate explicitly.

```rust
let (res, events) = VanillaIndexMap::insert_cdc(self, value, OffsetEqLink(link));
```

### C6. `latest_data_writes` orders physical writes by UUID

`src/persistence/operation/batch.rs:89`

```rust
if operation_id > entry.get().0 { entry.insert(...); }
```

`OperationId: Ord` compares the inner `Uuid` byte-wise (`src/persistence/operation/mod.rs:59`), and `OperationId::default()` is `Uuid::now_v7()`. v7 is time-ordered only at millisecond granularity; the remaining 74 bits are random (the `uuid` crate does not use a monotonic counter by default). Two operations on the same physical slot within the same millisecond therefore order **randomly**, and `latest_data_writes` can persist the older bytes.

At HFT rates, thousands of operations per millisecond on a hot slot is the normal case, not the edge case. This is not a regression (the old `order_on(OperationId, Desc)` had the same flaw), but the whole point of this function is "the newest operation must be the only write for a physical slot", and it does not currently establish "newest".

**Fix:** stamp each `Operation` with a monotonic `AtomicU64` sequence at creation and order on that. If `self.ops` is already in creation order, the simpler fix is to iterate and unconditionally overwrite, and drop the comparison; but the test at `:127` deliberately reverses the vector, which suggests `ops` order is not trusted. Clarify which it is, because those two answers imply different fixes.

**Also, allocation:** `bytes.to_vec()` is called for **every** operation (`:90` and `:94`), including the N-1 that will be superseded. For a batch with many updates to the same slot that is N-1 wasted heap allocations plus memcpys. Two passes (find the winning index per slot, then `to_vec` only the winners) removes them.

### C7. Cold-start hydration blocks all writers

`src/in_memory/pages.rs:189-212`, `published_slot_or_hydrate` takes `page_access.read()` for the entire hydrate (page lookup + `get_row` deserialize + map write). After a reload of a persisted table, the *first* read of every row takes that path, so every cold read blocks every writer for a full rkyv deserialization. Worth a note in `docs/versioned-row-publication.md`, and worth considering a warm-up pass on load.

### C8. Undocumented lock hierarchy

There are now six locks in `DataPages` (`published_rows`, `page_access`, `pages`, `empty_pages`, three retirement `Mutex`es) plus `EmptyLinkRegistry`'s internals. The current code is consistent (`page_access` -> `pages` -> `published_rows`; `reclaim_retired` holds `published_rows` while calling `empty_links.push`), but nothing states the order, and `reclaim_retired` is re-entrant-hostile: it relies on every caller using the statement-temporary form

```rust
self.retired_links.lock().push(link);   // guard dropped at the `;`
self.reclaim_retired();                 // re-locks retired_links
```

Hoisting that to `let g = self.retired_links.lock();` deadlocks (`parking_lot::Mutex` is not reentrant). Add a module-level doc comment stating the lock order and this constraint, and consider having `reclaim_retired` take the already-held guards.

---

## 5. Hot-path cost accounting

Per-operation deltas introduced by this PR, default build (`versioned-row-publication` **off**):

| Path | Added cost | Ref |
|---|---|---|
| `insert` | 1 full `Row` deep clone (**pure waste**) | B1 |
| `update` | 1 full `Row` deep clone (**pure waste**) | B1 |
| `select` hit | unchanged (`lookup_for_select` forwards) | `table/mod.rs:174` |
| `select` **miss** | **2x** index lookups (`lookup_for_select` then `confirm_lookup_for_select`) | `table/mod.rs:183` |
| `select_by_<unique>` | 1 field comparison (index predicate revalidation) | `index_fns.rs:112` |
| `upsert` (existing key) | 2 `contains_key` probes + the `get_value` inside `update_with_guard` = 3 index lookups | `impls.rs:169-185` |

The `select` miss cost deserves attention: probing for an absent key is an extremely common HFT pattern ("do I already have this order id?"). Doubling it by default, to close a race that only matters in a mode that is off by default, is the wrong trade. Gate `select_after_primary_index_miss` behind `stable-index-read-retry` so the default build keeps a single probe.

On `upsert`: the outer `contains_key` (`impls.rs:169`) and the inner `contains_key` (`impls.rs:184`) are both redundant on the existing-key path, since `update_with_guard` re-resolves the link anyway and already returns `NotFound`. Dropping straight into `update_with_guard` and treating `NotFound` as "fall through to insert" removes two index probes from the hottest upsert case. That likely explains most of the reported `+0.505%`.

Versioned build, additionally per read: one atomic RMW (guard) + one global `RwLock` read (publication map) + one hash lookup + one per-row `RwLock` read + one `Arc` clone + one full `Row` clone. Per write: one global `RwLock` write + up to three `Mutex` acquisitions from `reclaim_retired`. C1/C2/C3 address the avoidable parts.

Micro-notes:

- `ReadGuard` uses `SeqCst` for `fetch_add`/`fetch_sub` (`pages.rs:214`, `:57`). On x86 the RMW is a full barrier anyway, but on aarch64 `SeqCst` costs a `dmb ish` that `Acquire`/`Release` does not. Since the algorithm needs replacing (B2) this is moot, but do not carry `SeqCst` into the replacement by default.
- `UniqueIndex::with_value`'s default body (`src/index/unique.rs:41`) is `self.get_value(key).as_ref().map(read)`, which clones the value and then hands out a reference to the clone. That defeats the entire point of `with_value`. Every backend overrides it today, so it is latent, but the default should be `unimplemented`-by-required or at least documented as "clones unless overridden."
- `ArcticIndex::len` / `CongeeIndex::len` are `Relaxed` side counters. Fine for reporting, but `is_empty()` derives from them and is used in assertions; under concurrent insert/remove those counters are approximate. Document it.

---

## 6. Codegen

### D1. Backend detection by substring matching on token streams

`codegen/src/persist_index/generator.rs:286`:

```rust
let uses_upstream = field.ty.to_token_stream().to_string().contains("UpstreamIndexMap");
```

`codegen/src/persist_index/generator.rs:361`:

```rust
let is_unique = !index_type.contains("IndexMultiMap");
let uses_upstream = index_type.contains("UpstreamIndexMap");
```

This drives **which on-disk format is written**. A user type alias, a fully-qualified path, or a generic parameter whose name happens to contain the substring silently selects the wrong serialization. The `is_unique` case is pre-existing; the `uses_upstream` case is new and is the more dangerous one because a misclassification here corrupts persisted index pages rather than failing to compile.

You already have the answer as structured data: `Columns::primary_index_backend` and `Index::backend`. Thread `IndexBackend` through to the persist generators instead of round-tripping through a string. Same for `Parser::primary_key_uses_upstream` (`codegen/src/persist_table/parser.rs`).

### D2. `single_supported_field` compares stringified types

`codegen/src/generators/index_backend.rs:118`:

```rust
if !supported.contains(&field.to_string().as_str()) { ... }
```

`u64` matches; `std::primitive::u64` or a `type Id = u64;` alias does not. That produces a confusing error on legitimate code. Consider normalizing via `syn::Type` parsing and comparing the last path segment, and say so in the error message.

### D3. `error.into_compile_error()` in type position

`codegen/src/generators/in_memory/table/mod.rs:82`:

```rust
let node_type = unique_index_type(...).unwrap_or_else(|error| error.into_compile_error());
```

`compile_error!{...}` expanded where a type is expected produces the intended diagnostic plus a second, confusing "expected type" error. Every other call site propagates with `?`; this one should too (the enclosing function would need to return `syn::Result`, which is a small refactor).

### D4. `cfg!` in the proc-macro crate couples generated code to host-crate features

`codegen/.../index_fns.rs:85`, `:141`, and others use `cfg!(feature = "versioned-row-publication")` **inside the macro**, so the generated code shape is fixed by how `worktable_codegen` was compiled. It works because `worktable`'s feature forwards to `worktable_codegen`'s, but it means:

- Cargo feature unification is now load-bearing for *code generation*, not just behaviour. Any crate anywhere in the graph enabling `worktable/versioned-row-publication` silently changes the generated read path (and its latency profile) for every other consumer.
- A user cannot inspect the generated code's shape from their own `Cargo.toml` alone.

The alternative is to emit **both** branches gated by `#[cfg(feature = ...)]` referring to a cfg the `worktable` crate sets via `build.rs` (`cargo::rustc-check-cfg` + `cargo::rustc-cfg=worktable_versioned_publication`), so the decision is made at the consumer's compile time and is visible in the expansion. Not blocking, but worth a design note in `docs/versioned-row-publication.md`.

### D5. Upsert rewrite

`codegen/src/generators/in_memory/table/impls.rs:159-206`. The lock-across-decision change is correct and is the right fix for #169-class races. Notes:

- The `loop` still has no bound. The old doc comment explained at length *why* there is no limit; the new comment drops that rationale while keeping the unbounded loop. Keep the rationale.
- `row.clone()` occurs up to three times per iteration. For a wide row under churn that is measurable. Hoisting the clone (or taking `row` by `Arc`) would help.
- The `Err(NotFound)` retry arm should now be unreachable, since the guard is held across check and mutation. If a concurrent raw `delete()` can still produce it, that is worth an explicit comment; if not, consider `debug_assert!`ing it so a future regression surfaces in tests instead of silently spinning.

### D6. `#[cfg]` on `let` statements

`src/in_memory/pages.rs:667-670`:

```rust
#[cfg(not(feature = "versioned-row-publication"))]
let mut g = self.empty_pages.write();
#[cfg(not(feature = "versioned-row-publication"))]
g.push_back(page_id);
```

Legal but easy to break (add a third statement, forget the attribute, get a cryptic error in one configuration only). Wrap the whole thing in a single `#[cfg]` block instead.

---

## 7. API, packaging and dependencies

### E1. `arctic-map` and `congee` are mandatory dependencies

`Cargo.toml:86-87`:

```toml
arctic-map = "=0.1.4"
congee = "=0.4.1"
```

and `src/index/mod.rs:1,3` declares `mod arctic; mod congee;` unconditionally. Every WorkTable user, including the overwhelming majority who will never write `using congee`, now compiles and links two adaptive radix trees. Combined with `vanilla_indexset` (also unconditional), the crate now carries **four** index implementations plus two copies of the indexset codebase. That is compile time, binary size, and instruction-cache pressure imposed on everyone for an opt-in feature.

**Fix:**

```toml
arctic-map = { version = "=0.1.4", optional = true }
congee     = { version = "=0.4.1", optional = true }
vanilla_indexset = { package = "indexset", version = "=0.15.0", optional = true, features = [...] }

[features]
backend-arctic   = ["dep:arctic-map"]
backend-congee   = ["dep:congee"]
backend-indexset = ["dep:vanilla_indexset"]
```

with `#[cfg(feature = "backend-arctic")] mod arctic;` and a codegen error telling the user to enable `worktable/backend-congee` when they write `using congee` without it. The DSL already knows which backend was requested, so the error message can be precise.

### E2. Mutually exclusive features have no guard

`Cargo.toml:75-78` defines four `wti-*-search` features and the README says to enable exactly one, but nothing enforces it. Cargo features are additive and unify across the graph, so this *will* be violated in practice. Add to `src/lib.rs`:

```rust
#[cfg(any(
    all(feature = "wti-predictable-search", feature = "wti-std-search"),
    all(feature = "wti-predictable-search", feature = "wti-hybrid-search"),
    // ... all pairs
))]
compile_error!("enable exactly one `wti-*-search` feature");
```

Better still, express it as one feature with a value, or as a `build.rs` cfg, so the combinatorics do not have to be spelled out.

### E3. `versioned-row-publication` is not additive-safe

Enabling it changes `DataPages`'s layout, the read path, and per-operation latency. Because features unify, a transitive dependency enabling it silently changes the behaviour and performance of the top-level application. That is inherent to Cargo, but it should be called out loudly in the README and in `docs/versioned-row-publication.md`, right next to the "off by default" claim, because "off by default" is not the same as "off unless *you* turn it on."

### E4. Public surface

`src/lib.rs:46-49` adds `ArcticIndex`, `ArcticKey`, `CongeeIndex`, `CongeeKey`, `UniqueIndex`, `UpstreamIndexMap`, `UpstreamIndexPair` to the prelude. For a `1.0.0-beta.1` that is a semver commitment to four backend types and their key traits. `UpstreamIndexPair` in particular is a type alias for a third-party crate's `Pair`, which pins you to `indexset` 0.15's public types forever. Consider exporting these from a `worktable::index_backends` module rather than the prelude, and marking the ART adapters `#[doc(hidden)]` or `#[non_exhaustive]`-adjacent until they have production mileage. `docs/index-backend-dsl-proposal.md:3` still says "Status: Experimental implementation in draft PR #187," which is at odds with putting them in the 1.0 prelude.

### E5. Missing `# Safety` docs

`pages.rs:740` `pub(crate) unsafe fn move_row_for_vacuum` has a descriptive doc comment but no `# Safety` section stating the caller's obligations (link validity, destination capacity, no concurrent `with_mut_ref` on the same link). `clippy::missing_safety_doc` does not fire on `pub(crate)`, which is exactly why it is worth writing by hand.

---

## 8. Tests

Good additions: `index_backends.rs` (326 lines covering CRUD, ranges, rollback and the WTI <-> IndexSet reload switch), the predicate-revalidation tests in `float.rs` / `index/insert.rs` / `index/range.rs`, the `latest_data_writes` unit test, and the `pop_max` regression test.

Gaps:

1. **No test covers B2's structural leak.** Add: hold a `select_all()` builder without executing it, perform N deletes and inserts, assert the retirement queues (or `get_empty_links().len()`) do not grow without bound. It should fail today.
2. **No concurrent test for C1's torn read.** A writer thread looping `delete`/`insert` on one key while a reader loops `select_non_ghosted` will surface it, especially on aarch64.
3. **No ART-backed range/iteration benchmark.** B3 is invisible to the current suite.
4. **Leftover debugging instrumentation.** `tests/persistence/sync/many_strings.rs:133-140` replaces `assert!(table.select(pk).is_some())` with a `panic!` that formats **every entry in the primary index** into the message. On a failing 512-row test that is noisy; on a larger table it is unusable. Revert to a concise assertion.
5. `read_grace_period_prevents_link_aba` (`pages.rs:1077`) is single-threaded, so it validates the happy path of the counter but not the race it exists to prevent. Worth renaming to reflect what it actually checks.

---

## 9. Smaller items

- `src/in_memory/pages.rs:328` and `:395`: the empty-link reuse path does not `row_count.fetch_add(1, ...)` while the fresh-page path does. Pre-existing (present on master), but `row_count` feeds `system_info` and vacuum heuristics, so it drifts down over any delete/insert cycle. Worth a follow-up issue.
- `src/index/congee.rs:153` `insert_value_checked` relies on `compute_or_insert` returning `Ok(Some(existing))` when the closure returns the old pointer and `Ok(None)` when it inserts. That is a subtle contract from a `0.4.x` dependency pinned with `=`. Add a comment citing the congee API guarantee, and keep the pin.
- `src/index/arctic.rs:73`: `updated.old().cloned()` yields `Option<Box<V>>` unless arctic's `old()` derefs, while `remove_value` (`:96`) uses `(*old).clone()`. The inconsistency suggests one of the two is relying on an auto-deref that may change across arctic versions. Make both explicit.
- `src/index/unique.rs:35` `#[cold] #[inline(never)]` on `confirm_lookup_for_select`'s trait default: correct intent, but note that `#[cold]` on a default trait method does not propagate to overriding impls automatically. `IndexMap`'s override (`:53`) repeats it, which is right; the ART backends do not override it at all, so they get the (cold) default calling their hot `get_value`. Harmless, but check it is what you meant.
- `src/mem_stat/mod.rs:104,118`: `CongeeIndex`/`ArcticIndex` `heap_size()` returns `len() * size_of::<(K, V)>()`, which ignores ART node overhead entirely (typically 2-5x the payload) and, for Congee, ignores the per-value `Arc` allocation. That will materially under-report memory in `system_info`. Either estimate with a documented multiplier or return `0` with a doc comment saying it is unknown, rather than a number that looks authoritative and is not.
- `codegen/src/generators/in_memory/mod.rs:71`: `let _ = parser.parse_persist()?;` reads oddly (the `?` already discards). `parser.parse_persist()?;` is enough.
- `README.md:150`: "Congee and Arctic are explicitly memory-only and require `persist: false`" is accurate, but the table row above calls this "Per-index physical selection" while the doc is titled a "proposal" with status "Experimental". Align the vocabulary before 1.0.

---

## 10. Merge checklist

Blocking:

- [ ] **B1** cfg-gate the `row.clone()` in `insert`/`update`; re-run the update and insert benchmarks on the *default* build.
- [ ] **B2** replace the counter-based grace period with epoch/QSBR reclamation, or at minimum bound the retirement queues and add the failing regression test.
- [ ] **B3** native or rejected ordered scans for Congee/Arctic; fix `gen_table_iter_inner`'s per-row re-range.
- [ ] **B4** CI matrix over default / `versioned-row-publication` / `stable-index-read-retry`; stop using `--all-features`; add the `wti-*-search` `compile_error!`.
- [ ] **C1** single atomic (row, flags) version.
- [ ] **C5** disambiguate the inherent-vs-trait `*_cdc` calls.
- [ ] **C6** decide and document how "newest write per slot" is established; UUID ordering is not it.

Strongly recommended before 1.0:

- [ ] **C2** shard the publication map; amortize `reclaim_retired`.
- [ ] **C3** fibonacci-mix `PublicationHasher::write_u64`.
- [ ] **C4** bound the point-read replacement retry.
- [ ] **D1** thread `IndexBackend` into the persist generators instead of substring matching.
- [ ] **E1** make `arctic-map`, `congee` and `vanilla_indexset` optional.
- [ ] **E4** move backend types out of the prelude, or drop the "experimental" framing.
- [ ] §8.4 revert the debugging `panic!` in `many_strings.rs`.

Finally: please split this. The `using` backend work and the versioned-publication work are independent, and each is a large enough change that reviewing them together makes it easy for exactly the kind of defect in B1 and C1 to slip through. Landing the backend selection first (it is closer to ready) and the publication mode second would also let you get a clean before/after benchmark for each, which the current numbers cannot give you.
