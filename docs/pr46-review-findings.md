# PR #46 review findings — status & fixes

Fresh-eyes review of `fix/v1-blockers-consolidated` (the v1-blocker PR). This
document tracks each finding, whether it reproduced under test, and its fix.
Work lands on branch `fix/pr46-review-findings`.

## F1 — Mutation gate held across `.await` (was rated P1) — NOT REPRODUCED

`LockMap::mutation_guard` (`src/lock/map.rs`) is a blocking spin/yield ticket
lock. The generated `update`/`in_place`/`delete` paths keep the `MutationGuard`
inside `LockGuard` and hold it across `.await` (`update_with_guard(...).await`,
`reinsert(...).await`). The concern: two keys colliding on the same 1-of-64
stripe, guard-holder parked at its await while the other task spins.

**Result:** `tests/worktable/mutation_gate_deadlock.rs` reproduces the scenario
(colliding keys, single-worker and 2-worker runtimes, updates that await while
holding the gate) and **passes**. tokio schedules async tasks cooperatively and
the spinner falls back to `thread::yield_now()`, so the parked holder is still
polled to completion on the same thread. No livelock.

**Disposition:** finding downgraded. The test is kept as a standing guard (with
timeouts, so it can never hang the harness) to catch a future regression that
would make the hazard real (e.g. a blocking, non-cooperative holder).

## F2 — Vacuum can mark a live `page_from` empty (P1, data-loss) — NOT REPRODUCED

`src/table/vacuum/vacuum.rs`: `page_from` is `mark_page_empty`'d unconditionally
after the inner loop, with no `page_from != page_to` guard. The concern: the
destination search falls through to `allocate_new_or_pop_free()` and returns a
page that ends up holding moved-in rows, which is then reclaimed.

**Result:** `tests/worktable/vacuum_no_row_loss.rs` forces heavy cross-page
compaction (400 large rows, half deleted from many pages) with a concurrent
grace-period reader, then audits every survivor by primary key AND unique index
after vacuum quiesces. It **passes** — no row loss, no resurrection. The
grace-period deferral added in #46 (`allocate_new_or_pop_free` returns a temp
page instead of reusing an active source) appears to hold: `page_from` is not
handed back as a destination while it still holds live rows.

**Disposition:** finding not reproduced at this scale; kept as a standing audit.
A `debug_assert!(page_from != page_to)` in the loop would make the invariant
explicit and cheap to enforce — recommended as a belt-and-suspenders follow-up.

## F3 — Temp destination page mistracking (P2)

A destination from `allocate_new_or_pop_free()` may be `mark_page_full`'d while
partially empty (wasted capacity) or dropped from all tracking sets (leaked).

## F4 — Full-row unsized `update()` still always reinserts (P2, confirmed)

`codegen/src/generators/in_memory/queries/update.rs` full-row path still has
`if true { reinsert }` for unsized rows. The custom-update path's `gen_size_check`
(`need_to_reinsert = true` initializer) is the other half. The overwrite perf
blocker is only partially documented; see `tests/worktable/update_in_place_unsized.rs`.

### F4 — precise root cause of the corruption (why the one-liner isn't safe)

The generated in-place update mutates a field with
`std::mem::swap(&mut archived.inner.<field>, &mut archived_row.<field>)` inside
`with_mut_ref` (`archived` = the slot's bytes; `archived_row` = the freshly
serialized `bytes` buffer). WorkTable does **field-level** updates, so only the
changed field is swapped — the other fields are untouched.

- For inline-representable fields (`u64`, and **short** strings ≤
  `rkyv::string::repr::INLINE_CAPACITY`), the archived value is stored inline, so
  swapping the bytes is self-contained and correct.
- `ArchivedString` is a **union** (`ArchivedStringRepr`): a **long** string is
  stored **out-of-line** as a *relative pointer + length*, with the character
  bytes elsewhere in the buffer. `mem::swap` moves the relative pointer into the
  slot, but the characters it points at live in `archived_row`'s buffer, which is
  never written to the slot. The relative offset now points outside the slot →
  reads come back as raw/garbage bytes. **This** is the corruption seen in
  `update_parallel_more_strings` / `update_many_times` when the reinsert guard is
  naively flipped to `false`.

**Correct fix (not a one-liner; unsafe archived-memory work):**
`gen_size_check` should reinsert only when the new field value does **not** fit
its current slot region. When it fits (same-or-shorter serialized length,
including same-length), the in-place write for an out-of-line `String` must
overwrite the existing out-of-line byte region — e.g. via
`ArchivedStringRepr::as_bytes_seal` — instead of `mem::swap`ping the pointer.
Field-level semantics must be preserved (never overwrite the whole slot; only the
changed field's region). Because this manipulates archived memory directly, a
subtle error is silent data corruption, so it should not be rushed alongside a
release merge. `tests/worktable/update_in_place_unsized.rs` is the proof harness;
remove its `#[ignore]` once the in-place String write is correct.

## F5 — 64-stripe gate serializes unrelated hot keys (P2, efficiency)

`MUTATION_STRIPE_COUNT = 64`. Distinct hot keys colliding on a stripe are
serialized though they never touch the same row — relevant to the low-latency
claim; document the ceiling or scale it with expected concurrency.

## F6 — Wedged stripe if a guard is leaked, no timeout (P2)

`next_ticket`/`serving` are free-running with no timeout/poisoning; a leaked
`LockGuard` (mem::forget, Arc cycle) wedges 1/64 of keys forever with no
diagnostic. Low likelihood, but silent if it happens.

## Leak root cause (the ~190GB orphaned test processes) — FOUND, already fixed

The Activity Monitor screenshot showed two orphaned `mod-…` processes (WorkTable's
`tests/mod.rs` integration binary) at ~190GB each. Root cause: `vacuum_loop_test`
(`tests/worktable/vacuum.rs`) is a soak test that inserts a fresh row every 500µs
while vacuum reclaims outdated ones. If it runs unbounded (or vacuum can't keep up),
rows accumulate without limit and the harness process balloons.

Already fixed on this PR branch by commit `111f61e`
("test: bound vacuum soak to prevent orphaned harnesses"): the test is now
`#[ignore]`d and hard-bounded to a 10-second `SOAK_DURATION` with an explicit
`stop_at` deadline on both the insert loop and the vacuum-observe loop. It can no
longer run forever or orphan the harness.

Standing guard added: `tests/worktable/leak_probe.rs` asserts that 5000 same-key
updates keep physical page count bounded (reclamation keeps up) — passes, so the
reinsert-per-update path is not itself a leak.
