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

## F5 — 64-stripe gate serializes unrelated hot keys (P2, efficiency)

`MUTATION_STRIPE_COUNT = 64`. Distinct hot keys colliding on a stripe are
serialized though they never touch the same row — relevant to the low-latency
claim; document the ceiling or scale it with expected concurrency.

## F6 — Wedged stripe if a guard is leaked, no timeout (P2)

`next_ticket`/`serving` are free-running with no timeout/poisoning; a leaked
`LockGuard` (mem::forget, Arc cycle) wedges 1/64 of keys forever with no
diagnostic. Low likelihood, but silent if it happens.
