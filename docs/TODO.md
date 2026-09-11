# Open work

What is known to be unfinished, and enough context to act on it without the
conversation it came from. Ordered by whether it blocks a release.

Last reviewed 2026-09-07. Sections below still describe the repository as of
beta.17; `master` is now at 1.0.0-beta.19 and this file has not been swept for
what those two releases closed.

## Closed, and how

### `ps-reclaim` 0.1.1 is published and pinned

Was blocking beta.16 and did not stop it. 0.1.0 had two defects, both fixed on
`pathscale/ps-reclaim` master at `d207d06` and published as 0.1.1:

- **`Guard` was `Send`**, and its own documentation said it was not. Nothing
  enforced it: every field was `Send`, so the auto trait applied. Dropping a
  sent guard stores `NO_DOMAIN` into the *originating* thread's pin slot while
  that thread may still be reading, and decrements the wrong thread's `DEPTH`.
  Both are use-after-free windows. 0.1.1 makes it `!Send` by construction (the
  packed field is a raw pointer) with a `compile_fail` doctest holding it.
- **`Guard` was three words** (`&Domain`, `&'static Participant`, `usize`)
  against `crossbeam-epoch`'s one pointer. `partition_ref` returns a
  `PartRef { guard, &T }` per call, so it paid that size on every lookup: 16
  bytes became 32. 0.1.1 packs the entry into the participant pointer's spare
  alignment bits (`Participant` is `#[repr(align(128))]`) and is one word.

`pathscale/ps-reclaim` also had no workflows at all, which is why both defects
were found by hand from a downstream measurement rather than by a check. It now
runs build, test, doctests, fmt, clippy and Miri under strict provenance, and
publishes from master, on Ubicloud runners.

### CI has run on this branch

`.github/workflows/rust.yml` triggers on push to master and on pull requests
targeting master, so a branch with no PR is only ever checked on somebody's
laptop. PR #82 opened, all six jobs passed, and master has been green since.

### The `partition_ref` regression is measured and accepted for beta.17

The beta.17 validation grid confirmed a small microbenchmark regression, and
it is explicitly not a beta.17 release blocker. Three rotated-order passes,
using local WorkTable trees for beta.13 (`48f250f`), beta.15 (`e4dcfdf`) and
the beta.17 candidate, measured the median `partition_ref` cost as 0.69 ns,
3.09 ns and 3.35 ns respectively. Beta.17 is 8.3% slower than beta.15, and its
clean range (3.24-3.38 ns) did not overlap beta.15's (3.01-3.18 ns).

This is isolated to a nanosecond-scale primitive benchmark. Regressions of
roughly 1 ns to 3 ns can be stable without producing an application-level
regression once the primitive is composed into work several times larger. The
routed read `partition_ref_then_select` improved from 30.07 ns in beta.15 to
27.67 ns in beta.17 (-8.0%), and the unchanged `cached_handle` and `contains`
controls moved about 2%. CRUD was otherwise flat or faster. Do not describe
the bare `partition_ref` result as noise, but treat it as implementation
telemetry rather than a release gate: application-level impact was better.

The older beta.16 measurements below remain invalid and must not be used: one
set ran under severe host load, and another predates the fix for a benchmark
arm labelled `pinned_get` that was actually calling `partition_ref`.

The beta.17 grid also found and fixed a benchmark defect: insertion arms called
the now-async `insert` without awaiting the returned future. Corrected insert
results were rerun across all three local trees before drawing conclusions.

### WT DSL expansion and trailing commas are deterministic

The trailing-comma parser fix was already present on the beta.17 branch and is
covered for `config`, `delete`, `in_place`, block order, and the no-comma form.

Expansion is now deterministic too. `columns_map`, query maps, and generated
unique-type sets preserve declaration order with `IndexMap`/`IndexSet`. The
previously ignored repro is a normal release-gating test and includes multiple
columns, indexes, update queries, and delete queries. This matters beyond
cosmetics because generated enum variant order can determine discriminants.

### The beta.17 release-delta gate is green and published

The complete evidence and beta.13/beta.15/beta.17 performance grids are in
`docs/beta17-validation.md`. The exact local CI matrix passes in default,
`versioned-row-publication`, and all-feature configurations. The independent
benchmark workspace also passes its all-target test-mode gate against the
local WorkTable/WTI/DataBucket/ps-reclaim stack.

The S3 engine now has a stateful offline object-service test covering immutable
chunk upload, table-manifest restore, and an interrupted manifest commit. Configured
runtime coverage is also complete through the local-source support.cafe consumer.
Beta.17 downloaded the live Tigris dataset,
recovered three legacy tables with missing secondary entries, rebuilt them into
a rollback-safe prefix, strict-loaded all six tables, performed an S3-backed
mutation and reloaded it after restart. ACME, HTTPS and WebSocket startup also
passed on Fly. Full evidence is in `docs/beta17-validation.md`.

WorkTable PR #87 merged after all six clean-checkout CI jobs passed, and
`worktable` 1.0.0-beta.17 plus its local dependency train are available on
crates.io. The benchmark changes and summaries merged through wt-benchmarks PR
#6. Post-publication resolution passed in AgencyZero PR #205; that consumer also
rebuilt and strict-opened the complete 18-table QA profile under beta.17.

## Post-release administration

There is no unresolved beta.17 correctness or application-level performance
blocker. The release is published and its clean-checkout consumer smoke test
passes.

### Decide what happens to beta.16 on crates.io

1.0.0-beta.16 is published and resolves `ps-reclaim ^0.1.0`, so a lockfile
written before 0.1.1 landed keeps the `Send` guard. A fresh resolve now picks
0.1.1 on its own, since the requirement was always a caret and never an exact
pin. The open question is whether to yank ps-reclaim 0.1.0, which is what makes
the unsound version unreachable rather than merely unpreferred, and whether to
yank beta.16 once beta.17 supersedes it.

## Not blocking, but wrong today

### `congee-wt` no longer pulls `crossbeam-epoch`

Corrected 2026-09-07. This section said the port was outstanding and mechanical.
Both halves were wrong.

`congee-wt` dropped `crossbeam-epoch` in 0.4.4. `Cargo.toml:22` now reads
`ps-reclaim = { version = "0.1.4", default-features = false, features =
["libc", "spin"] }`, no `crossbeam_epoch` reference survives in its sources or
tests, and this repository's `Cargo.lock` already resolves `congee-wt 0.4.4`.
The two remaining `crossbeam` strings in that crate are attribution comments on
a seqlock and a backoff loop.

The call site this section worried about needed no change. It has moved to
`src/index/congee.rs:120` and still reads
`fn retire_old(pointer: usize, guard: &congee::epoch::Guard) -> Arc<V>`. The
`congee::epoch::Guard` path was deliberately preserved across the port, and the
lifetime the new guard carries elides in reference position.

The port was not the mechanical rename described here. A literal swap would have
kept one global epoch; what shipped gives each tree its own `Domain`, adds a
bounded pending-retire batch, checks guard provenance so a guard from another
tree panics rather than corrupting, and drains the tree's own domain on `Drop`.
Worth knowing because the new `Guard` is `!Send` and tree-scoped, so a guard may
not be created outside the thread and tree that uses it. Nothing in either
repository does; every threaded test builds its guard inside the spawned
closure.

### Persistence event gap: three leak sites found, instrumented, and fixed

Updated 2026-09-07. This section previously said the cause was unknown and that
the next step was instrumentation rather than a repro hunt. The instrumentation
was built, and reading for it found the leaks.

`IndexChangeEventId` is `indexset::cdc::change::Id`, allocated by
`event_id.fetch_add` in the same statement that stamps the event, so indexset
never consumes an id without emitting its event. Every leak is on our side: an
event handed back and then dropped. Three sites, all in generated persisted
query code, all on secondary streams, all confirmed by reading:

- `codegen/src/generators/persist/queries/update.rs:569`. The
  `IndexError::NotFound => Err(WorkTableError::NotFound)` arm returns with no
  acknowledge, while its sibling `AlreadyExists` arm immediately above builds an
  `Acknowledge` carrying `merged_events` and applies it. The events from
  `process_difference_insert_cdc` are dropped on the `NotFound` path. This
  asymmetry between two adjacent arms is the clearest of the three.
- `codegen/src/generators/persist/queries/update.rs:593`,
  `gen_process_diffs_remove_on_index`: `let (secondary_keys_events_remove, res)
  = ...; res?;`. On `Err` the `?` returns before
  `op.extend_secondary_key_events`, dropping the events bound on the line above.
- `codegen/src/generators/persist/queries/delete.rs:101`: the same `res?;` shape
  after `delete_row_cdc`.

Checked and NOT leaks: the rollback arms of `insert_cdc`, `insert_many_cdc` and
`reinsert_cdc` in `src/table/mod.rs` all merge forward and rollback events into
an `Acknowledge`, as does the data-delete-failure restore path. Vacuum's
`update_index_after_move` takes the non-CDC branch only when `persistence` is
`None`, so the one case commit `c0c06ba` named is closed for persisted tables.

The only structurally possible primary-stream leak is a refused
`apply_operation` after the index mutation already consumed ids, and more
generally any `?` between a CDC index mutation and its `apply_operation`.

**The failure text quoted in earlier versions of this section is stale.** It
said "attempt 9"; `GIVE_UP_AFTER_ATTEMPTS` is now 120, and
`COLLECT_WHOLE_QUEUE_AFTER_ATTEMPTS` plus its regression test
`collection_recovers_when_event_order_and_operation_order_disagree` were added
since, for a symptom that reads identically but is a collection failure rather
than a leak. Telling those two apart is exactly what the new ledger does.

`src/persistence/event_ledger.rs` records queued, collected, requeued, trimmed
and applied per stream in a bounded 8192-id window, and the guard's message now
ends in a verdict: either ASSIGNED BUT NEVER QUEUED with the id range and the
producer sites either side of the gap, or QUEUED BUT NOT APPLIED with the per-id
stage history. It says so plainly when part of the gap fell outside the retained
window, so it never claims "never queued" about an id it cannot answer for.
Always compiled, gated at run time on `debug_assertions` or `WT_EVENT_LEDGER`,
which puts it on exactly where the bug appears, since the stall needs a full
debug `--all-features` run.

**Fixed 2026-09-08.** All three sites now do what the rollback arms already did:
build an `Acknowledge` carrying the orphaned events and apply it before
propagating the error. The two `res?` sites became `if let Err(e) = res` so the
events are moved into the acknowledge and the error is returned explicitly, and
the `NotFound` arm acknowledges the events its sibling arm was already
acknowledging.

The events are **moved** into the acknowledge rather than cloned, and that is
load-bearing rather than tidy. Cloning them fails to compile: the events type is
still an inference variable at that point in the generated code, pinned only by
the `op.extend_secondary_key_events` call further down, and method resolution for
`.clone()` needs the type resolved where the call is written. The result is an
`E0282` reported against the `worktable!` invocation with no inner span, which is
an expensive thing to diagnose twice.

Covered by emitted-token assertions in both generators
(`indexed_update_write_failure_unwinds_and_acknowledges` and
`delete_data_failure_restores_indexes_and_acknowledges`), which assert the
acknowledge is emitted **before** the return or the extend rather than merely
present somewhere in the output. The write failure itself is not forcible through
the public API, so the wiring is pinned on the tokens, which is the same approach
those tests already took.

The in-memory generator has the same two `NotFound` arms
(`codegen/src/generators/in_memory/queries/update.rs:528` and `552`) and they are
correctly untouched: there is no persistence stream behind them to gap.
## Housekeeping

- `CHANGELOG.md` was backfilled to 0.3.10 on 2026-09-08. It previously stopped at
  beta.18.
- `.github/workflows/rust.yml` has no `cargo fmt --check` job, so formatting
  drift accumulates unnoticed; `scripts/ci-local.sh` does check it, which makes
  the script stricter than CI rather than equal to it.
