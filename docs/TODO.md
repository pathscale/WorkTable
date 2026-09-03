# Open work

What is known to be unfinished, and enough context to act on it without the
conversation it came from. Ordered by whether it blocks a release.

Last reviewed 2026-09-04, against `master` after beta.17 publication.

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

The placeholder ignored S3 probe still rejects its literal `test` endpoint
before I/O, but configured runtime coverage is now complete through the local-
source support.cafe consumer. Beta.17 downloaded the live Tigris dataset,
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

### `congee-wt` still pulls `crossbeam-epoch`

beta.16 removes crossbeam from WorkTable's own reclamation, not from the build.
`congee-wt` depends on it directly and re-exports its `Guard`, which
`src/index/congee.rs:101` names in a signature; `crossbeam-skiplist` also
arrives under `WorkTablesIndex` and `indexset`.

congee's use is shallow: 34 references, none of them `Atomic<>`, `Owned::` or
`Shared<>`, and most in tests. It only ever calls `pin()` and passes `Guard`
around as an opaque token, so porting it to `ps-reclaim` is mechanical. The
catch is that `Guard` is in congee-wt's public API, so it is a breaking change
there plus the call sites here.

`arctic-wt` should **not** be ported. It reclaims through `seize`, and is right
to: a trie with short reads reaches quiescence constantly, which is the exact
property that makes `seize` wrong for this crate, where `select` holds a read
guard.

### Persistence stalls on a primary index event gap, rarely

One run of `cargo test --workspace --all-targets --all-features` failed with

    persistence stalled on primary index event gap: last applied Id(1439),
    next available Id(1455) (attempt 9)

in `tests/persistence/loaded_index_growth.rs`. Not a flaky timeout: the guard
at `src/persistence/operation/batch.rs:346` is deliberate, added in `c0c06ba`,
and its comment says a gap that persists past eight deferrals means an event id
was consumed without its event being queued, which only non-CDC index mutations
do. The gap is 16 ids wide.

1 failure in 6 full runs on this branch, 0 in 3 on master, 0 in 15
persistence-only runs, so it needs whole-suite load and is not a beta.16
regression. Do not start with a repro hunt: instrument `IndexChangeEventId`
assignment against event queueing so the next occurrence names its own cause.
Evidence at `~/code/wt-event-gap-2026-09-01.txt`.

## Housekeeping

- `CHANGELOG.md` stops at 0.4.1, long before the 1.0.0-beta line.
- `.github/workflows/rust.yml` has no `cargo fmt --check` job, so formatting
  drift accumulates unnoticed; `scripts/ci-local.sh` does check it, which makes
  the script stricter than CI rather than equal to it.
