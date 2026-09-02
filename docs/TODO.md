# Open work

What is known to be unfinished, and enough context to act on it without the
conversation it came from. Ordered by whether it blocks a release.

Last reviewed 2026-09-02, against `deps/caret-not-locked`.

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

## Blocking beta.17

### Re-measure the partition regression

Unchanged from the beta.16 review, and still the reason to be careful about
what this release claims.

The claim in `82bfdf6` that `crossbeam-epoch` and `ps-reclaim` are "within
noise of each other (3.37 against 3.42)" is disputed by an interleaved A/B run:
`partition_ref` measured 3.16-3.35 ns on beta.15 and 3.60-3.68 ns here, in both
passes, with the two cleanest samples of the run showing the widest gap. The
guard size, now fixed in 0.1.1, is the likely cause, so this wants re-running
against 0.1.1 rather than re-running the old comparison.

Not yet confirmed either way. Every attempt so far ran on a machine at load 4
to 24, where the control (`partition_lookup/cached_handle`, a pure dereference
that cannot differ between versions) varied 3.6x. Re-run on a quiet box,
alternate the tree order between passes, and reject the run if the control
moves more than a few percent. Full brief, including exact commits and setup,
at `~/code/wt-beta16-perf-brief.md`.

A second set of numbers circulated during the beta.16 release, reporting
`partition_ref` at 7.78 ns on beta.15 falling to 3.31 ns flat. Do not use them.
They were taken at `8699b07`, before `673869c` showed that the benchmark arm
labelled `pinned_get` was calling `partition_ref`, and they were taken under
load. They contradict the interleaved run above by roughly a factor of two on
beta.15, and neither set has been reproduced on a quiet machine.

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

### Expansion is not deterministic

Expanding one `worktable!` declaration twice in one process produces different
code. Several generators iterate `Columns::columns_map`, a `std::collections::
HashMap`, to emit ordered constructs: the `RowFields` and `AvaiableTypes` enums
among them. `RandomState` seeds each map instance differently, so the variant
order differs between two expansions and can differ between two compilations of
the same source.

Recorded as an ignored test, `codegen/src/worktable/mod.rs`, module
`generator_determinism`. Run it with `cargo test -p worktable_codegen --
--ignored`; it fails on unmodified code.

The fix is to make `columns_map` an `IndexMap` built in declaration order,
which `field_positions` already records. A trial produced 13 mechanical compile
errors (`&Ident` not satisfying `Equivalent<Ident>`, and explicit `HashMap`
annotations). It changes the generated code of every table, so it wants
reviewing on its own.

**Open question worth answering first:** both enums derive `rkyv::Archive` and
`Serialize` with `#[repr(C)]`. If either discriminant reaches disk, this is a
persistence hazard rather than a cosmetic one.

### Trailing commas are accepted inconsistently

`parse_updates`, `parse_indexes` and `parse_queries` consume a comma after
their block; `parse_deletes`, `parse_in_place` and `parse_configs` do not. So
`config: { .. },` reaches the top-level dispatch as a `,` token and dies as
"Unexpected identifier", and the same for a `delete` block followed by another.
`config` happens to be written last everywhere in this repo, which is why
nobody has hit it.

Three `try_parse_comma()` calls, strictly more permissive. While there, make
the "Unexpected identifier" arms name the token they actually saw: a `,`
reported as an unexpected identifier is what makes this cost an afternoon.

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
