# Recovered branches

On 2026-09-02 a sweep found work in this repository and its siblings that
existed **only on one machine**: local branches never pushed, and in one case
files that were not in git at all. Everything below is now on the remote.

This file exists so the sweep does not have to happen twice. If you abandon a
branch, either delete it or add a line here; a branch that is neither pushed
nor recorded is one `rm -rf` from gone.

## WorkTable

| branch | head | status |
|---|---|---|
| `feat/partition-by` | `f0e05e6` | Already an ancestor of `master`. Nothing lost; an earlier note calling it unpushed was stale. |
| `backup/partition-by-local` | `2e3273f` | **Not in master, 4 commits.** An older `partition_by` variant plus a commit titled "save uncommitted local work before the machine is wiped". Superseded in the main line, but read it before deleting. |
| `feat/ps-reclaim-beta16` | `cca00d0` | The beta.16 release candidate. Superseded by beta.17 (#87). |
| `fix/codegen-clippy-and-ci-parity` | `40b2815` | Head is at beta.12. Long superseded. |

## WorkTablesIndex

| branch | head | status |
|---|---|---|
| `feat/pointer-free-topology-snapshots` | `275aacd` | Never pushed. 152 lines in the concurrent map and set. Now WorkTablesIndex#15. Needs a decision against #14, which touches the same structures. |
| `backup/full-upstream-sync-20260803` | `99ca45a` | Never pushed. Seven upstream `indexset` commits: node split fix, `MultiPair` customisation, flaky test fixes. |

## wt-benchmarks

| branch | status |
|---|---|
| `bench/agentcode-codegraph` | Never pushed. The AgentCode storage profile, 788 lines. Now wt-benchmarks#7. |
| `rescue/uncommitted-20260902` | A snapshot of work that was **not in git at all**: the MoE-PGO profile (598 lines, untracked and in no stash), `scripts/compare-worktable-versions.sh`, a second AgentCode benchmark, and 617 lines of uncommitted edits. Now wt-benchmarks#8. Not for merge; cherry-pick from it. |

## What made this necessary

Two independent AgentCode benchmarks were written by different people who did
not know about each other, and the MoE-PGO benchmark spent its life as
untracked files in a shared working tree. Both are registered in
`docs/BENCHMARK_CATALOG.md` in wt-benchmarks now, under "Consumer profiles",
which is the list to add to rather than starting a third.
