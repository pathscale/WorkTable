# Vacuum logic branch history

The historical branch was named `vaccum-logic` (with the misspelling). It is
archived here as design history and is not a branch that should receive new
work.

## Disposition

- Final branch tip: `50d22e88c7a4c174c2707a9803028588e0c41b90`.
- The exact tip was squash-merged by
  [pathscale/wt-dead#137](https://github.com/pathscale/wt-dead/pull/137) as
  `313039416fc28c417a7e3f97ad43f86829ed5431`.
- The first major correctness follow-up landed through
  [pathscale/wt-dead#147](https://github.com/pathscale/wt-dead/pull/147) as
  `a7f08e554295df2bd930f75f9d6e968ca650e783`.
- Every file introduced by the branch still exists on `master`. There is no
  production code or test that should be cherry-picked from the old branch.

The branch can therefore be deleted without losing implementation history.
Git retains its commits through the repository history and the archived pull
request.

## What survived

The branch established the current vacuum architecture:

- `EmptyDataVacuum` and table-level fragmentation analysis;
- `VacuumManager` and background scheduling;
- reverse primary-index lookup needed to relocate rows;
- `OffsetEqLink` for stable index comparisons across row moves;
- empty-link accounting and page-compaction machinery; and
- concurrent select, insert, upsert, and multi-page vacuum tests.

Later work made vacuum safe for persisted tables by routing row moves through
CDC, closed row-lock acquisition races, and replaced fixed reclamation delays
with epoch-based grace periods. The current implementation and tests on
`master` are the source of truth.

## Unfinished API scaffolding

The original scheduling design was never completed:

- `VacuumManagerConfig` exposes low, normal, high, and critical thresholds,
  but only the low threshold is read;
- `VacuumPriority` is public but unused;
- the current capacity-to-empty-bytes ratio is normally at least `1.0`, so a
  proposed critical threshold of `0.7` is unreachable under valid accounting;
  and
- registration retains a strong table reference and has no matching
  `unregister` or registration guard.

Before a stable 1.0 API, either implement priority-aware scheduling and
lifecycle-safe registration or remove these unused public controls.
