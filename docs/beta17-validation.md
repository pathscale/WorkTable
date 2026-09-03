# Beta.17 release validation

Validated 2026-09-04. The candidate is the local `release/beta17` working
tree based on `decdd9f`, including the uncommitted validation fixes described
below. No beta.17 WorkTable crate was resolved from crates.io.

## Local provenance

Both the WorkTable workspace and the independent `wt-benchmarks` workspace
were inspected with `cargo tree --offline`. They resolve this stack:

| crate | local source | revision |
| --- | --- | --- |
| worktable 1.0.0-beta.17 | this working tree | `decdd9f` plus validation changes |
| worktable_codegen 1.0.0-beta.17 | `codegen/` in this working tree | same tree |
| worktable_dsl 1.0.0-beta.18 | `dsl/` in this working tree | same tree |
| WorkTablesIndex 0.0.10 | `../WorkTablesIndex` | `c10c82a` |
| data_bucket 0.5.6 | `../DataBucket` | `fa0f8a9` |
| ps-reclaim 0.1.2 | `../ps-reclaim` | `5ee0523` |

The local patches are deliberately marked temporary. They are validation
plumbing, not publishable dependency configuration.

## Release-delta coverage

Beta.17 is not a small changelog. Relative to the local beta.15 point
`e4dcfdf`, it contains about 70 non-merge commits touching 177 files
(11,100 insertions and 1,461 deletions). The release claims were grouped and
checked as follows.

| release area | evidence exercised | result |
| --- | --- | --- |
| Async `insert`, guarded upsert, `insert` versus `insert_many` | normal/all-feature integration suites; insert-many persistence/reload and rejection tests; same-key churn, insert-publication/delete race, and cancellation tests; corrected CRUD and AgentCode benchmarks | pass |
| Batched async deletes and operation-wide activity | delete-many correctness, lock ordering, deadlock, persistence durability and generated-query tests; bulk-lease microbenchmark; reactive-vacuum stress | pass |
| Three interchangeable index backends | backend contract, signed Arctic key/range, native range, iterator, unique/non-unique and key-width tests; WTI/Arctic/Congee PGO, YCSB, concurrency and vacuum grids | pass |
| Persistence ordering, recovery and failure handling | logical-event ordering, reversed delivery, batch collection, torn WAL, checkpoint, corrupt-index, operation failure, migration, reload and bulk-delete suites | pass |
| Reclamation, partition pinning and page/link reuse | default and versioned-row-publication suites; ABA/read-grace, retirement backlog, partition create/remove/read races; CRUD/partition grid | pass |
| Reactive vacuum and exact memory return | deterministic first-batch exclusion; epoch/check-recheck tests; all-backend foreground stress, invariants, no-row-loss tests and three 10-second soaks | pass |
| Extracted WT DSL, schema IR, scanner, CLI, check, diff, emitter and UML | every DSL target with all features; malformed/adversarial corpus; repository round trip; CLI byte stability; doctests | pass |
| Macro/schema compatibility and deterministic generation | schema-const and emitted-declaration tests; backend/generator validation; composite-key ordering; strengthened normal (not ignored) expansion determinism test | pass after fix |
| Release graph and workflows | Cargo tree proves one local WorkTablesIndex/DataBucket identity; exact local CI script passed every feature/build/test/clippy leg | pass locally |
| S3 runtime integration | local-source support.cafe musl build; live Tigris download, recovery/rebuild, strict reload, S3 write/reload, ACME startup and Fly runtime smoke | pass |
| Registry publication/install | intentionally not exercised because beta.17 is not published and this validation must use local crates | post-publish/admin only |

## WT DSL findings and fixes

The trailing-comma fix already on the branch passes five focused cases:
`config`, `delete`, `in_place`, reordered blocks, and the existing no-comma
form.

Validation found a separate real beta.17 defect: macro expansion could vary
because columns, query maps, and generated type sets used randomized
collections while emitting ordered Rust constructs. They now preserve
declaration order with `IndexMap`/`IndexSet`. The old ignored reproduction is
now an ordinary release-gating test and covers multiple columns, two indexes,
two updates and two deletes. The schema model's public ordering tests were
updated to assert the same declaration-order contract.

All DSL targets and features pass: parser/check diagnostics, scanner, schema
round trip, diff planning, DSL emission, CLI, JSON, spans, UML, malformed-input
no-panic corpus and macro re-expansion.

## Downstream async compatibility fix

Compiling support.cafe against the local beta.17 train exposed a real generated
API defect: unique and brute-force delete futures retained a non-`Send` row
read guard across an `await`, so they could not be called from the service's
`async_trait` handlers. Generated deletes now extract or collect primary keys
before awaiting. WTI, Arctic and Congee regression tests prove the generated
unique and non-unique delete futures are `Send`, and the full WorkTable and
support.cafe all-target/all-feature gates pass with warnings denied.

## Configured S3 and support.cafe runtime

The production support.cafe dataset was tested on Fly with an image built from
the local WorkTable, WorkTablesIndex, DataBucket, ps-reclaim and honey-id-types
checkouts. No beta.17 release artifact was used.

Strict beta.17 load correctly rejected latent missing secondary-index entries
for primary key zero in `app_member`, `chat_session` and `support_message`.
Recovery mode loaded and validated all existing entries and authoritative rows,
showing this was old index divergence rather than a byte-format incompatibility.
The support.cafe migration utility rebuilt every table into the separate
`db-beta17-20260904-0216` prefix while leaving the original `db` prefix intact.
Strict beta.17 reload then passed with row counts 4, 4, 9, 19, 0 and 7.

The deployed service subsequently downloaded all six rebuilt tables, performed
its normal persisted admin-role update, restarted, and strict-loaded the result
again. ACME completed, all four configured bots were restored, HTTPS returned
200, and a public HTTP/1.1 WebSocket upgrade returned 101. This covers actual
S3 download, mutation/upload, and restart/reload on the production target.

## Vacuum result

The stress workload starts with 200,000 rows at 50% fragmentation, then keeps
continuous upsert, missing-key reinsert and delete pressure for three seconds.
Each backend/mode cell ran six times in rotated order. `off`, normal
`reactive`, and deliberately `unpaced` vacuum modes were compared.

| backend | off operations, median | reactive operations, median | reactive delete p99 | unpaced delete p99 | unpaced sweeps during load |
| --- | ---: | ---: | ---: | ---: | ---: |
| WTI | 692,198 | 675,781 | 1,541 ns | 4,417 ns | 59 |
| Arctic | 696,110 | 699,891 | 1,334 ns | 2,625 ns | 70 |
| Congee | 695,082 | 696,615 | 1,375 ns | 2,625 ns | 20 |

The small WTI operations-count difference is non-actionable on this host: the
repeated ranges overlap and the other two backends are flat. The unpaced
positive control repeatedly raises foreground tail latency, proving that this
load is strong enough to expose vacuum interference.

Zero completed sweeps during load is not used as proof of zero vacuum work.
A deterministic test holds a mutation open and asserts that vacuum cannot
process its first batch. The reactive path checks both live mutations and an
epoch derived from completed mutation tickets. The check/recheck quiet buffer
therefore observes work that begins and ends between polls. Bulk delete/update
operations hold one operation-wide activity lease across chunk gaps without
holding a row or stripe lock.

The bulk lease costs about 3.57 ns per whole operation. Against the existing
one-row `delete_many` measurement of about 7.28 us, that is roughly 0.05%.

After foreground load, reactive vacuum reaches the independently packed
control exactly: 196 in-use pages and 100,000 live rows in all 18 of 18
backend/repetition cells. `off` retains 392 pages. This satisfies the 100%
reclamation requirement, not merely an approximate percentage.

## Reactive-maintenance precedent

The design has precedent in principle, but the exact gate appears unusual.
MySQL documents that InnoDB merges buffered secondary-index changes when the
server is nearly idle. RocksDB assigns user I/O higher priority than compaction
I/O and offers a rate limiter specifically to reserve bandwidth for online
queries. PostgreSQL autovacuum instead uses accumulated I/O cost and a timed
delay, and conflicting lock acquisition can interrupt a non-wraparound
autovacuum. InnoDB purge is driven by a periodic history list and can delay
foreground writes when purge lag grows too large.

Those systems establish the general policy that reclaim/merge work should
yield resources to foreground traffic. None of their official documentation
describes WorkTable's exact mechanism: one operation-wide mutation lease,
including bulk-operation chunk gaps, plus a completed-mutation epoch and a
check/recheck quiet buffer before the first vacuum batch. RocksDB in particular
does not simply block compaction while writes are active; when compaction falls
behind, its write controller can slow or stop the writes so maintenance catches
up.

Primary references:

- [InnoDB change-buffer merging](https://dev.mysql.com/doc/refman/8.4/en/innodb-change-buffer.html)
- [InnoDB purge scheduling and lag](https://dev.mysql.com/doc/refman/8.0/en/innodb-purge-configuration.html)
- [PostgreSQL cost-based vacuum delay](https://www.postgresql.org/docs/17/runtime-config-resource.html#RUNTIME-CONFIG-RESOURCE-VACUUM-COST)
- [PostgreSQL autovacuum lock interaction](https://www.postgresql.org/docs/15/routine-vacuuming.html)
- [RocksDB I/O priority and rate limiting](https://github.com/facebook/rocksdb/wiki/Rate-Limiter)
- [RocksDB write stalls when compaction falls behind](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide)

## Beta.13 / beta.15 / beta.17 performance grid

Every version is a local WorkTable tree: beta.13 `48f250f`, beta.15 `e4dcfdf`,
and the beta.17 candidate. Three passes rotated the version order
(`13/15/17`, `15/17/13`, `17/13/15`). Values below are medians of those three
passes; ranges are retained in the Rust-generated raw summaries. PGO,
concurrency and YCSB exercise all three backends. AgentCode currently models
the default WTI backend; its catalog entry has been corrected to stop claiming
otherwise.

The PGO, concurrency and YCSB benchmark sources are byte-identical across the
old/new benchmark snapshots. The only compatibility adjustment elsewhere was
awaiting beta.17's newly async writes. An earlier CRUD run that dropped the
insert future was discarded and rerun correctly.

### Concurrent mixed workload

Elapsed milliseconds for 128,000 operations; lower is better.

| backend | writes | beta.13 | beta.15 | beta.17 | beta.17 vs beta.15 |
| --- | ---: | ---: | ---: | ---: | ---: |
| WTI | 0% | 68.027 | 63.573 | 65.405 | +2.9% |
| WTI | 10% | 81.787 | 81.270 | 84.145 | +3.5% |
| WTI | 50% | 159.120 | 155.230 | 163.190 | +5.1% |
| Arctic | 0% | 64.932 | 60.098 | 61.243 | +1.9% |
| Arctic | 10% | 76.313 | 78.206 | 81.476 | +4.2% |
| Arctic | 50% | 149.500 | 146.500 | 150.150 | +2.5% |
| Congee | 0% | 68.173 | 61.102 | 61.114 | +0.0% |
| Congee | 10% | 78.071 | 77.822 | 80.148 | +3.0% |
| Congee | 50% | 164.030 | 154.720 | 152.170 | -1.6% |

Most repeated ranges overlap. The largest median movement is WTI at 50%
writes (+5.1%, with non-overlapping ranges), small enough to document rather
than chase in isolation given the composed application results below.

### PGO publish and retire

The complete publish-plus-retire phase is the application operation. Elapsed
milliseconds at width 12,288; lower is better.

| backend | beta.13 | beta.15 | beta.17 | beta.17 vs beta.15 |
| --- | ---: | ---: | ---: | ---: |
| WTI | 49.535 | 52.398 | 47.935 | -8.5% |
| Arctic | 27.963 | 30.377 | 29.457 | -3.0% |
| Congee | 34.246 | 36.489 | 35.624 | -2.4% |

The fixed-work control is stable (1.410/1.429/1.426 ms). Isolated `retire`
is slower in beta.17 than beta.15 (WTI +25.6%, Arctic +95.6%, Congee +114.5%),
but isolating it omits the publish work it completes. The paired application
phase is faster on every backend, so the isolated result is retained as
implementation telemetry rather than a release blocker.

The PGO accumulate phase is WTI -9.2%, Arctic +11.8% and Congee -9.6% against
beta.15. Arctic and Congee ranges are noisy/overlapping; WTI's improvement is
clean. The array control is -1.0%.

### YCSB A/B/C/F

Elapsed milliseconds for the fixed 50,000-operation sample; lower is better.
Criterion calls this group `throughput`, but its time estimate is duration,
not operations/second.

| workload | backend | beta.13 | beta.15 | beta.17 | beta.17 vs beta.15 |
| --- | --- | ---: | ---: | ---: | ---: |
| A | WTI | 94.355 | 72.066 | 75.473 | +4.7% |
| A | Arctic | 88.969 | 67.412 | 70.165 | +4.1% |
| A | Congee | 90.785 | 67.647 | 70.741 | +4.6% |
| B | WTI | 19.350 | 20.295 | 19.859 | -2.1% |
| B | Arctic | 17.759 | 16.840 | 17.125 | +1.7% |
| B | Congee | 17.829 | 18.007 | 17.306 | -3.9% |
| C | WTI | 7.029 | 5.704 | 7.386 | +29.5% |
| C | Arctic | 6.056 | 5.206 | 4.893 | -6.0% |
| C | Congee | 6.960 | 5.114 | 5.356 | +4.7% |
| F | WTI | 85.543 | 74.406 | 78.410 | +5.4% |
| F | Arctic | 83.210 | 71.450 | 69.031 | -3.4% |
| F | Congee | 84.658 | 71.905 | 70.378 | -2.1% |

Workload A's first three passes pointed consistently 9.7-13% slower despite
overlapping ranges, so it received three additional balanced-order passes.
Across all six its gap contracts to 4.1-4.7%, and every backend range still
overlaps beta.15. WTI/C is also noisy and overlapping. Read p99 ranges are
broader still and do not support a magnitude claim. No YCSB cell is treated as
proof of an improvement or regression without the repeated range.

### AgentCode generation

The full 14,400-row fixture ran three times in rotated version order. Complete
persisted cost includes caller acceptance plus the required durability drain.
Lower is better.

| operation | beta.13 ns/row | beta.15 ns/row | beta.17 ns/row | beta.17 vs beta.15 |
| --- | ---: | ---: | ---: | ---: |
| persisted one-at-a-time, complete | 5,853.75 | 5,878.55 | 4,985.97 | -15.2% |
| persisted `insert_many`, complete | 6,876.02 | 5,867.25 | 5,041.09 | -14.1% |
| in-memory one-at-a-time | 1,101.44 | 1,080.08 | 1,112.13 | +3.0% |
| in-memory `insert_many` | 890.60 | 892.88 | 890.25 | -0.3% |
| persisted generation readback | 125.32 | 120.31 | 120.99 | +0.6% |

The async-insert path is flat in memory and materially better once the real
durability drain is included.

### CRUD and partition checks

Seventeen common WorkTable CRUD/partition cells ran in three rotated passes.
Corrected simple insert is 689.76 ns in beta.17 versus 696.32 ns in beta.15.
Full-featured insert is 2,810.5 ns versus 2,637.0 ns, with overlapping ranges.
Simple and full-featured delete improve 35.3% and 18.6% respectively.

The bare `partition_ref` primitive remains an accepted non-blocker: 3.35 ns
in beta.17 versus 3.09 ns in beta.15, with clean non-overlapping ranges. The
composed `partition_ref_then_select` improves 8.0% (27.67 ns versus 30.07 ns).
This is documented as nanosecond-scale implementation telemetry, not dismissed
as noise and not allowed to override the better application-level result.

## Gates run

- `scripts/ci-local.sh`: all jobs passed on Rust 1.97.1. This includes default,
  `versioned-row-publication`, and all-feature workspace/all-target builds and
  tests, plus default and all-feature clippy with warnings denied.
- Stronger direct all-feature/all-target run: the main integration target
  passed with zero failures; main library 284 passed; codegen 57 passed; every
  DSL target passed.
- Explicit ignored run: all three 10-second backend vacuum soaks and the
  persistence concurrency test passed. The fifth test is the unconfigured S3
  probe described above.
- All-feature doctests: four passed and one example intentionally ignored.
- WorkTable format check passed. The benchmark workspace's complete all-target
  clippy pass and the touched summarizer tests pass with warnings denied.
- The independent `wt-benchmarks` all-target test-mode gate passed against the
  local stack: 29 unit/invariant tests plus smoke execution of every Criterion
  target, including all-backend KV/JSON, deletes, concurrency, PGO and YCSB.

## Release closeout

The locally validated candidate was rebased and merged through
[WorkTable PR 87](https://github.com/pathscale/WorkTable/pull/87); all six CI
jobs passed on the exact release commit. The benchmark instruments and raw
summaries were rebased and merged through
[wt-benchmarks PR 6](https://github.com/pathscale/wt-benchmarks/pull/6).
WorkTable 1.0.0-beta.17 and its dependency train are published on crates.io.

Post-publication consumer resolution and persisted-data checks also pass.
support.cafe compiled and ran against the beta.17 train with its production S3
data, and [AgencyZero PR 205](https://github.com/pathscale/agencyzero/pull/205)
resolved beta.17 from a clean checkout, passed frontend and Rust CI, rebuilt
the complete 18-table QA profile, and strict-opened its 248 projects, 155 items,
and 2,212 messages.

No unresolved beta.17 correctness or application-level performance blocker
was found. The remaining work is release administration: decide whether to
yank the superseded unsound `ps-reclaim` 0.1.0 and WorkTable beta.16 releases,
and eventually restore release-note continuity because the root `CHANGELOG.md`
still stops at 0.4.1.
