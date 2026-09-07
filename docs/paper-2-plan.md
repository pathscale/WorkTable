# Paper two: plan and evidence map

Written 2026-09-06 against master `d5656e6` (1.0.0-beta.19). Companion to the CIDR 2027
submission (beta.6, submitted for the 2026-08-04 deadline; notification 2026-10-06).

## Thesis candidates

The CIDR paper argued *compile-time engine specialization*. It deferred (§5, §7): the lock
discipline scaling comparison, crash consistency, formal checking of the protocols, the cost
of monomorphization, and external baselines beyond redb/LMDB. Paper two should be the paper
those deferrals point at, not a re-statement of the thesis.

**A. Lifecycle paper (recommended).** "Row lifecycle in a specialized engine: exact-cell
locking, epoch reclamation, and reactive vacuum without a transaction manager." Everything
in it has landed since beta.6 and has numbers. Contributions:

1. Exact-cell synchronization replacing hashed stripes and the table-global barrier
   (`src/in_memory/data.rs` `CellLocks`; `docs/versioned-row-publication.md`).
2. Quiescent-state reclamation via a one-word `!Send` guard (`ps-reclaim`), replacing a
   global reader counter; the `Send`-guard use-after-free found on the way is a good
   cautionary section (`docs/TODO.md` "ps-reclaim 0.1.1").
3. Reactive vacuum planned from the free-range registry, gated by a mutation lease and a
   quiet epoch; root-cause note that all four reclaim bugs came from indexes storing
   physical addresses (`docs/vacuum-design-directions.md`, `src/table/vacuum/`).
4. The 1-32 thread grid across three index backends: the lock-discipline scaling result the
   CIDR paper promised (`docs/beta18-validation.md`).
5. `partition_by` with the loom model of the slot protocol (`src/partition/loom_tests.rs`),
   the first model-checked component; HFT review that produced `partition_ref`
   (`wt-review.md`).

**B. Persistence paper.** "Index persistence by replaying the tree's own CDC stream, and what
it costs." Needs the durability work first: structural CDC is ~28% of a 126 ns insert
(`docs/wti-dirty-generation-persistence-plan.md`); ART logical WAL exists
(`src/persistence/space/art_index.rs`) but data pages and WTI still end at `flush()`; the
watermark/fsync design is proposal only (`docs/durability-visibility-proposal.md`). Not
ready before a Q1 2027 deadline unless the journal lands.

**C. Schema-as-IR paper (PL venue).** `worktable_dsl`: schema read as data, diff with a
per-change cost model, declarations baked into generated code, `wt-dsl` CLI, migration
engine with on-disk version detection (`dsl/`, `docs/migration.md`). Fits PEPM / OOPSLA
better than a DB venue; overlaps with the PEPM plan in the other session.

## Evidence already in hand (all M4 Max, local trees; needs a pinned Linux rerun)

| Claim | Number | Source |
|---|---|---|
| Hot-page writer, exact-cell vs hashed | 322 ns vs 1,536 ns | beta18-validation |
| Per-page vs table-global barrier | +25% @4, +65% @8 disjoint writers | versioned-row-publication |
| Generation retire, beta.18 vs 15 | 33.6x (WTI), 16.1x (Arctic) | beta18-validation |
| Read scaling best/1T at 16 threads | 3.6x / 3.3x / 2.6x per backend | beta18-validation |
| 10%-write mix ceiling | peaks at 4 threads | beta18-validation (also beta.13/15) |
| Reactive vacuum foreground penalty | -1.1..-5.1% vs 16-41% unpaced | beta17/18-validation |
| Vacuum reclamation | 18/18 cells, 196 pages, 100% | beta17-validation |
| Point lookup vs stripped index | 15.65 ns vs 9.25 ns bare Arctic vs 33.5 Vec+BTreeMap | beta18-validation |
| Memory overhead | 0.14 B/row over control | beta18-validation |
| Partition route | 0.73 ns Vec vs 9.5 ns string hash; `partition_ref` 3.35 ns | TODO.md, partition docs |
| Persisted insert, beta.15 to 18 | -33..-42% (Arctic 6,130 to 3,753 ns/row) | beta18-validation |
| CDC share of insert | ~35 ns of 126 ns | wti-dirty-generation plan |

Not in hand: `paper-bench/results/` (never committed), `compile_cost.sh` never run, no
sled/SQLite/DashMap baselines, no beta.19 rerun of Table 2.

## Consumer workloads (surveyed 2026-09-06, all under ~/code)

| Repo | Shape | What it gives the paper | Open? |
|---|---|---|---|
| `agentcode` | 11 tables, 8 persisted, Arctic on nearly every index, u128 keys. Stress = 8 concurrent 800-file `update_latency` processes. | The concurrency bug that motivates the paper: `docs/known-defects.md:70-137`, torn/corrupt page header in secondary-index batch apply at beta.11, 6/8 runs failing, 28/28 after moving to Arctic. Map it to the beta.18 fix ("torn reads and premature physical-link reuse") and show the same harness clean on beta.19. Also: Arctic vs WTI at 20k rows, insert 2.18M/s vs 0.89M/s, lookup 17.0M/s vs 5.0M/s (`docs/benchmarks/index-backends.md`); state 42.4 to 22.2 MB after u128 keys (`state-growth.md`); the request for a non-unique fixed-width index that became `ArcticMultiIndex`. | Proprietary |
| `agencyzero` | 19 persisted tables, String PKs, migration engine, `LoadMode::Recovery`, single-writer flock, QA fixture of 248 projects (~30 MB store). | The reclamation case study: `docs/store-recovery.md` records four production corruptions, including a variable-width index page that forgot fragmentation across restart (174 live entries, 2,664 B dead, 64 B tail overlap) and beta.5 whole-row rebuilds churning `pr_project_idx` (38 disagreeing rows) fixed by in-place updates. Production migrations via schema fingerprint. This is the "why indexes must not store physical addresses" story with real data. | Private (GitHub) |
| `karen` | 2 persisted tables, tiny (~700 rows). | Row-level, queryable, durable learned session state instead of one blob: the per-turn Confirm write-through gives 15-20 points top-1. One paragraph of motivation, not evaluation. Uses `unload_gracefully`. | Closed |
| `ekopathrs` | No `worktable!` at all. Uses `worktable-vec::AtomicKeyTable` for two in-memory profiling tables. | The honest negative: `docs/STORAGE-REVIEW.md` rejects full WorkTable (318-package resolve, no `no_std`) for 399 entries. Cite as the boundary of the design space; `worktable-vec` is the lock-free, `no_std` sibling. | Private |

Use agentcode as the headline stress workload in §4 alongside the shadow-state harness; use
agencyzero as the recovery/fragmentation case study; mention karen and ekopathrs in one
paragraph each in the experience section. Get written OK before naming private repos.

## Gaps to close for option A

- Rerun the beta.18 grid and `paper-bench` on a quiet pinned x86 box; commit `results/`.
- Lock-discipline ablation as a proper figure: field vs row vs table lock, 1-32 threads,
  skewed keys (`paper-bench/src/bin/contention`).
- Semi-formal statement of the cell-lock + reclamation invariants; ideally extend loom
  beyond partitions to the cell/retire path (the CIDR reviewers will ask).
- Wart sweep: 9 `todo!()` sites remain (`codegen/.../queries/in_place.rs`, `update.rs`,
  `src/features/s3_support.rs`), `Avaiable` typo in 2 files.
- Merge or explicitly exclude `feat/columnar-fields-indexes` (branch, Aug 6, unmerged).

## Target: EDBT 2027, 3rd cycle (verified 2026-09-06)

- Submission **2026-10-07, 5pm PST** (31 days out). Author feedback 11-19, notification
  Acc/Rej/Revise 12-05, revised paper 2027-01-04, final 01-27, camera-ready 02-10.
  Conference Lille, April 6-9, 2027.
- Paper types: Research long (12p) or short (6p, title prefixed "[Short Paper]"),
  Experiments & Analysis, Vision (6p). Topics list includes "Concurrency control, recovery,
  and transaction management", "Storage, indexing, and physical database design",
  "Data management on modern hardware", "Benchmarking and performance evaluation".
- The revise cycle matters: a paper that gets "revise" on Dec 5 has until Jan 4 to add
  the pinned-Linux rerun, so the Oct 7 draft can ship on the M4 grid with the caveat stated.
- CIDR notification is Oct 6, one day before: paper two cannot depend on the outcome and
  must not overlap the CIDR text (still under review until then). Option A is disjoint by
  construction; cite the CIDR paper as "under submission".

Alternatives if A slips: ICDE 2027 R2 (2026-11-11), PVLDB rolling (monthly to 2027-03-01),
SIGMOD R4 (2026-10-17). DaMoN 2027 CFP not posted.

## 31-day schedule for option A (long paper)

| Week | Dates | Deliverable |
|---|---|---|
| 1 | Sep 7-13 | Freeze the claim list. Run `paper-bench` contention (field/row/table/inplace, 1-32 tasks) and beta.18 grid on the pinned Linux box if available, else M4 with three rotated passes; commit `results/`. Decide long vs short by Sep 13 based on whether the scaling figure holds. |
| 2 | Sep 14-20 | Draft §2 protocols (cell lock, retire, vacuum lease) with invariants stated; §3 partition + loom. Wart sweep PR (`todo!()`, `Avaiable`). |
| 3 | Sep 21-27 | Draft §4 evaluation from `results/`; figures; related work (Hekaton, epoch/QSBR: Fraser, Hart et al., Bw-tree, OLC, DaMoN vacuum/compaction lineage). |
| 4 | Sep 28-Oct 4 | Full read-through, internal review, page trim to 12. |
| 5 | Oct 5-7 | Buffer. Submit by Oct 6 evening local time (Oct 7 5pm PST is 07:00 Oct 8 in Bangkok, but do not use it). |

Short-paper fallback (6p): contributions 1-3 only, one scaling figure, one vacuum figure.
