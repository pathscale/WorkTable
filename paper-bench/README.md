# wt-paper-bench — benchmark harness for the CIDR 2027 paper

Fills the `\ph{...}` placeholders in the paper. Run **in your macOS terminal**
(not through Claude's sandbox — it has no crates.io access). For the paper's
*official* numbers, re-run on a quiet, dedicated Linux x86 box with cores
pinned; Mac runs are fine for development and go/no-go decisions.

## Run

```bash
cd ~/code/WorkTable/paper-bench
mkdir -p results

# 1. Lock-granularity contention matrix (~7 min: 4 modes x 5 task counts x 2 runs x 5s)
cargo run --release --bin contention | tee results/contention.csv

# 2. Specialization ablation (the make-or-break experiment)
ROWS=1000000 REPS=5 cargo run --release --bin ablation | tee results/ablation.csv

# 3. Hand-rolled baselines (Vec / RwLock<HashMap> / DashMap)
ROWS=1000000 REPS=5 cargo run --release --bin baselines | tee results/baselines.csv

# 4. Compile-time + binary-size cost of specialization
bash scripts/compile_cost.sh | tee results/compile_cost.csv
```

Knobs: `DURATION_SECS` (contention, default 5), `ROWS` (default 1M),
`REPS` (default 5, median reported).

When done, just tell Claude the results are in `paper-bench/results/` — they'll
be staged back, analyzed, and the paper tables/figures filled.

## What maps to what in the paper

| Output | Paper location |
|---|---|
| `contention.csv` | §5 lock-granularity ablation + scaling figure (C2) |
| `ablation.csv` | Table 1 (specialization ablation — C1, the thesis test) |
| `baselines.csv` | Table 2 rows: Vec/HashMap/DashMap |
| `compile_cost.csv` | §5 "cost of specialization" sentence |

Still TODO after this round: external baselines (sled, redb, LMDB/heed,
SQLite `:memory:`) as a follow-up crate, YCSB-shaped mixes, and multi-threaded
ablation runs.

## Fairness caveats to revisit before trusting Table 1

The dynamic twin (`src/dynamic.rs`) models the dynamism tax as: tagged-value
rows + runtime catalog lookup + encode/decode per access + coarse per-row
mutex. It does NOT share WorkTable's page allocator or B-tree (it uses a slot
vector + BTreeMap, which is *favorable* to the twin for point ops — so a
specialized win is conservative). A v2 twin reusing WorkTable's pages/indexes
with only the row representation dynamized would isolate the typing cost more
precisely. Review `dynamic.rs` before the official campaign.

## Notes

- The bench table (`src/lib.rs`) has an indexed column `a`, non-indexed `b`/`e`
  (the disjoint contention pair), an `f64`, and a fixed-length `String` (keeps
  the table on the unsized code path; same-length updates stay in place).
- `contention` modes: `disjoint` (b vs e — field locks shouldn't collide),
  `overlap` (both tasks write {b,e} — must serialize), `mutex` (external
  single lock), `inplace` (closure RMW). The C2 claim is
  disjoint >> overlap ≈ correct, with mutex as the floor.
- First build takes a few minutes (workspace + LTO). If `cargo` complains
  about the parent workspace, the `[workspace]` opt-out table in Cargo.toml
  should prevent it — report the error if not.
