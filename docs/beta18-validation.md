# Beta.18 validation

Status: release validation in progress.

Validated 2026-09-05 from local working trees only. No WorkTable-family result
in this document resolves a released crate.

## Local provenance

| package | local path | base revision |
|---|---|---:|
| WorkTable | `/Users/revenge/code/WorkTable` | `a046217` plus working-tree changes |
| Arctic | `/Users/revenge/code/arctic-wt` | `46f1895` plus working-tree changes |
| ps-reclaim | `/Users/revenge/code/ps-reclaim` | `5ee0523` |
| WorkTablesIndex | `/Users/revenge/code/WorkTablesIndex` | `c10c82a` |
| DataBucket | `/Users/revenge/code/DataBucket` | `fa0f8a9` |
| Congee | `/Users/revenge/code/congee-wt` | `35855d3` |
| benchmark suite | `/Users/revenge/code/wt-benchmarks` | `df76063` plus working-tree changes |

WorkTable, codegen, and DSL now identify as `1.0.0-beta.18`.

`cargo tree --workspace --all-features` confirms that WorkTable, codegen, DSL,
Arctic, ps-reclaim, WorkTablesIndex, DataBucket and Congee all resolve to the
paths above. The local Vec controls resolve Arctic to the same local checkout.

## Corrected benchmark methodology

The old random-lookup runner printed one construction observation for each
arm. Those figures mixed a published beta17 result with later local lookup
results and were too noisy to support a construction regression claim. The
construction lines have been removed from the lookup runner. Construction now
has a dedicated runner with rotated order, 31 samples, eight populations per
sample, plus independent-process first-population measurements.

The random lookup runner now performs eight million queries per sample and
rotates the six arm positions. It prints min, p25, median, p75 and max rather
than presenting one absolute median without its host spread. Full detail is in
the local benchmark document `docs/benchmarks/moe-resident-index-ab.md` in the
wt-benchmarks repository.

## Random successful point lookup

The latest local release run used eight million queries per sample, nine
rotated samples, and identical checksums:

| arm | median ns/query |
|---|---:|
| Vec linear scan | 205.44 |
| Vec + BTreeMap | 33.53 |
| Vec + Arctic | 9.25 |
| WorkTable + Arctic | 15.65 |
| WorkTable + WTI | 58.05 |
| WorkTable + Congee | 24.77 |

Arctic now defaults to ps-reclaim, so both Arctic arms use the release
reclamation backend. WorkTable+Arctic is 1.69x the stripped index-plus-Vec
lower bound while still 2.14x faster than Vec+BTreeMap.

ps-reclaim originally marked a second live domain pin as cold and scanned the
participant's atomic slots. WorkTable nests its page and Arctic domains on
every select, so that assumption was false. An exact per-thread four-bit
occupancy mask now handles nested and out-of-order guard drops without the
scan. The post-fix 15.65 ns restores the earlier 15.57 ns baseline. The
single-thread difference against Seize was a microbenchmark only; scalability
and application-level results decide the release. Miss and range distributions
remain pending.

## Construction

Audited local candidate results for 1,528 rows:

| arm | reused-process median ms | independent-process median ms |
|---|---:|---:|
| Vec + BTreeMap | 0.040 | 0.116 |
| WorkTable, `block_on` per row | 0.101 | 0.124 |
| WorkTable, one executor around loop | 0.101 | 0.121 |
| WorkTable `insert_many` | 0.095 | 0.134 |

The cold first-population WorkTable/BTree gap is 1.53–1.55x. The async wrapper
is not the cause: per-row `block_on` and one executor differ by less than 1%.

Equivalent local historical reused-process row-loop medians are 0.698 ms for
beta13, 0.662 ms for beta15 and 0.160 ms for the candidate. This does not
reproduce a release-to-release construction regression; the candidate is about
3.9x faster than beta15. It remains slower than the stripped BTree control.

## Memory

The isolated-process paired measurement reproduced exactly:

| arm | retained bytes | bytes after drop |
|---|---:|---:|
| Vec + Arctic | 105,760 | 0 |
| WorkTable + Arctic | 105,976 | 0 |

The WorkTable delta is 216 bytes, 0.14 bytes per live row, or 1.002x the
stripped Vec+Arctic control. Both arms produced the same checksum. The prior
candidate retained 114,344 bytes; exact-cell state plus the per-page live count
removed 8,368 bytes from that result.

## Hot-page concurrency

The short micro-run was rejected because absolute throughput moved materially
with host load. The benchmark was enlarged eightfold to two million reads per
reader and 160,000 upserts per mixed sample. The candidate was run before and
after an unmodified `a046217` control using the same benchmark executable.

| tree | writer ns/upsert | concurrent reader Mops/s |
|---|---:|---:|
| exact-cell candidate, run 1 | 321.88 | 80.482 |
| exact-cell candidate, run 2 | 469.29 | 49.707 |
| prior candidate, best | 1,028.96 | 86.313 |
| unmodified base | 1,535.69 | 22.218 |

Mixed reader scheduling remains noisy, and the raw range is retained rather
than hidden. Both exact-cell writer samples are more than twice as fast as the
prior candidate. The bounded standardized concurrency grid below supersedes
this diagnostic for release disposition.

## Standard scalability grid

The checked-in `concurrent_mix` suite now has bounded 1/2/4/8/16/32-thread
axes for all three primary-index backends. Each thread runs 4,000 pre-generated
operations over a disjoint key range in a 20,000-row table. This avoids
measuring RNG or same-key contention. The host has 12 performance and four
efficiency cores.

Pure-read median throughput in Mops/s:

| backend | 1 | 2 | 4 | 8 | 16 | 32 | best/1T |
|---|---:|---:|---:|---:|---:|---:|---:|
| WTI | 8.748 | 14.042 | 24.181 | 22.968 | 31.507 | 23.382 | 3.60x |
| Arctic | 8.918 | 14.002 | 21.829 | 29.631 | 27.446 | 24.830 | 3.32x |
| Congee | 9.539 | 18.847 | 24.446 | 22.006 | 19.542 | 24.456 | 2.56x |

The 10%-write median throughput in Mops/s:

| backend | 1 | 2 | 4 | 8 | 16 | 32 | best/1T |
|---|---:|---:|---:|---:|---:|---:|---:|
| WTI | 4.826 | 5.426 | 9.701 | 4.632 | 4.256 | 4.055 | 2.01x |
| Arctic | 4.852 | 5.066 | 8.601 | 4.651 | 4.709 | 5.164 | 1.77x |
| Congee | 5.436 | 6.241 | 8.605 | 4.449 | 4.477 | 4.703 | 1.58x |

The broad-table read path scales materially better than the one-page
false-sharing probe, but it is not close to linear. Mixed throughput peaks at
four threads and then drops for every backend. The same ceiling appears in
beta13 and beta15, so it is not introduced by beta18 or the ps-reclaim switch.
In adjacent equal-profile beta18/beta15 runs, beta18 is faster in every
targeted high-thread cell except Arctic at eight threads: 4.651 versus 5.022
Mops/s, a reproducible 7.4% loss. Arctic beta18 is 4.8% and 6.5% faster at 16
and 32 threads. The single eight-thread loss remains disclosed but is not an
overall scalability release blocker.

## Local beta13/beta15/beta18 MoE-PGO grid

The donor-width (`12,288`) PGO grid was built through an isolated local-only
harness. Beta13 and beta15 use local WorkTable, matching local DataBucket and
matching local WTI worktrees. Beta18 uses the candidate and every current
backend dependency from the local paths in the provenance table. The comparable
control medians are 1.5329, 1.5416 and 1.5501 ms respectively, a 1.1% span.

Raw phase medians:

| phase | backend | beta13 | beta15 | beta18 |
|---|---|---:|---:|---:|
| accumulate, 200k updates | WTI | 127.22 ms | 121.01 ms | 87.146 ms |
|  | Congee | 96.937 ms | 76.214 ms | 63.854 ms |
|  | Arctic | 85.409 ms | 75.556 ms | 73.496 ms |
| publish, 8 × 12,288 rows | WTI | 56.450 ms | 59.550 ms | 34.917 ms |
|  | Congee | 37.259 ms | 39.042 ms | 11.399 ms |
|  | Arctic | 34.202 ms | 34.881 ms | 7.7407 ms |
| retire, 8 maps under readers | WTI | 3.8748 µs* | 1.7007 ms | 50.655 µs |
|  | Congee | 3.7604 µs* | 2.4858 ms | 1.7240 ms |
|  | Arctic | 3.8184 µs* | 2.1884 ms | 135.64 µs |

The beta13 retire cells marked `*` do not reclaim: that implementation only
appends the old `Arc` to a permanently retained vector unless an exclusive
`&mut self` GC API is called. They are leak timings and are not used as a
performance baseline. Against beta15's real reclamation, beta18 retire is
33.6x faster on WTI, 1.44x on Congee and 16.1x on Arctic. Publish plus retire
is 42.9%, 68.4% and 78.8% faster respectively. A first beta15 pass was rejected
openly because its no-WorkTable control was 1.9398 ms; the immediately repeated
1.5416 ms pass is the table above.

## Local AgentCode generation grid

The AgentCode benchmark now explicitly selects WTI, Arctic and Congee for both
the primary key and dedup secondary index. It writes one 14,400-symbol
generation and reports acceptance separately from persistence drain. Each cell
below is the median of three warm processes; durable columns sum acceptance and
drain inside each run before taking the median.

| version | backend | memory row insert | memory batch | durable row insert | durable batch | readback |
|---|---|---:|---:|---:|---:|---:|
| beta13 | WTI | 1,212.14 | 1,041.73 | 6,598.21 | 7,817.54 | 130.91 |
| beta13 | Arctic | 910.42 | 728.90 | 6,159.13 | 7,243.97 | 103.27 |
| beta13 | Congee | 904.04 | 735.36 | 6,058.45 | 7,202.81 | 110.91 |
| beta15 | WTI | 1,193.00 | 987.45 | 6,634.12 | 6,475.49 | 129.73 |
| beta15 | Arctic | 899.16 | 721.78 | 6,129.71 | 5,822.36 | 107.42 |
| beta15 | Congee | 921.64 | 726.67 | 6,008.33 | 5,883.92 | 106.45 |
| beta18 | WTI | 835.06 | 728.80 | 4,416.68 | 3,971.17 | 104.06 |
| beta18 | Arctic | 506.03 | 379.56 | 3,753.34 | 3,377.61 | 80.30 |
| beta18 | Congee | 538.04 | 395.23 | 3,899.32 | 3,497.77 | 76.72 |

All values are ns/row. Against beta15, beta18 durable single-row generation
writes improve 33.4%, 38.8% and 35.1% for WTI, Arctic and Congee. Durable
`insert_many` improves 38.7%, 42.0% and 40.6%. Arctic is the fastest beta18
write backend; Congee is fastest for complete generation readback.

## Exact-cell synchronization and compatibility

The fixed hashed row stripes and four-byte per-row vacuum directory are gone.
Each archived cell carries one runtime synchronization byte, and each page
carries one live-cell counter. A writer on one cell does not block a different
cell on the same physical page. Full-row replacement deliberately skips the
active synchronization byte; the first implementation copied it and the
concurrent update/reclaim test exposed the resulting stuck cell.

The corrected implementation passes 35 focused page/reclamation tests, the
concurrent update/reclaim regression, and the checked-in pre-schema persisted
fixture. The archived inner row remains at the beta.17 offset.

## Generation unload

Generated persisted and read-only tables now implement `MemStat`. An
Arc-owned `unload_gracefully` runs a caller-supplied quiesce barrier under a
timeout, rejects any remaining Arc leases, drains persistence, drops the owned
generation, and reports the attributed bytes released. Its three focused tests
pass, including a live reader draining during the swap.

## Reclamation backend audit

WorkTable's row/page domain uses local `ps-reclaim` regardless of whether the
index is WTI, Arctic, or Congee. WorkTable's Arctic adapter now disables
Arctic's default Seize feature and instantiates only Arctic `PsReclaim`, even
under `--no-default-features`. WTI still owns a Crossbeam skip-list internally,
and Congee still owns Crossbeam epoch internally; they are not simultaneously
selected indexes, and those internal implementations are distinct from
WorkTable's row/page reclamation layer.

## Reactive vacuum

The balanced local grid uses all six mode-order permutations, three backends,
100,000 starting rows, one second of uninterrupted upsert/reinsert/delete
pressure, and an independently packed 50,000-row control.

| backend | off operations median | reactive operations median | delta | off range | reactive range | unpaced delta |
|---|---:|---:|---:|---:|---:|---:|
| WTI | 250,873 | 246,886 | -1.6% | 247,745–255,268 | 234,392–258,380 | -16.2% |
| Arctic | 253,660 | 250,832 | -1.1% | 246,944–264,580 | 246,255–254,977 | -29.9% |
| Congee | 257,808 | 244,726 | -5.1% | 241,337–263,049 | 238,078–258,549 | -41.1% |

Every reactive range overlaps its vacuum-off range. The reactive median deltas
are smaller than the corresponding 3.0%, 7.0% and 8.4% vacuum-off host spreads.
The unpaced positive control loses 16–41% of foreground throughput, proving
that the workload can detect vacuum interference.

Reactive vacuum reclaimed from 196 to the independently packed 98 in-use pages
in all 18 of 18 backend/repetition cells. Maximum excess pages were zero.

The manager's sweep counter increments only after a sweep completes. Therefore
zero completed sweeps during load is not treated as proof of zero work. Three
deterministic local tests supply the structural evidence and pass:

- a live mutation holds vacuum before its first batch;
- a mutation completed between checks resets the quiet-sample buffer;
- vacuum reaches the exact independently packed page count.

## Remaining release gates

- Complete the beta13/beta15/beta18 local YCSB grid across WTI, Arctic and
  Congee. The local PGO, concurrency and AgentCode grids are complete.
- Add random miss and range-search distributions after the release-blocking
  standard matrix; do not hold beta18 for nanosecond-only tuning.
- Rebuild AgencyZero against the local candidate and strict-open the complete
  qa-profile bundle to prove persisted-format compatibility.
- Run support.cafe/S3/Fly validation after local correctness and performance
  gates. Any deferral requires an explicit release choice.
- Put WorkTable, Arctic and benchmark changes on reviewable PRs and record the
  dependency merge/publish order.

## Workspace correctness gate

The current candidate passes `cargo check --workspace --all-targets`, 35/35
focused page/reclamation tests, the legacy persisted fixture, the concurrent
update/reclaim regression, and all three generation-swap tests. The complete
post-fix workspace gate passes: 271 core unit tests, 642 integration tests (four
explicitly ignored), 56 codegen tests, and the complete DSL suite. Arctic's
default ps-reclaim configuration passes 40 unit, 10 regression, and two
Shuttle tests. ps-reclaim passes its complete suite, including nested-pin,
out-of-order-drop, overflow-slot, and continuous-reader coverage.

The first default-parallel run had one 30-second timeout in
`test_duplicate_key_mutations_without_reload`. It did not report an event gap
or engine failure. The same test then passed alone in 14.63, 11.05 and 6.40
seconds, and passed in two complete four-thread integration runs. This is
recorded as suite-level persistence-task starvation under unbounded test
parallelism; it is not silently discarded or classified as a product deadlock.
