# Review — "Columnar fields and indexes" guide (August 2026 draft)

**Status:** Expert review of a design proposal. Not a description of shipped behaviour.

**Subject:** `worktable-columnar-fields-and-indexes-guide.pdf`, August 2026.

**Baseline:** [`columnar-index-plan.md`](columnar-index-plan.md) — the existing in-repo design
proposal covering the same feature.

**Verdict:** the strategy is right and the scoping instinct is excellent, but the guide is in
several respects a *regression* against the design already in this repository. One finding is a
correctness defect. Four more are design properties that `columnar-index-plan.md` had solved and
this draft silently drops.

---

## 0. Read this first: the draft supersedes a better document

`docs/columnar-index-plan.md` (821 lines, in tree) proposes the same feature with the same
top-level syntax shape. The guide does not reference it, and diverges from it on six substantive
points — in every case toward the weaker option.

| Property | `columnar-index-plan.md` | August guide | Assessment |
|---|---|---|---|
| Identity type | `ColumnRowId`, monotonic | `ImmutableSortId{8,16,32,64}`, slot-reusing | **Regression** — reintroduces ABA |
| Slot reuse | "IDs are not immediately reused, avoiding ABA hazards" | "makes the bounded integer slot reusable" | **Regression** |
| Read consistency | Deterministic `(column_id, chunk_id)` lock order; `ColumnRowState { generation, active_writers, live }` | Not addressed | **Regression** |
| Scan API | `scan_batches`, selection vectors, never materializes rows | `-> Vec<...>` of pairs | **Regression** |
| Predicates | `.host_id_eq(42).timestamp_range(a..b)` | Full-key equality only | **Regression** |
| Compression | `none` only in MVP; `auto` "accepted only after it has a deterministic implementation" | All policies accepted as inert metadata | **Regression** |
| Namespacing | `table.columnar().metrics()` | `table.columnar_scan_status()` | **Regression** — name collisions |
| Bounded ID width | Not offered | Offered, with explicit exhaustion error | **Genuine addition** |
| Format compatibility framing | Stage one is rebuild-on-load | Same | Agreement |

**Recommendation:** do not circulate the guide as a standalone proposal. Either fold these
findings back in, or state explicitly that it supersedes `columnar-index-plan.md` and justify each
divergence. Two live design documents describing the same feature differently is the worst
outcome, and `AGENTS.md` is explicit that docs describe what is true now.

---

## 1. Correctness — must fix before implementation

### 1.1 Slot reuse reintroduces an ABA hazard (blocking)

The guide states:

> Delete removes column values and clustered entries, then makes the bounded integer slot reusable.

and separately:

> Projection revalidates the pair. If a deleted row's compact slot has since been reused, a stale
> `{ primary_key, immutable_sort_id }` cannot alias the new row.

The second claim does not follow from the first, and `reinsert` is an explicitly supported
operation on this table type.

```text
t0  insert  pk=938271           -> slot 41207
t1  reader  holds { pk: 938271, sort_id: 41207 }
t2  delete  pk=938271           -> slot 41207 returned to free list
t3  reinsert pk=938271          -> allocator returns slot 41207
t4  reader  projects the retained ref
        pair matches the live directory entry exactly
        revalidation passes
        reader observes a different row generation as if it were its own
```

Pair-matching only defends against a *different* primary key landing on a reused slot. It provides
no defence at all against the same primary key being deleted and reinserted, which is the common
case in every bounded-window workload the guide recommends bounded IDs for. The HFT pattern —
fixed-size live window, constant eviction and insertion — is precisely the workload that will hit
this.

`columnar-index-plan.md` avoided this by construction ("IDs are not immediately reused, avoiding
ABA hazards") and by carrying `ColumnRowState { generation, active_writers, live }`. Bounded 8- and
16-bit identifiers make non-reuse impossible, so the bounded-width feature and the safety property
are in direct conflict. The guide chose the feature and dropped the property without saying so.

**Fix.** Two mechanisms, both needed, addressed in §2.1 of the grammar revision:

1. **Generation tag.** Carry a generation counter in the identity token, incremented when a slot is
   freed. A retained reference is valid only if slot *and* generation match. This makes the stale
   reference detectable rather than silently aliasing.
2. **Deferred reclamation.** Do not return a slot to the free list while any reader that could hold
   a reference to it is still in flight — epoch-based reclamation, quiescent-state detection, or
   the existing mutation gate extended over the read window. Generation tags alone leave a wrap
   window; deferred reclamation alone leaves retained-reference holders exposed.

### 1.2 "Immutable" is false across a restart

The guide asserts the sort ID "is stable for one row's lifetime", and separately that columnar
state is "omitted from `PersistIndex`, and rebuilt from authoritative rows after load".

These cannot both hold. A rebuild reassigns identifiers by whatever order the row scan produces.
A row's identifier before restart and after restart are unrelated. If a caller persists a
`ColumnarRowRef` — and the guide encourages retaining them — it is invalid after reload in a way
that revalidation will not necessarily catch, because some *other* row may legitimately now hold
that slot with a matching generation.

**Fix.** Three parts:

- Rename. It is not immutable and it is not a sort key (see §3.1).
- Make `ColumnarRowRef` non-serializable, or tag it with a process/incarnation epoch so a
  cross-restart reference fails loudly.
- State the lifetime contract precisely: *stable for one row's lifetime within one process
  incarnation.*

### 1.3 No multi-column read consistency

> Results can be retained after the internal read lock is released

Two projections over the same `&rows` — `columnar_project_cpu_percent` then
`columnar_project_status` — take their internal read locks independently. A row updated between
them yields a result set combining `cpu_percent` from before the update and `status` from after.
The caller has no way to detect this and no API to prevent it.

`columnar-index-plan.md` addressed exactly this ("one projected result must not combine field
values from different committed versions of the same RowId") with a deterministic
`(column_id, chunk_id)` acquisition order and a validated-read alternative. The guide drops the
section entirely.

**Fix.** Reinstate the plan's approach and expose a read guard so multi-column projection is a
single scoped operation rather than N independent ones. See §2.4 of the grammar revision.

### 1.4 The dirty-rebuild cliff

> In-place archived-field changes mark the derived replica dirty; the next columnar access rebuilds
> it from authoritative rows.

A single in-place write makes the *next reader* pay a full O(rows) rebuild. This is a latency
cliff on the read path triggered by an unrelated write path, and it is unbounded — on a large
table an interactive query can block for seconds behind a rebuild it did not cause.

**Fix.** Track dirtiness per field and per chunk, not per table. Expose `rebuild_columnar()` so
applications can schedule the cost, and a `columnar_is_dirty()` predicate so they can decide.
Consider rebuilding on the writer or on a background worker rather than on the reader.

### 1.5 Concurrent mutation of a `cluster_by` column is unspecified

Update "refreshes affected values and clustered keys" — which moves an entry within the ordered
structure. What a concurrent `columnar_scan_host_time()` observes during that move is undefined:
the row may be seen twice, once, or not at all. `columnar-index-plan.md` is honest here
(`cluster_by` "does not by itself promise that every result is globally sorted"; sealed segments
may overlap). The guide's flat "clustered traversal in `(host_id, captured_at_ns)` order" promises
more than the design delivers.

**Fix.** State the guarantee. "No duplicate live rows; ordering is per-segment, not global" is a
defensible v1 contract. "Ordered traversal" without qualification is not.

---

## 2. Design — the unstated property that decides whether this is fast

**Does an ordered clustered scan read column chunks sequentially, or does it gather?**

The guide never says. It is the single most load-bearing performance fact about the feature and
it determines whether the word "columnar" is earned.

- Base column chunks are addressed positionally by identity token, in allocation order.
- A clustered index orders by `(host_id, captured_at_ns)`, which is a *different* order.
- Therefore an ordered traversal walks a permutation of chunk positions.

A permutation walk is random access. It defeats prefetch, defeats SIMD, and defeats the memory
bandwidth argument that is the entire reason to build a column store. You get the cache-footprint
benefit of touching one column instead of a whole row, and none of the vectorization benefit.

This is not necessarily wrong — `columnar-index-plan.md` is clear-eyed that key values are
duplicated inside index segments precisely so pruning does not require touching base columns, and
it schedules an explicit aligned-versus-gathered benchmark. But the guide presents ordered scan as
if it were a sequential column read, and readers will size their expectations accordingly.

**Fix.** State it plainly, and publish the aligned-vs-gathered measurement before making
performance claims. The revised guide includes an explicit "access paths and what they cost"
section.

---

## 3. Naming

### 3.1 `ImmutableSortId` is wrong on both words

The guide itself says:

> It is not a mutable rank in the current clustered order.

So it does not describe a sort position. And per §1.2 it is not immutable across a restart. The
name will produce a predictable class of user bug: someone orders by it, gets free-list allocation
order, and ships it.

`columnar-index-plan.md` already named this correctly: **`ColumnRowId`**. Use that. If the width
parameter must appear in the type, `ColumnRowId32`.

### 3.2 `cluster_by` invites a false inference

In Snowflake and BigQuery vocabulary, clustering means the base data is physically reordered.
Here it names a sorted structure over positionally-ordered chunks. `columnar-index-plan.md` is
explicit that "base column stores remain in canonical `ColumnRowId` order" — the guide is not, and
the borrowed keyword does the rest.

Keeping `cluster_by` is defensible for familiarity, but then the documentation must state in the
same breath that base columns are *not* reordered. `order_by` or `sort_key` would carry less
baggage.

### 3.3 Generated method names collide

`columnar_scan_status` derives from a *column* named `status`. `columnar_scan_host_time` derives
from an *index* named `host_time`. Both live in the same `impl` block.

```rust
columns: {
    id: u64 primary_key,
    status: String columnar(...),
    host_id: u64 columnar(...),
},
columnar_indexes: {
    status: { columns: [host_id], cluster_by: [host_id] },  // index also named `status`
},
```

This generates `columnar_scan_status` twice and fails to compile with an error pointing at
generated code, not at the user's declaration. Note that the existing `indexes:` block is keyed by
*column* name, so users already expect index and column namespaces to be related — which makes the
collision more likely, not less.

`columnar-index-plan.md` avoided this with namespaced accessors: `table.columnar().status()` and
`table.columnar_index().host_time()`. Adopt that. If flat methods are kept for any reason, the
collision must be a macro-expansion error with an actionable message.

---

## 4. DSL

### 4.1 `columns:` inside an index entry is ambiguous — delete it

> `columns` documents the fields served by the access path and is validated against the base
> column declarations.

"Documents" is not a semantics. Given `columns: [host_id, captured_at_ns, cpu_percent]` and
`cluster_by: [host_id, captured_at_ns]`, what is `cpu_percent` doing? Two readings:

- **Documentation only.** Then it is a second place to edit that will drift from `cluster_by`, and
  it should be deleted and derived.
- **Materialized into the index.** SQL Server's included columns; real storage cost, real
  covering-scan benefit. Then it must be named for that and its cost documented.

`columnar-index-plan.md` resolves this correctly — key columns *are* duplicated inside index
segments for pruning; non-key projected fields are *not*. So the honest form is: `cluster_by`
determines what is duplicated, and everything else is fetched from base column stores. `columns`
adds nothing.

**Fix.** Remove `columns`. Derive it from `cluster_by`. Reserve `include: [...]` for a future
genuine covering projection, with cost documented at introduction.

### 4.2 The identity width is scoped wrong

```rust
columnar_indexes: {
    immutable_sort_id: ImmutableSortId16,   // a table-wide storage property
    host_time: { ... },                     // an index
},
```

A table-wide storage property is living inside an index block, and shares a namespace with index
names — declare an index called `immutable_sort_id` and see what happens. Worse, a table with
columnar fields and *zero* columnar indexes must declare an empty `columnar_indexes { }` block
purely to set the width.

**Fix.** This repository already has a home for table-level knobs: the `config:` block, which today
carries `page_size` and `row_derives`. Put it there — `config: { columnar_row_id: ColumnRowId32 }`.
No new top-level grammar, no namespace sharing, and it composes with the existing 1.0 grammar
freeze on `attributes:` sections.

### 4.3 Per-field `chunk_rows` blocks the stated roadmap

The example uses 65,536 for three fields and 16,384 for `status`. No two of those columns share
chunk boundaries, so no pair of them can be zipped as aligned slices.

The guide's own follow-up list contains "null bitmaps and fixed-width vector kernels" and "batched
multi-column projection". Freely-chosen per-field chunk sizes fight both. `columnar-index-plan.md`
anticipates this ("the macro may warn when fields commonly queried together use different chunk
sizes"; "correctness must not depend on equal chunk sizes. Performance may.") but a warning is a
weak instrument for a property this consequential.

**Fix.** Table-level default in `config:`. Per-field override permitted only as a power-of-two
multiple or divisor of that default, so chunk boundaries nest and aligned access remains
constructible. Reject anything else at macro expansion with a message that explains why.

### 4.4 Boilerplate

Every field in the HFT example repeats `chunk_rows(16_384)`. That is the tell that the default
belongs at table level. With §4.3 applied, the common case becomes bare `columnar`:

```rust
instrument:   u32 columnar,
timestamp_ns: u64 columnar,
price_ticks:  i64 columnar,
```

### 4.5 Accepting inert compression policies is a footgun

The guide is admirably honest in prose:

> The declared compression policy is currently retained as metadata. [...] `auto`, `delta`, `rle`,
> and `dictionary` are not yet performance or space-saving claims.

But the compiler accepts them silently. A user writes `compression(delta)`, benchmarks, measures
nothing, and concludes the columnar path is slow. Honesty in the document does not reach the person
who copy-pastes the example.

`columnar-index-plan.md` had the right rule in its MVP list: `compression(none)` initially, with
`auto` "accepted only after it has a deterministic implementation."

**Fix.** Accept `compression(none)` — or omit the clause. Make the unimplemented policies a
macro-expansion error: `compression(delta) is not implemented; only compression(none) is
supported in this release`. It is a one-line change that converts a silent disappointment into an
actionable message. Re-enable each policy as its codec lands.

### 4.6 Repo-grammar constraints the guide does not account for

- The column parser requires the type to be a single `Ident`
  (`codegen/src/common/parser/columns.rs`). `diagnostic_blob: Vec<u8>` in the guide's headline
  example **does not parse today**, and `columnar-index-plan.md`'s `metrics: [i64; 10]` does not
  either. Array types currently require a `type` alias. Either the example is wrong or the parser
  change is a prerequisite that neither document lists.
- Postfix column attributes parse in fixed order (`primary_key`, then `autoincrement`/`custom`,
  then `optional`, then `using`). `columnar(...)` needs a defined position in that sequence — after
  `optional`, before `using`, is the natural slot. Neither document states it.
- Unrecognized trailing idents on a column are currently *silently skipped* by the comma handler.
  A typo like `columnr(...)` will parse and be ignored. Adding a new attribute makes this legacy
  leniency materially more dangerous; it should be tightened in the same change.
- `worktable_version!` shares the parser loop and must get an explicit
  "does not support `columnar_indexes`" arm, matching the existing convention for `queries` and
  `config`.

---

## 5. API surface

### 5.1 Scans return materialized collections

```rust
let statuses = table.columnar_scan_status()?;
```

For an analytical path this is self-defeating: it allocates a copy of the entire column before the
caller filters anything. `columnar-index-plan.md` had this right with `scan_batches` and a
selection vector, explicitly "never materializ[ing] `HistoricalCpuRow`".

**Fix.** Return chunk iterators or a `scan_batches` callback exposing `&[T]` plus a selection
bitmap. Operating on slices is the entire difference between a column store and a `Vec` of rows.

### 5.2 No prefix lookup on a composite clustered key

`columnar_select_host_time(host_id, captured_at_ns)` is full-key equality only. "All rows for this
host" — the canonical composite-index query — is unreachable. Range predicates can reasonably wait
for a follow-up; prefix equality cannot, because a composite key without prefix lookup is a
single-value key with extra steps.

`columnar-index-plan.md` had `.host_id_eq(42).timestamp_range(start..end)` — a predicate builder
that gets prefix and range for free and does not change arity when `cluster_by` changes.

### 5.3 Per-row revalidation in projection

`columnar_project_cpu_percent(&rows)` revalidates each pair individually. Over a large selection
that is a directory lookup per row on what is supposed to be the fast path. Batch the validation
against a snapshot, per §1.3.

### 5.4 Public API leaks the identity representation

`ImmutableSortId16(41_207)` is a tuple struct the user can construct and destructure, and
`ColumnarRowRef` exposes it as a public field. That pins the representation permanently — you
cannot add the generation counter §1.1 requires without a breaking change, and you cannot change
the slot/generation bit split.

**Fix.** Make `ColumnarRowRef` opaque: private fields, `Debug`/`Eq`/`Ord`/`Hash`, and a
`primary_key()` accessor. Users need to *hold* these, not *inspect* them.

### 5.5 Unspecified behaviours

- **Nulls.** `Option<u64> columnar(...)` — what happens today? `optional` is an existing column
  attribute, so this combination is reachable in v1. Null bitmaps are on the roadmap; the current
  behaviour still needs a defined answer, even if that answer is a compile error.
- **`String` layout.** Variable-width values in a fixed `chunk_rows` count — offsets plus a byte
  buffer, presumably, but unstated. `chunk_rows(16_384)` bounds the count, not the bytes, so a
  chunk has unbounded size.
- **No explicit row-gather fallback.** `columnar-index-plan.md` has `collect_rows()` for reaching
  non-columnar fields, deliberately named to make the cost visible. The guide has no equivalent, so
  `diagnostic_blob` is simply unreachable from a columnar selection.

---

## 6. The identity-width feature, reconsidered

`ImmutableSortId8` caps a table at 256 live rows. The directory and index footprint saved at that
scale is a rounding error, and it costs a fourth monomorphized codegen path plus its share of the
test matrix. It also leaves no bits for the generation counter §1.1 requires.

More fundamentally, the capacity contract is on *simultaneously assigned* positions — peak
concurrent live rows — which is the hardest quantity in any system to predict or test. A workload
that passes every test at 60,000 rows fails in production during one burst, and the failure mode is
insert rejection.

**Recommendation.**

- Drop `ImmutableSortId8`.
- Keep 16 only as a documented embedded/test width, not a recommended production choice.
- Make 32 the default and 64 the explicit large option, with generation bits carved from the top
  (see grammar §2.1).
- Ship a high-water-mark metric — `columnar_slots_in_use()` / `columnar_slots_high_water()` — so
  applications can alarm at 80% rather than discover the ceiling by hitting it.
- Keep the exhaustion error. Returning `ColumnRowIdExhausted` rather than silently widening is
  correct and is one of the draft's better decisions.

---

## 7. What the draft gets right

Worth preserving explicitly, because the revision should not lose it:

- **Additive, opt-in per field.** No table-level `layout: columnar` switch. Correct, and matches
  how SQL Server columnstore indexes, Oracle In-Memory Column Store, and TiFlash all work.
- **The primary key stays authoritative.** Non-negotiable and correctly non-negotiated.
- **Row store remains the recovery boundary.** Deriving columnar state and rebuilding on load,
  rather than inventing a durable format in v1, is the right slice — it keeps existing persisted
  tables loadable and defers the format decision until benchmarks can inform it.
- **Explicit error over silent widening.** No wrapping, no truncation, no automatic migration.
- **Prose honesty about compression.** The refusal to claim working codecs is the strongest
  paragraph in the document. §4.5 asks only that the compiler match the prose.
- **"What this design deliberately avoids."** Every design document should have this section. Keep
  it, and add the new non-claims (no global scan ordering, no snapshot isolation, no cross-restart
  identity stability).

---

## 8. Findings summary

| # | Finding | Severity | Fix |
|---|---|---|---|
| 1.1 | Slot reuse reintroduces ABA on delete/reinsert | **Blocking** | Generation tag + deferred reclamation |
| 1.2 | Identity not stable across restart despite the name | **High** | Rename; epoch-tag or forbid serialization |
| 1.3 | No multi-column read consistency | **High** | Reinstate plan's lock ordering; scoped read guard |
| 1.4 | Dirty rebuild is O(rows) on the reader | **High** | Per-chunk dirtiness; explicit `rebuild_columnar()` |
| 1.5 | Scan-under-concurrent-update ordering unspecified | Medium | State the guarantee |
| 2 | Aligned vs gathered scan behaviour unstated | **High** | State it; benchmark before claiming |
| 3.1 | `ImmutableSortId` misnames both properties | Medium | `ColumnRowId` (already in tree) |
| 3.2 | `cluster_by` implies base reordering | Low | Document, or rename `order_by` |
| 3.3 | Column/index method namespace collision | **High** | Namespaced builders |
| 4.1 | `columns:` in index entry is ambiguous | **High** | Delete; derive from `cluster_by` |
| 4.2 | Identity width scoped inside index block | Medium | Move to `config:` |
| 4.3 | Free per-field `chunk_rows` blocks vector kernels | Medium | Table default; nested overrides only |
| 4.4 | Per-field boilerplate | Low | Bare `columnar` form |
| 4.5 | Inert compression policies accepted silently | Medium | Compile error until implemented |
| 4.6 | Examples don't parse under the current column grammar | **High** | Fix examples or list parser work |
| 5.1 | Scans materialize whole columns | **High** | Batch/chunk iterators |
| 5.2 | No prefix lookup on composite clustered key | **High** | Predicate builder |
| 5.3 | Per-row revalidation cost | Medium | Batch against a snapshot |
| 5.4 | Identity representation leaked publicly | Medium | Opaque `ColumnarRowRef` |
| 5.5 | Nulls, `String` layout, row-gather fallback unspecified | Medium | Specify |
| 6 | 8-bit width not worth its cost; capacity contract untestable | Medium | Drop 8; add high-water metric |

---

## 9. Next documents

- [`columnar-dsl-grammar-v2.md`](columnar-dsl-grammar-v2.md) — the revised grammar, with each
  change traced to a finding above.
- [`columnar-fields-and-indexes-guide-v2.md`](columnar-fields-and-indexes-guide-v2.md) — the
  rewritten guide at the same register as the August draft, for circulation.
