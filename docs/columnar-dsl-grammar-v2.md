# Columnar DSL — revised grammar (v2)

**Status:** Design proposal. Supersedes the syntax in the August 2026 guide draft and refines
[`columnar-index-plan.md`](columnar-index-plan.md).

**Companion:** [`columnar-dsl-review.md`](columnar-dsl-review.md) — every change below is traced to
a numbered finding there.

**Compatibility:** additive. A schema with no `columnar` attribute and no `columnar_indexes` block
parses and expands exactly as today.

---

## 1. Grammar

Notation follows the existing parser's shape: bare idents, brace-delimited groups, trailing commas
optional everywhere. Existing productions are shown only where the revision touches them.

```ebnf
worktable_macro   ::= "name" ":" ident ","
                      [ "version" ":" int_literal "," ]
                      [ "persist" ":" bool_ident "," ]
                      block*

block             ::= columns_block
                    | indexes_block
                    | columnar_indexes_block          (* new *)
                    | queries_block
                    | config_block

(* ---- columns: the `columnar` attribute ------------------------------- *)

columns_block     ::= "columns" ":" "{" column_decl { "," column_decl } [ "," ] "}"

column_decl       ::= ident ":" type_ident
                      [ "primary_key" ]
                      [ "autoincrement" | "custom" ]
                      [ "optional" ]
                      [ columnar_attr ]               (* new — fixed position *)
                      [ "using" ident ]

columnar_attr     ::= "columnar" [ "(" columnar_opt { "," columnar_opt } [ "," ] ")" ]

columnar_opt      ::= "chunk_rows" "(" int_literal ")"
                    | "compression" "(" compression_policy ")"

compression_policy ::= "none"                          (* only policy accepted in v1 *)
                    | "auto" | "delta" | "rle" | "dictionary"   (* reserved; hard error *)

(* ---- columnar_indexes ------------------------------------------------ *)

columnar_indexes_block
                  ::= "columnar_indexes" ":" "{" columnar_index { "," columnar_index } [ "," ] "}"

columnar_index    ::= ident ":" "{" columnar_index_body "}"

columnar_index_body
                  ::= "cluster_by" ":" ident_list [ "," ]
                      [ "include" ":" ident_list [ "," ] ]      (* reserved; hard error in v1 *)

ident_list        ::= "[" ident { "," ident } [ "," ] "]"

(* ---- config: table-level columnar settings --------------------------- *)

config_block      ::= "config" ":" "{" config_entry { "," config_entry } [ "," ] "}"

config_entry      ::= "page_size" ":" int_literal
                    | "row_derives" ":" ident { "," ident }
                    | "columnar_row_id" ":" row_id_type            (* new *)
                    | "columnar_chunk_rows" ":" int_literal        (* new *)

row_id_type       ::= "ColumnRowId16" | "ColumnRowId32" | "ColumnRowId64"
```

### Changes at a glance

| Change | Finding | Was | Is |
|---|---|---|---|
| Identity type renamed | 3.1 | `ImmutableSortId{8,16,32,64}` | `ColumnRowId{16,32,64}` |
| 8-bit width removed | 6 | `ImmutableSortId8` | — |
| Identity width relocated | 4.2 | inside `columnar_indexes` | `config.columnar_row_id` |
| Chunk size default | 4.3, 4.4 | per-field, required | `config.columnar_chunk_rows`, per-field override |
| Chunk size override constrained | 4.3 | any value | power-of-two multiple/divisor of default |
| Bare attribute form | 4.4 | `columnar(...)` mandatory | `columnar` alone is legal |
| `columns:` in index removed | 4.1 | `columns: [...]` + `cluster_by: [...]` | `cluster_by: [...]` only |
| `include:` reserved | 4.1 | — | parsed, hard error, defined for later |
| Compression narrowed | 4.5 | all policies accepted, inert | `none` only; others are errors |
| Attribute position fixed | 4.6 | unstated | after `optional`, before `using` |

---

## 2. Semantics

### 2.1 `ColumnRowId` — identity, and the ABA fix

Finding 1.1 is the blocking defect: bounded identifiers force slot reuse, and slot reuse plus
delete/reinsert of the same primary key lets a retained reference silently alias a new row
generation. The fix is two independent mechanisms, both required.

**Mechanism 1 — generation tag.** The identity token is split. The low bits address a slot; the
high bits carry a generation counter incremented every time that slot is freed.

| Type | Slot bits | Live slots | Generation bits | Frees before wrap | Intended use |
|---|---:|---:|---:|---:|---|
| `ColumnRowId16` | 12 | 4,096 | 4 | 16 | Tests and hard-bounded embedded tables only |
| `ColumnRowId32` | 24 | 16,777,216 | 8 | 256 | **Default.** General-purpose |
| `ColumnRowId64` | 48 | 281,474,976,710,656 | 16 | 65,536 | Explicit very-large logical range |

A retained reference is valid only if slot **and** generation match the directory. The delete /
reinsert sequence in review §1.1 now fails revalidation at `t4` instead of aliasing.

**Mechanism 2 — deferred reclamation.** A generation counter alone leaves a wrap window: after
2^g frees the counter returns to a value a very old reference could match. So a freed slot does not
return to the allocator until no reader that could hold a reference to it remains in flight —
epoch-based reclamation, quiescent-state detection, or extending the existing per-key mutation gate
over the read window, whichever fits the engine.

Together these give the property `columnar-index-plan.md` stated but the August draft dropped:
*IDs are not immediately reused, and a stale reference is detectable rather than silently valid.*

**Design fork worth deciding explicitly.** If deferred reclamation is implemented well, generation
tags become belt-and-braces for *retained* references specifically — references the caller holds
across a read boundary. If the API forbids retaining references beyond a read guard (§2.4),
generation tags could be dropped and the full width used for slots. That is a real alternative. It
trades API ergonomics for address space, and it should be chosen deliberately rather than by
default. The bit split is invisible to users either way, because `ColumnRowRef` is opaque (§2.5).

**Lifetime contract.** A `ColumnRowId` is stable for one row's lifetime *within one process
incarnation*. Columnar state is derived and rebuilt on load, so identifiers are reassigned across a
restart (finding 1.2). `ColumnRowRef` therefore does not implement `Serialize`, and carries an
incarnation epoch so a reference that somehow crosses a restart fails loudly.

**Exhaustion.** Unchanged from the draft, which had this right:

```rust
WorkTableError::ColumnRowIdExhausted(bits)
```

The insert rolls back. Existing rows remain valid. WorkTable never wraps, truncates, evicts another
row, or panics. Because the capacity contract is on *simultaneously live* slots — the hardest
quantity to predict (finding 6) — two observability methods are generated alongside:

```rust
table.columnar_slots_in_use()    -> usize
table.columnar_slots_high_water() -> usize
```

### 2.2 The `columnar` attribute

Marks a field for a derived column replica. The row store remains authoritative.

```rust
cpu_percent: f32 columnar,                                  // all defaults
status:      String columnar(chunk_rows(16_384)),           // override chunk size
host_id:     u64 columnar(compression(none)),               // explicit policy
```

Position in the attribute sequence is fixed (finding 4.6): after `optional`, before `using`.
`optional` composes — `latency_ms: u64 optional columnar` is legal, and the generated column
carries a validity bitmap.

`chunk_rows(N)` defaults to `config.columnar_chunk_rows`, itself defaulting to 65,536. An override
must be a power-of-two multiple or divisor of the table default, so chunk boundaries nest and
aligned multi-column access stays constructible (finding 4.3):

```rust
// config.columnar_chunk_rows = 65_536
chunk_rows(16_384)   // ok  — 65_536 / 4
chunk_rows(131_072)  // ok  — 65_536 * 2
chunk_rows(50_000)   // error: not a power-of-two multiple or divisor of 65_536
```

`compression(none)` is the only accepted policy in v1. The reserved policies parse and then fail at
macro expansion with an actionable message (finding 4.5):

```text
error: compression(delta) is declared but not implemented in this release.
       Only compression(none) is currently supported.
       Codecs apply to sealed immutable chunks; see docs/columnar-index-plan.md.
```

This is deliberately stricter than the draft, which accepted every policy as inert metadata. A
compile error is recoverable in seconds; a silently inert policy is discovered by a benchmark that
concludes the feature is slow.

### 2.3 `columnar_indexes`

```rust
columnar_indexes: {
    host_time: {
        cluster_by: [host_id, captured_at_ns],
    },
},
```

`cluster_by` is the ordered key and the only required member. Every field it names must carry
`columnar`. The set of columns served by the access path is *derived* from `cluster_by` — the
draft's separate `columns:` list is removed (finding 4.1) because it had no semantics distinct
from documentation and would drift.

Key values are duplicated inside index segments so pruning does not require touching base columns.
Non-key fields are never duplicated; they are gathered from their base column stores by selected
identity. This is the model `columnar-index-plan.md` describes, and removing `columns:` makes the
declaration match it.

`include: [...]` is parsed and rejected in v1, reserving the syntax for a genuine covering
projection later:

```text
error: `include` is reserved for a future covering projection and is not implemented.
       Non-key fields are gathered from base column stores; declaring them is not required.
```

**What `cluster_by` does not mean.** Base column chunks remain in canonical `ColumnRowId` order.
`cluster_by` orders the index, not the base data (finding 3.2). The name is retained for
familiarity; the consequence is documented wherever it appears.

### 2.4 Generated API — namespaced, batched, guarded

Flat `columnar_scan_*` methods are replaced by two namespaced accessors. This resolves the
column/index method collision (finding 3.3), makes prefix and range predicates expressible
(finding 5.2), and restores the batch surface (finding 5.1).

```rust
// Direct column scan — no index required.
table.columnar()
    .cpu_percent()
    .scan_batches(|batch| aggregate(batch.values(), batch.selection()))?;

// Clustered index: prefix equality, then range, then projection.
table.columnar_index()
    .host_time()
    .host_id_eq(42)                       // prefix — was unreachable in the draft
    .captured_at_ns_range(start..end)
    .project(|c| (c.cpu_percent(), c.status()))
    .scan_batches(|batch| {
        aggregate(batch.cpu_percent(), batch.status(), batch.selection());
    })?;

// Explicit, visibly expensive fallback for non-columnar fields.
let rows = table.columnar_index()
    .host_time()
    .host_id_eq(42)
    .collect_rows()?;                     // gathers diagnostic_blob from DataPages
```

Predicate method names are generated per `cluster_by` column: `<column>_eq`, `<column>_range`.
Adding a column to `cluster_by` adds a method rather than changing an arity, so existing callers
keep compiling and keep meaning what they meant.

**Read guard.** A multi-column projection is one scoped operation, not N independent ones
(finding 1.3). Chunk read locks are acquired in deterministic `(column_id, chunk_id)` order for the
whole projection, so a result set cannot combine values from different committed versions of the
same row. Retaining values beyond the guard is supported — they are owned copies — but retaining
*references* is what §2.1's generation tag protects.

### 2.5 `ColumnRowRef` is opaque

```rust
pub struct ColumnRowRef<Pk> { /* private */ }

impl<Pk> ColumnRowRef<Pk> {
    pub fn primary_key(&self) -> &Pk;
}
// Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord
// deliberately NOT Serialize / Deserialize
```

The draft exposed `ImmutableSortId16(41_207)` as a public tuple struct (finding 5.4), which would
have frozen the representation before the generation counter §2.1 needs could be added. Users hold
these; they do not inspect them.

### 2.6 Rebuild is per-chunk

An in-place archived-field change marks only the affected field's affected chunks dirty, not the
whole table (finding 1.4). Rebuild cost is proportional to what changed. Applications that would
rather schedule the cost than pay it on a reader get explicit control:

```rust
table.columnar_is_dirty() -> bool
table.rebuild_columnar()  -> Result<(), WorkTableError>
```

---

## 3. Worked example

The August draft's `HistoricalCpu` schema, revised.

```rust
use worktable::prelude::*;
use worktable::worktable;

type DiagnosticBlob = Vec<u8>;   // column types must be a single ident — see §5

worktable!(
    name: HistoricalCpu,
    persist: true,

    columns: {
        id: u128 primary_key,

        host_id:        u64 columnar,
        captured_at_ns: u64 columnar,
        cpu_percent:    f32 columnar,

        // Smaller chunks for a wide variable-length column. 65_536 / 4.
        status: String columnar(chunk_rows(16_384)),

        // Ordinary row-only data remains ordinary.
        diagnostic_blob: DiagnosticBlob,
    },

    columnar_indexes: {
        host_time: {
            cluster_by: [host_id, captured_at_ns],
        },
    },

    config: {
        columnar_row_id: ColumnRowId32,
        columnar_chunk_rows: 65_536,
    },
);
```

Both `config` entries are shown for illustration; both are the defaults and both may be omitted.
Compare with the draft, which required `chunk_rows` and `compression` on all four fields, a
`columns:` list duplicating `cluster_by`, and a storage-width setting inside the index block.

### Bounded window (the HFT pattern, revised)

```rust
worktable!(
    name: OrderBookEvents,

    columns: {
        event_id:     u128 primary_key,
        instrument:   u32 columnar,
        timestamp_ns: u64 columnar,
        price_ticks:  i64 columnar,
    },

    columnar_indexes: {
        instrument_time: {
            cluster_by: [instrument, timestamp_ns],
        },
    },

    config: {
        columnar_chunk_rows: 16_384,
        columnar_row_id: ColumnRowId32,
    },
);
```

The draft recommended a 16-bit identifier here. This revision does not, for two reasons. First,
12 usable slot bits after the generation split is 4,096 live rows, not 65,536 — the bounded-window
argument no longer reaches. Second, and more importantly, this is exactly the workload that
churns slots hardest: a fixed live window means constant delete-and-insert, which is what drives
generation wrap and what makes a peak-concurrency capacity contract fail during a burst rather
than in testing. `ColumnRowId32` costs three bytes per live row more and removes the entire class
of problem. Take the 16-bit width only when the domain itself guarantees the bound and the table
is small enough that the saving is measurable.

---

## 4. Validation

Rejected at macro expansion, each with a message naming the offending declaration:

**Structural**

- unknown field named in `cluster_by`;
- a `cluster_by` field lacking the `columnar` attribute;
- empty `cluster_by`, or an empty `columnar_indexes` entry;
- duplicate field within one `cluster_by`;
- duplicate columnar-index name;
- `columnar_indexes` present with no `columnar` field anywhere.

**Attribute**

- `chunk_rows(0)`, or a value that is not a power-of-two multiple or divisor of
  `config.columnar_chunk_rows`;
- `compression` with any policy other than `none` (§2.2);
- `include:` in a columnar index (§2.3);
- `columnar` on a field type without a column encoding, naming the type and the supported set;
- `columnar` on the primary key — identity participation is implicit and does not need declaring.

**Grammar hygiene** (finding 4.6)

- an unrecognized postfix ident on a column is now a hard error rather than being silently skipped
  by the comma handler. `columnar` is a near-miss magnet (`columner`, `colunmar`) and the existing
  leniency would swallow every typo;
- `columnar` appearing out of sequence, with a message showing the required order;
- `worktable_version!` gains an explicit
  `worktable_version! does not support columnar_indexes` arm, matching how it already rejects
  `queries` and `config`.

---

## 5. Prerequisite the draft did not list

The column parser requires the type to be a single `Ident`
(`codegen/src/common/parser/columns.rs`). Consequently:

- `diagnostic_blob: Vec<u8>` in the August draft **does not parse today**;
- `metrics: [i64; 10]` in `columnar-index-plan.md` **does not parse today**.

Both documents use such types in their headline examples. Until the parser accepts type paths and
array types, examples must route through a `type` alias, as §3 does. Extending the parser is a
reasonable prerequisite to schedule — it is independently useful — but it is work, and it belongs
on the plan rather than being assumed.

---

## 6. Codegen impact

Beyond the entry points `columnar-index-plan.md` already identifies:

| Area | File | Change |
|---|---|---|
| Column attribute | `codegen/src/common/parser/columns.rs` | Parse `columnar` at its fixed position; tighten unknown-ident handling |
| Column model | `codegen/src/common/model/column.rs` | `Option<ColumnarFieldConfig>` on `Row` |
| Index block | `codegen/src/common/parser/columnar_indexes.rs` *(new)* | Parse the block; reject `include` |
| Index model | `codegen/src/common/model/columnar_index.rs` *(new)* | `{ name, cluster_by }` |
| Config | `codegen/src/common/parser/config.rs` | Two new keys; note the greedy `row_derives` ident scan must learn to stop at them |
| Dispatch | `codegen/src/worktable/mod.rs` | New arm in the block loop; cross-block validation; both `expand` signatures gain a parameter |
| Dispatch (dead copy) | `codegen/src/generators/in_memory/mod.rs` | The `#[allow(dead_code)]` duplicate of the dispatch loop needs the same arm |
| Version macro | `codegen/src/worktable_version/mod.rs` | Explicit rejection arm |
| Naming | `codegen/src/common/name_generator.rs` | `<Name>ColumnRowId`, `<Name>ColumnDirectory`, `<Name>ColumnStores`, `<Name>ColumnarIndexes`, `<Name>ColumnarScan` |

`config.row_derives` currently consumes idents greedily until it recognizes another config key by
name. Adding `columnar_row_id` and `columnar_chunk_rows` to that block means adding them to that
stop set, or `row_derives: Default, columnar_row_id: ColumnRowId32` will silently absorb the
second key as a derive. This is a real hazard in the existing parser and should get a test.

---

## 7. Traceability

| Finding | Severity | Addressed in |
|---|---|---|
| 1.1 ABA on delete/reinsert | Blocking | §2.1 — generation tag + deferred reclamation |
| 1.2 Identity unstable across restart | High | §2.1 — lifetime contract, no `Serialize`, incarnation epoch |
| 1.3 No multi-column read consistency | High | §2.4 — scoped read guard, deterministic lock order |
| 1.4 Dirty rebuild is O(rows) | High | §2.6 — per-chunk dirtiness, explicit rebuild |
| 1.5 Concurrent scan ordering | Medium | Guide v2 — stated as a per-segment guarantee |
| 2 Aligned vs gathered | High | Guide v2 — "access paths and what they cost" |
| 3.1 `ImmutableSortId` misnamed | Medium | §1 — `ColumnRowId` |
| 3.2 `cluster_by` implication | Low | §2.3 — documented at the point of use |
| 3.3 Method namespace collision | High | §2.4 — namespaced builders |
| 4.1 `columns:` ambiguous | High | §2.3 — removed; `include` reserved |
| 4.2 Width scoped in index block | Medium | §1, §2.1 — `config.columnar_row_id` |
| 4.3 Free chunk sizes | Medium | §2.2 — nested override rule |
| 4.4 Boilerplate | Low | §2.2 — bare `columnar` |
| 4.5 Inert compression | Medium | §2.2 — hard error |
| 4.6 Examples don't parse | High | §5 — prerequisite stated |
| 5.1 Materialized scans | High | §2.4 — `scan_batches` |
| 5.2 No prefix lookup | High | §2.4 — predicate builder |
| 5.3 Per-row revalidation | Medium | §2.4 — batch validation under the guard |
| 5.4 Leaked representation | Medium | §2.5 — opaque `ColumnRowRef` |
| 5.5 Nulls / `String` / fallback | Medium | §2.2 (`optional`), §2.4 (`collect_rows`), guide v2 |
| 6 8-bit width, capacity contract | Medium | §2.1 — dropped; high-water metrics |
