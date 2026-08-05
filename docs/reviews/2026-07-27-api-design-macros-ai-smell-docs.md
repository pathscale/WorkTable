# WorkTable Review: API Design, Macros, AI Smell, Docs

**Date:** 2026-07-27
**Scope:** `codegen/**` (the whole proc-macro crate), `src/lib.rs`, `src/table/select/**`, `src/index/**`,
`src/in_memory/{row,mod}.rs`, `src/persistence/mod.rs`, error types across `src/**`, `README.md`,
`docs/queries.md`, `CLAUDE.md`, `AGENTS.md`, `CHANGELOG.md`, plus a census pass over `tests/**`.
**Commit:** `25074ae` (HEAD moved from `66d8cfc` to `25074ae` mid-review; another session committed the
index-reconstruction WIP while I was reading. Nothing I cite is in that diff.)
**Reviewer slice:** api-design-macros-ai-smell-docs. Sibling slices cover performance, concurrency and
`unsafe`; I deliberately did not judge lock protocols, page layout or the CDC event ordering logic.

## Summary

- The proc-macro crate is the entire public API of this project. A user writes `worktable!` and gets
  everything else. That makes the macro's **input validation the single highest-leverage surface in the
  repo, and it is the weakest**. Malformed or merely unsupported input reaches `todo!()`, `.unwrap()` and
  `.expect()` in the generator rather than a `syn::Error`, so users get `proc macro panicked` with no span
  instead of a pointed compile error.
- `codegen/` has **zero** compile-fail / `trybuild` tests, no `[dev-dependencies]`, and no `#[cfg(test)]`
  block anywhere under `codegen/src/generators/**` (about 50 files that emit all the real code). The
  generated tokens are never parsed back into a `syn` AST by anything. Error messages and spans are
  entirely unverified.
- **Two indexes declared on the same column silently collapse into one** (`codegen/src/common/parser/index.rs:88-96`
  keys the index map by the *column* ident, not the index name). This is a silent semantic loss in the DSL,
  not a compile error.
- The three generator trees (`in_memory`, `persist`, `read_only`) contain roughly **1,000 lines that are
  byte-identical apart from the `impl <X>Generator` line and one `use`**. A single trait with default
  methods would delete most of it.
- `README.md`'s query-API section describes an API that does not exist: `where_by(range, "column")` and
  `order_by(Order::Desc, "column")` versus the real `where_by(predicate_closure)` / `order_on(RowFields::X, Order)`.
  `docs/queries.md`'s only worked example does not compile. `CHANGELOG.md` stops at 0.4.1 for a 0.9.1 crate.
  Agents are told by `AGENTS.md:15` that "Docs describe what is true now"; here they do not.
- Top three things to do: (1) replace every reachable `todo!`/`unwrap`/`expect` in `codegen/` with spanned
  `syn::Error`s and add a `trybuild` UI suite; (2) fix the index-name/column-key collapse; (3) rewrite the
  README query-API and internals sections against the code.

## Findings

### [SEV-1] Proc macro panics instead of emitting a diagnostic on ordinary user mistakes

- **ID:** `worktable-api-macros-01`
- **Severity:** High
- **Category:** Design / AI-smell
- **Confidence:** High
- **Location:** `codegen/src/worktable/mod.rs:43`; `codegen/src/generators/persist/queries/in_place.rs:41,45,67`;
  `codegen/src/generators/in_memory/queries/in_place.rs:39,43`;
  `codegen/src/generators/persist/queries/update.rs:150,179,182,185`;
  `codegen/src/generators/in_memory/queries/update.rs:182,185`;
  `codegen/src/generators/persist/index/usual.rs:52,98,178,321`;
  `codegen/src/persist_index/generator.rs:46,56,64`; plus ~60 further `.unwrap()`/`.expect()` sites
  (`rg -c '\.unwrap\(\)|\.expect\(' codegen/src` totals 180 across 46 files).
- **What:** The macro's failure mode for unsupported-but-plausible input is a panic, not a `syn::Error`.
  Concrete reachable cases:
  - `worktable!(name: T)` with no `columns` block: `columns.expect("defined")` at
    `codegen/src/worktable/mod.rs:43` panics.
  - An `in_place` query keyed by an indexed column, or on a multi-column primary key:
    `if index.is_unique { todo!() } else { todo!() }` at `.../queries/in_place.rs:41` and `todo!()` at `:45`.
    `docs/queries.md:47` documents the restriction ("only `by {pk_field}` queries are supported") but the
    enforcement is a panic.
  - An `update` query keyed by a non-indexed, non-primary-key column: `todo!()` at
    `.../queries/update.rs:182`, and `:185` for the multi-PK case.
  - A query or index referencing a column that does not exist: `self.columns.columns_map.get(c).unwrap()`
    (`.../queries/in_place.rs:67`, `.../queries/update.rs:150`, `.../index/usual.rs:52`, and ~15 more).
    `parse_indexes` (`codegen/src/common/parser/index.rs:56-96`) never checks the referenced column exists.
  - `#[derive(PersistIndex)]` on a struct whose field type is not a generic path:
    `unreachable!()` at `codegen/src/persist_index/generator.rs:46,56,64`.

  Exactly one site does this correctly: `codegen/src/generators/persist/queries/type.rs:93` uses
  `.ok_or(syn::Error::new(i.span(), "Unexpected column name"))?`. The rest of the codebase never adopted
  that pattern, which is the classic signature of code written in several passes and never unified.
- **Why it matters:** Every one of these is a mistake a first-time user makes on day one. A panic in a proc
  macro surfaces as `error: proc macro panicked / message: not yet implemented` pointing at the whole
  `worktable!(...)` invocation, with no indication of which column, query or line is at fault. For a crate
  whose entire selling point is "macros that smell like SQL", the diagnostic quality *is* the product.
- **Fix:** Mechanical but wide. (a) Thread `syn::Result` through the generator functions that already return
  it (most `gen_*` fns already do) and replace `columns_map.get(x).unwrap()` with
  `.ok_or_else(|| syn::Error::new(x.span(), format!("unknown column `{x}`")))?`. (b) Replace each `todo!()`
  with `Err(syn::Error::new(op.by.span(), "`in_place` queries are currently only supported `by` the primary key"))`.
  (c) Validate the whole declaration once, after parsing, in `Columns`: every index field and every query
  `by`/column ident must exist in `columns_map`; emit one spanned error per offender. That single validation
  pass makes most of the downstream `unwrap()`s provably unreachable rather than merely hoped-to-be.
- **Effort:** L
- **Blast radius:** `codegen/` only, no public API change. Users currently relying on a panic get a better
  error.

### [SEV-2] Two indexes on the same column silently collapse; a duplicated index name produces a nonsense error

- **ID:** `worktable-api-macros-02`
- **Severity:** High
- **Category:** Correctness / Design
- **Confidence:** High
- **Location:** `codegen/src/common/parser/index.rs:44-49` and `:88-96`
- **What:** `parse_index` returns `(row_name.clone(), Index { name: ident, field: row_name, is_unique })`,
  that is, it keys the returned pair by the **column** ident, and `parse_indexes` does
  `rows.insert(name, row)` into an `IndexMap`. So:

  ```rust
  indexes: {
      by_val_unique: val unique,
      by_val: val,            // silently replaces the entry above
  }
  ```

  leaves exactly one index, non-unique, named `by_val`. The uniqueness constraint the user asked for is gone
  with no warning. Conversely, reusing an index *name* across two columns (`my_idx: a, my_idx: b`) produces
  two map entries whose `Index::name` is the same ident, and the index struct generator
  (`codegen/src/generators/persist/index/usual.rs`) emits a struct with two fields called `my_idx`, so the
  user gets `error: field `my_idx` is already declared` pointed at macro-generated tokens.
- **Why it matters:** A dropped unique index is a silent correctness hole: inserts that should fail with
  `WorkTableError::AlreadyExists` now succeed, and the on-disk index file for the dropped index is never
  created. Nothing in the test suite covers multiple indexes on one column
  (I found no such declaration under `tests/`).
- **Fix:** Key the map by `Index::name` (the index identifier) and carry the column in `Index::field`, which
  is already the field's purpose. Then add an explicit duplicate check on both name and (name, column) with
  `syn::Error::new(ident.span(), "index `x` is already declared")`. Downstream consumers iterate
  `self.columns.indexes.values()` or `.iter()` and use `idx.name` / `idx.field`, so most call sites are
  already name-driven; audit `codegen/src/generators/*/table/index_fns.rs:19-48`, which uses the map key `i`
  as the column, and switch it to `idx.field`. Needs a short design pass because `select_by_<x>` naming
  currently derives from the key.
- **Effort:** M
- **Blast radius:** `codegen/src/common/parser/index.rs`, the three `table/index_fns.rs`, the three
  `index/usual.rs`, `index/cdc.rs`, `persist_index/*`. Potentially a behaviour change for anyone who
  accidentally relied on the collapse. Not a source-level breaking change for correct declarations.

### [SEV-3] README describes a query API that does not exist

- **ID:** `worktable-api-macros-03`
- **Severity:** High
- **Category:** Docs
- **Confidence:** High
- **Location:** `README.md:250`, `README.md:284-305`, `README.md:311-357`, `README.md:386-392`;
  code at `src/table/select/query.rs:44-84`, `codegen/src/generators/persist/queries/select.rs:27-38`,
  `src/table/mod.rs:32-63`, `src/table/mod.rs:514-525`, `src/in_memory/pages.rs:34-51`
- **What:** Stale claims, each with both line references:

  | README | Claim | Reality |
  |---|---|---|
  | `README.md:284` | "select_all query returns `Result<SelectQueryBuilder>`" | Returns `SelectQueryBuilder` directly, no `Result` (`codegen/src/generators/persist/queries/select.rs:27`) |
  | `README.md:250` | `select_all<'a>(&'a self) -> SelectQueryBuilder<'a, <Name>Row, Self>` | `SelectQueryBuilder<Row, impl DoubleEndedIterator<Item = Row> + '_, ColumnRange, RowFields>`, four type params, no lifetime param (`src/table/select/query.rs:6-11`) |
  | `README.md:287` | `.where_by(std::ops::Range, "column")` | `where_by<F>(self, predicate: F) where F: FnMut(&Row) -> bool` (`src/table/select/query.rs:76-84`). It takes a closure, not a range and a string. |
  | `README.md:289` | `.order_by(Order::Desc, "column")` | **There is no `order_by` method.** It is `order_on(column: RowFields, order: Order)`, typed column enum first (`src/table/select/query.rs:54`) |
  | `README.md:288,294-302` | Range filtering via `where_by` | Ranges go through `range_on(column: RowFields, range: R)` (`src/table/select/query.rs:62`) |
  | `README.md:247-249` | `upsert`/`update`/`delete` shown as sync | All three are `async` (`codegen/src/generators/persist/table/impls.rs:261`, `.../queries/update.rs:83`, `.../queries/delete.rs:44`) |
  | `README.md:317-323` | `WorkTable` fields `pk_map`, `lock_map` | Fields are `primary_index`, `lock_manager`, plus an undocumented `update_state` (`src/table/mod.rs:49-62`). `WorkTable` also has 9 generic parameters, none mentioned. |
  | `README.md:326` | "`lock_map: LockMap` // from indexset crate" | `LockMap` is WorkTable's own type, `src/lock/mod.rs`, nothing to do with indexset |
  | `README.md:337` | `empty_links: Stack<Link>` | `empty_links: EmptyLinkRegistry<DATA_LENGTH>`, and there is an undocumented `empty_pages: Arc<RwLock<VecDeque<PageId>>>` (`src/in_memory/pages.rs:41-43`) |
  | `README.md:387-392` | `WorkTableError` has 4 variants ending in `PagesError` | 7 variants: `NotFound`, `AlreadyExists(String)`, `PrimaryAlreadyExists`, `SerializeError`, `SecondaryIndexError`, `PrimaryUpdateTry`, `PagesError(..)` (`src/table/mod.rs:514-525`). `AlreadyExists` now carries the index name. |
  | `README.md:347-356` | `DataPages` method list | Missing `select_non_ghosted`, `select_raw`, the vacuum entry points; still lists the pre-`EmptyLinkRegistry` shape |
- **Why it matters:** `AGENTS.md:15` makes doc accuracy an invariant, and `AGENTS.md:64-66` explicitly tells
  agents to trust repo docs over private memory. An agent that reads `README.md:287-302` will write
  `where_by(0..10u64, "test")`, which does not compile, and will then go hunting for a build problem that
  does not exist. This is the single highest-cost stale doc in the repo.
- **Fix:** Rewrite `README.md:241-305` from `src/table/select/query.rs` and the generated `select_all` /
  `select_by_*` signatures. Delete `README.md:309-392` ("WorkTable internals structure") outright: it
  enumerates private fields and method lists that will keep rotting. Replace it with a link to `docs.rs`
  and a two-paragraph conceptual overview. Mechanical.
- **Effort:** M
- **Blast radius:** Docs only.

### [SEV-4] The proc-macro crate has no negative tests, no UI tests, and no coverage of the generators

- **ID:** `worktable-api-macros-04`
- **Severity:** High
- **Category:** Maintainability
- **Confidence:** High
- **Location:** `codegen/Cargo.toml` (no `[dev-dependencies]`, no `codegen/tests/`); zero `#[cfg(test)]` under
  `codegen/src/generators/**`; existing weak tests at `codegen/src/persist_table/mod.rs:42-98`,
  `codegen/src/persist_index/mod.rs:29-84`, `codegen/src/worktable_version/mod.rs:43-153`
- **What:** No `trybuild`, no `macrotest`, no `compile_fail`, no `tests/ui/` anywhere in the repo. Of the 44
  unit tests in `codegen/`, 15 assertions across 5 tests are `output.contains("...")` string greps on a
  stringified `TokenStream`. `codegen/src/persist_table/mod.rs:68` asserts on
  `"fn into_worktable (self)"`, which depends on `quote!`'s pretty-print spacing and will break on any
  formatting change while telling you nothing about semantics. `codegen/src/persist_index/mod.rs:44` is a
  test literally named `test` whose entire body is `let _res = expand(input).unwrap();`. Nothing anywhere
  parses generated tokens back with `syn::parse2` to check they are even well-formed Rust.

  The specific high-risk untested behaviour, named as the brief asks: **the macro's error paths**. Every
  `syn::Error` message and span emitted by `codegen/src/common/parser/**` is unverified. The handful of
  negative tests that exist (`codegen/src/common/parser/name.rs:98,108,118`,
  `codegen/src/common/parser/columns.rs:232`, `codegen/src/persist_index/parser.rs:60,72`) assert only
  `is_err()`, never the message or the span. So the finding above (SEV-1) is invisible to CI: a `todo!()`
  panic and a good diagnostic are indistinguishable to this test suite.

  Second-highest: **the `read_only` generator tree** (`codegen/src/generators/read_only/**`, 2,211 lines
  driving `worktable_version!`) has exactly one integration test,
  `tests/worktable_version/basic.rs:31`, and no test of version mismatch or schema drift between
  `worktable!` and `worktable_version!`.
- **Why it matters:** The macro is the API. A refactor of the generators today has no safety net beyond
  "the integration tests still compile", which only proves the handful of table shapes under `tests/` still
  work. It gives no coverage at all of unusual-but-legal declarations (multi-column PK with secondary
  indexes, `optional` on an indexed column, `config` with `row_derives`, a table with zero indexes and zero
  queries).
- **Fix:** Add `codegen/Cargo.toml` `[dev-dependencies] trybuild = "1"` and a `codegen/tests/ui/` suite with
  one `.rs`/`.stderr` pair per malformed input class: missing `columns`, unknown column in an index, unknown
  column in a query, duplicate index, unsupported `in_place` key, non-ident column type, `version` after
  `columns`. That directly locks in the SEV-1 fix. Separately, replace the `output.contains` assertions with
  `syn::parse2::<syn::File>(output).unwrap()` plus structural checks. Mechanical once the error paths exist.
- **Effort:** L
- **Blast radius:** Test-only. Note the `trybuild` `.stderr` files will need regenerating on rustc upgrades.

### [SEV-5] The three generator trees are ~1,000 lines of copy-paste that a trait with default methods would delete

- **ID:** `worktable-api-macros-05`
- **Severity:** Medium
- **Category:** Design
- **Confidence:** High
- **Location:** `codegen/src/generators/{in_memory,persist,read_only}/**` (4,267 / 4,243 / 2,211 lines)
- **What:** Measured with `diff`, counting changed lines (`diff a b | grep -c '^[<>]'`):

  | File (relative to each tree) | lines | in_memory vs persist | in_memory vs read_only |
  |---|---|---|---|
  | `index/info.rs` | 91 | 4 | 4 |
  | `locks.rs` | 196 | 4 | 4 |
  | `table/index_fns.rs` | 166 | 4 | 4 |
  | `table/select_executor.rs` | 241 | 4 | 4 |
  | `wrapper.rs` | 107 | 4 | 4 |
  | `queries/select.rs` | 40 | 4 | 4 |
  | `queries/type.rs` | 168 | 4 | 101 |

  A diff of 4 means two changed lines on each side: the `use crate::generators::<tree>::<X>Generator;` import
  and the `impl <X>Generator {` header. Nothing else differs. That is **841 lines duplicated three ways and
  another 168 duplicated twice**, roughly 1,000 lines of pure copy with no semantic delta. Several more files
  (`primary_key.rs` diff 13, `row.rs` diff 10, `index/usual.rs` diff 17) differ only by doc comments that
  were kept in `in_memory` and dropped in the copies.
- **Why it matters:** Every generator bug must be fixed two or three times, and the diffs above prove that has
  already gone wrong: `in_memory/index/usual.rs:36-38,171-173,208-209,256-257` carries doc comments
  explaining `save_row`/`delete_row`/`process_difference_*` that `persist/index/usual.rs` and
  `read_only/index/usual.rs` simply lack. The recent commit history is full of one-line correctness fixes to
  generated code (`f23f22d`, `9c0d98f`, `5d71ec6`); each of those had to be applied N times or was applied once
  and silently diverged.
- **Fix:** Introduce a trait in `codegen/src/generators/mod.rs`:

  ```rust
  pub trait TableGenerator {
      fn name(&self) -> &Ident;
      fn columns(&self) -> &Columns;
      fn queries(&self) -> Option<&Queries> { None }
      fn version(&self) -> u32 { 1 }
      fn names(&self) -> WorktableNameGenerator {   // also fixes finding 09
          WorktableNameGenerator::from_table_name(self.name().to_string())
      }

      // everything currently identical becomes a provided method:
      fn gen_locks_def(&self) -> TokenStream { /* moved body, verbatim */ }
      fn gen_wrapper_def(&self) -> TokenStream { /* ... */ }
      fn gen_table_index_fns(&self) -> syn::Result<TokenStream> { /* ... */ }
      fn gen_table_select_query_executor_impl(&self) -> TokenStream { /* ... */ }
      fn gen_secondary_index_info_impl_def(&self) -> TokenStream { /* ... */ }
      fn gen_query_select_impl(&self) -> syn::Result<TokenStream> { /* ... */ }
  }
  impl TableGenerator for InMemoryGenerator { /* 4 accessors */ }
  impl TableGenerator for PersistGenerator  { /* 4 accessors */ }
  impl TableGenerator for ReadOnlyGenerator { /* 4 accessors */ }
  ```

  The seven files above collapse to one copy each; the tree-specific ones (`table/impls.rs`, diff 127/218;
  `table/mod.rs`, diff 82/121) stay per-tree and override. Do the identical files first, in one commit each,
  so review is a pure move. The genuinely-divergent files are a second project.
- **Effort:** L for the identical set, XL to unify everything
- **Blast radius:** `codegen/` internals only. No change to emitted tokens if done as a pure move, which
  makes it verifiable: expand a test table before and after and diff.

### [SEV-6] Generated type layout is nondeterministic because the column model is a `HashMap`

- **ID:** `worktable-api-macros-06`
- **Severity:** Medium
- **Category:** Correctness / Design
- **Confidence:** Medium (the mechanism is certain; the user-visible blast radius depends on whether anyone
  serializes `RowFields`, which I could not rule in or out)
- **Location:** `codegen/src/common/model/column.rs:17` (`columns_map: HashMap<Ident, TokenStream>`),
  `codegen/src/generators/persist/row.rs:110-118` (`RowFields` enum),
  `codegen/src/generators/persist/locks.rs:21-29` (`<Name>Lock` struct),
  `codegen/src/persist_index/generator.rs:18,84-101` and `codegen/src/persist_index/space/events.rs`
  (~20 sites iterating `field_types: HashMap`)
- **What:** `Columns::columns_map` is a `std::collections::HashMap`, whose iteration order is seeded per
  process by `RandomState`. Several generators emit **ordered constructs** straight from that iteration:

  - `gen_row_fields_enum` (`row.rs:110-118`) builds `pub enum <Name>RowFields { ... }` from
    `columns_map.keys()`. That enum is `#[repr(C)]` and derives `rkyv::Archive`, `rkyv::Serialize`,
    `rkyv::Deserialize` (`row.rs:121-123`), so its **discriminants change between compilations of identical
    source**.
  - `gen_locks_type` (`locks.rs:21-29`) builds `<Name>Lock`'s field order the same way, so `Debug` output
    order varies build to build.
  - `persist_index/generator.rs:84-101` builds the persisted-index struct's field order from
    `field_types: HashMap`.

  Note the contrast that makes this clearly unintentional: `Columns::indexes` is an `indexmap::IndexMap`
  (`column.rs:20`), deliberately order-preserving, and `gen_row_type` (`row.rs:66-70`) has to route around
  `columns_map` entirely by using a *separate* `field_positions: HashMap<Ident, usize>` to recover
  declaration order for the row struct. The ordering problem was noticed once and patched locally instead
  of fixed at the model.
- **Why it matters:** Builds are not reproducible. Any consumer who rkyv-serializes a `RowFields` value
  gets a format that is not stable across rebuilds of their own binary. Within `src/` the enum is only used
  as an in-memory sort/range key (`src/table/select/mod.rs:17-19`), so I do **not** claim a live on-disk
  corruption bug, but it is a public, rkyv-derived, `#[repr(C)]` type and users may treat it as stable.
  Diffing expanded output before/after a refactor (see SEV-5's verification plan) is also impossible today.
- **Fix:** One-line-ish and mechanical: change `columns_map` to `indexmap::IndexMap<Ident, TokenStream>`
  (the crate is already a dependency, `codegen/Cargo.toml:19`) and delete `field_positions` along with the
  `rows[*pos]` scatter in `row.rs:66-70`. Do the same for `Generator::field_types` in
  `codegen/src/persist_index/generator.rs:18`. `HashMap` lookups (`.get()`) all keep working unchanged.
- **Effort:** S
- **Blast radius:** `codegen/src/common/model/column.rs`, `codegen/src/generators/*/row.rs`,
  `codegen/src/persist_index/**`. Changes generated discriminants once, on the fix; harmless if done before
  anyone depends on them.

### [SEV-7] Generated code uses no absolute paths, so the macro is unhygienic and requires a glob import

- **ID:** `worktable-api-macros-07`
- **Severity:** Medium
- **Category:** Design
- **Confidence:** High
- **Location:** All of `codegen/src/generators/**`; representative:
  `codegen/src/generators/persist/table/impls.rs:93-124` (`PersistedWorkTable`, `PersistenceEngine`,
  `TablePrimaryKey`, `WorkTable`, `eyre::Result`, `IndexMap`, `OffsetEqLink`, `UnsizedNode`,
  `get_index_page_size_from_data_length` all unqualified);
  `codegen/src/generators/persist/queries/select.rs:27-37`; `src/lib.rs:28-72`
- **What:** `rg -n '::worktable::' codegen/src` returns **zero hits**. Every identifier the macro emits is
  unqualified and resolved at the call site, so `worktable!` only compiles if the user has
  `use worktable::prelude::*;` in scope, and the prelude (`src/lib.rs:28-72`) re-exports 60+ names including
  very generic ones: `Data`, `Query`, `Lock`, `Order`, `Difference`, `Link`, `Interval`, `PageType`,
  `align`, `update_at`, `From`, `Into`. A user with their own `Order` or `Query` type in scope gets a
  confusing conflict inside macro-generated code. This is exactly the problem `$crate` solves for
  `macro_rules!` and that proc macros solve by emitting `::worktable::prelude::Foo`.
- **Why it matters:** Two real costs. First, the glob import is mandatory and undocumented as such (the
  README's example at `README.md:79-105` shows no `use` line at all, so copy-pasting it fails). Second, it
  couples every generated table to the full prelude, which is why the prelude has grown to re-export
  `data_bucket` and `indexset` internals: not because users need them, but because generated code does.
  That is a leaky public API driven by a macro implementation detail.
- **Fix:** Emit `::worktable::prelude::X` for every crate-owned name. The blocker is that `worktable` itself
  uses `worktable!` internally (e.g. `src/persistence/operation/batch.rs` uses `BatchInnerRowFields`), so
  you need the standard `extern crate self as worktable;` trick or a `$crate`-equivalent path parameter.
  Once done, the prelude can shrink to the names users actually touch, and `data_bucket`/`indexset`
  re-exports can be demoted out of `prelude`. Needs design discussion, not mechanical.
- **Effort:** L
- **Blast radius:** All of `codegen/src/generators/**` plus `src/lib.rs`'s prelude. Source-compatible for
  users who already glob-import; a real improvement for everyone else.

### [SEV-8] Two disjoint error models on the public API; `IndexError` is not an `Error`

- **ID:** `worktable-api-macros-08`
- **Severity:** Medium
- **Category:** Design
- **Confidence:** High
- **Location:** `src/table/mod.rs:514-525` (`WorkTableError`),
  `src/index/table_secondary_index/mod.rs:91-97` (`IndexError`),
  `src/in_memory/pages.rs:432-444` and `src/in_memory/data.rs:291-309` (two enums both named `ExecutionError`),
  `src/persistence/mod.rs:35-59` and 45 further signatures returning `eyre::Result`
  (`rg -n 'fn .*-> eyre::Result' src | wc -l` = 45)
- **What:** In-memory operations return a typed, matchable `WorkTableError`. Everything persistence-related
  returns `eyre::Report`, an opaque application-level error type: `PersistedWorkTable::new`/`load`
  (`src/persistence/mod.rs:35-38`), the whole `PersistenceEngine` trait (`:44-59`),
  `WorkTableVacuum::vacuum` (`src/table/vacuum/mod.rs:53`), `detect_version`
  (`src/migration/mod.rs:19`). A library user cannot distinguish "table file does not exist" from "version
  mismatch" from "corrupt page" from "disk full" except by string-matching the report.

  Separately, `IndexError<IndexNameEnum>` (`src/index/table_secondary_index/mod.rs:91`) is `#[derive(Debug)]`
  only. It is exported from the crate root (`src/lib.rs:15` via `pub use index::*`) and appears in the
  public signature of `TableSecondaryIndex::save_row` and friends, yet implements neither `Display` nor
  `std::error::Error`, so it cannot be `?`-ed into an `anyhow`/`eyre` chain or printed sensibly. Its
  conversion to `WorkTableError` (`:100-113`) also throws away `inserted_already`, which is the rollback
  information a caller would need.

  Minor but related: `src/in_memory/data.rs:292` and `src/in_memory/pages.rs:433` both define
  `pub enum ExecutionError`, disambiguated only by rename at the re-export
  (`src/in_memory/mod.rs:6,8` -> `DataExecutionError`, `PagesExecutionError`). Two same-named public error
  enums in one module tree is a readability tax with no upside.
- **Why it matters:** `eyre` in a library's public API is widely considered a mistake precisely because it
  removes the caller's ability to react. Here it matters concretely: "the table file does not exist, create
  a new one" is a routine branch, and today `PersistedWorkTable::load` handles it internally
  (`codegen/src/generators/persist/table/impls.rs:117-119` does a `Path::exists` check) because the error
  type could not express it. Also note `eyre` is a *runtime* dependency of a library crate, which forces it
  on every consumer.
- **Fix:** Introduce `PersistenceError` (via `derive_more`, already a dependency) with the variants that
  callers actually branch on: `TableNotFound`, `VersionMismatch { found, expected }`, `CorruptPage { page_id }`,
  `Io(std::io::Error)`, `Serialize`, and a catch-all `Other(eyre::Report)` so the migration is incremental.
  Change the `PersistenceEngine` trait and `PersistedWorkTable` to return it. Add
  `#[derive(Debug, Display, Error)]` to `IndexError` and preserve `inserted_already` in the
  `From<IndexError> for WorkTableError` conversion or add a dedicated variant. Needs design discussion on
  the variant set; the `IndexError` part is S and mechanical.
- **Effort:** L (S for the `IndexError` derives alone)
- **Blast radius:** Breaking for anyone matching on persistence results, which today is nobody because they
  cannot. `IndexError` derives are purely additive.

### [SEV-9] Column types are restricted to a single token, with no diagnostic saying so

- **ID:** `worktable-api-macros-09`
- **Severity:** Medium
- **Category:** Design
- **Confidence:** High
- **Location:** `codegen/src/common/parser/columns.rs:65-73`; `codegen/src/common/model/column.rs:10-12,44-49`
- **What:** `parse_row` reads the column type as `self.input_iter.next()` and requires
  `TokenTree::Ident`, erroring with the bare message `"Expected type."` otherwise. So a column type can only
  be a single bare identifier. `Vec<u8>`, `HashMap<K, V>`, `[u8; 32]`, `std::time::Duration`,
  `Option<T>` written explicitly, and any path-qualified type are all rejected. The only escape hatch is the
  `optional` flag (`columns.rs:100-109`), which special-cases exactly one generic wrapper by wrapping in
  `core::option::Option<#type_>` (`column.rs:44-48`). Relatedly, `is_sized` (`column.rs:10-12`) hard-codes
  `matches!(ident.to_string().as_str(), "String")` as the only unsized type, and `is_unsized` /
  `is_unsized_vec` in `codegen/src/common/name_generator.rs:5-11` repeat that same single-string match.
- **Why it matters:** This is a hard, undocumented ceiling on the schema language. `README.md:120-135` and
  `docs/queries.md` never mention it, so a user reaches for `Vec<u8>` (a completely natural column type for
  a storage engine), gets `Expected type.` pointing at `<`, and has no way to know it is a design limit
  rather than a syntax slip. It is also why `optional` exists as a flag at all, which is itself a smell:
  the flag exists to work around the parser's inability to read a generic type.
- **Fix:** Parse the type with `syn`: collect tokens until the next flag keyword or comma and
  `syn::parse2::<syn::Type>`. That immediately admits generics, paths and arrays, and lets `optional`
  become sugar rather than a necessity. The `is_sized`/`is_unsized` string matching should then key off the
  parsed `Type` (any type containing `String`, `Vec` or a slice is unsized) rather than an exact
  string compare. If parsing arbitrary types is genuinely out of scope for the storage layer, then say so
  in the error: `"column types must be a single identifier; generic and path types are not supported"`.
  Either way, document the restriction in `README.md:120-135`.
- **Effort:** M
- **Blast radius:** `codegen/src/common/parser/columns.rs`, `codegen/src/common/model/column.rs`, and every
  `columns_map.get(x).unwrap().to_string() == "String"` comparison
  (`rg -n '== "String"' codegen/src` finds 20). Additive for users.

### [SEV-10] `if false { ... } else { quote!{} }` dead branches left in the in-memory generator

- **ID:** `worktable-api-macros-10`
- **Severity:** Medium
- **Category:** AI-smell
- **Confidence:** High
- **Location:** `codegen/src/generators/in_memory/index/mod.rs:17,79,80`;
  `codegen/src/generators/in_memory/queries/update.rs:196,265,325,397`;
  `codegen/src/generators/in_memory/queries/delete.rs:83`
- **What:** Eight literal `if false { <the persist branch> } else { <empty or the real branch> }` blocks.
  `index/mod.rs:79-80` is `let derive = if false { if false { ... } }`, a doubly-dead nest. Cross-referencing
  with `persist/queries/update.rs` shows what happened: the persist generator was copied to make the
  in-memory one, and the persistence-only code was disabled with `if false` instead of deleted. So
  `in_memory/queries/update.rs:196-206` still carries the full `Operation::Update` / `apply_operation`
  block, dead, while `persist/queries/update.rs:196-202` has the live copy, and the two have already drifted
  (the in-memory dead copy retains four explanatory comments at `:336,343,347,350` that the live persist
  version dropped).
- **Why it matters:** This is the strongest single signal in the repo that the generator trees were produced
  by copying rather than by abstraction, and it actively misleads: a reader debugging in-memory update
  behaviour will read code that never runs. It also means `rg` for a generated construct returns dead hits.
- **Fix:** Delete the `if false` arms and the now-unreachable code. Purely mechanical; the compiler will
  confirm nothing else referenced the removed idents. Do this *before* SEV-5's trait extraction, so the
  diffs being unified are honest.
- **Effort:** S
- **Blast radius:** `codegen/src/generators/in_memory/**`. No behaviour change by construction.

### [SEV-11] The published proc-macro crate carries a runtime `rkyv` 0.7 dependency for one test module

- **ID:** `worktable-api-macros-11`
- **Severity:** Medium
- **Category:** Design / supply chain
- **Confidence:** High
- **Location:** `codegen/Cargo.toml:16` (`rkyv = { version = "0.7.45" }` under `[dependencies]`);
  the only Rust-level use is `codegen/src/persist_index/mod.rs:32` inside `#[cfg(test)] mod tests`;
  `Cargo.lock:1877-1878` and `Cargo.lock:1895-1896` show both `rkyv 0.7.46` and `rkyv 0.8.17` resolved
- **What:** `worktable_codegen` is a published proc-macro crate (`codegen/Cargo.toml:9-11`). Its `rkyv`
  dependency is not optional and not a dev-dependency, but `rg -n 'use rkyv' codegen/src` finds exactly one
  hit, inside a test module. Every other appearance of `rkyv` in `codegen/` is inside a `quote!` block, that
  is, emitted text that resolves against the *user's* rkyv, not the macro crate's. Meanwhile the main crate
  pins `rkyv = "0.8.9"` (`Cargo.toml:44`).
- **Why it matters:** Every consumer of `worktable` compiles rkyv twice, at two major-incompatible versions,
  purely to satisfy a test. rkyv 0.7 is a large crate with its own dependency tree; this is measurable build
  time and lockfile noise for zero functional benefit. `AGENTS.md:13` treats publishing as irreversible, so
  the manifest deserves scrutiny.
- **Fix:** Move it: `[dev-dependencies] rkyv = "0.8"` in `codegen/Cargo.toml` (and update
  `codegen/src/persist_index/mod.rs:32`'s test fixture to the 0.8 derive API, which the main crate already
  uses everywhere). Also note `codegen/Cargo.toml` currently has no `[dev-dependencies]` section at all,
  which is where `trybuild` from SEV-4 belongs. Mechanical, but it is a published-manifest change so it
  needs a version bump.
- **Effort:** S
- **Blast radius:** `codegen/Cargo.toml`, one test module, `Cargo.lock`. Reduces the dependency graph for
  every downstream user.

### [SEV-12] `docs/queries.md` is two-thirds `TODO` and its one worked example does not compile

- **ID:** `worktable-api-macros-12`
- **Severity:** Medium
- **Category:** Docs
- **Confidence:** High
- **Location:** `docs/queries.md:5-33`, `:36-38`, `:63-85`, `:89-91`
- **What:** The file is the only thing under `docs/`. Concrete defects:
  - `docs/queries.md:36-38` (`### update queries`) and `:89-91` (`### delete queries`) have bodies that are
    the literal text `` `TODO` ``. Two of the three documented features are unwritten.
  - The declaration at `:6-33` does not compile: `:15` declares `value_idx: value unique` but there is no
    `value` column (the columns at `:8-13` are `id`, `name`, `amount`, `some_value`), and `:10` is missing
    its trailing comma before `amount`. Per SEV-2/SEV-1 the missing column reaches
    `columns_map.get(i).unwrap()` and panics the macro.
  - `:80` is `let row = table.select(pk)?;` in a function returning `eyre::Result<()>` (`:65`). `select`
    returns `Option<Row>` (`codegen/src/generators/persist/table/impls.rs:159`), and `?` on an `Option` in an
    `eyre::Result` function does not compile.
  - `:60-61` says the generated method "will have two arguments: your `by` field value and closure", but the
    example at `:78` and every real call site (`tests/worktable/in_place.rs:44,64,93`) passes the closure
    **first**. The prose contradicts the example directly below it.
- **Why it matters:** Same reason as SEV-3: `AGENTS.md:15,59-62` makes agents treat these docs as ground
  truth. A doc whose sample declaration panics the compiler is worse than no doc.
- **Fix:** Fix the declaration (add the missing comma, change `value_idx: value` to `amount_idx: amount`),
  change `:80` to `let row = table.select(pk).unwrap();`, correct the argument-order sentence at `:60-61`,
  and either write the two `TODO` sections or delete the headings. Mechanical.
- **Effort:** S
- **Blast radius:** Docs only.

### [SEV-13] `CHANGELOG.md` stopped five minor versions ago

- **ID:** `worktable-api-macros-13`
- **Severity:** Low
- **Category:** Docs
- **Confidence:** High
- **Location:** `CHANGELOG.md:4` (latest entry `[0.4.1]`); `Cargo.toml:5` (`version = "0.9.1"`)
- **What:** The changelog's newest entry is 0.4.1. The crate is 0.9.1 and `git log` shows a `release: 0.9.0`
  commit (`7ebb42f`) with no changelog update. Worse, the surviving entries actively misinform:
  `CHANGELOG.md:26-28` documents `SelectQueryBuilder` as having "`limit` and `order_by` methods" and
  `:29-31` describes a `SelectResult` object. Neither `order_by` nor `SelectResult` exists in the code today
  (`src/table/select/query.rs` has `order_on`/`range_on`; `rg -n 'SelectResult' src` is empty).
  `CHANGELOG.md:35-36` describes a `DatabaseManager` object that also no longer exists.
- **Why it matters:** Five versions of undocumented breaking changes for a published crate. Users upgrading
  0.4 -> 0.9 have no migration information at all, and what is there points at removed APIs.
- **Fix:** Either backfill from `git log` between the 0.4.1 and 0.9.1 tags, or replace the file's contents
  with a pointer to the GitHub releases page and delete the stale entries. The current state, stale entries
  presented as current, is the worst of the three options.
- **Effort:** M to backfill, S to redirect
- **Blast radius:** Docs only.

### [SEV-14] Tests that assert nothing, guarding exactly the behaviour that recently broke

- **ID:** `worktable-api-macros-14`
- **Severity:** Medium
- **Category:** AI-smell / Maintainability
- **Confidence:** High
- **Location:** `tests/persistence/duplicate_key_index_reload.rs:171` and `:361`;
  `tests/worktable/vacuum.rs:214-215`; `tests/worktable/base.rs:44,72`;
  `tests/persistence/sync/mod.rs:44`; `tests/worktable/bench.rs` (whole file);
  `tests/worktable/delete.rs` (whole file);
  `tests/persistence/sync/string_secondary_index.rs:412-456`
- **What:**
  - `tests/persistence/duplicate_key_index_reload.rs:171`,
    `test_duplicate_key_secondary_index_survives_reload`, is 86 lines with **zero assertions**, under a
    15-line doc comment (`:156-169`) claiming it "proves the reload left persistence in an addressable
    state". It proves only that nothing panicked. Its sibling at `:361` is the same shape, 72 lines, no
    assertions. This file was added by commit `b4e3b08` and is the regression guard for the duplicate-key
    reload bug fixed in `b171183`; a guard that cannot fail is not a guard.
  - `tests/worktable/vacuum.rs:215` `vacuum_loop_test`: 50 lines, zero assertions, and `#[ignore]`d with no
    reason given (`:214` is a bare `#[ignore]`). The `VacuumManager` loop is therefore entirely untested.
  - `tests/worktable/base.rs:44,72` (`iter_with`, `iter_with_async`): the closure is `|_| Ok(())`, so the
    rows are never inspected.
  - `tests/worktable/bench.rs` is 79 lines with every test commented out (`:23`), and
    `tests/worktable/delete.rs` is 23 lines declaring a `DeleteTest` table and **no test at all**. Both are
    compiled on every `cargo test`, and `delete.rs`'s `worktable!` expansion is pure compile-time cost.
  - `tests/persistence/sync/string_secondary_index.rs:412-456` is a 45-line commented-out test that *does*
    contain real assertions (`:452`).
  - 5 tests are `#[ignore]`d; 3 of them (`vacuum.rs:214`, `tests/persistence/s3/mod.rs:18`,
    `tests/persistence/concurrent/mod.rs:56`) carry no reason string. The `tests/persistence/concurrent`
    and `tests/persistence/s3` modules contribute zero executed tests to a default `cargo test`.
- **Why it matters:** These are the specific untested behaviours the brief asks me to name. The
  highest-risk one is **duplicate-key secondary-index reload**: it was broken (`b171183`), it is
  load-bearing for persisted tables, and its regression test cannot fail. Second is the
  **`VacuumManager` scheduling loop**, which has no assertions and does not run.
- **Fix:** For the reload tests, assert the reconstructed index actually contains every
  (key, link) pair the pre-reload table had, and that `select_by_<idx>` returns the same row set before and
  after. For `vacuum_loop_test`, assert `VacuumStats` after a known number of deletions, then un-ignore it or
  record why it cannot run. Delete `tests/worktable/bench.rs` and `tests/worktable/delete.rs` or give them
  live tests. Give the three bare `#[ignore]`s a reason string. Mechanical except for the reload assertions,
  which need someone who knows the intended post-reload invariant.
- **Effort:** M
- **Blast radius:** Tests only.

### [SEV-15] No property-based or fuzz testing anywhere, in a crate whose core is an on-disk B-tree

- **ID:** `worktable-api-macros-15`
- **Severity:** Medium
- **Category:** Maintainability
- **Confidence:** High
- **Location:** `Cargo.toml:52-57` (`[dev-dependencies]` is `chrono`, `criterion`, `rand`,
  `tracing-subscriber`); no `fuzz/` directory; zero hits for `proptest`, `quickcheck`, `arbitrary`, `loom`
  anywhere in the repo or `Cargo.lock`
- **What:** The data structures here are exactly the ones property testing was invented for: a paged store
  with a free-link registry (`src/in_memory/empty_link_registry.rs`, 492 lines), a persisted B-tree
  reconstructed from a CDC event stream (`src/persistence/space/index/**`), and a vacuum that relocates
  rows. All are tested only by hand-written scenarios. The recent commit history is a run of exactly the
  bug class property tests find: `24ff847` (wrong max page id), `e99a0ca` (event-id gap scan window),
  `b171183` (duplicate-key reload), `25074ae` (same-max-key node ordering). Each was found by hand and each
  got one bespoke scenario test.
- **Why it matters:** Every one of those four bugs is a "generate a random operation sequence, replay, compare
  against a `BTreeMap` oracle" finding. Without a generative harness the next one is found the same way, in
  production.
- **Fix:** Add `proptest` as a dev-dependency and one model-based test: generate a `Vec<Op>` over
  `{Insert(k,v), Update(k,v), Delete(k), Reload, Vacuum}`, apply to a `PersistedWorkTable` and to a
  `std::collections::BTreeMap` oracle, assert full-content equality after every op. Start with a single
  `u64`-keyed table and one non-unique secondary index, which is precisely the shape that broke in
  `b171183`. This is a self-contained project, ideal for a follow-up agent.
- **Effort:** L
- **Blast radius:** Tests only.

### [SEV-16] Unknown and misordered column flags are not diagnosed at the flag

- **ID:** `worktable-api-macros-16`
- **Severity:** Low
- **Category:** Design
- **Confidence:** Medium (code path is certain; I did not compile an example to capture the exact rustc
  output)
- **Location:** `codegen/src/common/parser/columns.rs:75-111`; `codegen/src/common/parser/punct.rs:29-39,74-87`
- **What:** `parse_row` reads the three flags with a fixed peek order: `primary_key`, then
  `autoincrement`/`custom`, then `optional`, each falling through to a default when the peeked ident does
  not match. Nothing rejects an unrecognised ident, and nothing enforces the order. So
  `id: i64 optional primary_key` parses the `optional` flag, ignores `primary_key`, and then fails one token
  later inside the *next* `parse_row` call with `"Expected `:` found: `,`"` pointed at the wrong place.
  A typo (`primry_key`) behaves the same way. The repo has a test that documents this:
  `codegen/src/common/parser/columns.rs:253-266` `test_row_parse_no_comma` feeds
  `id: i64 primary_key TreeIndex` and asserts `row.is_ok()`, that is, it asserts that trailing garbage is
  silently dropped.

  Contributing: `try_parse_comma` (`punct.rs:29-39`) returns `syn::Result<()>` but **can never return
  `Err`**; it peeks, consumes on match, and unconditionally returns `Ok(())`. The `comma()` helper at
  `punct.rs:74-87` builds a `syn::Error` that is immediately discarded by `.is_ok()`. That is dead defensive
  scaffolding, and it is why a missing comma is never reported.
- **Why it matters:** Low individually, but it compounds with SEV-1: the parser's permissiveness is what
  lets bad input reach the panicking generators. Diagnosing at the flag turns a downstream panic into a
  pointed error.
- **Fix:** After the three flag peeks, if the next token is an `Ident` that is not a known flag and not the
  start of the next column (that is, not followed by `:`), return
  `syn::Error::new(ident.span(), "unknown column flag `x`; expected one of primary_key, autoincrement, custom, optional")`.
  Make `try_parse_comma` return `bool` (or make it `parse_comma` returning a real `Err`) so the dead error
  construction goes away. Update `test_row_parse_no_comma` to assert the error rather than the silent drop.
- **Effort:** S
- **Blast radius:** `codegen/src/common/parser/{columns,punct}.rs` and two tests. Turns previously-accepted
  malformed declarations into compile errors, which is the point.

### [SEV-17] `SpaceIndex` and `SpaceIndexUnsized` are parallel implementations of one 16-method shape

- **ID:** `worktable-api-macros-17`
- **Severity:** Low
- **Category:** Design
- **Confidence:** High
- **Location:** `src/persistence/space/index/mod.rs` (525 lines) and
  `src/persistence/space/index/unsized_.rs` (451 lines)
- **What:** The two files implement the same 16 methods in the same order with the same names
  (`new`, `add_new_index_page`, `add_index_page`, `insert_on_index_page`, `remove_from_index_page`,
  `process_insert_at`, `process_remove_at`, `process_create_node`, `process_remove_node`,
  `process_split_node`, `parse_indexset`, `primary_from_table_files_path`,
  `secondary_from_table_files_path`, `bootstrap`, `process_change_event`, `process_change_event_batch`).
  The only structural difference is `IndexPage<T>` versus `UnsizedIndexPage<T, DATA_LENGTH>` and the node
  type. Normalising the type names and diffing still leaves ~330 changed lines, so unlike SEV-5 this is not
  a pure copy, but the shape is identical and both are ~500-line files.
- **Why it matters:** Every fix to index persistence has to be made twice and reasoned about twice. The
  commit history bears this out: `c0c06ba`, `e99a0ca` and `66d8cfc` all touch this area.
- **Fix:** Extract the shared shape into a trait parameterised over the page type
  (`trait IndexPageOps { fn split(..); fn insert_at(..); ... }`) and make one generic `SpaceIndex<T, P>`.
  This is genuinely non-trivial because the unsized variant carries a `DATA_LENGTH` const parameter and a
  different node type; it needs a design pass and coordination with the concurrency reviewer, who owns this
  file. I raise it as a boundary observation, not a directive.
- **Effort:** XL
- **Blast radius:** `src/persistence/space/index/**`, `src/lib.rs` prelude exports, `codegen/src/persist_index/**`.

<details>
<summary>Nits (one line each)</summary>

- `codegen/src/lib.rs:13`: `// TODO: Refactor this codegen stuff because it's now too strange.` The most
  honest line in the repo; SEV-5 and SEV-7 are the actionable form of it.
- `src/persistence/mod.rs:25`: `// TODO: remove this` sits directly above the **public** `PersistenceConfig`
  trait, which is re-exported in the prelude and named in every generated `PersistedWorkTable` impl. Either
  remove it or delete the comment.
- `codegen/src/common/parser/attribute.rs:6`: `// TODO: Move this to separate attributes section because now
  it only parses persist.` Live and accurate.
- `codegen/src/generators/in_memory/mod.rs:42-79`: `expand` is marked `#[allow(dead_code)]` and is genuinely
  dead. The only live path is `crate::worktable::expand` -> `expand_from_parsed` (`worktable/mod.rs:48-52`).
  38 lines, including a `"persist"` match arm that can never fire. Delete.
- `codegen/src/generators/in_memory/row.rs:135` and `codegen/src/generators/in_memory/primary_key.rs:149`:
  `// TODO: tests...` at end of file. Still true (SEV-4).
- `tests/worktable/base.rs:1194,1199`: `.expect("TODO: panic message")`, IDE autocompletion left in.
- `tests/persistence/read.rs:5`: `// TODO: Fix naming.`
- `codegen/src/common/parser/punct.rs:8-26`: `parse_colon` accepts the first `:` of a `::` because a Rust
  `::` is two `Punct` tokens; check `colon.spacing() == Spacing::Alone`.
- `codegen/src/common/mod.rs:5`: `#[allow(unused_imports)] pub use model::*;` suppresses the signal that
  would tell you which model types are dead.
- `codegen/src/common/model/column.rs:17,19`: `columns_map: HashMap` plus `field_positions: HashMap<Ident, usize>`
  should be one `IndexMap` (see SEV-6). The two-map arrangement is the workaround, not the design.
- `codegen/src/persist_index/generator.rs:27`: `.expect("index type nae should end on `Index`")`, typo in a
  user-facing panic message ("nae").
- `codegen/src/common/name_generator.rs:40,44`: `&String` parameters where `&str` would do.
- `WorktableNameGenerator::from_table_name(self.name.to_string())` is reconstructed **180 times** across 46
  files, usually several times per generated item (13 times in
  `codegen/src/generators/persist/table/impls.rs` alone). Cache it on the generator struct, or make it the
  `names()` provided method in SEV-5's trait.
- `codegen/src/generators/persist/queries/in_place.rs:38-39`: `let _index_name = &index.name;` immediately
  before `todo!()`, a binding that exists only to silence a warning about code that cannot run.
- `src/table/mod.rs:64`: `// Manual implementations to avoid unneeded trait bounds.` is a genuinely useful
  comment; the surrounding file has few others.
- `tests/mod.rs` is the sole `tests/*.rs`, so the whole 324-test integration suite compiles into one binary
  named `mod`. `tests/worktable/upsert.rs:28` blames "full-suite parallel load" for a flake; splitting into
  several `[[test]]` targets would isolate that.
- `tests/non-existent/test_persist/` contains committed `.wt.idx`/`.wt.data` fixtures under a directory
  named "non-existent".
- `codegen/src/persist_index/mod.rs:44`: a test named `test` whose body is `expand(input).unwrap()`.
- `src/in_memory/pages.rs:731,762,793`: `_bench`, `bench_set`, `bench_vec` are benchmarks wearing `#[test]`,
  printing rather than asserting. `benches/` already exists; move them.
- `README.md:46` pins `features = ["s3-support"]` in the example; per the test census the s3 feature has
  exactly one test and it is `#[ignore]`d, and CI runs `cargo test` without `--all-features`. Consider
  labelling it experimental.

</details>

## Cross-cutting recommendations

1. **Make the macro fail politely before making it fail less.** SEV-1 plus SEV-4 are one project: add a
   single post-parse validation pass over `Columns` (every index field and query column must resolve), then
   a `trybuild` UI suite that pins the resulting messages. Everything else in `codegen/` becomes safer to
   touch once malformed input produces a diagnostic instead of a panic and the diagnostics are tested. Plan:
   validation pass (S) -> replace `todo!()` with spanned errors (M) -> `codegen/tests/ui/` with ~8 cases (M).
   What breaks: declarations that currently expand into garbage or panic will now fail with an error, which
   is the point; no valid declaration changes.

2. **Collapse the generator triplication, in that order: delete `if false`, then extract the trait.**
   SEV-10 first (S, pure deletion), then SEV-5's `TableGenerator` trait for the seven files that are already
   byte-identical (L, pure move). Verify by expanding a fixture table with `cargo expand` before and after
   and diffing to empty. Only then consider unifying `table/impls.rs` and `table/mod.rs`, which genuinely
   differ. This removes ~1,000 lines and makes the next generated-code bugfix a one-place change instead of
   a three-place change. What breaks: nothing, if done as a move; the risk is a botched move, which the
   before/after expansion diff catches.

3. **Fix the column model, which is the root of three separate findings.** Changing
   `Columns::columns_map` to `IndexMap` (SEV-6) makes output deterministic and lets `field_positions`
   disappear; parsing types with `syn::Type` (SEV-9) lifts the single-ident ceiling and kills the six
   `== "String"` string comparisons; keying `Columns::indexes` by index name (SEV-2) fixes the silent
   collapse. All three touch `codegen/src/common/model/column.rs` and
   `codegen/src/common/parser/{columns,index}.rs`. Doing them together is one coherent change to the schema
   model rather than three drive-by patches. What breaks: SEV-2 changes behaviour for
   duplicate-index declarations; SEV-6 changes generated discriminants once.

4. **Reconcile the docs against the code in one sitting, then keep them honest.** SEV-3, SEV-12, SEV-13.
   The README's "WorkTable internals structure" section (`README.md:309-392`) should be deleted rather than
   corrected: it documents private fields and will re-rot within a release. `AGENTS.md:15` already commits
   the project to this; the gap is that nothing checks it. A cheap mitigation: turn the README's usage
   examples into `#[doc]` tests or a compiled example under `examples/` (which is currently a stub,
   `examples/src/main.rs` is `fn main() {}`), so a stale snippet fails the build.

5. **Give the persistence surface a real error type.** SEV-8. `eyre::Report` in a library's public API is
   the single biggest ergonomics problem outside the macro. Start narrow: a `PersistenceError` with
   `TableNotFound`, `VersionMismatch`, `Io`, and `Other(eyre::Report)`, so existing `?` sites keep working
   and variants get carved out of `Other` incrementally. Add `Display`/`Error` derives to `IndexError`
   in the same change (that part is 10 minutes). What breaks: the `PersistenceEngine` trait signature, so
   any out-of-tree engine implementation.

6. **Add one property test before adding more scenario tests.** SEV-15. The last four correctness commits
   in `git log` are all in the class a single model-based `proptest` would have caught. One harness
   comparing a `PersistedWorkTable` against a `BTreeMap` oracle across `{insert, update, delete, reload,
   vacuum}` sequences is worth more than another twenty hand-written cases, and it is a well-scoped
   standalone task.

## What I did not cover

- **Performance, concurrency, `unsafe`.** A sibling slice owns these. I did not evaluate the lock protocols
  in `src/lock/**`, the CDC event ordering in `src/persistence/operation/**`, the vacuum's row relocation
  in `src/table/vacuum/vacuum.rs`, any `unsafe` block, or the atomics in `src/in_memory/pages.rs`. Where I
  cite those files (SEV-17, SEV-14) it is about shape and test coverage, not about whether the logic is
  correct.
- **On-disk format correctness.** I did not read `data_bucket` or verify page layouts. My SEV-6 claim is
  explicitly limited to build determinism; I state there that I could not establish whether `RowFields` is
  ever written to disk.
- **`src/features/s3_support.rs`** beyond noting its test is ignored. No review of the S3 request signing,
  credential handling or retry behaviour.
- **`paper-bench/`, `performance_measurement/`, `benches/`, `util/`** (the top-level `util/` directory, not
  `src/util/`). Not read.
- **The `migration_engine!` macro's generated migration logic.** I noted its total absence of unit tests
  (`codegen/src/migration_engine/**` has no `#[cfg(test)]`) but did not audit whether the migrations it
  generates are correct.
- **I did not run the test suite.** `cargo check --all-targets` passes clean with zero warnings on
  `25074ae`, which is the only build I ran. All test-suite claims come from static reading and are cited to
  file:line.
- **`.github/workflows/`** beyond the second-hand observation that CI runs plain `cargo test` without
  `--all-features` or `-- --ignored`.

## Quick-start for the follow-up agent

**Read in this order:**

1. `codegen/src/lib.rs` (64 lines): the seven macro entry points. Note the TODO at `:13`.
2. `codegen/src/worktable/mod.rs` (53 lines): the whole dispatch. `is_persist` at `:14` picks between the
   `persist` and `in_memory` generator trees; `worktable_version!` uses the third (`read_only`) tree via
   `codegen/src/worktable_version/mod.rs:40`.
3. `codegen/src/common/model/column.rs` (76 lines): the schema model. `columns_map` vs `field_positions` vs
   `indexes` here explains findings 02, 06 and 09.
4. `codegen/src/common/parser/columns.rs:52-120` and `codegen/src/common/parser/index.rs:56-96`: where user
   input becomes the model, and where validation is missing.
5. `codegen/src/generators/persist/table/impls.rs` (449 lines): the largest generator, and the best single
   sample of what emitted code looks like.
6. Then `diff codegen/src/generators/in_memory/locks.rs codegen/src/generators/persist/locks.rs` to see
   finding 05 in four lines.

**Commands:**

```bash
cargo check --all-targets            # ~19s warm, clean on 25074ae
cargo test                           # single binary; 5 tests are #[ignore]d
cargo fmt && cargo clippy --all-targets   # AGENTS.md:12 treats lints as build failures
cargo expand --test mod 2>/dev/null | less   # not installed here; the way to verify SEV-5 is a pure move

# reproduce the duplication measurement in SEV-5
cd codegen/src/generators
for p in index/info.rs locks.rs table/index_fns.rs table/select_executor.rs wrapper.rs queries/select.rs; do
  echo "$p: $(diff in_memory/$p persist/$p | grep -c '^[<>]')"
done
```

**Surprises about the layout:**

- `codegen/` is the crate `worktable_codegen`, but note commits `9787b2f` ("publish as `worktable_macros`")
  then `2b20316` ("publish as `worktable_codegen` again"). The name has flip-flopped recently; check
  `codegen/Cargo.toml:2` rather than assuming.
- There are **three** parallel generator trees, not two. `read_only` is not dead code: it backs
  `worktable_version!`.
- `codegen/src/generators/*` emits the table; `codegen/src/persist_index/*` and
  `codegen/src/persist_table/*` are the `#[derive(PersistIndex)]` / `#[derive(PersistTable)]` macros that
  the generated code *itself* applies to the types it generates. So `worktable!` output contains derives
  that run a second macro pass. This bidirectional structure is not documented anywhere and is the main
  reason the codegen is hard to follow.
- `Cargo.toml:27-28,34-35` carry four commented-out alternative dependency sources for `data_bucket` and
  `indexset` (path, git branch, alternative package). Useful context for local development; do not
  uncomment casually, `data_bucket` is pinned with `=` for the reason in `README.md:57-60`.
- The tree moved during this review (`66d8cfc` -> `25074ae`); there may be other sessions committing. Check
  `git log` before assuming line numbers in `src/persistence/space/index/**` still hold. Everything I cite
  in `codegen/`, `README.md` and `docs/` was untouched by that commit.
</content>
</invoke>
