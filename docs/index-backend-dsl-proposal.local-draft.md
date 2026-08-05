# Proposal: Per-index physical backends in the WorkTable DSL

**Status:** Discussion draft; no API commitment  
**Decision needed:** Is this compelling enough to add before the CIDR 2027 paper freeze?  
**Proposed keyword:** `using`  

## Executive summary

WorkTable currently fixes its ordered-index implementation at code-generation time. This proposal would let a schema author choose the physical implementation of each index while preserving the same generated, typed table API:

```rust
worktable!(
    name: Order,
    columns: {
        id: u64 primary_key autoincrement,
        cloid: String,
        timestamp: i64,
        exchange: Exchange,
    },
    config: {
        primary_index: congee,
    },
    indexes: {
        exchange_idx: exchange,                    // existing WorkTablesIndex default
        cloid_idx: cloid unique using arctic,
        timestamp_idx: timestamp using worktables,
    },
);
```

The generated code would contain concrete backend types. There would be no runtime backend enum, virtual dispatch, or per-operation selection branch.

The strongest research case is not “WorkTable supports three maps.” It is:

> A generated table can statically select a physical ordered-index implementation per access path while preserving a stable typed API and rejecting incompatible semantic combinations at compile time.

This matters because the candidate implementations have materially different strengths. Current exploratory ARM64 measurements show no universal winner: Arctic is especially strong for read-heavy in-memory point lookups, Congee is compact and fast for 8-byte keys, and WorkTablesIndex supports the generic key and persistence/CDC behavior on which WorkTable currently depends. Per-index selection could turn those differences into an explicit, capability-checked physical-design decision rather than forcing one compromise across every workload.

However, this is a late feature. The recommendation is **conditional go**, with a deliberately narrow first version:

- preserve WorkTablesIndex as the unchanged default;
- support direct selection only for non-persisted tables initially;
- start with primary and unique indexes, not non-unique multimaps;
- require optional Cargo features for Arctic and Congee;
- reject unsupported key, value, scan, or persistence combinations at compile time;
- include it in the paper only if it yields one dense, reproducible result without delaying the main evaluation campaign.

If that scope cannot be implemented and validated quickly, the right outcome is to defer the feature and describe the capability interface as future work. A narrowly scoped, honest feature is more useful than a broadly advertised but semantically inconsistent one.

## Why this could strengthen the paper

The current paper thesis is that WorkTable occupies a useful point between raw Rust collections and general embedded databases: collection-like access costs combined with generated operations, coordinated fields and indexes, and explicit concurrency semantics.

Backend selection can reinforce that thesis in three ways.

### 1. It separates the WorkTable architecture from one index algorithm

Without this feature, a reviewer can reasonably ask whether an observed result comes from the WorkTable programming model and AOT specialization or simply from WorkTablesIndex. Running the same generated table and operation stream with several physical indexes is a controlled ablation:

- the logical schema remains fixed;
- the generated public API remains fixed;
- the workload remains fixed;
- only the physical ordered-index implementation changes.

That makes the claim more defensible: WorkTable is a schema compiler and coordination layer that can specialize physical access paths, not merely a wrapper around a particular B-tree.

### 2. It exposes a real physical-design tradeoff

The candidates do not form a simple fastest-to-slowest ranking:

| Backend | Apparent strength | Important constraint |
|---|---|---|
| `worktables` | Generic ordered keys; existing WorkTable integration; structural CDC used by persistence | Slower in several exploratory in-memory point-lookup cases |
| `congee` | Very fast, compact 8-byte-key ART | Fixed-width key/value constraints; semantic adapter work; no compatible structural CDC |
| `arctic` | Strong read-heavy point lookup and competitive range performance, including packed NanoID experiments | Higher audit surface; large memory cost for some wider keys; scans are not linearizable; no compatible structural CDC |

The absence of one universal winner is the reason a physical-design control is useful. It also matches the intended HFT deployment model: sequential `u64` primary keys, NanoID-style external identifiers, read-heavy secondary access paths, and a strong preference for ARM.

### 3. It makes generated specialization visible in the DSL

The `using` clause is small, local, and declarative. It lets a domain expert state a physical decision next to the index it affects, while code generation resolves the concrete implementation and its capabilities ahead of time.

This is more interesting than a global Cargo switch. A table may legitimately want:

- Congee for a sequential `u64` primary key;
- Arctic for a packed NanoID unique secondary key;
- WorkTablesIndex for an ordered or persisted access path.

The compiler can make this heterogeneous layout zero-cost at the dispatch boundary.

## Proposed syntax

### Secondary indexes

The existing form remains valid and retains its current meaning:

```rust
indexes: {
    exchange_idx: exchange,
    cloid_idx: cloid unique,
}
```

An optional `using` clause selects a physical backend:

```rust
indexes: {
    exchange_idx: exchange using worktables,
    cloid_idx: cloid unique using arctic,
}
```

Conceptually:

```text
index   := name ':' field ['unique'] ['using' backend] ','
backend := 'worktables' | 'arctic' | 'congee'
```

`unique` describes the logical constraint. `using` describes its physical implementation. Keeping those concepts separate is important for both readability and future extension.

### Primary index

The primary key is declared on a column today, so its physical choice belongs in `config` rather than adding more modifiers to the column grammar:

```rust
columns: {
    id: u64 primary_key autoincrement,
},
config: {
    primary_index: congee,
},
```

Omitting `primary_index` preserves the current WorkTablesIndex implementation.

### Complete HFT-oriented example

```rust
worktable!(
    name: Order,
    persist: false,
    columns: {
        // Internal locality-friendly identifier.
        id: u64 primary_key autoincrement,

        // External/public identifier, encoded as a fixed-width NanoID key.
        public_id: NanoId,

        // Business access paths.
        account_id: u64,
        timestamp: i64,
        exchange: Exchange,
    },
    config: {
        primary_index: congee,
    },
    indexes: {
        public_id_idx: public_id unique using arctic,
        account_idx: account_id using worktables,
        timestamp_idx: timestamp using worktables,
        exchange_idx: exchange, // unchanged default
    },
);
```

This example is illustrative, not a promise that all non-unique combinations are available in the first implementation.

## Static semantics and capability checking

The same method names do not imply that the backends have identical capabilities or guarantees. Code generation should model those differences explicitly and fail early.

At minimum, the implementation needs separate capabilities for:

- unique point lookup and mutation;
- non-unique/multimap lookup and mutation;
- ordered range traversal;
- supported key representation and ordering;
- supported stored value representation;
- scan consistency contract;
- persistence-compatible structural CDC.

Possible internal traits include:

```rust
trait UniqueIndexBackend<K, V> { /* point operations */ }
trait MultiIndexBackend<K, V> { /* one-to-many operations */ }
trait OrderedIndexBackend<K, V> { /* range operations */ }
trait PersistentIndexCdc { /* structural change stream */ }
```

These are illustrative capability boundaries, not a required public API. The generated table should still use concrete types so the optimizer sees the actual implementation.

Examples of useful compile-time diagnostics:

```text
index `cloid_idx`: congee requires an order-preserving 8-byte key encoding
```

```text
index `account_idx`: backend `arctic` does not provide a non-unique index adapter
```

```text
persisted table `Order`: backend `congee` does not implement WorkTable structural CDC;
use `worktables` for this index or make the table non-persistent
```

Silently weakening semantics is not acceptable. In particular, a backend must not be treated as persistence-compatible merely because its logical insert/delete operations can be observed. WorkTable persistence currently relies on structural CDC from WorkTablesIndex.

## Recommended first-version scope

To keep the change reviewable and credible:

1. Add a backend enum to the code-generation model, defaulting to WorkTablesIndex.
2. Parse `using <backend>` on secondary indexes and `primary_index: <backend>` in config.
3. Add adapters for primary and unique indexes only.
4. Permit Arctic and Congee only when `persist: false`.
5. Keep non-unique indexes on WorkTablesIndex until a correct multimap adapter exists.
6. Gate third-party adapters behind optional `index-arctic` and `index-congee` Cargo features.
7. Generate direct concrete calls; do not introduce a runtime enum or trait object.
8. Add differential tests that replay the same operation trace against every compatible backend.

This scope is intentionally smaller than the syntax can eventually express. Unsupported combinations should produce precise macro errors.

### Persistence follow-up, not first-version behavior

For persisted tables, an eventual design could retain WorkTablesIndex as the authoritative CDC-producing index and maintain a second in-memory accelerator:

```rust
config: {
    primary_index: worktables accelerate congee,
},
indexes: {
    public_id_idx: public_id unique
        using worktables
        accelerate arctic,
}
```

That is a different feature with real memory, recovery, consistency, and write-amplification costs. It should not be smuggled into the initial `using` implementation. The syntax above is included only to show that direct physical selection does not close off a persistence-safe future design.

## Preliminary evidence

The following numbers are exploratory local measurements on Apple ARM64, release builds with ThinLTO and the default allocator. They are not publication-grade and should not be cited as final results. Their purpose is to establish whether the design question is worth testing rigorously.

| Workload | WorkTablesIndex | Congee | Arctic |
|---|---:|---:|---:|
| `u64 -> 12-byte Link`, point read, 1 thread | 183 ns/op | 62 ns/op | 37 ns/op |
| Same, 8 threads | 31 ns/op | 7.8 ns/op | 4.9 ns/op |
| 99/0.5/0.5 read/insert/delete mix | 27.6 Mops/s | 266 Mops/s | 327 Mops/s |
| Range width 8 | 49.9 ns/item | 7.2 ns/item | 10.4 ns/item |
| Range width 1024 | 10.7 ns/item | 4.1 ns/item | 3.5 ns/item |
| Packed NanoID lookup, 1 thread | 240 ns/op | unsupported | 106 ns/op |
| Packed NanoID lookup, 8 threads | 38.8 ns/op | unsupported | 12.9 ns/op |

Memory measurements likewise show a workload-dependent result:

| Population | WorkTablesIndex | Congee | Arctic |
|---|---:|---:|---:|
| 2M `u64 -> u64` entries | 66.6 MB | 32.2 MB | 34.2 MB |
| 2M random `12-byte -> u64` entries | 70 MB | unsupported | 222 MB |

The potentially compelling result is therefore not “backend X wins.” It is that a generated table can choose the appropriate point in a throughput/latency/memory/semantics space for each access path. The wider-key Arctic memory result is especially important: a throughput-only comparison would lead to an incomplete physical-design decision.

The current evidence also carries engineering caveats:

- Congee is limited to fixed-width keys and values in its fast path.
- Arctic's scan contract is explicitly non-linearizable.
- Congee's documented range boundary and observed implementation behavior require normalization in an adapter.
- Arctic has a substantially larger unsafe-code audit surface than the current WorkTablesIndex implementation.
- Neither alternative emits the structural CDC required by existing WorkTable persistence.

These are reasons for capability checking and contract tests, not footnotes to hide.

## Evaluation needed before inclusion

The feature should earn its place in a six-page paper with one compact result, not create a new benchmark catalog.

The cleanest experiment is:

> Hold the WorkTable schema, generated operations, data set, and operation trace fixed; vary only the declared index backend.

Measure at least:

- median throughput and p50/p99 latency;
- bytes per indexed row at steady state;
- 1 thread and a representative contended thread count;
- ARM as the primary target, with x86 only as a portability check;
- sequential `u64` primary keys and packed NanoID secondary keys;
- point-heavy HFT mixes plus a bounded-range workload;
- construction, steady-state mutation, and reclamation behavior separately.

Correctness gates should precede performance claims:

- differential state-machine traces across compatible backends;
- duplicate-key and deletion/reinsertion behavior;
- exact inclusive/exclusive range contracts;
- concurrent publication and removal races;
- memory reclamation under sustained churn;
- generated API compatibility tests;
- compile-fail tests for unsupported backend capabilities.

The official campaign should follow the measurement rules in the companion benchmark campaign documents, and the result should respect their paper/website evidence split. Raw map numbers remain supporting evidence; the paper-worthy experiment is the same generated table with one controlled physical choice.

## Hard go/no-go bar

Because this is late, all of the following should be true before it becomes a paper contribution:

1. **The API remains tiny.** Existing schemas compile unchanged; the new surface is one optional `using` clause and one optional config value.
2. **The implementation is statically specialized.** No runtime dispatch is added to the HFT hot path.
3. **At least two choices are genuinely useful.** A result must show a material, repeatable workload-dependent benefit, not noise or a universal ranking that could be handled by replacing the default.
4. **The tradeoff is multidimensional.** The evaluation includes latency and memory, not throughput alone.
5. **Semantics stay explicit.** Unsupported persistence, key, scan, or multimap behavior fails at compile time.
6. **The evidence strengthens the existing thesis.** It demonstrates capability-checked physical specialization within WorkTable, rather than opening a second paper about index algorithms.
7. **It does not delay the main campaign.** If implementation or validation threatens the freeze or dedicated AWS measurements, defer it.

Suggested decision rule: require a repeatable improvement of at least 10% in a relevant end-to-end WorkTable workload, or a similarly material memory reduction at comparable latency, before spending scarce paper space on the result. Larger microbenchmark gains are encouraging but are not sufficient by themselves.

## What not to do

- **Do not change the default.** Existing WorkTablesIndex behavior, including persistence, remains the compatibility path.
- **Do not choose automatically yet.** An opaque heuristic would complicate reproducibility and make performance changes surprising.
- **Do not use a runtime backend enum.** It adds a branch and obscures static specialization for little benefit.
- **Do not use one global Cargo switch to replace every index.** It prevents heterogeneous per-index physical design and weakens the ablation.
- **Do not claim semantic equivalence.** Publish a capability matrix and normalize only contracts that can be normalized honestly.
- **Do not generalize from raw map benchmarks.** The decision must be validated through generated WorkTable operations.
- **Do not expose persisted alternatives until CDC/recovery semantics are real.** Logical operation logging is not a drop-in substitute for the current structural CDC path.

## Implementation shape

The expected code changes are narrow in concept:

1. Extend the codegen index model with `backend`, defaulting to `worktables`.
2. Extend the index parser with optional `using <ident>`.
3. Extend config parsing/modeling with `primary_index`.
4. Introduce internal adapters and capability bounds for each supported index class.
5. Make the generators emit the selected concrete type and operations.
6. Add optional dependency features and precise compile-time errors.
7. Add parser snapshots, compile-pass/compile-fail cases, differential correctness tests, and generated-code benchmarks.

Each fix or feature should remain one reviewable commit. In particular, parser/model changes, WorkTablesIndex abstraction, Congee adapter, Arctic adapter, compile-time diagnostics, correctness tests, and benchmarks should not be collapsed into one commit.

## Alternatives considered

### Replace WorkTablesIndex globally

This is simpler mechanically but loses generic-key and persistence behavior, prevents mixed physical layouts, and turns every workload into the same compromise. It also makes regression isolation harder.

### Select a backend with Cargo features only

Cargo features are appropriate for making adapters available, but not for expressing which access path uses which backend. A global feature also cannot compare two backends in the same generated application.

### Select a backend at runtime

Runtime selection may help a general database optimizer, but it is poorly aligned with this HFT/AOT design: it adds representation and dispatch costs, moves errors later, and makes the generated code less transparent.

### Automatically infer the backend from the key type

Key type is only one dimension. Workload mix, memory budget, scan semantics, persistence, and audit tolerance also matter. Automatic selection would be premature without an optimizer and workload statistics.

### Ship only an internal adapter without DSL syntax

That could support experiments, but it would not demonstrate user-controlled physical design and would make configurations difficult to reproduce. It is a reasonable staging step, not the intended endpoint.

## Proposed paper wording if the result clears the bar

> WorkTable separates logical index declarations from their physical implementations. An optional `using` clause selects a capability-compatible ordered-index backend per access path. The macro validates key representation, uniqueness, range, and persistence requirements and emits concrete backend types, preserving static dispatch. This lets one generated table combine, for example, a compact ART for an auto-incrementing primary key with a wider-key index for an external NanoID, while retaining WorkTablesIndex wherever structural CDC is required.

This wording should be shortened to match the eventual implementation. It must not imply support for combinations that remain rejected.

## Feedback requested

1. Does per-access-path physical selection strengthen the paper's central WorkTable argument, or does it distract from it?
2. Is the `using` syntax readable and sufficiently unsurprising in the existing DSL?
3. Is a first version limited to non-persisted primary and unique indexes still useful enough to expose?
4. Should this appear as a design contribution, a controlled ablation, or only an artifact capability?
5. What end-to-end result would be strong enough to justify adding it this late?
6. Should the persistence-safe `accelerate` design be mentioned at all, or deferred completely?

## Current recommendation

Prototype the smallest statically dispatched path and run the controlled WorkTable experiment. Do not merge the public feature or change the paper unless it clears the hard bar above.

If it succeeds, the contribution is compelling because it shows capability-checked, per-index physical specialization through a stable generated API. If it does not, the prototype still serves as a clean backend ablation and helps determine whether current WorkTable performance comes from the table architecture or from WorkTablesIndex itself.

The detailed result tracks, comparison layers, and campaign procedure remain in the companion `wt-benchmarks` repository rather than becoming part of this WorkTable API proposal.
