# Per-index backends with `using`

**Status:** PR #187 is merged; native ART persistence is implemented on `feat/art-native-persistence` and remains experimental pending validation.

**Default:** `worktables_index`

**Backends in this change:** `worktables_index`, `indexset`, `congee`, `arctic`

## Why this exists

WorkTable no longer has to make one physical-index tradeoff for every access path. The optional `using` modifier selects a concrete backend for a primary or unique secondary index while preserving the generated WorkTable API.

The generated table contains concrete map types. Selection is resolved by the macro; there is no runtime backend enum, trait object, virtual call, or per-lookup selection branch.

This has two distinct uses:

- **Production migration:** persisted tables can select a backend per index. WorkTablesIndex and vanilla IndexSet share the existing DataBucket page representation; Congee and Arctic use backend-native topology checkpoints plus logical WAL records.
- **Research and measurement:** the same schema can compare memory-only and persisted Congee/Arctic access paths without runtime backend dispatch.

The useful paper claim is not that WorkTable bundles several maps. It is that a generated table can statically select a physical implementation per access path, keep a stable typed API, and reject incompatible persistence or key semantics at compile time.

## Syntax

`using` is optional on a primary-key declaration and on an index declaration:

```rust
worktable!(
    name: Order,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement using congee,
        account_id: u64,
        sequence: u64,
        public_id: PackedNanoid,
    },
    indexes: {
        account_idx: account_id unique using arctic,
        sequence_idx: sequence unique using indexset,
        public_id_idx: public_id unique using worktables_index,
    },
);
```

There is no separate `config` syntax for this feature. The physical choice stays next to the access path it controls.

### The absent-`using` default

Omitting `using` always means `worktables_index`:

```rust
columns: {
    id: u64 primary_key autoincrement, // WorkTablesIndex
},
indexes: {
    account_idx: account_id unique,    // WorkTablesIndex
}
```

The explicit equivalent is:

```rust
columns: {
    id: u64 primary_key autoincrement using worktables_index,
},
indexes: {
    account_idx: account_id unique using worktables_index,
}
```

This default is intentional. Vanilla `indexset` is an explicit fourth backend; it is not the default and does not silently replace WorkTablesIndex.

## Persistence is controlled by the existing `persist` declaration

No new persistence keyword is introduced.

| Declaration | Meaning | Allowed backends |
|---|---|---|
| `persist` omitted | Existing non-persisted table behavior | WorkTablesIndex or vanilla IndexSet; ART use requires an explicit persistence choice |
| `persist: false` | Explicitly memory-only | All four backends, subject to key and uniqueness constraints |
| `persist: true` | Local durable persistence plus in-memory indexes | All four; ART persistence is experimental |
| S3 support | Existing S3 sync layered over local persistence | File paths are compatible; ART end-to-end S3 validation remains required |

Congee and Arctic require an explicit `persist: true` or `persist: false`; omitting `persist` is not sufficient acknowledgement. This makes the durability choice visible during review:

```rust
worktable!(
    name: QuoteCache,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement using congee,
        symbol_id: u64,
    },
    indexes: {
        symbol_idx: symbol_id unique using arctic,
    },
);
```

The macro accepts the same schema with `persist: true` and selects native ART persistence. It rejects the schema when `persist` is omitted.

## Current capability matrix

| Capability | `worktables_index` | `indexset` | `congee` | `arctic` |
|---|---:|---:|---:|---:|
| Primary index | Yes | Yes | Yes | Yes |
| Unique secondary index | Yes | Yes | Yes | Yes |
| Non-unique secondary index | Yes | No | No | No |
| Persisted local disk | Yes | Yes | Experimental | Experimental |
| Existing S3 persistence path | Yes | Yes | Files compatible; validation pending | Files compatible; validation pending |
| Variable-sized keys | Yes | Not in this change | No | No |
| Ordered point/range API | Yes | Yes | Adapter snapshot for scans | Adapter snapshot for scans |
| Default when `using` is absent | Yes | No | No | No |

Alternative backends currently require `unique`. A non-unique declaration such as `value_idx: value using arctic` fails at macro expansion and tells the author to use `worktables_index`.

### Key constraints

- **Congee:** `u8`, `u16`, `u32`, `usize`, and `u64` on 64-bit targets. Its native key and payload are one machine word. Composite, NanoID, string, signed, and floating-point keys are rejected.
- **Arctic:** `u16`, `u32`, `u64`, and `u128` in this initial adapter. Its crate supports more representations, but WorkTable exposes only the shapes covered by the current contract tests.
- **Vanilla IndexSet:** sized ordered keys in this change. Variable-sized keys remain on WorkTablesIndex.
- **WorkTablesIndex:** retains the existing generic and variable-sized key support.

WorkTable wraps a declared primary key in a generated newtype. For Congee and Arctic, code generation emits a lossless codec from that newtype to the supported native integer key. Unsupported primary-key shapes fail during macro expansion.

NanoID is deliberately not claimed for either ART yet. Current NanoID/PackedNanoid indexes must use WorkTablesIndex or vanilla IndexSet until an order-preserving, contract-tested Arctic codec is added. UUID-specific work is outside this feature; downstream schemas can continue the separate UUID-to-NanoID migration.

## Persistence and provider switching

WorkTablesIndex remains the persistence format boundary used by DataBucket. The vanilla IndexSet adapter performs two normalizations:

1. vanilla IndexSet structural CDC events are converted to the equivalent WorkTablesIndex event type before they reach persistence;
2. vanilla IndexSet node pairs are converted to and from the existing WorkTablesIndex/DataBucket page representation during snapshot and reload.

The selected provider is therefore an in-memory implementation detail, not a new disk format. The test suite covers this sequence with split indexes:

1. create and persist a table using WorkTablesIndex;
2. reload the same files using vanilla IndexSet;
3. delete and insert rows through vanilla IndexSet and persist the resulting CDC;
4. reload the same files using WorkTablesIndex again.

It also separately covers vanilla IndexSet persist → reload → mutate → reload. This is the technical basis for deploying the two providers in parallel without a full data rebuild.

Congee and Arctic deliberately do **not** normalize into WorkTablesIndex pages. Their `*.wt.idx` files contain a checksummed pointer-free checkpoint of the selected ART's physical topology followed by logical Set/Remove WAL frames. Compaction reconstructs a temporary native ART, applies the WAL, and atomically replaces the checkpoint; it does not retain a duplicate authoritative tree during normal operation. See [Native ART index persistence](art-index-persistence-plan.md).

Because those physical formats differ, switching an existing index between an ART and a B-tree requires an explicit rebuild or migration. WorkTablesIndex ↔ vanilla IndexSet remains the format-compatible provider switch.

This is still a sensitive storage path. Production rollout should retain backups, verify the exact downstream schema/version, and run crash/torn-write and sustained post-reload mutation tests before changing a live table.

## Hot-path and performance details

Backend dispatch itself is static and should compile away. That does **not** mean every adapter operation has identical cost.

### WorkTablesIndex and vanilla IndexSet

- Point operations call the selected B-tree directly.
- Ordered iteration and ranges stream from the selected tree.
- `OffsetEqLink` values are copied out of backend guards; no heap allocation is introduced for a point lookup.
- Both can emit persistence-compatible structural CDC.

Direct dispatch preserves a strict generated point-read contract with a
provider-specific implementation. WorkTablesIndex 0.0.5 holds its structural
mapping stable until the selected node is locked, so both hits and misses are
definitive; a contended lookup drops the structural guard before waiting and
then retries the mapping. Vanilla IndexSet does not expose a comparable
structural validation primitive; `using indexset` therefore remains
experimental and is excluded from concurrent correctness and published
performance claims. This WorkTablesIndex visibility guarantee composes with
the mandatory immutable row-publication protocol, which addresses concurrent
page bytes, ghost publication, and reclamation rather than index routing.

### Congee

- Point lookup and mutation call Congee directly.
- WorkTable links do not fit in Congee's one-word payload. The adapter stores an `Arc` pointer, so inserts allocate and reads clone the `Arc` before copying the link.
- Ordered reads use Congee's native range scan and materialize only the requested key interval into a `Vec`; a full iteration is therefore O(n) with one result allocation, while a narrow range no longer dumps, re-probes, and sorts the whole tree.
- Memory-only Congee point reads remain native and concurrent. Mutations use a
  WorkTable adapter mutex because congee-wt 0.4.1 can otherwise lose disjoint
  structural insert/remove updates. With `persist: true`, the persistence layer
  additionally takes a key-striped sequencing lock before producing one logical
  WAL event.

### Arctic

- Point lookup and mutation call Arctic directly.
- WorkTable links are stored in `Box` values because Arctic's inline value is limited to 64 bits. Inserts allocate; reads copy the link from the box.
- Ordered reads use Arctic's native bounded traversal and materialize the requested interval into a `Vec`.
- Concurrent scan behavior inherits Arctic's non-linearizable traversal contract.
- With `persist: true`, mutations use the same persistence-only sequencing wrapper as Congee. Point reads remain direct and lock-free.

Generated table traversal snapshots its ordered link list once instead of
restarting a range at every row. The ART adapters are still candidates for
point-heavy paths—not automatic wins for `select_all`,
wide ranges, iteration, or vacuum, because ordered results are materialized
rather than streamed. Measurements must separate point lookup,
write/allocation cost, range width, full iteration, and reclamation rather than
reporting one blended throughput number.

### Memory diagnostics

WorkTablesIndex and vanilla IndexSet expose node capacity and topology used by existing `system_info` reporting. Congee and Arctic do not expose equivalent stable allocator statistics. For those adapters:

- reported used/heap bytes are only a payload-size lower bound;
- reported capacity equals logical length;
- reported node count is zero/unknown.

Use allocator/RSS measurements for comparative memory results; do not treat the ART `system_info` fields as total resident memory.

## Dependency and fork status

This implementation pins two narrow forks for typed topology import/export:

- `WorkTablesIndex 0.0.5` as the default `indexset` dependency alias already used by WorkTable;
- vanilla `indexset 0.15.0` under the `vanilla_indexset` Cargo name;
- `congee-wt` at commit `005bfb1968e781800176f2d7e465e6a1af630e1a`;
- `arctic-wt` at commit `e13fc7df3c040f14ae66c1cb56b1bd0a3f6da3fc`.

The forks add no point-operation fields or branches. They expose pointer-free topology values and exact reconstruction; WorkTable owns checksums, framing, WAL, recovery, and compaction.

## Correctness coverage in this PR

The implementation includes:

- parser/default tests for all four names;
- compile-time acceptance of explicit persisted ARTs plus rejection of implicit ART persistence choices, alternative non-unique indexes, and unsupported key shapes;
- shared unique-index contract and concurrent mutation-integrity tests for all four providers;
- adapter contract and concurrent checked-insert tests for Congee and Arctic;
- immediate disjoint insert/read/remove tests for Congee and Arctic;
- a generated table using all four providers simultaneously;
- generated primary-key CRUD tests for vanilla IndexSet, Congee, and Arctic;
- native Arctic-primary/Congee-secondary and Congee-primary/Arctic-secondary persist/reload/mutate/reload coverage;
- native topology codec, WAL, torn-tail truncation, and compaction tests;
- vanilla IndexSet persist/reload/post-reload mutation coverage;
- WorkTablesIndex → vanilla IndexSet → WorkTablesIndex disk-provider switching coverage.

These are correctness gates, not performance evidence.

## Benchmark handoff

Use draft PR #187 and vary only `using` between otherwise identical generated schemas. The first downstream targets are `web3.trading-backend` and `agencyzero`.

Minimum useful ARM campaign:

1. sequential autoincrement `u64` primary key;
2. unique `u64` secondary key with the production hit/miss distribution;
3. 1 thread and representative contended thread counts;
4. point-read, insert, delete, and production mixed traces measured separately;
5. range widths 1, 8, 64, and 1,024 plus full iteration;
6. p50, p99, throughput, allocations/op, RSS/entry, and post-churn reclamation;
7. persisted WorkTablesIndex, vanilla IndexSet, Congee, and Arctic, including reload and writes after reload;
8. memory-only versus persisted ART to isolate the sequencing/WAL overhead.

Run release builds on the actual ARM deployment class. SIMD should not be treated as a reason to prefer a backend; any x86 result is a portability check, not the primary HFT decision.

For the paper, the strongest controlled experiment keeps the WorkTable schema, generated methods, data set, and operation trace fixed and changes one `using` clause. Include this feature only if an end-to-end WorkTable workload shows a material, repeatable gain or memory reduction without weakening the required persistence and scan semantics.

## Production versus research classification

- **WorkTablesIndex:** production default.
- **Vanilla IndexSet:** experimental provider. It preserves local/S3 persistence through the existing format boundary, but is excluded from concurrent correctness and published performance claims until upstream offers a stable structural-read primitive or the adapter gains a low-cost algorithm.
- **Congee and Arctic:** research/experimental backends with native local persistence. Promotion requires crash/S3 validation, relevant downstream evidence, allocation/reclamation review, and a workload that does not depend on the current allocating scan path.

That boundary is deliberate: `using` exposes optional physical specialization without quietly weakening WorkTable's in-memory/on-disk coordination contract.
