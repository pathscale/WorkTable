# WorkTables as a Vector Database

## Status

This document is a design investigation, not documentation of an implemented
feature. The syntax, APIs, storage structures, performance expectations, and
roadmap below are proposals.

## Executive summary

WorkTables could become a compelling embedded vector database. The most
realistic opportunity is:

> A typed, in-process vector store combining very fast metadata operations,
> scalar indexes, and approximate nearest-neighbor search without a network or
> serialization boundary.

Replacing LanceDB for some embedded workloads is plausible. Replacing
single-node Qdrant would require a substantial database project. Replacing
distributed Qdrant is several orders of magnitude more work than adding vector
search.

Vector databases do not power LLM inference or training. They usually provide
retrieval for RAG, semantic search, recommendations, memory, and document lookup
around an LLM. WorkTables could replace that retrieval layer, not embedding
generation or tensor execution.

The recommended initial architecture is:

- WorkTables remains the authoritative row and metadata store.
- Every indexed vector receives a stable, dense `VectorId` independent of its
  physical row location.
- Original vectors live in a dedicated aligned vector slab rather than inside
  ordinary serialized row pages.
- Exact search uses vectorized distance kernels over the slab.
- Approximate search is initially supplied by a composable ANN library rather
  than a new WorkTables implementation of HNSW.
- Scalar WorkTables indexes produce typed filter candidates expressed as
  `VectorId` sets or bitmaps.
- ANN structures and quantized vectors are initially rebuildable accelerators.
- Immutable index generations and logical mutation sequences provide safe
  publication and recovery boundaries.

Microsoft's current DiskANN3 is an especially promising integration path. It is
a Rust library designed to delegate vectors, graph adjacency, identifier
mapping, deletion, and storage to a host database through a `DataProvider`
interface.

## Current Rust reference points

| System | What it represents |
|---|---|
| Qdrant | Mature Rust vector database service: HNSW, filtering, WAL, segments, quantization, snapshots, sharding, replication, REST, and gRPC |
| LanceDB/Lance | Rust-based embedded and columnar retrieval engine: Arrow, MVCC, vector/scalar/FTS indexes, IVF, and IVF+HNSW |
| DiskANN3 | Composable Rust ANN library designed to be integrated into a host database |
| USearch | High-performance ANN library with Rust bindings, but a C++ core rather than a native Rust database |

Qdrant currently uses HNSW for dense-vector indexing and combines it with
payload indexes and filter-aware graph edges. Its planner can switch between
HNSW and exact scanning based on estimated filtered cardinality. Data is divided
into segments, with a WAL and per-point versions for recovery.

LanceDB is a particularly relevant comparison because it is embedded and built
over an Arrow-native columnar format. It supports IVF, IVF+HNSW, quantization,
scalar indexes, full-text search, and exact rescoring. Lance tables use
immutable manifests and MVCC snapshots.

## Where WorkTables stands today

WorkTables has some valuable ingredients:

- Embedded, in-process execution.
- Generated typed rows and queries.
- Concurrent scalar primary and secondary indexes.
- Optional persistence and an operation log.
- Bidirectional primary-key/physical-link mapping.
- Row-level update coordination.
- An emerging stable-publication design.

The missing pieces are substantial:

- No vector type in the DSL.
- No distance kernels.
- No exact vector scan engine.
- No ANN index.
- No stable logical vector identifier.
- No filtered ANN planner.
- No vector quantization.
- No index generation and rebuild lifecycle.
- No vector-specific persistence format.
- No snapshot-consistent range scans.
- No distributed service layer.

The current index abstraction is essentially scalar value to physical `Link`:
[`src/index/table_index/mod.rs`](../src/index/table_index/mod.rs). That is too
narrow for a graph index, where inserting one vector may rewrite the adjacency
lists of multiple existing vectors.

The existing primary index is bidirectional, but it is based on physical links:
[`src/index/primary_index.rs`](../src/index/primary_index.rs). ANN structures
need identifiers that remain stable across vacuum, row relocation, and slot
reuse.

The current concurrency feature also explicitly does not provide snapshot range
scans: [`docs/versioned-row-publication.md`](versioned-row-publication.md). A
vector query visiting many nodes needs a pinned visibility boundary.

## Current DSL obstacle

The column parser currently consumes a single `Ident` as the type:
[`codegen/src/common/parser/columns.rs`](../codegen/src/common/parser/columns.rs).

Consequently, neither of these declarations works today:

```rust
embedding: [f32; 768],
embedding: Vector<f32, 768>,
```

A proof of concept could use:

```rust
embedding: Embedding768,
```

where `Embedding768` is a user-defined newtype. The production DSL should
support proper Rust types or dedicated vector syntax.

## Proposed DSL

```rust
worktable!(
    name: DocumentChunk,
    version: 1,
    persist: true,

    columns: {
        id: u64 primary_key autoincrement,
        tenant_id: u64,
        document_id: u64,
        language: Language,
        text: String,
        embedding: Vector<f32, 768>,
    },

    indexes: {
        tenant_idx: tenant_id,
        document_idx: document_id,

        embedding_idx: embedding using diskann {
            metric: cosine,

            storage: mmap,
            original_vectors: true,
            quantization: scalar_i8,

            graph_degree: 64,
            build_complexity: 100,
            default_search_complexity: 100,

            filter_by: [tenant_id, language],
            maintenance: async,
            persistence: rebuildable,
        },
    },
);
```

Alternative backends could eventually include:

```rust
embedding_idx: embedding using flat {
    metric: cosine,
}

embedding_idx: embedding using hnsw {
    metric: cosine,
    m: 16,
    ef_construction: 200,
}

embedding_idx: embedding using diskann {
    metric: cosine,
    storage: mmap,
}
```

Here `using` is appropriate. Unlike a columnar projection, an ANN structure
really is an alternate index implementation.

The public vocabulary should separate stable concepts from backend-specific
knobs. For example, `search_complexity` can remain stable while a backend maps
it to HNSW `ef`, DiskANN search-list size, or IVF probe counts.

## Generated query API

A generated query could look like:

```rust
let neighbors = table
    .nearest_by_embedding(&query)
    .top_k(20)
    .filter(DocumentChunkFilter::TenantId.eq(tenant))
    .search_complexity(128)
    .select((
        DocumentChunkField::Id,
        DocumentChunkField::DocumentId,
        DocumentChunkField::Text,
    ))
    .execute()?;
```

Avoid returning the embedding by default. Cloning 768 or 1,536 floats into
every result would undermine otherwise cheap retrieval.

A useful result type would preserve the approximate-search metadata:

```rust
struct Neighbor<T> {
    row: T,
    distance: f32,
    vector_id: VectorId,
}

struct VectorSearchResult<T> {
    neighbors: Vec<Neighbor<T>>,
    index_generation: u64,
    applied_sequence: u64,
    exact: bool,
}
```

The API should distinguish distance from similarity and document whether lower
or higher values are better for each metric.

## Vectors should not be ordinary row fields

WorkTables currently serializes complete rows into byte pages:
[`src/in_memory/data.rs`](../src/in_memory/data.rs).

A 1,536-dimensional `f32` vector occupies:

```text
1,536 * 4 bytes = 6,144 bytes
```

That can exceed the default page capacity before other row fields and
serialization metadata. WorkTables supports configurable larger pages, but that
does not create a good vector-search layout.

Distance evaluation needs vectors to be:

- Fixed-width.
- Contiguous.
- Aligned for SIMD.
- Addressable by dense integer ID.
- Readable without deserializing the containing row.
- Optionally available in original and quantized forms.

The correct structure is a vector sidecar:

```text
WorkTable row
    id, tenant, document, text, ...
                |
                | stable VectorId
                v
Vector slab
    [vector 0][vector 1][vector 2]...
                |
                +--> quantized slab
                |
                +--> ANN adjacency/index
```

This is effectively a specialized form of the columnar projection described in
[`docs/worktables-columnar.md`](worktables-columnar.md). The columnar work,
including stable identity, logical mutation sequences, immutable generations,
and rebuildable projections, directly benefits vector storage.

## Required identity model

Use a stable dense `VectorId`, not `Link`:

```text
PrimaryKey <-> VectorId
                  |
                  +--> current RowLink
                  +--> vector slab offset
                  +--> ANN node
                  +--> deletion/version state
```

An ideal internal ID is a dense `u32` until a table may exceed four billion
vectors. Dense IDs make adjacency lists, bitmaps, candidate sets, and vector
offsets much smaller.

Physical WorkTables links can change during update or vacuum. If links are
embedded in an ANN graph, every relocation potentially requires graph changes.
Stable IDs isolate the graph from row movement.

The mapping also needs an explicit lifecycle:

```rust
enum VectorSlotState {
    Staged,
    Visible,
    Deleted,
    Retired,
    Reclaimable,
}
```

Reusing an internal ID while an older search can still reach it creates an ABA
problem. Reclamation must wait for all readers and graph references that could
observe the old identity.

## The most promising shortcut: DiskANN3

Microsoft's current DiskANN3 is a Rust library explicitly designed to add
vector indexing to a host database. Rather than owning all durability and
storage, it defines a `DataProvider` interface for:

- External ID to internal ID translation and the reverse mapping.
- Vector storage and access.
- Graph adjacency access and mutation.
- Soft deletion and final release.
- Insert guards and rollback.
- Attribute-filter hooks.
- Memory, disk, or mixed storage tiers.

Its stated design goal is to inherit durability and availability from the host
database. That is an unusually close match for WorkTables.

A WorkTables provider could map:

```rust
type ExternalId = DocumentChunkPrimaryKey;
type InternalId = VectorId;
```

and implement:

```text
set_element
    -> allocate VectorId
    -> write vector slab
    -> establish PK <-> VectorId

get vector
    -> direct aligned vector-slab access

get/set neighbors
    -> adjacency sidecar pages

delete
    -> tombstone VectorId

release
    -> reclaim after readers and graph references drain
```

DiskANN3 includes distance and quantization components for x86 and ARM,
real-time update research, memory-tier selection, and hooks for filtered search.

Important caveats include:

- Its current workspace version is `0.55.0`, so API stability should not be
  assumed.
- The provider interface is large and asynchronous.
- WorkTables still must supply correct persistence, identity, visibility, and
  reclamation.
- Vendor benchmarks must be reproduced independently.
- The dependency should be pinned tightly behind an experimental Cargo feature.
- WorkTables should not tie its public DSL permanently to DiskANN-specific
  vocabulary.

Nevertheless, this is a much more credible starting point than implementing
HNSW from scratch.

## Filtered vector search is the real differentiator

Plain nearest-neighbor search is only part of a useful vector database. Real
queries look like:

```text
nearest embedding
where tenant_id = 42
and language = English
and created_at >= last_week
```

There are several execution strategies:

1. Search ANN and post-filter the results.
2. Precompute eligible IDs, then traverse ANN with a predicate.
3. Use the scalar index and exactly score the filtered candidates.
4. Use filter-aware graph edges.
5. Oversample ANN candidates and exactly rescore them.

No single strategy wins for every filter cardinality.

The WorkTables planner should estimate the candidate count from typed scalar
indexes:

```text
Filter matches 100%       -> ordinary ANN
Filter matches 10%        -> filtered ANN
Filter matches 0.1%       -> scalar index + exact SIMD scoring
Filter unindexed/unknown  -> oversampled ANN or reject in strict mode
```

Qdrant already performs this kind of cardinality-sensitive planning and
augments HNSW with filter-aware edges. LanceDB supports prefiltering,
postfiltering, adaptive IVF probing, and exact-index bypass for highly selective
filters.

WorkTables' possible advantage is that metadata is statically typed and already
declared in the table schema. But the existing indexes return physical links.
Efficient vector filtering would benefit from an additional representation:

```text
tenant_id = 42 -> bitmap or sorted set of VectorId
language = EN  -> bitmap or sorted set of VectorId
```

These sets can be intersected before or during ANN traversal without fetching
and deserializing complete rows.

Cardinality estimates can initially be exact `len()` values from simple index
lookups. Compound boolean expressions need lower and upper bounds or bitmap
operations. The planner should expose its selected path for benchmarking and
debugging.

## Exact search remains necessary

ANN is not always faster. Exact search is useful for:

- Small tables.
- Highly selective metadata filters.
- Ground-truth generation and recall measurement.
- Distance thresholds that require complete evaluation.
- Index construction and validation.
- Queries using a metric different from the built ANN index.

The first vector implementation should therefore be an exact engine, not an
ANN graph. It establishes correct semantics and a benchmark oracle.

An exact engine should support:

- Cosine distance.
- Dot product.
- Euclidean/L2 distance.
- Normalized-vector insertion for fast cosine search.
- Batched candidate IDs.
- Top-k selection without sorting every result.
- Architecture-specific SIMD with a portable fallback.
- Optional parallel scanning for large candidate sets.

## Persistence and update architecture

A vector insert affects far more than one row:

```text
row bytes
scalar indexes
vector bytes
external/internal ID directory
several graph adjacency lists
quantized representation
WAL state
```

The current WorkTables persistence operations store row bytes and structural
index changes:
[`src/persistence/operation/operation.rs`](../src/persistence/operation/operation.rs).
They do not provide an atomic transaction over an ANN graph.

The first implementation should therefore treat the ANN index as a rebuildable
accelerator:

```text
Authoritative:
    WorkTables rows
    original vectors
    stable IDs
    logical mutation sequence

Rebuildable:
    quantized vectors
    ANN graph
    filter sidecars
```

On recovery:

1. Load authoritative rows and vectors.
2. Load the last valid ANN generation if its schema fingerprint matches.
3. Replay vector mutations after the generation's applied sequence.
4. Rebuild if validation fails.

Updates to a vector should initially behave as:

```text
tombstone old VectorId
allocate new VectorId
insert new vector
publish new PK -> VectorId mapping
reclaim old node later
```

Trying to modify a live graph and reuse its ID immediately creates ABA and
reader-safety hazards.

An index generation should record at least:

```rust
struct AnnGenerationHeader {
    format_version: u32,
    schema_fingerprint: [u8; 32],
    index_fingerprint: [u8; 32],
    vector_dimension: u32,
    metric: DistanceMetric,
    generation: u64,
    applied_sequence: u64,
    vector_count: u64,
    checksum: [u8; 32],
}
```

## Snapshot requirements

A query may visit hundreds or thousands of graph nodes. During that time:

- Rows may be deleted.
- Vector mappings may change.
- Adjacency lists may be replaced.
- An index generation may be swapped.
- Physical links may be reclaimed.

A query should pin:

```rust
struct VectorSearchSnapshot {
    row_epoch: u64,
    index_generation: Arc<AnnGeneration>,
    applied_sequence: u64,
}
```

Immutable index generations plus `Arc` retention are easier to reason about
than mutating every structure in place. A small mutable delta can cover newly
inserted vectors between rebuilds.

Possible freshness policies are:

```rust
freshness: exact
freshness: at_least(sequence)
freshness: bounded_staleness(Duration::from_millis(100))
freshness: best_effort
```

Until an atomic row/index publication protocol exists, exact read-your-writes
behavior can fall back to an exact scan of the committed vector delta plus the
stable ANN generation.

## Potential performance improvements

There are three different comparisons.

### Against current WorkTables

Current WorkTables has no vector-search path. A contiguous SIMD exact scan would
be a major improvement over deserializing full rows, while ANN can reduce the
number of distance calculations drastically on large datasets.

For one million 768-dimensional `f32` vectors, a full scan reads approximately:

```text
1,000,000 * 768 * 4 = 3.072 GB per query
```

Even at 100 GB/s of sustained memory bandwidth, reading the vectors alone has a
theoretical floor of roughly 31 ms, before distance arithmetic, filtering, or
result selection.

For 1,536 dimensions, that becomes approximately 6.1 GB per exact query. ANN
search avoids touching most vectors, trading exact recall for a graph traversal.

### Against Qdrant

WorkTables could plausibly improve end-to-end latency when:

- WorkTables is embedded in the application.
- Qdrant would otherwise be accessed over REST or gRPC.
- The collection is small or medium-sized.
- Metadata filters are strongly typed and selective.
- Results are immediately joined with other WorkTables rows.
- The application currently duplicates data between its primary store and
  vector database.

The gains would come primarily from:

- No RPC.
- No protobuf or JSON payload conversion.
- No separate connection pool.
- No second metadata representation.
- Direct generated filters.
- Direct row lookup after vector candidate selection.
- Compile-time known dimension and metric.
- Fewer allocations in the query path.

However, WorkTables has no inherent reason to beat Qdrant's core HNSW
distance-search loop. Qdrant already uses SIMD, quantization, memory mapping,
filter-aware HNSW, segment optimization, and query planning.

For large unfiltered ANN searches, an early WorkTables implementation is more
likely to lose than win.

### Against LanceDB

WorkTables may have an advantage for:

- In-memory mutable tables.
- Very cheap primary-key operations.
- Typed application metadata.
- Fine-grained row updates.
- Rust-native embedded integration.

LanceDB has major advantages for:

- Arrow interchange.
- Columnar scans.
- Large on-disk datasets.
- MVCC snapshots.
- IVF and IVF+HNSW.
- Quantization.
- Full-text and hybrid search.
- ML and Python ecosystem integration.

The initial WorkTables target should not be to beat LanceDB at its complete
workload. It should be to beat a separate row database plus vector database for
a tightly integrated embedded workload.

## Quantization opportunity

Raw `f32` storage is expensive. Common quantization choices include:

- Scalar `f32` to `uint8`: approximately four times smaller.
- Binary quantization: up to 32 times smaller.
- Product quantization: potentially greater compression, but less
  SIMD-friendly.
- Quantized candidate generation followed by exact rescoring against original
  vectors.

These improvements are not WorkTables-specific. WorkTables would need to
implement or integrate equivalent algorithms.

A sensible progression is:

```text
f32 exact
    -> normalized f32 cosine/dot
    -> scalar i8 candidates + f32 rescore
    -> binary candidates + f32 rescore
    -> product/RaBitQ quantization
```

Normalization should happen during insertion for cosine search so the query
path can use a dot-product kernel. The API must either reject zero vectors or
define their cosine behavior.

## Where WorkTables could genuinely outperform

| Workload | Potential |
|---|---|
| Embedded application, small top-k, typed filters | Strong opportunity |
| Highly selective metadata filter | Strong opportunity through scalar-index candidate generation and exact scoring |
| Fetching complete typed rows after vector search | Strong opportunity |
| Small collections where exact SIMD beats ANN | Good opportunity |
| Large unfiltered in-memory ANN | No inherent WorkTables advantage |
| High update rate while preserving recall | Difficult; likely behind mature systems initially |
| Data larger than RAM | Requires mmap, asynchronous I/O, and cache work |
| Billion-scale indexing | Far beyond an initial implementation |
| Distributed highly available vector service | Not currently comparable |

The biggest product win may be architectural rather than kernel-level:

> One embedded database owns the row, metadata indexes, vector, and retrieval
> lifecycle.

That eliminates dual writes and cross-database consistency problems.

## Benchmark requirements

No speedup should be published without recall-normalized comparisons.

The test matrix should cover:

```text
Rows:       10K, 100K, 1M, 10M
Dimensions: 128, 384, 768, 1,536
Metrics:    cosine, dot, L2
Top-k:      10, 50, 100
Recall:     0.90, 0.95, 0.99
Filters:    100%, 10%, 1%, 0.1%, 0.01%
Storage:    resident, mmap-warm, mmap-cold
Writes:     append-only, 1% update, sustained churn
```

Compare:

- WorkTables flat scalar.
- WorkTables flat SIMD.
- WorkTables plus DiskANN3.
- Qdrant locally over gRPC.
- LanceDB embedded.
- A raw ANN library to isolate database overhead.

Measure:

- p50, p95, and p99 query latency.
- QPS at controlled concurrency.
- Recall at k.
- Index build duration.
- Insert, update, and delete throughput.
- Query latency during sustained writes.
- Bytes per vector.
- Peak build and compaction memory.
- Recovery and rebuild duration.
- Filter cardinality-estimation accuracy.
- Time from row commit until vector-search visibility.

Service-level and core-index results must be reported separately. Otherwise,
eliminating gRPC can be mistaken for a better ANN algorithm.

Datasets should include both traditional ANN benchmarks and embeddings from the
actual target models. Results from SIFT-like low-dimensional vectors do not
necessarily predict behavior for 768- or 1,536-dimensional text embeddings.

## Recommended implementation plan

### Phase 0: feasibility spike

Estimated duration: four to eight weeks.

- Introduce an internal `Embedding<const D: usize>` outside the macro.
- Create a contiguous aligned `VectorSlab`.
- Implement cosine, dot, and L2 exact scan.
- Generate exact ground truth.
- Benchmark scalar versus SIMD kernels.
- Run DiskANN3 using its in-memory provider.
- Compare against local Qdrant and embedded LanceDB.
- Do not modify persistence yet.

Go or no-go questions:

- Does embedded execution produce meaningful end-to-end improvement?
- Are typed filtered queries a real advantage?
- Is DiskANN3's provider interface stable and usable enough?
- Can the desired recall be achieved within the memory budget?

### Phase 1: experimental embedded index

Estimated duration: two to three months.

- Extend the DSL parser to accept real types or `Vector<T, D>`.
- Add stable dense `VectorId` values.
- Add PK to `VectorId` and `VectorId` to row-link directories.
- Add a generated nearest-neighbor query builder.
- Implement read-only and manual index builds.
- Add projection-style field selection so result rows do not clone embeddings.
- Keep the ANN index rebuildable.

### Phase 2: mutable and recoverable

Estimated additional duration: three to six months.

- Add a logical vector mutation stream.
- Add soft deletion and deferred reclamation.
- Implement asynchronous index maintenance.
- Add immutable index generations and reader pinning.
- Add fingerprints and recovery/rebuild handling.
- Exercise crash points and partial-index publication.
- Expose freshness and applied-sequence information.

### Phase 3: filtering and quantization

Estimated additional duration: three to six months.

- Generate filterable `VectorId` bitmaps or sets.
- Add filter cardinality statistics.
- Choose exact, ANN, prefilter, and postfilter paths.
- Add scalar quantization and exact rescoring.
- Add mmap vector and graph storage.
- Add compaction and memory budgets.
- Benchmark under mixed reads and writes.

### Phase 4: production embedded vector store

A rough judgment is nine to eighteen months total for one experienced
database/ANN engineer. The estimate depends heavily on DiskANN3 integration
quality and required durability guarantees.

### Phase 5: Qdrant-class service

A network service adds:

- Collection and index management.
- REST and gRPC protocols and client SDKs.
- Authentication and authorization.
- Quotas and backpressure.
- Snapshots and online restore.
- Sharding.
- Replication.
- Consensus and topology management.
- Resharding.
- Rolling upgrades.
- Observability and operational tooling.
- Multi-tenancy and noisy-neighbor controls.

That is approximately an eighteen-to-thirty-six-month project for a small,
experienced team, not a feature added to the macro crate.

## Principal limitations and fragility

1. **Approximation.** ANN results require continuous recall measurement against
   exact ground truth.
2. **Model evolution.** Changing the embedding model or dimension usually
   requires complete re-embedding and index rebuilding.
3. **Write amplification.** One logical vector mutation can alter vectors,
   directories, graph adjacency, quantized forms, and persistence state.
4. **Deletion complexity.** Deleted nodes may remain graph-reachable until the
   graph is repaired or the node is safely reclaimed.
5. **Quantization loss.** Compression trades accuracy for memory and speed.
6. **Large row values.** Storing vectors in normal rows would dominate page and
   WAL size and impair scan locality.
7. **Filtered-search recall.** Naive prefiltering and postfiltering can miss
   relevant neighbors or return too few results.
8. **Snapshot semantics.** Current WorkTables range reads are not snapshots.
9. **Identifier safety.** Existing physical links cannot safely serve as
   permanent graph node IDs.
10. **Atomicity.** Current persistence cannot atomically publish arbitrary graph
    mutations alongside all row and scalar-index changes.
11. **Portability.** SIMD behavior and available instructions vary by CPU and
    architecture.
12. **Memory management.** Large mmap indexes require careful page-cache, I/O,
    and prefetch behavior.
13. **Language ecosystem.** Python and RAG adoption still require the PyO3 and
    Arrow interface described in [`Python-WorkTables.md`](../Python-WorkTables.md).
14. **Feature breadth.** A complete vector database also needs sparse vectors,
    multivectors, FTS, hybrid fusion, reranking, operational APIs, and backup
    tooling.
15. **Dependency risk.** DiskANN3 is pre-1.0 and could introduce breaking API or
    format changes.

## What not to do first

- Do not implement HNSW from scratch before benchmarking an existing library.
- Do not store large embeddings only as normal serialized row fields.
- Do not use physical `Link` values as durable graph node IDs.
- Do not make ANN graph mutations synchronous with every row commit before an
  atomic publication protocol exists.
- Do not omit exact search; it is required for correctness and recall
  measurement.
- Do not compare QPS without holding recall constant.
- Do not call an embedded-versus-gRPC improvement an ANN algorithm improvement.
- Do not promise Qdrant compatibility before defining the intended product
  boundary.

## Final recommendation

Build this as a separate experimental crate or feature:

```text
worktable-vector
```

Start with:

```text
WorkTables rows
    + stable VectorId
    + dedicated vector slab
    + exact SIMD scan
    + DiskANN3 provider
    + typed scalar filtering
    + rebuildable ANN generations
```

The best initial positioning would be:

> WorkTables Vector: a compile-time typed, embedded retrieval engine for
> applications that need transactional metadata and low-latency vector search
> in the same process.

That is technically plausible, differentiated, and narrow enough to benchmark
honestly. The strongest potential improvement is not a magically faster
distance calculation. It is eliminating the boundary between the application's
primary typed data and its vector retrieval system.

## References

- [Qdrant indexing](https://qdrant.tech/documentation/manage-data/indexing/)
- [Qdrant storage and WAL versioning](https://qdrant.tech/documentation/manage-data/storage/)
- [Qdrant quantization](https://qdrant.tech/documentation/manage-data/quantization/)
- [Qdrant filtered vector-search design](https://qdrant.tech/articles/vector-search-filtering/)
- [Qdrant distributed deployment](https://qdrant.tech/documentation/scaling/distributed_deployment/)
- [Qdrant large-scale memory sizing](https://qdrant.tech/documentation/tutorials-operations/large-scale-search/)
- [LanceDB vector indexes](https://docs.lancedb.com/indexing/vector-index)
- [LanceDB metadata filtering](https://docs.lancedb.com/search/filtering)
- [Lance file-format architecture](https://lance.org/format/)
- [Lance table format and MVCC](https://lance.org/format/table/)
- [Microsoft DiskANN3 repository](https://github.com/microsoft/DiskANN)
- [DiskANN3 `DataProvider` source](https://github.com/microsoft/DiskANN/blob/main/diskann/src/provider.rs)
- [DiskANN project and research overview](https://github.com/microsoft/DiskANN/wiki/DiskANN-Project-and-Research-Overview-%282018%E2%80%90present%29)
- [DiskANN3 workspace manifest](https://raw.githubusercontent.com/microsoft/DiskANN/refs/heads/main/Cargo.toml)
- [Rust CV HNSW implementation](https://github.com/rust-cv/hnsw)
- [USearch Rust API](https://docs.rs/usearch/latest/usearch/)
