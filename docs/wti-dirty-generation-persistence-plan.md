# WTI dirty-generation persistence

**Status:** first stage implemented behind `logical-index-persistence`; dirty-generation checkpoints remain follow-up work.

**Scope:** reduce caller-thread persistence overhead for WorkTablesIndex (WTI) without changing point-read behavior or weakening recovery.

## Why investigate this

WTI currently emits structural change events while mutating the in-memory tree. Encoding, batching, and file I/O already run in WorkTable's asynchronous persistence task, but the mutation thread must still construct the exact split/insert/remove event stream.

A focused local ARM probe measured:

| WTI mutation path | Median |
|---|---:|
| Direct in-memory insert/remove | 125–126 ns/op |
| Structural CDC insert/remove | 160–161 ns/op |
| Synchronous persistence-specific delta | approximately 35 ns/op (27.7%) |

These are two interleaved ten-trial microprobe runs, not publication results. They exclude row work and disk I/O. They establish that caller-side CDC is large enough to optimize.

The first implementation was then measured with another ten-trial interleaved
ARM probe over 2,000,000 existing-key mutations and 8,000,000 point reads per
trial:

| Path | Median | Delta |
|---|---:|---:|
| Structural CDC update | 80.916 ns/op | baseline |
| Logical foreground update | 71.440 ns/op | 11.71% faster |
| Raw WTI point read | 105.096 ns/op | baseline |
| Wrapped WTI point read | 103.068 ns/op | 1.93% faster (noise; no observed regression) |

These repeat-run results were tight across the ten trials, but they remain local
engineering probes rather than paper numbers. Generated-table select
measurements were noisy enough to require a dedicated quiet-window run before
drawing a sub-percent conclusion. The feature therefore remains opt-in.

WorkTable already uses lifecycle flags (`GHOSTED`, `DELETED`, and `VACUUMED`) and optional immutable row versions to prevent readers from observing partially published rows. Those are visibility states, not persistence dirty states, but the staged-publication pattern is relevant.

## Why one dirty bit is insufficient

A Boolean has a lost-update race:

1. a writer sets `dirty = true`;
2. the flusher snapshots the node;
3. a second writer mutates the node while `dirty` is already true;
4. the flusher writes its older snapshot and clears `dirty`;
5. the second mutation is no longer represented by either the disk image or the dirty bit.

The persistence marker must carry a generation or an explicit redirty state. Clearing is conditional: the flusher may mark a node clean only if no writer advanced its generation after the snapshot began.

## Implemented first stage: background structural translation

The first implementation deliberately avoids a new WTI disk format. With the
`logical-index-persistence` Cargo feature enabled:

1. each persisted primary or unique-secondary WTI is wrapped by `PersistentWtiIndex`;
2. point reads delegate directly to WTI, with no runtime feature branch or new
   read-side lock;
3. each successful foreground mutation emits one logical Set/Remove event;
4. the existing persistence worker owns a private shadow WTI reconstructed from
   the current `.wt.idx` pages;
5. that shadow translates logical events into native structural CDC; and
6. the existing `SpaceIndex`/`SpaceIndexUnsized` writer applies those events to
   the unchanged DataBucket page format.

Foreground stripes establish same-key mutation order only. They use
`DefaultHasher`, so stripe identity has no relationship to key or range order.
The queue analyzer sorts each per-index stream by its global event ID before
dispatch, and the logical shadow sorts again at its boundary as defense in
depth. Reversed-delivery regression coverage proves that a Set/Remove pair is
applied in event-ID order.

Each logical WTI contains 64 inline `parking_lot::Mutex<()>` stripes. Their
target-dependent fixed footprint is paid once per persisted primary or unique
WTI; point reads never access them. This is a deliberate write-concurrency
tradeoff and should be included in schema-level memory measurements.

This is an intentionally smaller step than the checkpoint/WAL design below. It
moves the measured structural-CDC work off the caller thread while retaining
format compatibility in both directions: a store written with the feature can
be reopened without it, and a pre-feature store can be opened with it. The
existing WorkTable persistence queue remains the recovery authority, including
its documented best-effort crash-durability boundary; this stage does not add a
new durable logical WAL.

Shadow divergence is reported as `PersistenceIndexCorruption`, not a generic
worker error. It quarantines the table's whole persistence engine, rejects later
persistence submissions, and is surfaced by `wait_for_ops` and `close`.
Continuing row or sibling-index writes after one index diverges would knowingly
create an inconsistent store, so the current architecture cannot safely
quarantine only that index.

The code generator selects this path using the forwarded
`worktable_codegen/logical-index-persistence` feature. That check intentionally
runs in the proc-macro crate: emitting a downstream `#[cfg]` would inspect the
consumer package's feature namespace rather than WorkTable's dependency
feature. Feature-off and feature-on expansion tests cover both selections.

The DSL contract is unchanged:

- omitting `using` selects WorkTablesIndex;
- `using congee` selects `congee-wt`;
- `using arctic` selects `arctic-wt`.

Congee and Arctic already use their native topology checkpoint plus logical WAL
implementations. Their `export_topology`/`from_topology` APIs are sufficient, so
this first stage requires no source change in either fork.

Non-unique WTI secondary indexes continue to emit structural multimap CDC in
this stage. Their logical record must identify both key and row link, and should
be added only with the same rollback, duplicate-ordering, and reload coverage.

## Longer-term architecture

Separate crash authority from physical checkpoint maintenance:

```text
caller mutation
  ├─ commit in-memory WTI mutation
  ├─ append/enqueue a small logical redo record
  └─ advance dirty generation for affected topology

background persistence task
  ├─ persist logical redo in operation order
  ├─ snapshot dirty topology while pinned
  ├─ write native WTI checkpoint pages
  └─ retire redo only after checkpoint durability
```

The logical redo log guarantees recoverability while physical WTI pages catch up. Dirty generations make physical mirroring coalescing and asynchronous: ten mutations to one node can become one checkpoint write.

### Dirty-generation state

The conceptual state is:

```text
Clean(g) -> Dirty(g + 1) -> Flushing(g + 1)
                                 ├─ unchanged -> Clean(g + 1)
                                 └─ mutated   -> Dirty(g + n)
```

This can be represented by one atomic generation plus state bits, or by separate generation and queued/flushing flags. The required properties are:

- every committed mutation advances a generation;
- only the clean-to-dirty transition needs to enqueue work;
- a writer during `Flushing(g)` advances the generation and leaves the node dirty;
- the flusher records the generation before copying and rechecks it after copying;
- the flusher clears dirty state only with a compare/exchange against the generation it copied;
- failed compare/exchange requeues or retains the existing queued marker.

Memory ordering must publish node bytes before the dirty generation becomes visible to the flusher and must prevent a successful clean transition from moving before snapshot completion.

### Logical redo record

The minimum WTI record is backend-native logical state, not a structural IndexSet event:

```text
Set { sequence, key, link }
Remove { sequence, key }
```

Rollback/acknowledgement must remain gapless in the persistence analyzer. A complete frame has a checksum; an incomplete final frame is a torn tail. Records are encoded and written by the existing background task.

A logical log does not promise that replay recreates the exact intermediate split history. It promises the correct WTI key/link state. A successful physical checkpoint still stores an exact native WTI topology for its checkpoint generation.

If exact post-checkpoint topology must also be reproduced solely from redo, then the mutation linearization order must be captured globally. That cost must be measured before making it a requirement; logical correctness does not need it for commuting keys.

## Structural changes are groups, not isolated nodes

A split, merge, root replacement, or table-of-contents change can affect a parent, old node, new node, and root metadata. Incremental flushing must not publish an arbitrary mixture as one durable checkpoint.

Two implementation levels are possible:

### Level 1: logical WAL plus whole-index native checkpoints

- Main thread creates only the logical record and advances one index generation.
- Background task periodically snapshots/reconstructs the complete WTI index.
- Atomic checkpoint replacement establishes one generation boundary.
- No per-node durable grouping is required.

This is the recommended first implementation. It has the smallest correctness surface and directly tests how much of the approximately 35 ns structural-CDC delta is recoverable.

### Level 2: incremental dirty-node checkpoints

- Every structural mutation reports the complete affected-node set.
- Nodes are pinned against reclamation while copied.
- A checkpoint transaction or generation manifest commits the affected pages and root/TOC metadata together.
- Old pages remain reachable until the new generation manifest is durable.

This can reduce checkpoint bandwidth for large indexes, but it is substantially more complex. It should follow, not precede, Level 1 evidence.

## Reclamation and identity

The background task must never follow a node pointer that can be freed or reused. Acceptable designs include:

- queue an owning `Arc`/pin rather than a raw pointer;
- retain the node through WTI's existing epoch/reclamation mechanism until the flush generation finishes;
- queue a stable node ID and validate its generation before copying;
- use a quiescent whole-index snapshot for Level 1.

A stable ID without a generation is insufficient because an allocator may reuse it for another node.

## Durability and recovery ordering

The intended recovery protocol is:

1. validate and load the last complete native WTI checkpoint;
2. replay complete logical redo frames after that checkpoint generation;
3. reject complete checksum corruption and ignore only an incomplete final frame;
4. rebuild from authoritative data pages if the checkpoint/log contract cannot be validated;
5. truncate or rotate redo only after the replacement checkpoint and generation manifest are durable.

The data/index ordering rule remains essential: a durable index record must not point to row bytes that were never made durable. If WorkTable continues to acknowledge operations before fsync, documentation and `wait_for_ops()` semantics must state that boundary precisely.

S3 synchronization must copy a self-consistent checkpoint generation plus its required redo tail. Uploading files independently without a generation manifest can create a valid-but-mismatched restore set.

## Hot-path target

The mutation thread should do only:

- the existing WTI mutation;
- one small logical record in an inline/stack-backed event container;
- one sequence allocation needed by rollback/ordering;
- a generation transition and at most one queue publication per clean-to-dirty transition.

It should not:

- serialize a node;
- allocate one heap `Vec` for every single event when an inline event suffices;
- wait for file I/O or checkpoint reconstruction;
- take a new lock on point reads;
- scan or diff a node after releasing the mutation's synchronization.

## Implementation stages

1. **Background structural translation (implemented, feature-gated).** Preserve the existing disk format and move structural CDC onto a worker-owned shadow WTI.
2. **Measure and instrument.** Keep the direct-versus-structural-CDC probe, add allocations/op and p50/p99, and measure full generated WTI table operations.
3. **Evaluate the result.** Continue only if the caller-thread savings survive end-to-end WorkTable benchmarks.
4. **Define stronger recovery authority.** Make index rebuild from authoritative data pages an explicit, tested fallback and version any new WTI format.
5. **Add durable logical WTI redo and whole-index checkpoints.** Use atomic replacement and a checkpoint generation; prove redo truncation ordering with crash injection.
6. **Optionally add incremental dirty nodes.** Introduce the generation state machine, pinning, structural groups, and generation manifest.
7. **Validate local disk and S3.** Test interrupted append, checkpoint, rename, upload, download, and writes after recovery.

## Correctness gates

- A writer racing a flusher cannot lose its dirty state.
- Multiple writes while queued coalesce without losing the latest generation.
- Split/merge/root transitions recover to a valid WTI with exactly the expected key/link set.
- Failed unique inserts and rolled-back multi-index operations leave no durable logical mutation.
- Delete/reinsert and vacuum link relocation replay correctly.
- Torn final redo is recoverable; complete corruption is a hard error or explicit rebuild path.
- Node reclamation cannot race a background snapshot.
- Restart, replay, further mutation, and a second restart remain correct.
- Memory-only WTI and WTI point reads have no new field access, branch, lock, or measurable regression.

## Performance gates

- Report absolute nanoseconds and percentages; the absolute caller cost is the engineering target.
- Compare direct WTI, current structural CDC, logical redo, and full persisted WorkTable operations.
- Run at least ten interleaved trials on quiet ARM hardware.
- Include 1 thread and representative contention, allocations/op, throughput, p50, p99, checkpoint bandwidth, recovery time, and WAL growth.
- Feature-gate the new protocol if any production-relevant workload regresses measurably.

The first success criterion is to recover a meaningful portion of the approximately 35 ns synchronous structural-CDC delta without moving cost onto reads or weakening crash recovery. Incremental dirty-node flushing is justified only if whole-index background checkpoints then become the limiting cost.
