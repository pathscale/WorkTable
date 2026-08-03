# Native ART index persistence

**Status:** Implemented on `feat/art-native-persistence`; validation in progress.

**Backends:** `arctic-wt` and `congee-wt`.

## Decision

Persisted ART indexes use a backend-native, pointer-free topology checkpoint followed by a logical Set/Remove write-ahead log. They do not serialize WorkTablesIndex pages, and WorkTablesIndex is not retained as an authoritative shadow index.

The physical checkpoint therefore identifies the selected implementation:

- an Arctic index stores Arctic compressed edges, exact `Node3`/`Node15`/`Node47`/`Node256` kinds, slot counts, physical branch slots, and logical WorkTable links;
- a Congee index stores Congee prefixes, exact `Node4`/`Node16`/`Node48`/`Node256` kinds, physical child slots, the `Node48` free-list order, and logical WorkTable links.

Raw pointers, locks, atomics, allocator addresses, epoch/SMR state, and frozen/version bits are never persisted.

## File layout

Each existing `*.wt.idx` path contains one ART file when that access path selects an ART backend:

1. fixed header: magic, format version, backend tag, key width, table version, snapshot length, and snapshot checksum;
2. backend-native topology snapshot;
3. zero or more checksummed logical WAL frames.

Each WAL frame contains an event id, Set/Remove operation, fixed-width unsigned key, and WorkTable `Link` for Set. A complete frame with a bad checksum is an error. An incomplete final frame is treated as a torn tail. Opening the file truncates that tail before any new append, so later durable records cannot become hidden behind it.

This is intentionally not the same byte layout as either crate's volatile memory. It is the same *logical physical topology*: node representation, path compression, and physical slot choices survive a clean checkpoint without persisting process-local state.

## Mutation ordering

WorkTable's existing persistence pipeline expects gapless, monotonic per-index event ids. A persisted ART therefore uses a persistence-only wrapper around the native index:

- point reads still delegate directly to the native ART with no selection branch or new read lock;
- mutations take one of 64 key-hashed stripe locks;
- the native mutation commits and its logical event id is allocated before releasing that stripe;
- failed checked inserts allocate no event id.

The stripe guarantees that non-commuting operations on the same key are replayed in their mutation order. Operations on different keys can receive event ids in a different order from their in-memory linearization, which is safe because those logical operations commute. Hash collisions only reduce write concurrency; they do not weaken correctness.

Memory-only ART indexes do not use this wrapper and retain the original native hot path.

## Checkpoint, recovery, and compaction

Normal persistence appends logical frames and flushes them through WorkTable's existing asynchronous persistence task. It does not keep a second ART in memory.

When the WAL reaches the compaction threshold:

1. read and validate the native checkpoint and WAL;
2. reconstruct a temporary instance of the selected ART;
3. replay Set/Remove records;
4. export its exact pointer-free topology;
5. write and sync a temporary checkpoint file;
6. atomically rename it over the previous file;
7. drop the temporary ART.

Recovery follows the same validation and replay rules but returns the reconstructed ART to the generated table. The reverse primary-key map is rebuilt from the recovered primary ART, as it is for the existing backends.

Compaction temporarily needs memory proportional to that one index, but normal operation does not retain an authoritative shadow tree. Compaction runs in the disk persistence task rather than on point-read threads.

## Fork boundary

The forks add typed topology import/export and no point-operation fields or branches:

- [`pathscale/arctic-wt`](https://github.com/pathscale/arctic-wt) exposes exclusive pointer-free topology export and exact import;
- [`pathscale/congee-wt`](https://github.com/pathscale/congee-wt) exposes the equivalent contract, including `Node48` free-list order and allocator-failure cleanup.

WorkTable owns durable framing, checksums, table versions, WAL semantics, compaction, and WorkTable key/link codecs. The forks deliberately do not own a WorkTable-specific disk format.

## Correctness gates

The implementation is not ready to merge until all of these remain green:

- fork topology round trips for every adaptive node kind and delete-created holes/free slots;
- generated Arctic-primary/Congee-secondary and Congee-primary/Arctic-secondary persist/reload/mutate/reload tests;
- checked-insert rollback and acknowledgement without event-id gaps;
- torn final WAL truncation followed by successful new appends;
- checksum, backend, key-width, topology, and table-version rejection;
- compaction from WAL to a native checkpoint and subsequent reload;
- concurrent same-key mutation ordering;
- full workspace tests, formatting, and clippy with warnings denied;
- release-mode performance comparison for memory-only ART versus persisted ART, reported separately.

## Performance boundary

The feature is selected only by explicit `persist: true` plus `using arctic` or `using congee`. It introduces no cost to WorkTablesIndex, vanilla IndexSet, or memory-only ART tables.

Expected persisted-ART costs are:

- one short striped mutex acquisition per mutation;
- one synthetic logical event allocation in the existing operation object;
- WAL encoding/checksum work in the asynchronous persistence task;
- periodic temporary reconstruction during compaction.

Reads have no new wrapper lock. Any measurable memory-only regression is a blocker. Persisted-ART write latency, throughput, allocations, p99, WAL growth, recovery time, and compaction pause must be measured before a production recommendation.

## Remaining production work

- Validate the existing flush-versus-fsync durability contract against the desired crash guarantee; WAL appends currently match WorkTable's existing flush behavior, while checkpoint replacement calls `sync_data`.
- Run S3 upload/download and interrupted-sync validation. ART files use the existing index paths, but that does not substitute for an end-to-end S3 test.
- Add format migration policy before declaring a stable 1.0 disk contract.
- Measure fork maintenance cost and upstream the generic topology API if maintainers are receptive.
- Keep Congee/Arctic range-scan allocation and isolation limitations explicit; persistence does not change their scan semantics.

## Paper claim

The defensible claim is not “serialize an ART.” It is that WorkTable can statically specialize each generated access path while preserving a coherent durability protocol across physically different indexes: structural CDC for B-trees, and native topology checkpoints plus logical redo for lock-free/concurrent ARTs. The same typed table API selects those mechanisms at compile time without runtime backend dispatch.
