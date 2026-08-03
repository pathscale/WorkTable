# Versioned row publication

Status: feature-gated prototype behind `versioned-row-publication`.

## Problem

The original fast path deserializes directly from archived bytes in an
`UnsafeCell` page while same-size updates mutate those bytes in place. Returning
an owned row prevents references from escaping, but it does not make a read
that overlaps a write data-race-free: the deserializer can still access bytes
while a writer changes them. A sequence counter cannot repair this in Rust,
because detecting a race after it happened does not make the racy byte access
defined.

Physical links introduce a second issue. A reader can obtain a link from an
index, pause, and resume after delete or vacuum has reused that address for a
different row. Predicate revalidation helps, but reclamation must also cover
the interval from index lookup through acquisition of a stable row version.

## Protocol

With the feature enabled, `DataPages` maintains two representations:

- Archived page bytes are the compact persistence and mutation image. All
  accesses that can overlap a mutation are serialized by an internal page
  barrier.
- A concurrent link map holds an immutable application-visible row version.
  Each slot contains one `Arc<Row>` and its ghost, deleted, and vacuum lifecycle
  bits in a single version protected by a short per-slot lock. A reader cannot
  observe a row from one publication together with flags from another.

The generated API follows these publication rules:

1. **Read.** Acquire a read-grace guard before consulting an index. Resolve the
   link, acquire an `Arc` to its complete published version, check lifecycle and
   index predicates, clone the owned row, and release the guard. Unique and
   primary-key point reads retry when the mapping swings to a replacement link
   while it is being resolved. Point lookup itself uses each provider's strict
   visibility path: WorkTablesIndex 0.0.4 holds its structural mapping stable
   until the selected node is locked, making hits and misses definitive, while
   the ART providers retain their native concurrent point algorithms. Vanilla
   `using indexset` is experimental and excluded from this concurrent-read
   guarantee. A reader never accesses mutable archived bytes after a slot has
   been hydrated.
2. **Insert.** Serialize the complete row and stage a ghosted version. Install
   the primary and secondary indexes. Only after every checked index insert
   succeeds does the lifecycle transition publish the version with release
   ordering. Failed inserts retire an unpublished version.
3. **Update.** Hold the generated row/field lock, mutate the archived image
   under the page barrier, deserialize the completed wrapper, then replace the
   immutable version. A concurrent reader can return the complete old version
   or the complete new version, never a partially updated row.
4. **Delete.** Remove index reachability, mark the version deleted, and retire
   its physical link. The empty-link allocator cannot reuse it while a reader
   that could have captured the old index entry remains active.
5. **Vacuum.** Copy a complete row to a staged destination version, swing its
   indexes, and retire the source publication and page. Retired links, slots,
   and pages become reusable only after a read-side grace period.
6. **Reload.** Persisted tables hydrate immutable slots lazily under the page
   barrier. The first read of a row holds the barrier's shared side across
   deserialization and publication, so it temporarily excludes writers.
   Latency-sensitive applications can warm the table with a scan before
   admitting write traffic. Subsequent generated reads use the published
   version map.

The grace period is quiescent-state reclamation: a feature-only atomic counter
tracks generated reads, and retirement queues are drained when that counter is
zero. `Arc` ownership independently keeps a version alive after a reader has
acquired it.

Creating a lazy `SelectQueryBuilder` does not enter the grace period. The guard
is acquired when iteration first starts, before the backend can yield its first
link, and is released when that iterator is consumed or dropped. A partially
consumed iterator is still an active read: retaining one intentionally delays
link, publication, and page reuse. Retirement backlogs emit progressively
spaced warnings after 1,024 entries so an abandoned or unusually long scan is
observable.

Retirement follows a strict unlink-before-retire rule. Delete and vacuum remove
or replace every index reference before queueing the old physical link. A
reader that could still resolve the old reference therefore entered the grace
period before reclamation observed quiescence; a later reader cannot acquire
that retired reference.

## Guarantees and non-guarantees

For generated table APIs in this mode:

- reads do not race with mutation of archived page bytes;
- a read returns a complete row version;
- ghosted or deleted versions are not returned;
- a retired physical link is not reused while a pre-existing generated read
  can still resolve it; and
- unique, non-unique point, and secondary-index range lookups revalidate each
  resolved row predicate.

This is not MVCC and does not add multi-operation transactions or snapshot
range scans. A scan may include or omit a concurrently inserted or updated row.
A point read retries a mapping that changes while its row version is resolved,
but returns `None` after 64 consecutive replacement races rather
than spinning without a bound. The guarantee also does not cover callers that
bypass generated table methods and directly invoke low-level `Data` page
mutation APIs.

## Cost model and rollout

The feature is off by default. Cargo features unify across a dependency graph,
so any dependency enabling it enables it for every WorkTable consumer in that
build. It adds one owned row copy plus slot/map
metadata per live physical link, an atomic increment/decrement per generated
read, a sharded publication-map lookup, and writer-side page serialization.
The index-visibility algorithm is always active and separate from row
publication: WorkTablesIndex acquires the selected node while its structural
mapping is pinned on the uncontended path, and may retry after node contention.
Those costs are inappropriate to impose silently on latency-sensitive users.
The default path remains unchanged; benchmark results for both modes must be
reported before this feature is proposed for default enablement.
