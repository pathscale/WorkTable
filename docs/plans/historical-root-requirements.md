# Historical release requirements to reconcile

Recovered 2026-11-06 from the beta18 backlog, CIDR notes, the September 1
loaded-index failure, and the 1.13.0 report. These are historical requirements
and leads, not current defect confirmations or release acceptance. Reconcile each
against current source and existing persistence, ART and release plans before work.
No builds, tests, benchmarks or runtime checks were performed in this reconciliation.

## Reclamation, unload and accounting

- Audit local ps-reclaim coverage in WorkTable, Arctic, WTI and Congee; avoid
  accidentally activating redundant reclamation engines. Prove progress with
  continuously overlapping readers, bounded retired-object memory and all three
  backends in one process. Compare read/write/mixed/retirement/memory behavior.
- Preserve the requested always-available `unload_gracefully(timeout, quiesce)`
  contract without a DSL keyword or feature gate. A report must account for
  before/released/retained bytes, active readers/writers, elapsed time and outcome.
  Fence new operations, drain readers and persistence, release pages and all
  indexes despite surviving outer handles, and define repeat unload, timeout,
  callback failure and cancellation. Reconcile the ignored generation-swap
  requirements and select/mutate/concurrent-unload races.
- Account for page capacity/usage, each index backend, retired objects,
  persistence queues and fixed overhead; accounting must remain honest after
  unload. Verify actual memory return after unload and vacuum against plain
  vectors and equivalent naked indexes.
- Recheck Arctic destructor traversal across adaptive nodes, replacements,
  indirect values and variable-length keys. If destruction moves off the caller,
  bound pending memory and retain an explicit synchronous drain. Historical
  timing improvements are not current performance evidence.

## Vacuum, index selection and compatibility

- Vacuum admission needs a live mutation-busy check and quiet-period recheck,
  with a bulk-mutation lease covering insert/update/delete and range deletion.
  Measure foreground impact within a noise envelope, require no vacuum work
  during sustained mutation and full reclaim of reclaimable pages when quiet.
  Cover all backends, starvation, cancellation, shutdown and persistence order;
  expose start/defer/sweep/move/reclaim/completion diagnostics.
- Reconcile Arctic String support, variable-key framing/checkpoints, borrowed
  reads without per-lookup allocation, ordering, both scan directions, exclusive
  bounds, NUL, Unicode, empty keys and long prefixes. Any automatic default
  selection must preserve an explicit WTI override and show final DSL examples.
- Legacy primary-index adoption must structurally distinguish WTI, fixed-key
  Arctic and variable-key Arctic; reject corrupt/ambiguous or wrong-schema input.
  Reconstruct the complete key/link map and validate against rows before a
  synced temporary replacement and atomic rename, including directory syncing
  where required. Make interrupted migration recoverable; retain recoverable
  input until validation. Record effective backend metadata and format breaks.
  This historical request does not override the current explicit restore-token
  and known-layout adoption contract in docs/TODO.md.

## Historical correctness leads and ergonomics

- Recheck CIDR issues: tuple keys missing after reload; WTI assertion failures;
  terminal persistence failure/graceful close; insertion outside mutation
  serialization; concurrent row publication safety; phantom rows and crash
  recovery. Resolve whether weak multi-field read consistency is intentional.
- The September 1 failure reported a primary-index event gap from 1439 to 1455
  after nine retries in `loaded_index_growth`. Recheck queue/id publication and
  bounded terminal-error behavior rather than assuming the old failure persists.
- Recheck unique-index predicate revalidation and lock ordering before making
  paper claims. Improve migration/file-inspection tooling.
- Optimize persistence page sorting/coalescing and batched row materialization
  under one read guard before prefetch. First create a memory-pressure-heavy
  range/non-unique workload, then measure cache stalls; any target-specific
  prefetch needs a private abstraction and no-op fallback.
- The 1.13.0 report lists malformed-input panic leads: missing column blocks in
  codegen/version/in-memory expansion; migration names without the WorkTable
  suffix; PersistIndex names without Index, tuple/unit/non-generic fields or
  malformed attributes; malformed PersistTable fields/generics/attributes.
  Recheck current diagnostics; prefer spanned errors to user-triggerable panics.
- Preserve validation of primary-key update rejection for paged tables while
  retaining vector re-keying, duplicate-column indexes, empty updates and
  delete column lists. The old expansion-equality claim is historical only.

## Acceptance and publication gates

- Use disposable restored application fixtures, never a live store. Establish
  a pristine WTI baseline and verify rows, indexes, counts and application queries
  after legacy adoption. Compare attach/startup, memory, point/range reads,
  writes, generation swap and unload before downstream dependency adoption.
- The historical grid requests beta13/15/17/candidate comparisons across WTI,
  Arctic and Congee: publish/reclaim, bulk operations, resident/persisted load,
  exact-token source search, hits/misses, scans, 1/2/4/8/16-thread mixes, async
  insert throughput/tails and vacuum pressure/quiet recovery. Select currently
  relevant versions; record matching revisions, local dependency resolution,
  protocol, bounded runs, raw results, medians/ranges and control movement.
  Record stable micro-regressions instead of hiding them or chasing indefinitely.
- Map DSL/changelog changes to acceptance for overrides, options/composite keys,
  strings/enums, secondary indexes, persistence and awaited async insertion.
  Audit backend concurrency and avoid allocating all backends in one table.
- Verify dependency order (reclamation, backend ports, WorkTable, benchmark and
  application consumers) and publication prerequisites. For the old 1.13.0
  release the reported order was DSL, codegen, WorkTable; verify current versions.
  S3 runtime coverage and downstream rollout require explicit disposition.
- Paper owners must decide authorship, venue scope, implementation-change scope,
  benchmark machine/protocol and permission for anonymized production facts.
  No historical deployment identity or benchmark number is republished here.
