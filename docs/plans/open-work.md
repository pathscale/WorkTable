# Open work

This page consolidates actionable follow-ups that do not already have a durable
design or implementation plan. See the linked documents for scope and technical
constraints.

## Persistence durability and space-layer efficiency

- Decide and document the crash guarantee for ordinary persisted writes, then
  align flush/sync ordering and recovery behavior with that guarantee. The
  current best-effort contract and the need for a layer-wide ordering policy
  are described in [persistence durability](../persistence-durability.md) and
  [space-layer issues](../space-layer-known-issues.md).
- Reduce full table-of-contents rewrites on single index events and avoid
  scanning on-disk value slots to find free entries. Preserve correct page,
  TOC, and recovery ordering while doing so. Details are in
  [space-layer issues](../space-layer-known-issues.md).
- Resolve the documented oversized TOC-entry behavior: the persistence layer
  currently reports a typed failure when an entry exceeds its page budget.
  Decide whether early rejection or segment spilling is appropriate; retain
  conservative size accounting where needed. See
  [space-layer issues](../space-layer-known-issues.md).

## ART persistence follow-through

The native ART checkpoint and logical WAL design is recorded in
[the ART persistence plan](../art-index-persistence-plan.md). Remaining work
includes:

- Validate the flush versus sync contract against the selected crash guarantee.
- Exercise S3 synchronization, including interrupted transfers.
- Define a disk-format migration policy before promising a stable format.
- Measure persistence, recovery, and compaction costs against documented
  correctness gates.

## Paper evidence

The lifecycle paper plan remains in [paper-2-plan.md](../paper-2-plan.md).
Refresh its claims and venue status before use. Open technical work includes a
reproducible lock-discipline comparison, a concise statement of cell-lock and
reclamation invariants, and stronger model checking of the cell/retire path.
Any performance claims require current, reproducible measurements with their
operation definitions and environment recorded. Do not reuse stale numeric
summaries from historical drafts.

## Columnar side-index follow-up

The implementation boundary and API are described in the
[columnar side-index plan](../columnar-index-plan.md) and
[current guide](../columnar-fields-and-indexes-guide-v3.md). Evaluate the
listed query and storage follow-ups against that scope and current code before
starting new work. Keep side indexes distinct from a future authoritative
columnar storage engine, and do not make performance claims without relevant
measurements.
