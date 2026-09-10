# PR 105 source-only follow-up

These changes are best-effort source fixes against revision
`5fa24e1b1dd85a7a44b57b60ee247dbe4f54639d`. No compilation, tests, or
benchmarks were performed.

## Behavioral changes

- Columnar tables use a shared publication gate around insert/reinsert/delete
  index maintenance and primary-row publication. Dirty rebuilds acquire it
  exclusively. Mutation stripes must be acquired before this gate; the gate
  precedes the columnar data lock. Non-columnar implementations return no guard.
  Low-level callers that manually combine secondary-index and primary-row changes
  must also hold the publication guard through the whole operation.
- Clean columnar reads avoid taking an exclusive rebuild lock. A dirty rebuild
  still blocks relevant writers for its entire scan. The additional shared
  acquisition on columnar mutation paths and contention cost are unmeasured.
- Live and persisted index capacities are capped at the u16 slot-count limit.
  The byte stride itself is not capped at 64 KiB.
- Persisted CDC deletion clones a secondary key before consuming it, retaining
  the complete row for columnar removal.
- Canonical schemas preserve columnar fields, per-field settings, clustered
  indexes, slot width, and default chunk size. Columnar changes are classified
  as derived-index rebuilds by the planner. The checker validates clustered keys.
- Snapshot-loading opens request read permission only.
- TOC fallback recovery uses the configured stride.
- A persistence engine owns one private worker for blocking HostFile operations,
  separate from compute pools. A scope guard shuts its pool down when the task
  completes or is dropped. This costs one thread per live persistence engine;
  synchronous file operations remain blocking, including foreground loaders.
- Disabled event-ledger batch submission no longer constructs the event-ID vector.

The DSL version and exact consumer pins are advanced to the proposed
`1.0.0-beta.19`. This is a local source proposal, not a published release.
Check version availability and release all dependent packages together before
publishing. The new schema field and diff variant require downstream consumers
with struct literals or exhaustive matches to adapt.

## Still open

Runtime selectors and per-query/section profiles are not fully connected to
execution. This patch does not implement that wiring or remove those APIs.
In particular, the private persistence worker deliberately does not follow a
compute-pool profile. Do not interpret an accepted selector as evidence of
runtime isolation or changed scheduling behavior.

## Regression source

`dsl/tests/columnar_schema.rs` covers metadata round trips, rebuild classification,
and invalid clustered keys. Columnar integration tests cover the gate contract
and representable index capacity. Existing concurrent-reinsert tests remain.
The gate test checks exclusion, not every race interleaving; no test result or
latency improvement is claimed.
