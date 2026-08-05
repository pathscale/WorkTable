# Upstream proposal: a recovery-mode load for WorkTable

**For:** WorkTable (pathscale/WorkTable), the beta.4 torn-store refusal.
**From:** AgencyZero, which consumes WorkTable and ships `wt-migrate` to recover
torn stores.
**Decision context:** AgencyZero picked this over (a) shipping beta.4 with its
recovery tool broken, and (c) working around the gate in the consumer, because
the real defect is that beta.4's correct new guard has no exception for the one
caller that legitimately needs an inconsistent store: recovery.

## What beta.4 added, and why it is right on the normal path

`9c4148c` ("torn-store refusal") added a load-time guard in
`src/table/mod.rs:173`:

```rust
if self.primary_index.reverse_pk_map.len() != links.len() {
    return Err(/* "torn or corrupt persisted table ... different number of
                  rows than the primary index" */);
}
```

On the ordinary open path this is exactly right: a primary index whose key count
disagrees with the data links means a torn write, and loading it silently would
read rows through a wrong map — the corruption class the fingerprint and this
guard exist to stop. Keep it. Live data must be protected by default.

## Why it breaks recovery, which is the point of a recovery tool

`wt-migrate::recover_task_log_index` recovers a store whose *primary* index is
torn by reading the intact *secondary* index. Its flow (simplified):

1. copy the torn table into scratch,
2. read every key straight from the surviving secondary index,
3. rename the corrupt `primary.wt.idx` out of the way so WorkTable bootstraps a
   fresh **empty** primary,
4. re-open the table and rebuild the primary from the recovered keys.

Step 4 is where beta.4 now refuses: the fresh primary has **0** rows while the
secondary/data have **N**, so `reverse_pk_map.len() != links.len()` fires and the
load is rejected before recovery can run. Recovery *inherently* operates on a
store where the indexes disagree — that inconsistency is the thing it is there
to repair. A guard with no recovery exception makes the store unrecoverable by
the very tool built to recover it.

Reproduction (fails on `9c4148c`, passed on `37cf288`):

```
cargo test -p wt-migrate an_intact_secondary_index_recovers
# panics at crates/wt-migrate/src/lib.rs:864:
# "torn or corrupt persisted table ... secondary index project_idx contains a
#  different number of rows than the primary index"
```

Binary compatibility is **not** the issue — a beta.4 build reads a beta.2-written
store cleanly (verified against a real 20 MB live store: projects, messages with
12 KB bodies, items, usage JSON all deserialize). This is purely the load guard
being unconditional.

## Proposed fix: a recovery load that permits the mismatch

Add a load mode that skips the primary/secondary equality gate (and any sibling
consistency gates on the same path), for callers that are explicitly recovering.
The default stays strict.

Sketch — the smallest surface that solves it:

```rust
/// How strict `load` is about on-disk consistency.
pub enum LoadMode {
    /// The default: reject a torn or inconsistent store rather than read it
    /// through a wrong map. What live opens should always use.
    Strict,
    /// For recovery tools only: permit a primary/secondary (or index/data)
    /// row-count mismatch, so a store with a deliberately-emptied primary can
    /// be opened and rebuilt. Never use for a normal open.
    Recovery,
}

impl<...> PersistedTable for ... {
    fn load(engine: E) -> ... { Self::load_with(engine, LoadMode::Strict) }
    fn load_with(engine: E, mode: LoadMode) -> ... {
        // ...
        if matches!(mode, LoadMode::Strict)
            && self.primary_index.reverse_pk_map.len() != links.len()
        {
            return Err(/* torn-store */);
        }
        // ...
    }
}
```

Then `wt-migrate` step 4 calls `load_with(engine, LoadMode::Recovery)` and the
default `load` (every live open) keeps the guard.

### Why a mode flag rather than the alternatives

- **A no-op `load` + separate `repair()` API** would work too, and may be cleaner
  long-term, but it is a larger surface. The mode flag is the minimal change that
  unblocks the bump today and keeps the default safe.
- **Relaxing the guard globally** is wrong — it would reopen the corruption hole
  the guard closed.
- **Consumer-side workarounds** (pre-seeding a matching primary before re-open)
  are fragile and fight the engine; the engine is the right place to say "this
  open is a recovery".

## Ask

A `LoadMode::Recovery` (or an equivalent `repair`-path load) on the same rev, so
AgencyZero can bump to beta.4 — which fixes the index event-gap write failures,
the page/link reclamation faults, and the vacuum memory leak it is hitting daily
— without shipping its recovery tool broken.
