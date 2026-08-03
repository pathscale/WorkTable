# Response to `review.md`

Thank you for the detailed review. The review was made against `0218d40`; the
branch has moved since then, including the stable WorkTablesIndex read contract
from WorkTablesIndex PR #5 and the follow-up changes described here.

The four blocking hot-path/CI findings and the publication tearing issue have
been addressed. The counter-based reclamation design has been made materially
safer and observable, but it has not been replaced with a full epoch/QSBR
implementation. That remaining boundary is called out explicitly below.

## Blocking findings

### B1: default-build row clones — fixed

- `insert` and `update` now move the row into its wrapper when versioned
  publication is disabled.
- The clone exists only in the versioned configuration, where the owned row is
  also needed for publication.
- Successful insert paths move the publication row rather than cloning it a
  second time.

The separate `insert_cdc` reserialization optimization remains a follow-up; it
is not mixed into this correctness patch.

### B2: grace period and retained query builders — contained, not converted to epochs

The counter scheme remains, but the structural idle-builder leak is fixed:

- Lazy `SelectQueryBuilder`s do not acquire a read guard at construction.
- The guard is acquired when iteration starts, before the backend can yield its
  first link, and lives only as long as the active iterator.
- A regression test holds an unconsumed builder across delete/insert and proves
  that it does not prevent physical-link reuse.
- Delete and vacuum follow a documented unlink-before-retire invariant. A new
  reader cannot resolve a retired link after all index references have been
  removed; readers that could have resolved it entered the grace period first.
- Retirement backlogs warn at powers of two starting at 1,024 entries.
- An atomic pending count avoids taking the retirement queue locks when there
  is no reclamation work.

A partially consumed or abandoned active iterator still delays reuse. That is
now documented and observable, but it is not the same isolation property as a
real epoch implementation. A future epoch/QSBR conversion remains worthwhile.
A hard queue cap cannot safely discard retirement records; it would need to
apply backpressure or move reclamation to an epoch collector.

### B3: ART ordered scans — fixed

- Arctic range operations translate Rust bounds to Arctic's native range API.
- Congee range operations use Congee's native range traversal under a pinned
  epoch instead of enumerating all keys and doing one lookup per key.
- Generated table iteration snapshots the ordered links once. It no longer
  restarts a range for every row, removing the quadratic behavior.
- Inclusive, exclusive, unbounded, empty, and maximum-key bounds have coverage.
- Criterion now includes single-row primary-key ranges over 10,000-row Congee
  and Arctic tables.

The adapters still materialize the requested interval because the common API
returns an owned double-ended iterator. The cost is now proportional to the
requested interval rather than the entire index per result row.

### B4: default CI coverage — fixed

CI now builds and tests a matrix containing:

- default features;
- `versioned-row-publication`;
- all features, to retain an additive-feature-unification check.

Strict Clippy runs for both default and all-feature configurations.
WorkTablesIndex PR #5 defines deterministic precedence when multiple search
features are unified. The README now documents that behavior, so WorkTable does
not add a contradictory `compile_error!` for a valid unified Cargo graph.

There is no longer a separate `stable-index-read-retry` WorkTable feature. The
updated WorkTablesIndex `lookup_for_select` contract is definitive for both
hits and misses.

## Correctness and concurrency findings

### C1: torn `(row, flags)` publication — fixed

The row `Arc` and lifecycle flags now live in one `PublishedVersion` protected
by one short per-slot lock. `load()` reads both under the same lock. A concurrent
test alternates paired row/flag states for 100,000 writes and rejects mixed
versions.

### C2: global publication-map contention — fixed

- The single map is replaced with 64 publication shards.
- Shards are selected from a mixed physical offset.
- Reclamation has a pending-work fast path and no longer takes publication
  locks when all retirement queues are empty.

### C3: identity publication hash — fixed

The physical offset is passed through a SplitMix-style avalanche before it is
used by the hash table, distributing both bucket bits and hashbrown control
tags.

### C4: unbounded point-read retry — fixed

Primary-key and generated unique-index point reads retry at most 64 mapping
replacements and issue `spin_loop()` on the retry edge. Perpetual churn can now
produce a bounded `None`, not an indefinitely spinning read.

### C5: CDC method resolution — fixed

Both WorkTablesIndex and upstream IndexSet CDC calls now use explicit inherent
type qualification. They cannot silently turn into recursive trait calls.

### C6: UUID ordering — clarified and dependency floor raised

The resolved `uuid` implementation does use a shared monotonic v7 context.
Its `Uuid::now_v7()` contract states that UUIDs generated by the same process
are ordered by creation. WorkTable-generated operations use that function.

The minimum dependency is now `uuid 1.24.0`, matching the documented guarantee,
and `latest_data_writes` documents the constraint. Manually constructed
operations must preserve the ordering contract. A separate `AtomicU64` was not
added because it would duplicate the current operation ID's guaranteed ordering
and expand the persisted/CDC operation representation.

Reference: <https://docs.rs/uuid/1.24.0/uuid/struct.Uuid.html#method.now_v7>

### C7 and C8: hydration and lock hierarchy — documented

- Cold hydration's writer-exclusion cost and the warm-up option are documented.
- `DataPages` now states its multi-lock hierarchy, the retirement queue rule,
  and the unlink-before-retire obligation.
- `move_row_for_vacuum` now has an explicit `# Safety` contract.

## Code generation findings

### D1: token substring backend detection — fixed

Persisted-index layout is derived from parsed type paths and structured
`pk_upstream` metadata. Unknown types and aliases are rejected instead of being
guessed into an on-disk representation. The primary backend is threaded into
the generated persistence attribute rather than recovered from rendered
tokens.

### D2: stringified primitive types — improved

Primitive checks parse `syn::Type::Path` and compare the final path segment.
The diagnostic explicitly says that a directly named primitive is required and
that proc macros cannot resolve Rust type aliases. The current DSL grammar still
accepts identifiers rather than arbitrary qualified type syntax.

### D3: `compile_error!` in type position — fixed

The affected table generators now return `syn::Result` and propagate the
original diagnostic with `?`.

### D4: macro-host feature coupling — documented, not redesigned

The Cargo feature-unification effect is now explicit in the publication docs.
Replacing the forwarded proc-macro feature with a build-script cfg would be a
larger packaging change and is deferred.

### D5: upsert rewrite — unchanged in this follow-up

The single logical row lock and retry rationale remain. Removing redundant
probes and reducing wide-row clones is still a useful measured optimization,
but is separate from these correctness fixes.

### D6: statement-level cfgs — fixed

The non-versioned `mark_page_empty` branch is grouped in one cfg block.

## API and packaging findings

- **E1:** making Arctic, Congee, and upstream IndexSet optional is deferred. It
  changes the feature/API contract and deserves its own binary-size and compile-
  time change.
- **E2:** WorkTablesIndex PR #5 deliberately supports additive Cargo feature
  unification with deterministic search-policy precedence; this is documented.
- **E3:** the publication docs now state that enabling the feature anywhere in
  a dependency graph enables it for all WorkTable consumers in that build.
- **E4:** moving backend types out of the prelude is deferred because it is a
  public API decision rather than a correctness patch.
- **E5:** the unsafe vacuum move now documents all caller obligations.

## Tests and validation

Added or expanded coverage includes:

- coherent row/flag publication under concurrent replacement;
- idle query builders and retirement/link reuse;
- native Congee and Arctic range-bound translation;
- generated range-result predicate revalidation;
- persisted index aliases rejected instead of misclassified;
- the ART primary-key range Criterion smoke benchmark.

The persistence test that formatted the entire primary index on failure has
been restored to a concise assertion.

Validation against DataBucket 0.5.1 and the updated WorkTablesIndex PR #5 head:

- strict Clippy, default configuration: pass;
- strict Clippy, all features: pass;
- full default suite: 127 library tests, 333 integration tests (3 ignored), and
  64 codegen tests passed, plus all benchmark smoke targets;
- full all-feature suite: 132 library tests, 336 integration tests (4 ignored),
  and 64 codegen tests passed, plus all benchmark smoke targets.

One initial all-feature run hit an assertion inside `arctic-map 0.1.4` during
its disjoint concurrent mutation test. The unchanged isolated test then passed
20/20 repetitions and the complete all-feature suite passed on rerun. This is
being reported rather than hidden because the dependency is young and pinned.

No latency claims are made from this host while unrelated benchmarks are
running. The Criterion cases are in place, but publishable before/after numbers
should be collected on a quiet machine.

## Remaining follow-ups

The intentional remaining work is:

1. replace the counter grace period with a true epoch/QSBR collector if active
   scans must coexist indefinitely with prompt physical-link reuse;
2. make alternate backend dependencies optional and decide whether their types
   belong in the prelude before the stable API;
3. measure and then optimize duplicate insert serialization and redundant
   upsert probes/clones;
4. improve ART memory accounting and address the pre-existing row-count reuse
   drift separately;
5. run the default/versioned latency matrix on a quiet benchmark host.

Those are kept explicit so the current patch is not represented as eliminating
every cost or lifecycle caveat raised by the review.
