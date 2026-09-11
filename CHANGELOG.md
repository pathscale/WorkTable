Change Log
==========

## [1.9.0-alpha1]


### Changed

- **`s3-support` no longer drags in an async HTTP stack, and no longer needs a
  tokio reactor.** It used `reqwest`, which meant hyper, h2, tower and tokio —
  91 crates — to make four calls: a PUT and three GETs against presigned URLs,
  with no streaming, no multipart and no auth headers, because `rusty-s3` puts
  the signature in the URL.

  Worse than the size, it was silently incompatible with the default runtime. An
  S3 write from the persistence worker panicked with `there is no reactor
  running, must be called from the context of a Tokio 1.x runtime`, because the
  worker runs on nagoya and `reqwest` looks for tokio's thread-local handle.
  `cargo test --all-features` had been red on that for as long as the feature
  existed.

  It now uses a blocking client. The `async fn` signatures are unchanged, so no
  call site moved; the blocking happens inside them, which is what this crate's
  filesystem layer already does deliberately — neither `tokio::fs` nor
  `async-fs` performs asynchronous file I/O either, and `fsx` measured 12,316
  rows/sec through `tokio::fs` against 74,728 blocking.

  | | before | after |
  |---|---:|---:|
  | crates in the s3 build | 198 | **170** |
  | what the feature costs | 91 | **63** |
  | tokio present | yes | **no** |

  A persistence worker makes one request at a time from its own thread, which is
  the shape a blocking call fits. Async HTTP exists to multiplex many
  connections onto few threads, which this is not.

### Added

- `using fxhash`, a hash-shaped index backend. Accepted on `vec: true` and
  **refused on a paged table**, which is the whole story: `UniqueIndex` requires
  `range_values` and `range_links` and a hash map cannot answer either, a paged
  table generates `select_by_<column>_range` for every index, and a persisted
  index's on-disk form *is* sorted pages — `from_persisted` rebuilds each one
  with `attach_nodes`. The `vec: true` generator is the one that asks its index
  only for point operations and a single order-independent walk, so it is the
  one place a hash map fits.

  Worth 4.9x on build and 4.0x on lookup at a million rows against the default
  arctic backend (`perf-benchmarks/benchmarks/fx-index.rs`), which is a larger
  factor than anything else in the backend list.

  A table using it has **no `range` and no `range_by_`**. Not a method that
  panics and not one that returns insertion order while claiming key order: the
  methods are not generated, so asking for one is a compile error at the call
  site. A paged declaration that asks for it is refused with an error naming
  `vec: true`.

  `with_capacity` now reserves an `fxhash` index alongside the row vector, and
  that is most of the build win — without it the same table measured 2.4x rather
  than 4.9x. It reserves nothing for the tree backends, deliberately: making
  allocation completely free measures at **0.92x** for Arctic, below one,
  because it changes where nodes land and sequential order is worse for a tree
  walked in key order.
- Ranges on a `vec: true` table: `range(bounds)` by primary key and
  `range_by_<column>(bounds)` for each unique secondary index, both
  `DoubleEndedIterator` so `.rev()` works. This cost nothing to add and was
  simply never exposed. Every backend the `using` clause can name is an ordered
  tree, `UniqueIndex` has always required `range_links`, and the index was
  answering ranges the whole time.

  It is not a sorted vector: the keys arrive in order and the rows they name are
  wherever insertion put them, so a long range is a walk of random accesses.
  `range_by_` is emitted for unique indexes only, because a non-unique one holds
  a posting list per key and has no single row to yield.

- Ghosted deletes on a `vec: true` table, with `compact` to reclaim. `delete` is
  now O(1): the row leaves its slot and its index entries, and no other position
  changes. It used to close the hole with `Vec::remove`, which meant a memmove of
  every row above it plus a rewrite of every index entry above it — **21
  milliseconds per delete at a million rows**, so two hundred deletes took four
  seconds.

  The cost is that slots accumulate until `compact()` is called, which is the
  paged table's ghost-and-vacuum model applied to a vector. `ghost_count()` and
  `slots()` report the state so a caller can decide when compaction is worth its
  cost; `compact()` keeps the row vector's capacity for reuse and
  `shrink_to_fit()` gives it back.

  Two consequences worth reading before upgrading. `select_all()` returns
  `impl Iterator<Item = &Row>` instead of `&[Row]`, because with a hole in it the
  live rows are no longer a contiguous slice — call `.iter()` on the result no
  longer, and `.count()` where you had `.len()`. And a slot now costs
  `size_of::<Option<Row>>()`, which for a row with no spare bit pattern is the
  row plus its alignment; `used_bytes()` counts slots for that reason.

- `vec: true` composes with `partition_by`. It was refused, on the grounds
  that a `Vec` table "is one contiguous `Vec` and has nothing to partition",
  which reads the relationship backwards: partitioning is what makes the `Vec`
  shape correct, because a `Vec` table is single-writer and grows linearly and
  cutting the data into many small independent ones is how you stop both from
  mattering.

  The router needs `Default`, `used_bytes` and `row_count` from whatever it
  holds. A `vec: true` table already had the first, and now has the other two.
  Its `insert` still takes `&mut self`, so a partition is populated and then
  handed over with `partition_or_insert_with` rather than mutated through the
  `Arc` the router returns.

- `AtomicKeyTable`, ported from `worktable-vec`, which is now deprecated: this
  was the last thing in that crate living nowhere else. A fixed-capacity,
  open-addressed table whose rows are claimed with one `compare_exchange` and
  then updated through `&V`, so many writers share a key without a lock and
  without a reallocation. The case it exists for is a counter table: sixteen
  workers recording timings against a handful of named sites.

  **There is no row snapshot, and there will not be one.** A reader of two
  fields reads two atomics, so a count of 10 beside a total of 900 is
  observable even though no writer left the row that way. A sequence lock or a
  lock per row would put back the contended cache line the type exists to
  avoid. A caller who needs two values to agree packs them into one atomic: two
  `u32` counters in an `AtomicU64`, one `fetch_add`, one load. That is the
  supported answer and there is a worked example in the tests.

- A lint on a narrow primary key. `u8` or `bool` as the primary key of an
  **unpartitioned** table means a table that can never hold more than 256 or 2
  rows, which is usually a key that was meant to be wider. Beside
  `partition_by` the same key is correct and the lint is silent: a narrow key
  is what makes a dense partition possible, and warning about it there would be
  telling people to undo the optimisation.

  A lint and not a ban. It arrives as a deprecation warning, because a proc
  macro cannot emit one directly; the note names the column and the row count,
  and `#[allow(deprecated)]` on the module turns it off for a table where 256
  rows is what was meant. Everything it emits lives inside an anonymous `const`
  and is not nameable.

- A **dense partition**, generated when `partition_max_size` is `bool`, `u8`
  or `u16`. `<Name>DenseTable` addresses rows by position: the primary key *is*
  the row's index, so there is no primary index, no pages, no links, no free
  list, no epoch domain, no lock map and no CDC. A lookup is a bounds check and
  a load.

  Measured on one declaration at two widths, 200 partitions of 23 rows,
  counting what the allocator was asked for:

  | | bytes per partition |
  |---|---:|
  | full table, empty | 28,404 |
  | **dense, empty** | **108** |
  | full table, 23 rows of an 88-byte row | 32,900 |
  | **dense, same** | **3,180** |

  The empty figure is the one to read. The saving is fixed apparatus allocated
  at partition creation, so it is about 28 KB per partition whatever the row
  width is; the ratio falls as rows get wider only because the rows themselves
  grow. At 2,000 symbols that is roughly 56 MB.

  `insert`, `upsert`, `update`, `delete` and `select` all take `&self`, because
  `partition_or_create` hands out an `Arc`. A generated `update_<column>` edits
  one field in place without cloning the row. Writes serialise per partition
  rather than per cell, which is stated in the module documentation rather than
  implied: nothing here is async, so no write spans a suspension point, and a
  partition is a much smaller thing to lock than a table.

  The width is a **bound, not a reservation**: the row vector grows to the
  highest key used, so a `u16` partition holding three rows holds three slots
  and an empty one allocates nothing at all.

  Refused, by name and with the way out: a primary key that is not a single
  unsigned column, a width the key cannot count to (`u16` beside a `u8` key
  declares 65,536 rows into a partition that holds 256), and `persist: true`,
  which a dense partition has no engine to honour.

  It carries `queries:`. An `update` or `delete` query keyed by the primary key
  generates the same method name against the same `<Name>Query` struct the
  paged table generates, so a call reads identically; the signature does not,
  deliberately, because there is no `.await` and no `WorkTableError`, and a
  call that moved between the shapes should fail to compile rather than
  quietly change what it guarantees. A query keyed by any other column is
  refused: a dense partition has no secondary index, and scanning it instead
  would be a keyed operation silently becoming a linear one. `in_place` is
  refused as a synonym, because every update here is already in place.

  Note that `memory_by_key` and `memory_total` **cannot see this saving**. They
  report `used_bytes`, which is rows plus indexes and excludes the fixed floor
  by definition, so the two shapes measure the same through them. That is
  pinned by a test so the conclusion is not drawn twice.

- `partition_max_size`, required beside `partition_by`. It says how many rows a
  single partition holds, written as an index width rather than a count:
  `bool` is 2 rows, `u8` is 256, `u16` is 65,536, and `u32` or `u64` mean
  unbounded in practice. It is positional, directly after `partition_by`.

  A **type** and not a literal, for the same reason `columnar_slot_id:
  ColumnSlotId16` already is one: it is an index width, which is what the
  generator needs, and a count is not a power of two and duplicates a constant
  that lives in the caller's code and will drift.

  Required rather than defaulted, because a default would pick one of the two
  shapes for the author and generate the other one silently. Nothing in a
  partitioned declaration said which shape it was getting: `exchange_id: u8
  primary_key` reads as a big table with a suspiciously tiny key, when the
  truth is many little tables that each need only a byte, and two declarations
  differing by 28 KB a partition looked identical.

  **Breaking for any existing partitioned declaration.** Adding
  `partition_max_size: u64,` after `partition_by` restores exactly the previous
  behaviour.

- `no_std` support. A consumer with `default-features = false` can invoke
  `worktable!` and use `insert`, `select` and `select_all`. Verified by
  `tests/nostd-consumer`, a crate outside the workspace that invokes the macro:
  this crate builds without `std` whether or not the macro is sound, because the
  expansion only happens where the macro is called.
- Columnar fields and columnar indexes: `columnar(chunk_rows(n),
  compression(name))` on a column, a `columnar_indexes` block with `cluster_by`,
  and `columnar_slot_id` / `columnar_chunk_rows` in `config`.
- A schema-selected runtime: `runtime: nagoya(<flavor>)` or `runtime: tokio`.
  Six flavors, all sharing one pool implementation, so the choice costs no extra
  code and no rebuild.
- `page_size` on a persisted table, at any size with a 512-byte floor. It was
  refused outright while the on-disk seeks used a hardcoded constant.
- `vec: true`, a `worktable!` whose rows live in one contiguous `Vec` with
  an index of positions into it, and which pays for none of the paging,
  archived rows, lock map, CDC or async surface a paged table carries.

  It is a key rather than a second macro. `worktable_vec!` existed briefly and
  emitted `<Name>VecRow` and `<Name>VecTable`, which is a parallel vocabulary
  to learn and a redefinition error when one table was declared both ways. One
  macro means one `<Name>Row` and one `<Name>WorkTable` whatever the storage
  is. `vec` is positional: name, version, vec, persist, partition_by,
  partition_max_size, then the blocks.

  It is a flag rather than a `storage:` key, which is what it was called for
  half a day. The grammar keeps the shape `persist:` already has and gains no
  new noun; the model still resolves it to one enum, because the schema is
  serialized, round-tripped and handed to a TypeScript emitter, and serde
  enforces no cross-field invariant.

  The two are **not** interchangeable, deliberately. The signatures differ four
  ways, so moving a declaration between them fails to compile at every call
  site rather than silently weakening its guarantees. A paged `insert` is
  `async fn(&self, Row) -> Result<Pk, WorkTableError>`; a vec one is
  `fn(&mut self, Row) -> Result<(), Row>`. Select clones a row out of the first
  and lends one from the second.

  `persist`, `queries`, `columnar_indexes`, `runtime`, `partition_by` and
  `config` are refused with an error naming what to use instead, rather than
  accepted as no-ops. `persist` in particular: this table has no engine, no
  task and no flush, so it pays no synchronisation for durability it was not
  asked for. Rows go to bytes and back when you call for it.

  It honours `using` as a paged table does and defaults to the same backend:
  arctic, with `worktables_index`, `congee` and `indexset` (a plain `BTreeMap`)
  available. A non-unique index needs a multimap, which only arctic and
  indexset have, so the other two are refused for one by name.

  Measured at 200,000 rows, nine interleaved rounds, p50: 6.4 ms against the
  13.6 ms a hand-written `Vec` plus `BTreeMap` takes, and level with
  `worktable-vec`'s own `ArcticTable` at 6.4 ms. 10.0 ms once it also
  maintains a secondary index nothing it is compared against has.

  `using indexset` is the reason to pick `BTreeMap` deliberately: `delete`
  moves every position above the hole, which a `BTreeMap` does in place and an
  ART does by reinserting each affected entry.
- `vec: true` generates `unload` and `load`: rows out
  as 16 KiB pages and back, each page standing alone so damage is local and an
  append does not rewrite the file. Every page carries a CRC-32 of its body and
  a row directory, and a row-type fingerprint refuses another table's file
  rather than reading it as debris.

  The indexes are not written. They are positions into the row vector, so they
  are rebuilt on load, which is cheaper than writing, validating and keeping
  them consistent with the rows on disk.

  The codec is ported from `worktable-vec`'s `hydrate`, where the format was
  designed. **The files are not interchangeable**: that crate stores
  `Vec<(K, V)>` because its value type has no key in it, and a `worktable!` row
  already carries its primary key as a column, so this stores `Vec<Row>` and
  does not write the key twice. Different archives, different fingerprints, and
  the fingerprint is what turns that from silent misreading into a refusal.

  rkyv's derives are unconditional, since `persist` is refused and there is
  nothing left to gate them with. Measured at 20 tables of five columns: 305 ms
  without them, 470 ms with, so about 8 ms a table. Real, and not worth a key.
- The default index backend is `arctic`, not `worktables_index`. A composite
  primary key keeps `worktables_index`, because arctic cannot represent a tuple
  key. **Arctic cannot key an optional or variable-width column**, so an index
  over `String optional` must now say `using worktables_index` where it
  previously needed nothing.
- Only `congee` requires `persist` to be stated explicitly. Arctic no longer
  does, having become the default.
- The filesystem goes through `worktable::prelude::fsx` and names no async
  runtime. Measured: `tokio::fs` ran scattered updates at 12,316 rows per second
  against 74,728 on `std::fs`.

### Fixed

- A page gap in the persisted batch save. Two writers allocating at once could
  hand the queue the higher page first, leaving the skipped ids as holes of
  zeros that the file spanned; the next batch touching one parsed the hole and
  looked up a key it never held. Reduced from a 40 minute reproduction to a
  0.01s unit test.
- Batch collection was quadratic on scattered writes. It grouped by page while
  validity is decided by event order, so a workload writing to scattered pages
  re-collected almost everything each round: 4,000 operations cost 202,000
  collections and 198,000 requeues. Selection is event-ordered now, 16.1s to
  0.14s.
- A 500 ms sleep on every collection retry that needed no wait.
- Eight tests named for concurrency ran on a current-thread runtime, where
  spawned tasks never overlap.
- The macro emitted names a `no_std` consumer could not resolve, and
  `futures::future::join_all` where the prelude should have been.
- `worktable-schemas` counted the `tests/ui` refusal corpus as rejections, so it
  reported nine failures on a healthy tree.
- `Schema` parsed `columnar_indexes` and dropped it, so `to_dsl` emitted a
  columnar table without its clustering and every consumer downstream, including
  the TypeScript emitter, was blind to it.

## [1.0.0-beta.19]

### Changed

- Require WTI 0.0.12 and the backend/reclamation releases validated with
  beta.19 so an existing lockfile cannot retain an incompatible pre-fix
  version.
- Require Arctic 0.1.9, including normalized validated-key prefix scans.

### Fixed

- Generated S3 engines now use the table's selected primary-index persistence
  adapter. Loaded default-Arctic tables can update, insert, delete, flush, and
  reopen without feeding logical Arctic events into the structural WTI disk
  format.

## [1.0.0-beta.18]

### Added

- Selectable `worktables_index`, `arctic`, and `congee` backends for generated
  primary and supported secondary indexes, including persisted topology load.
- Async and batched insert/delete paths, with bulk-mutation signaling used by
  reactive vacuum scheduling.
- `MemStat` for generated persisted/read-only tables and Arc-owned
  `unload_gracefully` for generation swaps.
- Strict persisted-state validation, schema metadata, recovery loading, and
  Arctic string/non-unique index support.

### Changed

- WorkTable row/page reclamation uses the local `ps-reclaim` domain regardless
  of the selected index backend. Arctic and Congee also select their local
  `ps-reclaim` SMR implementations; WorkTablesIndex retains its structural
  skip-list reclamation internally.
- Readers now synchronize on the exact physical cell. Unrelated rows cannot
  block because of a hashed lock collision.
- Vacuum discovers move candidates from a transient primary-index snapshot and
  keeps only one live-cell counter per page, removing the previous four-byte
  per-row directory.
- Vacuum waits for three quiet observations after mutation activity and yields
  throughout a bulk mutation instead of competing with foreground work.
- The archived wrapper retains the beta.17 inner-row position so legacy stores
  without bundled schema metadata remain readable.

### Fixed

- Torn reads and premature physical-link reuse during concurrent update,
  delete, and vacuum activity.
- In-place replacement synchronizes through the runtime side-table cell lock;
  the beta.17 archived row bytes remain unchanged.
- Whole-map Arctic destruction uses an unordered physical drain instead of
  repeatedly searching for the next logical key.
- Persisted primary/secondary index reconstruction and validation failures that
  could otherwise expose missing, duplicate, or mismatched rows.

## [1.0.0-beta.17]

### Added

- `worktable_dsl`, a standalone crate holding the schema language. A schema can
  now be read as data and written back, two schemas can be compared and the
  cost of the difference reported, and declarations can be found across a
  source tree.
- Every generated table embeds its own declaration, so the schema is
  recoverable from the code the macro produced.

### Changed

- Dependency requirements on the index and reclamation crates are carets rather
  than exact pins, and `ps-reclaim` moved to 0.1.1 taken from the registry.
- Retirement runs through the reclamation domain rather than through the guard.

## [1.0.0-beta.16]

### Changed

- A batch pins its reclamation domain once instead of once per row, and
  reclamation goes through `ps-reclaim`.
- Requires data_bucket 0.5.5.

## [1.0.0-beta.15]

### BC Breaks

- Non-unique index entries are identified and ordered by their `(key, value)`
  pair, and the discriminator is gone. This is a persisted format change. An
  index file written by beta.14 or earlier orders entries within a key by
  discriminator, so it must be reindexed rather than loaded.

### Fixed

- Inserting into a non-unique index no longer scans every entry sharing the
  key. On a table that puts a whole generation under one key, a one-file update
  measured 698 ms on beta.13 and 15.1 s on beta.14; the per-row cost is back
  from 330 us to roughly 9 us.
- Index pages reconstruct in order of their minimum rather than their node id,
  so a page that merely ends late no longer sorts ahead of one that starts
  earlier.

## [1.0.0-beta.14]

### Added

- `insert_many` with all-or-nothing semantics and CDC batch operations, and
  `reserve_pks` for atomic primary key range reservation, both generated on
  in-memory and persisted tables.
- Per-table epoch pin domains. The global reader counter is replaced by
  epoch-based retirement reclamation, and removed partitions are reclaimed
  through the shared router under the same grace period.
- Non-unique Arctic indexes for fixed-width integer keys, generated for
  in-memory and persisted tables, with a pair-list checkpoint and WAL.

### Changed

- The persistence queue takes batches on a single wakeup, deduplicates page
  queries when collecting a multi-row batch, and caps rows per group id so the
  analyzer drain stays linear.
- The table-global page barrier is narrowed to one barrier per page.
- Requires WorkTablesIndex 0.0.8 and data_bucket 0.5.4.

### Fixed

- Mutation stripes are acquired as a batch without deadlocking.
- A unique-collision unwind on a persisted table survives reload.

## [1.0.0-beta.13]

The audit-fix release. Most of it is durability and concurrency correctness
rather than new surface.

### BC Breaks

- Persisted tables reject any `page_size` other than 16384 instead of writing a
  file that cannot be read back.
- A torn table-of-contents page 1 fails loudly instead of silently starting
  from an empty table.
- In-place update is rejected on indexed columns rather than leaving the index
  stale.
- An exhausted autoincrement generator panics instead of wrapping around and
  handing out keys that are already in use.

### Fixed

- A failed data write no longer leaves published index keys behind. Insert,
  update and delete each roll their index changes back.
- Index pages are written before the table of contents that references them,
  and table-of-contents key updates are guarded against segment overflow.
- Data-file accounting: the u32 page-offset wrap when writing the last page's
  data length, files whose length is an exact page multiple failing to reopen,
  and non-extending writes being counted into the last page's length.
- Vacuum no longer panics on a failed row move, no longer counts its scratch
  pages in `pages_freed`, and never reports a source page fully moved when a
  row was skipped.
- A cancelled lock wait releases its registered op-lock, and a row that
  vanishes mid-update returns `NotFound` instead of panicking.
- The persistence worker refuses new operations once `Drop` has aborted it,
  propagates `insert_cdc` serialization failure instead of panicking, and keeps
  surviving data-only writes when event removal empties a batch.
- ART checkpoints are atomic and clean up stale temporaries.
- Arctic returns an empty range for `Excluded` bounds with no neighbour.
- Row counts include inserts and deletes that reused a slot, and the
  `PageIsFull` page switch is serialized against racing inserters.
- A misplaced `persist` or `partition_by` in a declaration now names the
  position it belongs in.

## [1.0.0-beta.12]

### Added

- `partition_by`: one declared table type, many routed instances, with
  `partition_ref` for borrowing a partition rather than cloning it.

### Changed

- `system_info` no longer copies every data page, and partition metrics scan
  and allocate once instead of three times.

### Fixed

- A use-after-free in partition removal.
- `close` reported success having persisted nothing.
- One panic inside the router no longer disables the router.

## [1.0.0-beta.11]

### Changed

- Requires the reviewed ART backend releases.

## [1.0.0-beta.10]

### Fixed

- Table-of-contents inserts carry across persisted segments, reload insertion
  stays on the fast path, and the insert API keeps its previous shape.

## [1.0.0-beta.9]

### Fixed

- Persistence health is preserved across page splits.

## [1.0.0-beta.8]

### Fixed

- Multi-row persistence order is preserved, and overlapping durable row writes
  are ordered against each other.

## [1.0.0-beta.7]

### Fixed

- The sized indexed update path is preserved.

## [1.0.0-beta.6]

### Changed

- Fixed-width updates stay in place.

### Fixed

- Vacuum revalidates links after row locking.

## [1.0.0-beta.5]

### Added

- Checked offline recovery load.

### Changed

- WorkTablesIndex structural persistence moved off the mutation path.
- Logical WTI mutation stripes hash with FxHash, reusable data ranges are
  subtracted in one pass, and full-row updates generate distinct paths.

### Fixed

- Same-size unsized updates apply in place instead of going through reinsert.
- Full-table scans re-resolve stale links.
- Vacuumed pages are reusable after reload.
- Cancelled lock acquirers are cleaned up.
- A panic while loading a persisted table is contained instead of unwinding
  into the caller.

## [1.0.0-beta.4]

### Fixed

- Release hardening and torn-store refusal are consolidated, so a torn store
  refuses cleanly.

## [1.0.0-beta.3]

### Changed

- Depends on the published index dependency chain rather than git revisions.
- Row publication is concurrency-safe by default.
- Persistence failures are terminal instead of leaving the table in a state
  that looks usable.

### Fixed

- Synchronous insert is serialized against row mutations.
- Page and link reclamation no longer overlap, and vacuum page reuse is
  deferred through the read grace period.
- Upsert retry backoff is bounded and its shift is capped, so same-key churn
  cannot livelock.
- Fragmented unsized index pages are compacted.
- A stale multimap removal lookup is avoided.

## [1.0.0-beta.2]

### Added

- Native ART index backends persist.

### Changed

- The temporary rusty-s3 fork is retired in favour of the published crate.
- Stable index reads use the specialized path by default.

### Fixed

- Same-key upserts linearize.
- Bounded retry for transient index misses is gated rather than always on.
- Reused persistence slots coalesce.

## [1.0.0-beta.1]

### Added

- Per-index backend selection in the `worktable!` declaration, with unique-index
  adapters for Arctic, Congee and a parallel upstream indexset. Persistence is
  preserved across indexset providers.

## [0.9.4]

### Changed

- Requires data_bucket 0.4.1, and the temporary git patch is retired.

### Fixed

- A torn store refuses cleanly instead of terminating the process by signal.

## [0.9.3]

### Fixed

- `worktable_version!` stays read-only when the primary key is unsized.

## [0.9.2]

### Fixed

- Duplicate-key secondary indexes reconstruct correctly on reload.
- Nodes sharing a maximum key order correctly, and pages are no longer re-sorted
  on reload.
- Space files flush before an operation reports done.

## [0.9.1]

### Changed

- The proc-macro crate is published as `worktable_codegen` again, after a brief
  release under the name `worktable_macros`.

## [0.9.0]

### Changed

- Moves to WorkTablesIndex 0.0.1 and data_bucket 0.4.0.
- The unsound lock-free persistence queue is replaced with a mutexed
  `VecDeque`.

### Fixed

- Row lock acquisition and vacuum no longer race between check and act.
- `wait_for_ops` no longer returns while a popped operation is still in flight.
- Upsert retries an existence flip instead of surfacing it to the caller.
- A multi-row update locks one validated snapshot, predicate included, and
  delete by non-unique index snapshots validated primary keys.
- Gapped event streams are never force-applied to the on-disk index, and the
  whole batch is scanned for event-id gaps rather than the last thirty events.
- A failed batch sub-operation is reported without cancelling the rest of the
  work.
- `save_batch_data` tracks the real maximum created page id.
- Vacuum persists row moves through CDC, so persisted tables survive
  defragmentation.

## [0.9.0-beta0.2.3]

### Fixed

- Primary key generator state is preserved across migration reinserts.

## [0.9.0-beta0.2.2]

### Changed

- Range ordering query logic reworked.

## [0.9.0-beta0.2.1]

### Changed

- Update locks spin before returning a `Pending` state.

## [0.9.0-beta0.2.0]

### Added

- Migrations.

## [0.9.0-beta0.1.4]

### Fixed

- Page-not-found bug in the table of contents.

## [0.9.0-beta0.1.1]

### Fixed

- Persistence bug affecting operations that fail.

## [0.9.0-alpha8]

### Changed

- S3 integration moves to a different client crate.

## [0.9.0-alpha7]

### Fixed

- S3 integration bug.

## [0.9.0-alpha6]

### Changed

- Moves to rustls.

## [0.9.0-alpha5]

### Added

- nanoid support for primary keys.

## [0.9.0-alpha4]

### Fixed

- Vacuum logic.

## [0.9.0-alpha3]

### Fixed

- The S3 macro.

## [0.9.0-alpha2]

### Added

- S3 sync feature.

## [0.9.0-alpha1]

### Changed

- Persistence is moved behind separate traits.

## [0.8.23]

### Changed

- `Lock`s are reworked around RAII guards.

## [0.8.22]

### Added

- `MemStat` derive on the generated primary key type.

### Changed

- `DataPages` select is generic over the input link type.

## [0.8.21]

### Changed

- `delete` is generic, matching `insert` and `update`.

## [0.8.20]

### Added

- Vacuum.

## [0.8.19]

### Fixed

- Optional fields in persisted tables.

## [0.8.18]

### Fixed

- Persisted table code failed to compile when the declaration used `optional`
  fields.

## [0.8.17]

### Changed

- Updated `indexset`.

## [0.8.16]

### Changed

- Dependencies are pinned to exact versions.

## [0.8.15]

### Fixed

- Empty link registry.

## [0.8.13]

### Changed

- Bumped `indexset`.

## [0.8.12]

### Changed

- Bumped `data_bucket` to 0.3.5 and `wt-indexset` to 0.12.11, and the crate now
  declares its repository.

## [0.8.11]

### Changed

- Bumped `indexset`.

## [0.8.10]

### Changed

- Bumped `data_bucket` to 0.3.3 and `wt-indexset` to 0.12.9.

## [0.8.9]

### Fixed

- Empty node bug.

## [0.8.8]

### Added

- Every `AtomicU*` and `AtomicI*` type is usable as a primary key.

## [0.8.7]

### Changed

- Dependency bumps.

## [0.8.6]

### Fixed

- An `update`-related bug.

## [0.8.5]

### Fixed

- Another `update`-related bug.

## [0.8.4]

### Changed

- Codegen version bump.

## [0.8.3]

### Fixed

- `delete` queries on a table whose primary key is not named `id`.
- An update bug, by way of an `indexset` update.

## [0.8.1]

### Added

- The macro reports an error when an index names a column that does not exist,
  and declaration errors are raised as `syn::Error`s with usable messages.

### Fixed

- `UnsizedNode` split.

## [0.8.0]

### Fixed

- Unsized node bug.

## [0.7.2]

### Fixed

- A further `update` bug.

## [0.7.1]

### Fixed

- An update violation.

## [0.7.0]

### Fixed

- Reinsert bug.

## [0.6.14]

### Added

- Ghost inserts. A row is staged invisible and becomes visible only once its
  index entries are in place, so a concurrent reader never observes a
  half-inserted row.

## [0.6.13]

### Fixed

- Concurrency bugs in `select`.

## [0.6.12]

### Fixed

- A further locking bug.

## [0.6.11]

### Fixed

- Locking bugs for unsized types, and an `UnsizedNode` bug on `update`.

### Changed

- Dependency bumps.

## [0.6.10]

### Changed

- Republished against `worktable_codegen` 0.6.9. No library change.

## [0.6.9]

### Fixed

- Concurrent persistence issues.

## [0.6.8]

### Fixed

- `wait_for_ops` logic.

## [0.6.7]

### Fixed

- `delete` on persisted tables.

## [0.6.5]

### Added

- Custom derives can be attached to the generated row type.

## [0.6.4]

### Fixed

- `uuid` usage.

## [0.6.3]

### Fixed

- A debug `println!` on the persistence batch path no longer writes to stdout.

## [0.6.2]

### Fixed

- Table-of-contents corrections.

## [0.6.1]

### Added

- `update_in_place`.

### Changed

- The persistence queue is optimized.

### Fixed

- `insert` with an already-existing key.
- A `use rkyv::Archive` import was required for some declarations.
- `wait_for_ops`.

## [0.5.6]

### Changed

- Updated `indexset`.

## [0.5.5]

### Fixed

- Array-typed fields.

### Changed

- Moves to the newer Rust edition.

## [0.5.4]

### Added

- Unsized index space, so index keys are no longer limited to fixed-width
  types.
- `SystemInfo` for the table and its indexes.
- `where_by` on `SelectBuilder` for any column, indexed or not.
- Float columns are usable in indexes, including ranges.

### Fixed

- Re-reading a table from file.
- Index difference logic for `update` queries.

## [0.5.1]

### Changed

- Persistence I/O is asynchronous.

## [0.5.0]

### Added

- `select_where_{field}` queries for selecting data ranges.
- `count` on the table.
- Persist sync logic.

### Changed

- Non-unique indexes are backed by `IndexMultiMap`.

### Fixed

- Secondary index left inconsistent after an update.
- Diff logic for a full-row update.

## [0.4.1]

### Added

- add ability to choose index type in `worktable!` declaration.
- added `index_set` and `tree_index` features to use index type as default in declaration.
-

### BC Breaks

- `.wt` files which are generated now have names as snake-case of table's name.
- `new` function now has only `DatabaseManager` as argument.

### Fixed

- `new` function generated if `persist: true` now is public.
- Bugs with insets and deletes after table load from file.

## [0.4.0]

### Added

- `SelectQueryBuilder` object that is used to customize `select_all` query. It has `limit` and `order_by` methods that
  can be used to limit returned row's count. `order_by` has not full functionality and is only available for indexed
  columns
  and only `Oreder::Asc`.
- `SelectResult` object with is partially same to `SelectQueryBuilder`. It allows to limit/order returned rows. Both
  `Oreder::Asc` and `Oreder::Desc` are available. No issues with not indexed columns.
- added `offset` for `SelectQueryBuilder` and `SelectResult`.
- added `optional` column attribute instead of explicit `Option` type declaration.
- support for enums in queries
- Added generation of `Space` object that represents file that stores table's data.
- Added `DatbaseManager` object that is used to control multiple tables.
- Added methods for `Worktables` to use data in files. `persist` is used to save data to file. `load_from_file` is
  used to load table from file.

### BC Breaks

- `select_all` now returns `SelectQueryBuilder` instead of `Vec<Row>`. To have same functionality old `select_all` users
  must call `execute` on returned builder.
- `select_by_{}` now returns `SelectResult` instead of `Vec<Row>`. To have same functionality old `select_all` users
  must call `execute` on returned builder.

## [0.3.10]

### BC Breaks

- Users don't need to define `<{ TestRow::ROW_SIZE }>` for `insert`, `update` and `upsert`.

### Added

- Support for `Option` types in columns.
- Support of `delete` queries.

### Fixed

- `Clippy` errors in macro declaration about unused `Result`'s.
