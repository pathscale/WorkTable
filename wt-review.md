# WorkTable PRs 70–72: HFT performance review

Review date: 2026-08-31  
Reviewed range: `752a90b..d60eb6f`  
Pull requests: [#70](https://github.com/pathscale/WorkTable/pull/70), [#71](https://github.com/pathscale/WorkTable/pull/71), [#72](https://github.com/pathscale/WorkTable/pull/72)

## Verdict

Do not call the reviewed `partition()` API on every tick from multiple threads. The public path is not the sub-nanosecond `Vec` lookup used to justify the design: it performs an `Arc` increment and decrement on every call, turning the strong-count cache line into a same-symbol contention point.

The feature can be operated safely before a complete fix if all partitions are pre-created, each worker caches its `Arc` instead of routing per tick, the router is treated as append-only, and partition metrics run only on a control-plane thread. Those restrictions need to be explicit because the current API makes all four unsafe usage patterns look normal.

PR 71's shutdown fix is sound from a performance perspective. Its additional queue poll occurs only during `Closing`, and its `yield_now()` is behind `cfg(test)`. PR 72 changes code generation tests and CI tooling, with no production runtime cost.

## Findings

### P1 — `partition()` creates a shared refcount write hotspot

Reviewed code: [`src/partition/mod.rs:171`](https://github.com/pathscale/WorkTable/blob/d60eb6f8092b57f68ec09f49235897c5f2532b34/src/partition/mod.rs#L171-L177), [`codegen/src/generators/partitions.rs:40`](https://github.com/pathscale/WorkTable/blob/d60eb6f8092b57f68ec09f49235897c5f2532b34/codegen/src/generators/partitions.rs#L40-L42)

`partition()` performs:

1. A bounds check.
2. An acquire load of the chunk pointer.
3. An acquire load of the slot pointer.
4. `Arc::increment_strong_count`.
5. A refcount decrement when the returned `Arc` is dropped.

Steps 4 and 5 are atomic read-modify-writes to the same strong count for every thread routing to the same symbol. That is exactly the cache-coherence traffic partitioning is meant to remove from the data structures below it.

A release-mode sanity benchmark against the actual public `PartitionSet<u64>` API on the same Apple M4 Max class named in the design notes produced:

| case | observed result |
| --- | ---: |
| cached `Arc` dereference | 0.37–0.38 ns/op |
| `contains()` (the two routing loads, no refcount) | 0.79–1.06 ns/op |
| `partition()` plus returned `Arc` drop | 3.52–3.69 ns/op |
| one thread, same key | about 284 Mops/s |
| eight threads, same key | about 15–17 Mops/s |
| eight threads, distinct keys | about 160–249 Mops/s |

The threaded result is the important one: aggregate throughput goes down as same-key readers are added. The benchmark was not core-pinned and does not measure percentiles, so it is directional evidence rather than a production latency claim. It is nevertheless sufficient to reject the documentation's implication that the public call is equivalent to a plain `Vec` lookup.

Required action:

- Add a borrowed hot-path API, `partition_ref(&self, key) -> Option<&T>`, whose lifetime is tied to the set. The existing retire discipline makes this sound: reclamation and drop need `&mut self`, so they cannot run while the borrow is live.
- Keep the `Arc` API for handles that must outlive the router borrow or move to another task.
- Add a committed benchmark for the actual generated facade, covering cached-handle, borrowed lookup, `Arc` lookup, and 1/2/4/8 same-key readers. For HFT sign-off, pin threads and collect p50/p99/p99.9 rather than reporting only average throughput.

The shared worktree currently contains an uncommitted `partition_ref` implementation. A quick release measurement put that path at about 0.74 ns/op, so the direction is promising, but it was not part of PR 72 at the reviewed head and still needs its own contended benchmark and review.

### P1 — Partition metrics copy table pages and allocate repeatedly

Reviewed code: [`codegen/src/generators/partitions.rs:108`](https://github.com/pathscale/WorkTable/blob/d60eb6f8092b57f68ec09f49235897c5f2532b34/codegen/src/generators/partitions.rs#L108-L145), [`src/table/system_info.rs:64`](https://github.com/pathscale/WorkTable/blob/d60eb6f8092b57f68ec09f49235897c5f2532b34/src/table/system_info.rs#L64-L84), [`src/in_memory/pages.rs:769`](https://github.com/pathscale/WorkTable/blob/d60eb6f8092b57f68ec09f49235897c5f2532b34/src/in_memory/pages.rs#L769-L775)

`memory_by_key()` and `rows_by_key()` call `table.system_info()` for every live partition. At the reviewed head, `system_info()` calls `get_bytes()`, which builds a `Vec<([u8; DATA_LENGTH], u32)>` and copies every full page image merely to sum each page's `free_offset`. It also collects the empty-link registry just to read its length.

The partition wrapper compounds the work:

- `inner.iter()` allocates a key vector, scans all allocated 1,024-slot chunks, performs another lookup/refcount operation per live key, and collects an `(id, Arc)` vector.
- The generated typed wrapper maps and collects that into another vector.
- `memory_total()` first builds the entire `memory_by_key()` vector and then sums it.
- `rows_by_key()` copies all page images even though it only needs `primary_index.pk_map.len()`.

At the default 16 KiB page, 500 one-page partitions cause roughly 8 MiB of page copying per call per partitioned table, before vector and index-info allocations. Eight such table sets would stream roughly 64 MiB through cache on each metrics poll. On an HFT process this can evict useful order-book/index data and create latency spikes even when the metrics task runs on another thread.

Required action:

- Read `free_offset` counters directly; never materialize page byte arrays for accounting.
- Expose direct lightweight accessors for row count, used bytes, empty-slot count, and index bytes instead of routing `rows_by_key()` through the full `SystemInfo` builder.
- Fold `memory_total()` directly without first allocating `memory_by_key()`.
- Prefer a callback/iterator that visits slots once over `keys() -> partition() -> collect()`.
- Keep metrics off latency-critical cores and set a deliberately low polling frequency.

The shared worktree currently has an uncommitted `used_bytes()` change that removes the full-page copies. That addresses the largest cost, but `rows_by_key()` still goes through the full `system_info()` path and the partition aggregation still allocates and scans multiple times.

### P1 — `remove()` retains whole tables indefinitely in the normal shared-router shape

Reviewed code: [`src/partition/mod.rs:261`](https://github.com/pathscale/WorkTable/blob/d60eb6f8092b57f68ec09f49235897c5f2532b34/src/partition/mod.rs#L261-L285), [`codegen/src/generators/partitions.rs:124`](https://github.com/pathscale/WorkTable/blob/d60eb6f8092b57f68ec09f49235897c5f2532b34/codegen/src/generators/partitions.rs#L124-L136)

Every successful `remove()` clones the table `Arc` into `grow`, which doubles as an unbounded retire list. Reclamation requires `gc(&mut self)`. The expected production shape is an `Arc<PartitionSet<T>>` shared across threads, where obtaining `&mut self` requires every other router handle to disappear. In a long-lived service, `gc()` is therefore effectively unreachable.

Delist/relist or evict/recreate cycles retain one complete table every time. The PR's own measurements put a table at roughly 15.7–48.3 KiB in memory and about 110 KiB when persisted. This becomes allocator pressure, larger RSS, more TLB/cache pressure, and eventually latency variance or OOM. `memory_total()` makes the operational risk worse by reporting only live slots; after `remove()` the reported total falls even though the retired table is still resident.

Required action:

- Implement reclamation usable under shared ownership (epoch-based reclamation, hazard pointers, or an explicit reader guard/quiescence protocol), or
- Remove/disable runtime `remove()` and document the router as append-only until reclamation exists.
- Report retired bytes, not only `retired_len`, if retirement remains part of the API.

Adding a warning to `remove()`/`gc()` documentation is useful but does not make eviction real. This is a resource-lifetime design gap, not just a documentation gap.

### P2 — One global mutex serializes construction of unrelated partitions

Reviewed code: [`src/partition/mod.rs:224`](https://github.com/pathscale/WorkTable/blob/d60eb6f8092b57f68ec09f49235897c5f2532b34/src/partition/mod.rs#L224-L253)

`get_or_create()` takes the single `grow` mutex before calling the caller-supplied `make()`. Different keys and different chunks therefore cannot initialize concurrently, and `remove()` waits behind any constructor as well.

The PR's measurements make the tail-latency consequence concrete:

- About 25.7 µs to construct an in-memory instance.
- About 6.1 ms to construct a persisted instance.

A burst of new symbols can turn first-touch routing into a serialized queue. A persisted constructor blocks every other create/remove for milliseconds. This is acceptable only if all creation happens before the process begins serving latency-sensitive traffic.

Required action:

- Immediate mitigation: pre-create every routable symbol before market open and make `partition_or_create()` forbidden on the tick path.
- Structural fix: use chunk-local or slot-local initialization state. If construction must remain exactly-once because closures can have side effects, use a per-chunk lock/once state rather than constructing outside the only global lock. If duplicate construction is acceptable, construct outside the lock and publish with compare/exchange, dropping the loser.
- Benchmark concurrent creation of distinct keys, including a slow constructor, so head-of-line blocking stays visible.

### P2 — The memory API under-reports the memory that affects latency

Reviewed code: [`codegen/src/generators/partitions.rs:104`](https://github.com/pathscale/WorkTable/blob/d60eb6f8092b57f68ec09f49235897c5f2532b34/codegen/src/generators/partitions.rs#L104-L122)

The API describes `memory_by_key()` as “memory held per partition,” but it sums used row bytes and secondary-index heap size. It omits at least:

- The table's measured fixed floor (about 14.5 KiB irreducible and roughly 30.9 KiB with the default page in the PR's probes).
- Reserved but unused page capacity.
- Router spine/chunks and `Arc` allocation overhead.
- Every table on the retire list.

This is not just naming. A residency budget or alert based on `memory_total()` can say memory dropped after eviction while RSS only rises. Capacity planning based on it will over-pack the process and increase cache/TLB pressure.

Required action:

- Rename the existing value to `used_row_and_index_bytes`, or make it report actual resident/capacity bytes.
- Provide separate used, reserved/capacity, live-table overhead, and retired-byte metrics.
- Test empty, sparse, full, and removed partitions against the semantics promised by each metric.

### P2 — A panicking initializer permanently disables router mutation

Reviewed code: [`src/partition/mod.rs:216`](https://github.com/pathscale/WorkTable/blob/d60eb6f8092b57f68ec09f49235897c5f2532b34/src/partition/mod.rs#L216-L275)

At the reviewed head the global lock is `std::sync::Mutex`. Because `make()` runs while it is held, any unwinding panic poisons the mutex. Every later create returns `PartitionError::Poisoned`, while `remove()` converts the same failure to `None` and misleadingly reports the key as absent. In a long-lived market-data process, one bad symbol initializer can prevent every future symbol from being added or removed.

Required action:

- Do not run untrusted/complex construction while holding a poisonable global lock.
- If the protected invariants remain valid after unwind, use non-poisoning lock semantics or explicitly recover the inner guard.
- Make removal failures typed; do not collapse lock failure into “not found.”

The shared worktree currently switches the native build to `parking_lot::Mutex`, which is a reasonable recovery for this specific failure mode. It is not part of reviewed PR 72 head yet.

### P3 — Maintenance APIs scan and allocate more than their signatures suggest

Reviewed code: [`src/partition/mod.rs:190`](https://github.com/pathscale/WorkTable/blob/d60eb6f8092b57f68ec09f49235897c5f2532b34/src/partition/mod.rs#L190-L213)

`keys()` scans every slot in every allocated chunk and allocates a vector. `iter()` calls `keys()`, then looks up each key again, increments every table's refcount, and allocates another vector. The generated typed `iter()` collects a third vector. This is acceptable as a clearly labeled control-plane snapshot, but expensive if used for periodic fan-out or telemetry.

Required action:

- Document these calls as O(allocated chunks + live partitions) and allocating.
- Add a visitor or borrowing iterator that scans each slot once.
- Do not expose `memory_total()` as a cheap scalar if it internally builds multiple vectors.

## Benchmark and regression gap

The committed probes measure table construction/memory and the design documents quote plain `Vec` and string-map routing experiments. No committed benchmark exercises the new public `PartitionSet::partition()` or generated facade. There is also no same-key contended-reader benchmark, despite same-symbol contention being the relevant HFT case.

Before claiming a routing win, benchmark the complete operation the application will execute:

1. Resolve/cached handle only.
2. Borrowed router lookup only.
3. `Arc` router lookup and drop.
4. Router lookup plus representative table select/update.
5. Same key and distinct keys at 1/2/4/8 pinned threads.
6. Warm L1, warm LLC, and deliberately cold key distributions.
7. p50, p99, p99.9 and max pause, with metrics polling both off and on.

The benchmark should live in `benches/` so future refcount, ordering, allocation, or telemetry regressions are visible. The documentation's “within 0.2 ns of a flat Vec” claim should cite that benchmark or be removed.

## What does not regress the hot path

- Tables without `partition_by` generate no router facade; the new generic module has no per-operation runtime cost for them.
- Integer conversion, power-of-two chunk division/modulo, and the two acquire loads are reasonable for the routed read path.
- Lazy 8 KiB chunk allocation is a sensible dense-key trade-off for the stated ~500-symbol HFT case.
- PR 71's extra `immediate_pop()` runs only during orderly shutdown.
- PR 72's parser tests and CI script do not affect production binaries.

## Verification performed

- Audited the effective 23-file delta `752a90b..d60eb6f`; PR 71's commit is also the first commit shown in PR 70 after rebase.
- `cargo test --release --lib partition`: 26 passed.
- `cargo test --release --test mod partitioned`: 18 passed.
- Targeted PR 71 shutdown regression: passed.
- `git diff --check 752a90b..d60eb6f`: clean.
- Release sanity benchmark: Apple M4 Max, macOS 26.5, rustc 1.97.1, 50 million single-thread iterations and 5 million operations per worker for threaded runs.

The tests establish behavior and memory-safety coverage; they do not invalidate the performance and resource-lifetime findings above.

## Recommended order for PR 72 fixes

1. Land and benchmark the borrowed `partition_ref` API; update the HFT usage example to cache or borrow instead of cloning per tick.
2. Remove page-image copies and full `system_info()` calls from partition metrics.
3. Decide whether removal is append-only-for-now or implement shared-owner reclamation; do not leave the current API implying usable eviction.
4. Make metrics truthful about fixed, reserved, and retired memory.
5. Remove global construction head-of-line blocking or enforce preload as a runtime invariant.
6. Make initializer panic recovery explicit and keep mutation errors typed.

