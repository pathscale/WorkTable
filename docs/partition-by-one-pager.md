# `partition_by`: one table type, N routed instances

**Status: design proposal for WorkTable, not implemented.** This page exists to
find out whether more than one codebase wants it. If you recognise the shape in
section 2, say so; if you don't, that is equally useful.

## 1. What it is

WorkTable's `worktable!` macro generates a typed table from a compile-time
schema. Today it generates exactly one table. `partition_by` would generate one
table *type* plus a router that holds N instances of it, keyed by an integer.

The table type is generated once, so nothing about code size or compile time
multiplies. N is a runtime quantity. The schema stays compile-time.

## 2. The tell: do you have this shape?

You want this if your code contains something like:

```rust
HashMap<SomeKey, Arc<SomethingWorkTable>>     // or Arc<RwLock<...>>, or DashMap
```

where:

- **access is key-local** — a request knows its key and touches only that entry,
- **the key partitions the workload** — different keys are genuinely independent,
- **you built the map by hand** because one big table would mean every lookup
  walking an index over every key's rows.

If you also hand-wrote an "allocate for these keys" function and a "get by key"
function next to that map, you have already built this feature. The proposal is
to generate it.

Real instance: a trading backend keeps `HashMap<Symbol, Arc<OrderBook>>`, where
each `OrderBook` wraps one `OrderBookWorkTable` holding 23 rows, one per
exchange. 500 symbols means 500 tables of 23 rows rather than one table of
11,500. A read walks no index at all.

## 3. Syntax

```rust
// today
worktable!(
    name: OrderBook,
    columns: { exchange_id: u8 primary_key, bid: f64, ask: f64, ts: i64 }
);
// plus a hand-written HashMap<Symbol, Arc<OrderBookWorkTable>> and its accessors

// proposed
worktable!(
    name: OrderBook,
    partition_by: symbol_id: u16,
    partitions: 1024,
    columns: { exchange_id: u8 primary_key, bid: f64, ask: f64, ts: i64 }
);
```

Two lines. Columns, primary key, queries and config are untouched. The
hand-written map and its accessors are deleted.

## 4. Usage

```rust
let books = OrderBookPartitions::new();

// hot path: array index, no hashing, no allocation
let book = books.partition(symbol_id).ok_or(...)?;
book.update_top_price(feed.into(), exchange_id).await?;

// across partitions: for sweeps and maintenance, not the hot path
let stale = books.select_all().filter(|r| r.ts < cutoff).execute()?;
let stat  = books.mem_stat();          // aggregate
let per   = books.mem_stat_by_key();   // which key is costing you
```

## 5. The partition key is an unsigned integer, deliberately

It is never selected on. It is not a column, it never appears in a row, and no
query can reference it. It is a routing coordinate, so it does not need to be a
rich type; it needs to be an array index.

Measured, 500 partitions, 20M lookups, single thread, cache-warm, M4 Max:

| router | ns per lookup |
| --- | --- |
| `HashMap<String, _>`, std SipHash | 11.45 |
| `HashMap<String, _>`, rapidhash or foldhash | 5.41 |
| minimal perfect hash (PtrHash) | 6.71 |
| 32-byte packed key, `HashMap` | 5.92 |
| `HashMap<u128, _>`, key packed into a register | 2.42 |
| **`Vec` index by dense id** | **0.73** |

Hashing cannot close that gap, because hashing the string *is* the cost. The
string step therefore belongs at the edge, resolved once when a subscription
opens, never in the router. Names live in a separate registry table, and
querying by name becomes a registry query that yields ids.

Two notes on the middle rows, since both are tempting and both are traps at
this scale. Minimal perfect hashing is state of the art for hundreds of
millions of keys, where a hash map's probe misses cache; at a few thousand keys
everything is in L1 and a plain map beats it. And packing a short name into a
`u128` is excellent when it fits, but a real symbol set contains things like
`10000000AIDOGEUSDT` at 18 bytes, and a 32-byte packed key is no faster than
hashing the string.

### 5.1 Two modes

Declaring a bound gets you an array; omitting it gets you a map.

```rust
// Mode A: dense and bounded. Ids are assigned and contiguous, id < partitions.
partition_by: symbol_id: u32,
partitions: 4096,

// Mode B: sparse and unbounded. Ids may be derived, for example by hashing.
partition_by: symbol_key: u64,
```

| | Mode A, dense | Mode B, sparse |
| --- | --- | --- |
| router | `Vec<Option<Arc<T>>>` | `HashMap` + fast hasher |
| lookup | 0.73 ns | 2 to 6 ns |
| ids | assigned, contiguous | derived, anywhere in range |
| collisions | impossible by construction | see 5.2 |
| registry needed to route | yes, once per key | no |

The width and the bound are independent: `u32` with `partitions: 4096` is a
4096-slot vector, not a four-billion-slot one. Mode B exists for the case where
two processes must independently derive the same id for a key without sharing a
counter.

### 5.2 If you derive ids by hashing, check at startup

Collision probability follows the birthday problem, and the intuition that "under
a million is surely fine" is wrong for 32-bit ids:

| partitions | u32 | u64 |
| --- | --- | --- |
| 1,000 | 1.2e-04 | 2.7e-14 |
| 10,000 | 0.0116 | 2.7e-12 |
| 100,000 | 0.688 | 2.7e-10 |
| 1,000,000 | **1.000** (about 116 colliding pairs) | 2.7e-08 |

A partition-key collision is not a slowdown, it is two keys silently sharing a
table. So the bar is not "unlikely", it is "detected".

The cheap and correct answer, if the keys are a namespace you control:

```rust
// At boot, before serving: hash every canonical key and assert no two collide.
registry.assert_no_collisions()?;   // fails loudly, naming both offenders
```

This works because a canonical key is a name you assigned, typically downstream
of a normalisation step that already maps many external spellings onto one
internal name. A collision is therefore a naming conflict in your own namespace,
fixed by renaming one key in configuration before the system is live, not a
runtime hazard. Rejecting a colliding key at first touch stays as the backstop
for keys added after boot.

If your keys are *not* a namespace you control, use Mode A and assign ids.

## 6. Semantics that change

These need to be acceptable, or the feature is not for you.

- **The primary key is unique per partition, not globally.** Two partitions may
  hold the same key value. This is the intended meaning.
- **`autoincrement` counts per partition.** A global counter would be a shared
  atomic on every insert, which is the contention this exists to remove.
- **`unique` secondary indexes are unique per partition.**

## 7. When not to use it

- **Cross-partition scans dominate.** `select_all()` over 500 partitions
  acquires 500 locks and merges 500 result sets. If most queries are not
  key-local, this is slower than one table.
- **You just need to look up by more than one column.** That is a compound
  index, a different and much smaller feature: `idx: (a, b) unique`. Partition
  when the key partitions the *workload*; use a compound index when you only
  need a multi-column lookup.
- **Partitions would be tiny and numerous.** Fixed per-partition overhead
  (page allocator, free list, index roots, lock map) dominates past some
  crossover. Where that crossover sits has not been measured.

## 8. What it is expected to buy

Ranked by expected size, and stated as predictions so they can be falsified:

1. **Lock and CDC contention.** Per-partition structures give independent
   contention domains. Contention degrades throughput non-linearly, so this
   should dominate under concurrent writers.
2. **Index locality.** An index over `keys * rows` does not fit cache; an index
   over `rows` does. The comparison count barely moves; the cache-miss profile
   moves a lot.
3. **Blast radius.** Vacuum, rebuild and snapshot touch one partition.
4. **Not writing the router again.** One codebase has three hand-rolled
   versions already, each slightly different, none with fan-out, aggregate
   memory accounting, or per-partition persistence.

None of this is measured yet. The bounded experiment: hold total rows constant,
split K ways for K in 1, 4, 16, 64, 256, and measure single-partition select
latency, concurrent update throughput at rising writer counts, and resident
bytes.

## 9. The question

Do you have the shape in section 2? If yes, what is your key, roughly how many
partitions, and is access key-local or does it cross partitions? If you have the
shape but the section 6 semantics are unacceptable, that is the most useful
answer of all.

---

## 10. Response from AgentCode

Answering section 9, from measurements in `pathscale/agentcode`.

### 10.1 Yes, we have the shape

```rust
RwLock<HashMap<SnapshotId, HotPartition>>
// HotPartition.tables: Arc<GenerationIndexTables>
```

`GenerationIndexTables` bundles **eight** WorkTables: symbol postings, symbol
lexemes, symbol index state, text postings, text lexemes, text index state,
dependency facts, dependency index state. Next to that map sit hand-written
`admit_generation_partition`, `hot_partition`, `enforce_hot_fact_budget`,
`generation_is_hot`, a per-partition `verify`, and per-partition persistence with
its own on-disk manifest. That is section 8.4, and we hand-wrote the aggregate
memory accounting too.

### 10.2 The axis is file revision, not generation and not repository

**Generation is the wrong axis, even though it is what we built.** An incremental
update currently sits 13.7x above its floor because a generation rewrites every
fact rather than the 0.125% that changed. Fixed cost is 9.7 ms per publish plus
10.24 us per row; at 14,400 rows the marginal term is 147 ms of a 161 ms call.
Partitioning by generation would generate a faster version of exactly that.

**Repository is safe but small.** About 17,000 text terms per partition against
68,172 today, and about 64 partitions for a developer daemon. Real, but a
constant factor.

**File revision is the right axis, and low row count is exactly why.** A file
yields on the order of 20 symbols and a few hundred distinct terms. An index over
20 rows is a different object from an index over 68,172: it is L1 resident, and
the comparison count stops mattering. It is also the axis that makes writes
O(change) rather than O(repository), because an unchanged file keeps its revision,
so its partition is already correct and is never rewritten. We have a proposed
design, ADR 0013, that moves fact identity to file revision for that reason, so
`partition_by` on that key is aligned with where the store is going rather than
where it has been.

Partition count: **10^3 to 10^5**. AgentCode itself is 17 Rust source files in a
57 file repository; our dogfood state holds 4,655 snapshot file records; a large
monorepo is 20,000 to 100,000. Times eight tables, that is up to 800,000 table
instances. Mode B, sparse, because a file revision is already a BLAKE3 hash and no
dense id survives a restart cheaply.

That count is also the whole risk, and it lands on the one section 7 bullet that
says the crossover has not been measured. See 10.6.

### 10.3 What we would additionally need

Ranked. The first two decide whether we can adopt it at all.

1. **Bounded residency with eviction.** Our router is not a map, it is a cache. It
   holds a fact-weight budget of 16,000, evicts least-recently-used partitions,
   and protects the partition serving the current request. `partition_by` as
   proposed generates allocate and get; without eviction, a weigher and a
   protected key we keep most of the hand-written code. At 10^5 partitions this
   stops being a nicety, because everything cannot be resident.
2. **Lazy load per partition, not eager allocation of N.** `partitions: 100_000`
   must not mean 100,000 live tables. A partition should materialise on first
   touch from its own durable state and drop back to nothing on eviction, durable
   state untouched. That is what `admit_generation_partition` does today.
3. **A partition group.** One key routes eight tables that are admitted, verified,
   evicted and persisted as a unit. Eight independent routers keyed by the same id
   still leaves the unit hand-written, which is the half that is actually hard.
4. **Per-partition persistence.** Each partition needs its own `DiskConfig` and
   its own flush and load lifecycle, so eviction and recovery are per key. We do
   this by hand today, one directory per key with a manifest.
5. **Cheap creation and small fixed overhead.** At 10^5 partitions the page
   allocator, free list, index roots and lock map dominate unless each is small.
   This is the measurement in 10.6.
6. **`mem_stat_by_key()` as proposed in section 4.** We surface per-partition
   weight through a `store.stats` operation and need it to stay available.

Two unrelated WorkTable features would compound with this and matter more to us
than the router does: a **non-unique fixed-width index**, which would move our
last string indexes to Arctic, and a **batch insert path**, since 10.24 us
marginal per persisted row is our dominant cost while non-persisted Arctic inserts
measure 0.46 us.

### 10.4 Usage, close to what we would actually write

Declaration, applied to each of the eight tables with the same key:

```rust
worktable!(
    name: SymbolPosting,
    persist: true,
    partition_by: file_revision: u64,   // Mode B, derived from the BLAKE3 revision
    columns: {
        id: u64 primary_key autoincrement using arctic,
        posting_hash: u128,
        normalized_name: String,
        records_blob: String
    },
    indexes: {
        posting_idx: posting_hash unique using arctic
    }
);
```

Note what leaves the schema. `snapshot_id` is currently a column and an index on
every one of these tables, and the posting hash is currently
`hash(snapshot_id, name)` so generations do not collide in one shared table. Under
section 6 semantics that scoping is implied by the partition, so the column, the
index and the snapshot term in the key all go away. Our lexeme key spends 32 of
its 128 bits on a snapshot discriminator today, leaving 7 bytes of ordered term
prefix for range scans; per-partition uniqueness gives those bits back and raises
the ordered prefix from 7 bytes to 11.

The write path, which is the one that matters:

```rust
// Today: every fact in the repository is rewritten under the new generation's
// key. 14,400 rows for a change that touched 18 of them.
self.put_symbols(snapshot.id, &carried_plus_new)?;

// Partitioned by file revision: only the changed file is touched, and unchanged
// files are already correct because their revision did not move.
let facts = self.symbol_postings.partition_or_create(revision_key(changed.revision))?;
for symbol in engine.outline(&view)? {
    facts.insert(SymbolPostingRow { /* no snapshot_id */ .. })?;
}
```

The read path, and why this is not a naive fan-out. A query names a generation,
and the generation's manifest already enumerates its file revisions, so the
candidate set is known without a membership table. A repository-level routing
index narrows it further, so a lookup probes only the files that contain the term
rather than all of them:

```rust
pub fn search_symbols(&self, snapshot_id: SnapshotId, query: &str, ..)
    -> Result<Page<SymbolRecord>>
{
    let snapshot = self.snapshot(snapshot_id)?;
    let normalized = normalize_symbol_name(query);

    // Which file revisions contain this name at all. Updated only for the
    // changed file, so this stays O(change) on the write side.
    let routes = self.name_routes.select_by_name(normalized.clone()).execute()?;

    // Intersect with the generation. The manifest is already loaded for this request.
    let live: HashSet<u64> =
        snapshot.files.iter().map(|f| revision_key(f.revision)).collect();

    let mut records = Vec::new();
    for route in routes.into_iter().filter(|r| live.contains(&r.revision_key)) {
        let facts = self.symbol_postings.partition(route.revision_key).ok_or(Stale)?;
        records.extend(facts.select_by_posting_hash(posting_hash(&normalized))..);
    }
    Ok(Page::from_slice(&records, cursor, limit))
}
```

Prefix search is the case that constrains the design, because it is an ordered
range scan rather than a point lookup. It still works per partition, since Arctic
supports `select_by_..._range` on a `u128` and every term in a partition is
ordered by the same key:

```rust
let (lower, upper) = lexeme_prefix_bounds(&prefix);   // no snapshot term any more
for revision in candidate_revisions {
    let lexemes = self.text_lexemes.partition(revision).ok_or(Stale)?;
    terms.extend(
        lexemes
            .select_by_lexeme_key_range(lower..=upper)
            .execute()?
            .into_iter()
            .filter(|row| row.normalized_term.starts_with(&prefix)),
    );
}
```

Residency, which is the part we would want generated rather than written again:

```rust
let facts = SymbolPostingPartitions::new()
    .with_weigher(|table| table.len() as u64)   // fact weight, not partition count
    .with_budget(16_000)
    .with_loader(|key| SymbolPostingWorkTable::load(disk_config_for(key)));

let table = facts.partition(key).await?;        // materialises on first touch
facts.evict_to_budget(Protected(key));          // drops handles, durable state kept
let per_key = facts.mem_stat_by_key();          // feeds our store.stats operation
```

Maintenance, which is genuinely cross-partition and rare:

```rust
let stats = facts.mem_stat();        // aggregate for one response field
let resident = facts.resident_keys(); // what a restart would not have to reload
```

### 10.5 What we do not need

The 0.73 ns router. Our lookup happens once per query and that query then does
thousands of row operations, so 11.45 ns against 0.73 ns is invisible at our call
rate. Under the fan-out in 10.4 the router runs once per candidate file rather
than once per query, which raises its weight, but the index probe next to it still
dominates. If Mode B at 2 to 6 ns is simpler to ship than Mode A, ship Mode B. We
would not notice, and it removes dense id assignment and recycling entirely for a
key space of 10^5 that grows forever.

What we want from section 8 is item 2, index locality, and item 3, blast radius.
Item 1, contention, is real for a daemon serving several repositories at once, but
it would not have prevented the failure we actually hit: a secondary index tear
under load, reproduced as six failures in eight concurrent runs, where every run
was a separate process with its own state directory and a single repository. There
was no cross-partition contention there to remove.

### 10.6 The two measurements that gate this for us

Both are section 7 bullets, and neither is answered in the current draft.

1. **Fixed overhead per partition, at 10^3 to 10^5 partitions.** Bytes and
   construction time for an empty partition, and the same for one holding 20 rows.
   This decides whether the file axis is buildable at all. Your bounded experiment
   holds total rows constant and splits K ways for K up to 256; we need it carried
   to 10^5, where the interesting quantity stops being latency and becomes
   resident bytes and load time.
2. **Read fan-out.** For a term present in many files, how many partitions does a
   query probe, and what does a probe cost once the index is small enough to be
   cache resident? Our own design note flags this as unmeasured too, so the answer
   serves both repositories.

If per-partition overhead turns out to be large, the file axis is dead and the
honest fallback is repository, at about 64 partitions, for the constant factor in
10.2.
