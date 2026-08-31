# Partitioned tables: a routing layer above the index

**Status:** Design proposal, 31 August 2026. Not implemented.

**Motivation source:** `web3.trading-backend`, where the absence of this feature
has produced two different workarounds, both of which cost something real.

**Compatibility:** additive. A schema with no `partition_by` parses and expands
exactly as today.

---

## 1. The problem, as it appears in real code

WorkTable can express one table. It cannot express *N micro tables routed by a
key*. Users who need the second thing have two options today, and
`web3.trading-backend` contains both.

### Workaround A: fold the routing key into a column

`src/db/strategy/common/funding_rates.rs`:

```rust
columns: {
    id: u32 primary_key autoincrement,
    exchange: Exchange,
    exchange_symbol: String,      // "BinanceFutures|BTCUSDT"
    rate: f64,
    ...
}
indexes: {
    exchange_symbol_idx: exchange_symbol,
    exchange_idx: exchange
}
```

with `ExchangeSymbol::new(ex, sym)` doing `format!("{ex}|{symbol}")` and
`.inner()` handing back a bare `String` at the table boundary.

This is a reasonable response to the DSL as it stands, and it costs:

1. **The type is thrown away at the boundary.** `ExchangeSymbol` is a careful
   newtype with `SizeMeasure`, `VariableSizeMeasure`, `Ord` and the rest, and
   then `.inner()` reduces it to `String` because that is what the column has to
   be. Nothing prevents inserting `"garbage"`.
2. **The exchange is stored twice**, once as `exchange` and once as the prefix
   of `exchange_symbol`, with two indexes over correlated data that can drift.
3. **A format and an allocation on every lookup.** In a hot path that is not
   free.
4. **Lexicographic order is not tuple order.** The delimiter `|` is `0x7C`,
   above every alphanumeric, so ordering by the concatenated string does not
   agree with ordering by `(exchange, symbol)`. Any range scan over that index
   is subtly wrong.
5. **It is not a partition at all.** The routing identity lives inside a value,
   so there is still one table, one index, one lock domain. A query for one
   symbol still walks an index over every symbol's rows.

The root cause of this particular workaround is narrower than partitioning:
**WorkTable has no compound index.** `parse_index` takes a single `Ident` for
the column, so indexing on `(exchange, symbol)` requires fabricating a single
column that holds both. See section 5, which proposes fixing that separately
because it is a much smaller change and would delete `ExchangeSymbol` outright.

### Workaround B: leave WorkTable

This is the more expensive one, and it is easy to miss. Where the trading
backend genuinely needs a micro table per symbol, it does not use WorkTable at
all:

```rust
order_book_managers: HashMap<Symbol, Arc<OrderBook>>,
last_symbol_signal_map: Arc<RwLock<HashMap<Symbol, S3Signal>>>,
last_event_timestamps: Arc<RwLock<HashMap<Symbol, i64>>>,
```

Hand-rolled maps behind `RwLock`, for the hottest data in the system. Everything
WorkTable offers is gone: indexes, persistence, change-data-capture, `MemStat`,
lock-free concurrent access. Traded away because the shape did not fit.

That is the strongest argument for this feature. Workaround A degrades
performance; workaround B loses the library.

## 2. The insight that makes this cheap

The obvious objection is that a schema is compile time, so N partitions would
mean N generated table types, and one `worktable!` declaration expands to
roughly 1,940 lines and 84 KB of Rust. A thousand partitions would be 84 MB of
generated code. That objection is real and it is also avoidable.

**The table type is generated once. Partitions are instances of it.** N is a
runtime quantity, the generated code is not multiplied, and the compile-time
schema guarantee is untouched. What the DSL needs to generate is not N tables
but *one router* around the existing type.

This is exactly what workaround B does by hand with `HashMap<Symbol, Arc<T>>`.
The proposal is to generate it, correctly, with the rest of WorkTable attached.

## 3. Naming

The honest answer includes rejecting two candidates.

- **`domain`: rejected.** SQL already has `CREATE DOMAIN`, and it means a
  constrained scalar type, not a partition. In a library whose own description
  is "macros that smell like SQL", borrowing that word for routing would
  actively mislead anyone with SQL reflexes. It has already caused confusion
  once in discussion.
- **`shard`: reserved, not used here.** In an embedded single-process library
  "shard" usually means hash striping for contention, as in `DashMap`. That is
  a genuinely different future feature: striping is by hash for lock spreading,
  partitioning is by value for semantic routing. Keeping `shard` free means it
  is available when striping is wanted.
- **`topology`: kept as prose, not as a keyword.** It names the whole
  arrangement well and reads badly as a per-table declaration.
- **`partition`: chosen.** It is what Postgres, Kafka and Hive all call exactly
  this, so nobody has to learn a new word, and value-routed partitioning is
  precisely what those systems mean by it.

So: the keyword is `partition_by`, one unit is a **partition**, and the
generated router is the **partition set**.

## 4. Proposed syntax

```rust
worktable! (
    name: Price,
    partition_by: symbol_id: u16,
    partitions: 1024,
    columns: {
        exchange: Exchange primary_key,
        bid: f64,
        ask: f64,
        ts: u64
    },
    indexes: {
        ts_idx: ts
    }
);
```

`partition_by` names the routing key. The key is deliberately **not** a column:
it is the identity of the partition, so it is stored once per partition rather
than once per row. A ten-row price table for one symbol stores no symbol at
all. That is the storage half of the same win as the query half.

### 4.0 The key is an unsigned integer, and that is the whole design

The partition key must be an unsigned integer in a declared range. Not a
`String`, not a newtype over one, not an arbitrary `Hash + Eq` type.

The reason is that the partition key is **never selected on**. It is not a
column, it does not appear in a row, and no query inside a partition can
reference it. It is a routing coordinate and nothing else. A coordinate does
not need to be a rich type; it needs to be an array index.

That distinction is worth a measurement rather than an assertion. Routing cost
per lookup, 500 partitions, single thread, cache-warm, on an M4 Max:

| router | ns per lookup |
| --- | --- |
| `HashMap<Symbol(String), _>::get` | 9.52 |
| `Vec<Option<_>>` indexed by `u32` | 0.38 |

**25x.** That is the routing overhead alone, paid on every tick, before any
work is done. It is the difference between hashing a heap string and a bounds
check plus a pointer load. For a hot path that is not a rounding error, and it
is only available if the key is an integer.

Making the key an integer is therefore not a limitation to work around. It is
the point. Allowing arbitrary key types would be a trap: callers would reach
for `String` because it reads better, get a hash map, and never learn why the
feature did not deliver what it promised.

`partitions: N` declares the id space so the router can pre-size its slot
vector and so persistence can enumerate. Ids must be dense in `0..N`.

Omitting `partitions` selects the other mode: a map-backed router over sparse
ids, three to eight times slower per lookup, in exchange for ids that can be
derived rather than assigned. That matters when two processes must agree on the
id for a key without sharing a counter. One rule, no extra keyword: declare a
bound and get the array, omit it and get the map.

Where ids are derived by hashing, collisions are handled by asserting at
startup rather than by probing or rehashing. Hash every canonical key at boot
and refuse to start if two collide, naming both. This is only correct when the
keys are a namespace the application owns, which is the common case: a
canonical key is usually the output of a normalisation step that already maps
many external spellings onto one internal name, so a collision is a naming
conflict fixed in configuration before the system is live. Rejecting a
colliding key at first touch is the backstop for keys added after boot.

### 4.0.1 How fast can a string key possibly be? Measured.

Since "just hash the string" is the obvious counter-proposal, here is the whole
field measured on the target machine rather than argued about. 500 keys,
20 million lookups, single thread, cache-warm, M4 Max.

| structure | ns per lookup |
| --- | --- |
| `std::HashMap` (SipHash-1-3) | 11.33 |
| `HashMap` + rapidhash | 5.20 |
| `HashMap` + foldhash | 5.22 |
| `HashMap` + rustc-hash (FxHash) | 5.47 |
| `HashMap` + ahash (AES) | 5.97 |
| PtrHash minimal perfect hash | 6.80 |
| pack to `u128`, `HashMap<u128>` | 2.35 |
| pack from `&str` per call, `HashMap<u128>` | 4.20 |
| `Vec` index by dense `u32` | **0.75** |

Four things fall out.

1. **The standard library hasher is a free 2x.** SipHash-1-3 is DoS-resistant
   and it costs double. Any of rapidhash, foldhash or FxHash halves it, and the
   change is one type parameter on the map.
2. **The top three are tied.** rapidhash, foldhash and FxHash are within noise
   of each other. rapidhash is portable and passes SMHasher3; gxhash is faster
   still on some benchmarks but requires AES instructions and is not portable.
   For a general library, rapidhash or foldhash.
3. **Minimal perfect hashing loses at this scale, which is counterintuitive.**
   PtrHash is state of the art and is built for hundreds of millions of keys,
   where a hash map's probe misses cache and an MPHF's does not. At 500 keys
   everything is in L1, so the pilot lookup is pure overhead and a plain map
   wins. MPHF is the wrong tool for a symbol table and the right tool for a
   genome index.
4. **Not hashing at all is faster than any hash.** A symbol of at most 16 bytes
   packs into a `u128` losslessly, so the "hash" is a load. It is injective, so
   collisions are impossible rather than merely unlikely, and it is *reversible
   with no side table*: the name reads straight back out of the integer. That
   is the property a hash can never have, and it is why packing rather than
   hashing is the right canonical form for symbol-like keys.

And the conclusion the table exists to support: the best string lookup
available is 5.2 ns, packing gets it to 2.35 ns, and a dense integer index is
0.75 ns. **Hashing cannot close that gap, because hashing the string is the
cost.** The string step belongs at the edge, once per subscription, not in the
router.

### 4.1 Names live in a registry, not in the partition key

Nothing is lost by dropping named partitions, because names move one level out
to an ordinary table:

```rust
// The registry. One string-keyed table, at the edge, consulted rarely.
worktable!(
    name: SymbolRegistry,
    persist: true,
    columns: {
        id: u16 primary_key autoincrement,
        name: String
    },
    indexes: { name_idx: name unique }
);

// Everything hot. Integer-partitioned, array-indexed router.
worktable!(
    name: Price,
    partition_by: symbol_id: u16,
    partitions: 1024,
    columns: { exchange: Exchange primary_key, bid: f64, ask: f64, ts: u64 }
);
```

The string is handled exactly once, when a subscription is set up:

```rust
let symbol_id = registry.select_by_name(name)?.id;      // once, at the edge
let book = prices.partition(symbol_id);                 // every tick, sub-ns
```

The strongest version of this drops `String` from the system entirely. A symbol
is a packed `u128` everywhere, which is fixed-size, `Copy`, allocation-free and
readable back into text without a lookup, and the registry maps that `u128` to
the dense `u16` the router indexes on. Reverse lookup for logs and metrics is
then a `Vec<u128>` indexed by id, with no string storage anywhere.

And querying *by name* still works, because the registry is an ordinary table
with an ordinary index. "Every symbol starting with BTC" is a registry query
that yields ids, followed by visiting those partitions. The capability moves;
it does not disappear.

This is also what `web3.trading-backend` already does for exchanges without
calling it that: `Exchange` is a fieldless enum with `TOTAL = 22`, and the hot
path writes `feed_data.exchange as u8`. Exchange is already a dense integer id.
Symbol simply has not been given the same treatment yet.

### 4.1 What is generated

- `PriceWorkTable`, exactly as today, unchanged. One partition is an ordinary
  table and nothing about it is special.
- `PricePartitions`, the router:
  - `partition(&Symbol) -> Option<&PriceWorkTable>`
  - `partition_or_create(&Symbol) -> &PriceWorkTable`
  - `keys()` and `iter()` over live partitions
  - `select_all()` fanning out and merging
  - `MemStat` aggregating across partitions
  - persistence writing one space per partition, keyed by the partition value

Routing is a map lookup. The generated query methods on a partition are the
methods that exist today, so a caller who has a partition writes ordinary
WorkTable code.

### 4.2 Semantics that change, and must be stated

These are the parts that need a decision rather than an implementation.

1. **The primary key is unique per partition, not globally.** Two partitions may
   both hold `exchange: BinanceFutures`. This is the intended meaning, and it is
   also the single biggest behaviour change, so it should be loud in the docs
   rather than discovered.
2. **`autoincrement` counts per partition.** Simplest and fastest. A global
   counter would be a shared atomic on every insert, which is the contention
   this feature exists to remove.
3. **A `unique` secondary index is unique per partition.** Enforcing global
   uniqueness would require checking every partition on every insert, which
   defeats the purpose. If global uniqueness is ever wanted it should be an
   explicit `unique global` that pays for itself openly.
4. **Persistence needs a filename-safe rendering of the partition key.** A
   `Display` that produces `BTC/USD` cannot become a path. Either require a
   dedicated trait, or hash the key and keep a manifest mapping hash to value.
   The manifest is safer and survives keys that are not human readable.
5. **Vacuum, purge and migration operate per partition.** That is a benefit,
   smaller blast radius, but the driving APIs need to iterate rather than assume
   one table.

### 4.3 What this is expected to buy, and how to check

Stated as a prediction so it can be falsified rather than assumed:

- **Lock and CDC contention** is the dominant win. Per-partition structures give
  independent contention domains, and contention degrades throughput
  non-linearly, so this should show up first and largest under concurrent
  writers.
- **Index locality** is second and is under-rated. An index over `symbols * N`
  keys does not fit cache; an index over `N` does. The comparison count barely
  moves; the cache-miss profile moves a lot.
- **Blast radius** for vacuum, rebuild and snapshot shrinks to one partition.
- **Fixed per-partition overhead** is the cost, and it dominates once partitions
  are small enough. There is a crossover and it should be measured, not guessed.

A bounded benchmark settles it: total rows held constant, split K ways for K in
1, 4, 16, 64, 256, measuring single-partition select latency, concurrent update
throughput at rising writer counts, and resident bytes. The prediction is that
most of the gain arrives by K = 16 and is driven by contention rather than index
depth, and that fixed overhead begins eating it after that.

## 5. Compound indexes, proposed separately

This is a smaller change with immediate value and no dependency on
partitioning.

```rust
indexes: {
    exch_sym_idx: (exchange, symbol) unique,
}
```

An index whose key is a tuple of columns, ordered by tuple comparison rather
than by a concatenated string. It deletes `ExchangeSymbol` and every defect in
section 1 with it: no format, no allocation, no duplicated column, no
lexicographic-versus-tuple ordering mismatch, and the type survives to the
boundary.

The two features compose but neither needs the other. If only one is built,
build this one first: it is smaller, it fixes existing production code, and it
is a strict improvement with no semantic changes to argue about.

## 6. A separate defect this work would trip over

`worktable!` cannot currently be used from a crate outside this repository
without that crate adding WorkTable's own dependencies by hand. The expansion
emits bare `futures::` and `derive_more::` paths, so a downstream user needs
`futures`, `derive_more` and `rkyv 0.8` as direct dependencies or the macro
fails to resolve them. Tests inside this repository do not show it because the
crate already has them.

The usual fix is a private re-export module, with the macro emitting
`::worktable::__private::futures::...`.

Related: `codegen/Cargo.toml` pins `rkyv = "0.7.45"` while the main crate uses
`0.8.17`.

## 7. Open questions

1. Should the partition key also be addressable as a column, for callers who
   want it in the row? Cheaper storage says no; familiarity says yes.
2. Should partitions be created lazily on first write, eagerly from a declared
   list, or both?
3. Is per-partition primary key uniqueness acceptable, or does some caller
   depend on global uniqueness today?
4. Does hash striping, the `shard` sense, belong on the roadmap as a separate
   feature for contention on a single unpartitionable table?
