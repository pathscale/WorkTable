# Partitioned tables: a worked example from production code

**Companion to:** [`partitioned-tables-proposal.md`](partitioned-tables-proposal.md).

**Status: design record, written before the implementation. Do not copy the
API from this page.** It uses `with_initializer`, `live_ids`, `mem_stat()` and
`select_all()` fan-out, none of which exist. The shipped surface is
`partition`, `partition_ref`, `partition_or_create`, `partition_or_insert_with`,
`contains`, `remove`, `gc`, `retired_len`, `keys`, `iter`, `len`, `is_empty`,
`memory_by_key`, `memory_total` and `rows_by_key`; see
[`partitioned-tables-implementation.md`](partitioned-tables-implementation.md).
This page is kept for the problem statement and the numbers.

Every "today" snippet below is real code from `web3.trading-backend`, quoted
rather than invented.

---

## 1. The point of this document

`partition_by` is not a new pattern. It is a pattern that already exists in
production, hand-written, in at least three places in one codebase. The
proposal is to generate it instead of writing it again each time.

The clearest instance is the order book. Read the "today" column and notice
that it is already one table per symbol, already with a router, already with
eager allocation. Everything `partition_by` would do is there. It was just
built by hand, and the hand-built version is missing the parts that are tedious
rather than the parts that are interesting.

## 2. What exists today

Three pieces, in three files.

**The table type.** `src/db/strategy/common/order_book.rs`, abridged:

```rust
worktable!(
    name: OrderBook,
    columns: {
        exchange_id: u8 primary_key,
        timestamp: i64,
        best_bid_price: f64,
        best_bid_size: f64,
        best_ask_price: f64,
        best_ask_size: f64,
        bids_size: u8,
        rest_bid_prices: OrderBookRestDepth,
        rest_bid_sizes: OrderBookRestDepth,
        asks_size: u8,
        rest_ask_prices: OrderBookRestDepth,
        rest_ask_sizes: OrderBookRestDepth,
    },
    queries: {
        update: {
            TopPrice(best_bid_price, best_bid_size, best_ask_price, best_ask_size) by exchange_id,
            RestPrices(bids_size, rest_bid_prices, rest_bid_sizes,
                       asks_size, rest_ask_prices, rest_ask_sizes) by exchange_id,
        }
    },
    config: { row_derives: Default }
);
```

Note the primary key: `exchange_id`, not symbol. The symbol is not in the row
at all. This table already *is* one partition.

**The partition wrapper.** `order_book_manager/order_book.rs`:

```rust
pub struct OrderBook {
    pub symbol: Symbol,
    pub table: Arc<OrderBookWorkTable>,
}

impl OrderBook {
    pub fn new(symbol: Symbol) -> Self {
        let table = Arc::new(OrderBookWorkTable::default());
        for row_id in 0..=Exchange::TOTAL {
            table.insert(OrderBookRow { exchange_id: row_id as u8, ..Default::default() }).unwrap();
        }
        Self { symbol, table }
    }
}
```

A key plus a table. `Exchange::TOTAL` is 22, so every partition holds exactly 23
rows, one per exchange.

**The router.** `order_book_manager.rs`:

```rust
pub struct OrderBookManager {
    order_book_managers: HashMap<Symbol, Arc<OrderBook>>,
    order_book_updated_connector: BroadcastPipeConnector<OrderBookUpdated>,
}

impl OrderBookManager {
    pub fn allocate_for_symbols(&mut self, symbols: &HashSet<Symbol>) {
        for s in symbols {
            self.order_book_managers.insert(s.clone(), Arc::new(OrderBook::new(s.clone())));
        }
    }

    pub fn get_order_book(&self, symbol: &Symbol) -> Option<&Arc<OrderBook>> {
        self.order_book_managers.get(symbol)
    }
}
```

That is a partition set: eager creation from a key list, and lookup by key.

**Why it was built this way.** With 500 symbols, one table would hold 500 x 23 =
11,500 rows and every read would walk an index over all of them. As built, a
read is against 23 rows with no index walk at all. In a hot path that is the
whole game.

## 3. The same thing with `partition_by`

```rust
worktable!(
    name: OrderBook,
    partition_by: symbol_id: u16,
    partitions: 1024,
    columns: {
        exchange_id: u8 primary_key,
        timestamp: i64,
        best_bid_price: f64,
        best_bid_size: f64,
        best_ask_price: f64,
        best_ask_size: f64,
        bids_size: u8,
        rest_bid_prices: OrderBookRestDepth,
        rest_bid_sizes: OrderBookRestDepth,
        asks_size: u8,
        rest_ask_prices: OrderBookRestDepth,
        rest_ask_sizes: OrderBookRestDepth,
    },
    queries: {
        update: {
            TopPrice(best_bid_price, best_bid_size, best_ask_price, best_ask_size) by exchange_id,
            RestPrices(bids_size, rest_bid_prices, rest_bid_sizes,
                       asks_size, rest_ask_prices, rest_ask_sizes) by exchange_id,
        }
    },
    config: { row_derives: Default }
);
```

Two lines added. The columns, the primary key, the queries and the config are
untouched, because the shape was already right.

`OrderBook` and `OrderBookManager` are then deleted, and their behaviour comes
from the generated `OrderBookPartitions`.

**The key is `symbol_id: u16`, not `Symbol`.** That is not a compromise, it is
where most of the remaining win is. `Symbol` is a newtype over `String`, so
`HashMap<Symbol, _>::get` hashes a heap string on every tick. Measured on an
M4 Max at 500 partitions, single thread, cache-warm:

| router | ns per lookup |
| --- | --- |
| `HashMap<Symbol(String), _>::get`, what `OrderBookManager` does today | 9.52 |
| `Vec<Option<_>>` indexed by `u16` | 0.38 |

25x, on the routing step alone, before any work happens. The partition key is
never selected on, never appears in a row, and no query can reference it, so it
does not need to be a rich type. It needs to be an array index.

The symbol string then lives in a registry, consulted once per subscription
rather than once per tick:

```rust
worktable!(
    name: SymbolRegistry,
    persist: true,
    columns: { id: u16 primary_key autoincrement, name: String },
    indexes: { name_idx: name unique }
);
```

This is already the shape `Exchange` has in this codebase: a fieldless enum with
`TOTAL = 22`, used in the hot path as `feed_data.exchange as u8`. Exchange is a
dense integer id already. Symbol has simply not been given the same treatment.

## 4. How it is actually used

### 4.1 Startup

```rust
// today
let mut manager = OrderBookManager::new();
manager.allocate_for_symbols(&symbols);

// with partition_by: resolve names to ids once, then work in ids
let books = OrderBookPartitions::new();
for name in &symbols {
    let id = registry.intern(name)?;   // string handled once, here and nowhere else
    books.allocate(id);
}
```

Or drop `allocate` entirely and let partitions appear on first write. Eager
allocation still has a reason: it keeps the first tick off the allocation path,
which in a trading loop is worth having.

Seeding each new partition with its 23 exchange rows is the one thing the
generated router cannot know. It needs a hook:

```rust
let books = OrderBookPartitions::with_initializer(|table| {
    for row_id in 0..=Exchange::TOTAL {
        table.insert(OrderBookRow { exchange_id: row_id as u8, ..Default::default() })?;
    }
    Ok(())
});
```

This is the piece worth arguing about, because it is the only genuinely new
concept. See section 7.

### 4.2 The hot write path

```rust
// today: hashes the symbol string on every tick
let book = manager.get_order_book(&feed.symbol).ok_or_else(|| eyre!("unknown symbol"))?;
book.table.update_top_price(feed.into(), row_id).await?;

// with partition_by: array index
let book = books.partition(feed.symbol_id).ok_or_else(|| eyre!("unknown symbol"))?;
book.update_top_price(feed.into(), row_id).await?;
```

One indirection fewer, because there is no wrapper struct holding the key
alongside the table, and 9.5 ns fewer, because the lookup stopped being a hash
of a heap string. Everything else is identical: `update_top_price` is the
generated query it always was, and it runs against a 23-row table.

The feed handler resolves `symbol_id` once when the subscription is opened,
not per message. That is the only behavioural change the hot path sees.

### 4.3 The hot read path

```rust
// today
let book = manager.get_order_book(&symbol)?;
let row = book.table.select(exchange_id)?;

// with partition_by
let row = books.partition(symbol_id)?.select(exchange_id)?;
```

### 4.4 Crossing partitions

This is what the hand-written router does not have, and where code today
reaches for a loop over the map.

```rust
// every partition, merged
let stale = books.select_all().filter(|r| r.timestamp < cutoff).execute()?;

// a named subset: names resolve in the registry, then the ids drive the visit
for id in registry.ids_for(&watchlist)? {
    if let Some(t) = books.partition(id) { ... }
}

// which partitions are live
for id in books.live_ids() { ... }
```

### 4.5 Purge, which today is manual

`PurgeableTable` is implemented per table and the purger holds
`HashMap<u64, Vec<Arc<dyn PurgeableTable + Sync + Send>>>`. With partitions,
one implementation covers the set:

```rust
impl PurgeableTable for OrderBookPartitions {
    async fn purge(&self, before_ms: u64) -> eyre::Result<()> {
        for t in self.iter() {
            t.purge(before_ms).await?;
        }
        Ok(())
    }
}
```

Better still, purging one partition does not touch or lock any other, so a
purge sweep stops being a global stall.

### 4.6 Memory accounting

```rust
// today: nothing aggregates across the map, so this is hand-summed or absent
// with partition_by
let stat = books.mem_stat();          // summed over live partitions
let per_symbol = books.mem_stat_by_key();  // where the memory actually went
```

For a system holding hundreds of symbol tables, "which symbol is costing me"
is a question worth being able to ask.

### 4.7 Persistence

The order book is in memory today. If it were persisted, the hand-written
router would need one space per symbol, a naming scheme, and load-time
reconstruction of the map. That is the tedious part nobody wants to write
twice, and it is the strongest argument for generating it:

```rust
worktable!(
    name: OrderBook,
    partition_by: symbol: Symbol,
    persist: true,
    ...
);

let books = OrderBookPartitions::load(config).await?;   // rebuilds every partition
books.wait_for_ops().await;
```

The open question is how a partition key becomes a directory name. See section 7.

## 5. What is gained, precisely

Nothing in section 4 makes a read faster than the hand-written version, because
the hand-written version already got the important thing right. What is gained
is everything around it:

| | hand-written today | generated |
| --- | --- | --- |
| routing by key | yes, string hash at 9.52 ns | yes, array index at 0.38 ns |
| eager allocation | yes | yes |
| one table per symbol | yes | yes |
| fan-out query across partitions | no, loop by hand | yes |
| aggregate `MemStat` | no | yes |
| per-partition memory attribution | no | yes |
| persistence per partition | no | yes |
| purge across the set | one impl per table | one impl |
| written once per use site | no, three times so far | yes |

The last row is the real one. `OrderBookManager` is the third hand-rolled
router in this codebase, after `last_symbol_signal_map` and
`last_event_timestamps`, and each is slightly different.

## 6. What changes, and what breaks

**Per-partition uniqueness.** The primary key is unique within a partition.
Here that is already true and already intended: `exchange_id` is meant to repeat
across symbols. Any caller assuming global uniqueness would break, and in this
table there is none.

**`autoincrement` counts per partition.** Not used here.

**`unique` secondary indexes are per partition.** Not used here either. Where
this matters is `funding_rates`, and that table should not be partitioned. See
section 8.

**Fan-out is not free.** `select_all()` across 500 partitions acquires 500 locks
and merges 500 result sets. It is for maintenance sweeps, not for the hot path.
If a workload is mostly cross-partition, partitioning is the wrong tool and it
will be slower than one table.

## 7. The two things that need deciding, not implementing

**Partition initialisation.** Section 4.1 needs a hook because a new partition
here is not empty: it is 23 default rows. Options: a closure at construction, a
declared `on_create` block in the DSL, or an `Initialise` trait on the row type.
The closure is least magical and hardest to serialise into persistence
metadata. This is the only genuinely new concept the feature introduces.

**Partition key to storage name.** For persistence, `Symbol` must become a
directory name. A `Display` producing `BTC/USD` cannot. Either require a
dedicated trait with a documented character set, or hash the key and keep a
manifest mapping hash to key. The manifest is safer, survives keys that are not
human readable, and makes the on-disk layout stable if `Display` ever changes.

## 8. The case that should *not* be partitioned

`src/db/strategy/common/funding_rates.rs` looks superficially similar and is a
different problem:

```rust
columns: {
    id: u32 primary_key autoincrement,
    exchange: Exchange,
    exchange_symbol: String,          // "BinanceFutures|BTCUSDT"
    rate: f64,
    ...
}
indexes: {
    exchange_symbol_idx: exchange_symbol,
    exchange_idx: exchange
}
```

The string key exists because there is no compound index, so `(exchange,
symbol)` has to be flattened into one column. The fix is a compound index, not
a partition:

```rust
columns: {
    id: u32 primary_key autoincrement,
    exchange: Exchange,
    symbol: Symbol,
    rate: f64,
    ...
}
indexes: {
    exch_sym_idx: (exchange, symbol) unique,
    exchange_idx: exchange
}
```

That deletes `ExchangeSymbol` and with it the `format!` on every lookup, the
duplicated exchange, and the ordering bug where `|` at `0x7C` sorts above every
alphanumeric so lexicographic order does not match tuple order.

The distinction is worth stating as a rule: **partition when access is
key-local and the key partitions the workload; use a compound index when you
just need to look up by more than one column.** The order book is the first.
Funding rates is the second.

## 9. One thing this codebase already does that is worth noting

`rest_bid_prices: OrderBookRestDepth` is a composite column: several values
under one type name, used as a single column. That already works today with no
DSL change, and it is one cell taking one lock rather than several. It is
mentioned here only because the same discussion that produced this proposal also
asked whether composite columns were possible. They are, and this table is the
proof.
