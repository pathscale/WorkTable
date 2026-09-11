# Small tables: when the index costs more than it saves

Every `worktable!` declaration gets a primary index, unconditionally. There is
no way to say "this table is small, do not index it". This document measures
what that costs, where the lines are, and what it means for a table of forty
rows.

All measurements taken 2026-09-11 on an Apple M4 Max (16 logical cores, 12
performance and 4 efficiency), aarch64, `--release`. Every arm is interleaved
with the others so a machine warming up over the run cannot be charged to
whichever arm ran last, and every figure is a median rather than a mean.

## The short version

| question | answer |
|---|---|
| Below how many rows does a linear scan beat the index on **time**? | **32** |
| Below how many rows does the index cost more **memory** than the rows? | about **1,000** |
| What does the index cost at 40 rows? | **601 bytes per row**, against a 24-byte row |
| What does it save at 131,072 rows? | **1,333x** on lookup |
| Do any of our benchmarks measure a small indexed table? | **No. Not one.** |

## What "the index" is, and what it is not

Named, typed columns are free. `PointRow { id: u64, value: u64, tag: u64 }` and
`(u64, u64, u64)` are the same bytes in the same order. Tabular shape costs
nothing.

What costs is answering **"where is the row for key K"** without looking at
every row. That is the index, that is the only thing being measured here, and
it is the only thing a small table can decline.

A table with no index is not a table with a missing feature. It is a table that
answers a narrower question: it can hand you row number seven, and it can walk
every row, but it cannot find the row for key `K` except by looking.

## Time: a scan wins below 32 rows

Median nanoseconds per successful point lookup. Every key is looked up once per
pass, and passes are repeated so that small sizes still take measurable time.
The scan is `rows.iter().find(|r| r.0 == key)`; the index is `ArcticIndex`,
which is what `worktable!` defaults to.

| rows | scan ns | arctic ns | ratio | winner |
|---:|---:|---:|---:|---|
| 4 | 1.0 | 9.8 | 0.10 | **scan** |
| 8 | 1.1 | 8.9 | 0.13 | **scan** |
| 16 | 2.3 | 10.8 | 0.22 | **scan** |
| 32 | 4.4 | 11.2 | 0.39 | **scan** |
| 64 | 9.1 | 7.6 | 1.20 | arctic |
| 128 | 19.3 | 7.4 | 2.62 | arctic |
| 256 | 35.4 | 7.5 | 4.72 | arctic |
| 512 | 65.4 | 9.6 | 6.84 | arctic |
| 1,024 | 124.1 | 9.2 | 13.44 | arctic |
| 4,096 | 485.5 | 9.6 | 50.47 | arctic |
| 16,384 | 1,956.2 | 8.6 | 226.16 | arctic |
| 65,536 | 8,155.3 | 8.9 | 918.28 | arctic |
| 131,072 | 16,413.5 | 12.3 | **1,332.98** | arctic |

Two things to read off this.

**The scan is fast at small sizes for a real reason.** It is sequential,
prefetchable and has no pointer chasing. At four rows it is a single cache
line. The index cannot beat that, because an ART lookup is a few dependent
loads no matter how small the tree is.

**Arctic does not degrade.** Its lookup is flat at roughly 9 ns from 64 rows to
131,072. That is the point of a radix tree, and it means there is no upper
crossover where the scan comes back. Once the index wins it keeps winning, and
the margin grows without bound.

## Memory: the index is never free, and is grotesque when small

Bytes held by the index alone, per row, measured against a counting global
allocator. Arctic's own `allocated_node_bytes` is taken as a floor where it is
larger. **A row is 24 bytes**, so anything above 24 in the arctic column means
the index outweighs the data it indexes.

| rows | HashMap B/row | BTreeMap B/row | arctic B/row | arctic vs a row |
|---:|---:|---:|---:|---:|
| 64 | 34.1 | 31.5 | **601.4** | **25.06x** |
| 1,024 | 34.0 | 34.4 | 22.2 | 0.93x |
| 16,384 | 34.0 | 34.2 | 22.2 | 0.92x |
| 131,072 | 34.0 | 34.3 | **16.1** | 0.67x |
| 1,048,576 | 34.0 | 34.3 | 16.1 | 0.67x |

**Arctic has two crossovers and they are at different sizes.** It starts
winning on time at 32 rows. It does not stop being wasteful with memory until
somewhere around a thousand.

At 64 rows arctic holds 601 bytes for every 24-byte row: a radix tree's fixed
node structure amortised over almost nothing. Both std maps are flat at about
34 bytes a row at every size, so at 64 rows **either std map is 18x smaller
than arctic**, and at 131,072 rows **arctic is 2.1x smaller than either**.

That reversal is worth remembering. Arctic is the right default for a large
table on both axes at once. It is the worst of the three choices for a small
one.

## Where the time actually goes

200,000 rows, build and query timed separately, so the cost of having an index
is separated from the cost of using one.

| arm | build ms | query ms | total ms | ns/lookup |
|---|---:|---:|---:|---:|
| `Vec` alone, lookup by position | 0.04 | 0.04 | 0.08 | **0.2** |
| `Vec` + `HashMap` | 3.48 | 1.21 | 4.69 | **6.0** |
| `Vec` + `BTreeMap` | 7.61 | 5.32 | 12.93 | **26.6** |
| `Vec` + `ArcticIndex` | 3.70 | 2.06 | 5.77 | **10.3** |
| `Vec`, linear scan (4,000 rows) | - | - | - | **456.9** |

**Building is the larger half.** 3.70 ms to build against 2.06 ms to run
200,000 lookups. A table filled once and queried many times amortises that; a
table rebuilt constantly does not, and for such a table the crossover sits
higher than 32 rows.

**The scan row is the honest alternative.** 456.9 ns per lookup at only 4,000
rows, growing linearly, so at 200,000 rows it would be roughly 23 microseconds,
about 2,000x arctic. The index is not costing 9x. It is saving 2,000x on the
question a bare `Vec` never asks.

**`HashMap` beats arctic on point lookups: 6.0 ns against 10.3.** Arctic is
paying for ordering, and `BTreeMap` gives the same ordering for 26.6 ns. So
arctic is 2.6x better than `BTreeMap` at equal capability and 1.7x worse than a
hash that cannot do ranges at all. **There is no hash-shaped backend in the
grammar**, and for a table that declares no range queries and no ordered scans
that is 1.7x left on the hottest path at every size above the crossover.

### A caution about the bare-`Vec` baseline

The first row above was measured two ways during this work, and it moved by 8x:

- built with `(0..n).map(..).collect()`, which pre-sizes and vectorises: **0.08 ms**
- built with a `push` loop: **0.63 ms**

An earlier note in this session quoted "the index costs 9.38x" from the second
form; the same comparison against the first reads about 72x. **Neither number
is wrong and neither is meaningful**, which is why "1:1 with a native `Vec`" is
not a target this project should quote. The stable claim is the one against the
hand-written `Vec`-plus-index pattern an application writes when it has no
table, and there the generated table is at parity: 6.4 ms against
`worktable-vec`'s `ArcticTable` at 6.5, and 13.6 for `Vec` + `BTreeMap`.

## Could the table decide for itself?

A prototype: hold the rows, skip the index until the table crosses a threshold,
build it once at the crossing, and branch on `len()` in `select`. Never tear it
down, so a delete that drops back under the line cannot thrash the rebuild.

| rows | always indexed ns | adaptive ns | ratio | what the adaptive table did |
|---:|---:|---:|---:|---|
| 8 | 9.4 | **1.3** | 0.14x | scanning, no index built |
| 16 | 8.3 | **2.6** | 0.32x | scanning |
| 32 | 8.1 | **4.4** | 0.54x | scanning |
| 64 | 7.6 | 10.7 | **1.40x** | scanning, **and losing** |
| 128 | 7.5 | 7.6 | 1.00x | indexed |
| 1,024 | 9.5 | 9.6 | 1.00x | indexed |
| 16,384 | 8.5 | 8.7 | 1.02x | indexed |
| 131,072 | 12.0 | 11.3 | 0.94x | indexed |

**The branch is free.** 1.00, 1.00, 1.02, 0.94 at the four large sizes.
Adaptivity would cost real tables nothing measurable.

**The prototype's threshold was wrong**, and the measurement caught it. It was
set at 64 from a first reading of the crossover table, and at exactly 64 rows
the adaptive table is still scanning and is 1.40x *slower*. Arctic has already
won by then. **The line is 32.**

## The production case: web3.trading

Everything above was measured in the abstract. This section is a real
workload, and it moves the conclusion.

### It is not one table, it is one table per partition

`web3.trading-backend` declares `OrderBook` keyed `exchange_id: u8`, with a row
carrying four 24-wide depth arrays. **832 bytes a row.** It is read on every
orderbook update and written at the same rate, concurrently, at around ten
thousand a second, and it is partitioned by symbol.

Each partition holds **one row per exchange, so two or three rows**. The
partition count is expected to be about **2,000**, possibly as low as 200, and
maybe 40 if squeezed.

That matters because `PartitionSet<T>` holds **a whole table per partition**.
Every partition carries its own `DataPages`, its own index, its own lock map,
its own empty-link registry and its own epoch domain. Three rows per partition
means all of that apparatus, two thousand times over.

### Measured, at all three partition counts

Three rows of 832 bytes per partition, memory measured against a counting
global allocator.

| partitions | total | row payload | overhead | bytes/partition | overhead |
|---:|---:|---:|---:|---:|---:|
| 40 | 1.15 MB | 97 KB | 1.06 MB | 29,508 | 10.8x |
| 200 | 5.56 MB | 487 KB | 5.07 MB | 28,459 | 10.4x |
| 2,000 | **55.5 MB** | 4.9 MB | **50.6 MB** | 28,424 | 10.4x |

**At 2,000 partitions the process holds 55 MB to store 4.9 MB of rows.**

### The rows are not the cost. The partition is.

| | bytes |
|---|---:|
| an **empty** partition, no rows at all | **28,395** |
| the same partition holding three 832-byte rows | 28,459 |
| difference | **64** |

Three rows, 2,496 bytes of data, add sixty-four bytes. The whole 28 KB is
allocated at partition creation and is fixed. This is not the index being
wasteful at small sizes, which is what the rest of this document is about. It
is the entire table apparatus replicated per partition, and it would cost the
same if the partitions were empty.

### This is now fixable in the declaration

`partition_max_size: u8` beside `partition_by` generates `<Name>DenseTable`
instead of the full table. The primary key is the row's position, so the index,
the pages, the links, the free list, the lock map and the CDC all go, and a
lookup becomes a bounds check and a load.

Measured on one declaration at two widths, 200 partitions of 23 rows each,
counting bytes the allocator was actually asked for
(`tests/dense_partition_memory.rs`):

| shape | bytes per partition |
|---|---:|
| full table, empty | 28,404 |
| **dense, empty** | **108** |
| full table, 23 rows of an 88-byte row | 32,900 |
| **dense, same** | **3,180** |

The empty row is the one that matters. The saving is the fixed apparatus, so it
is about 28 KB per partition whatever the rows weigh: at 2,000 symbols, roughly
56 MB. The ratio falls for wider rows only because the rows themselves grow.

The 28,404 here and the 28,395 above were measured independently and by
different means: the figure above came from process memory across a range of
partition counts, this one from a counting `#[global_allocator]` around a single
construction loop. They agree to nine bytes, which is the strongest thing that
can be said for either of them.

### Time is not the problem

| | |
|---|---:|
| `select` of one row through its partition | **48 ns** |
| at 10,000 reads/sec | 0.48 ms/s |
| at 10,000 reads **and** 10,000 writes/sec | **0.96 ms/s** |

About **a tenth of one percent of a core**. The 48 ns is dominated by copying
an 832-byte row out, because a paged `select` returns an owned row; the index
lookup is roughly 10 ns of it. Nobody should change anything here for speed.

### What actually helps, in order

**1. A smaller page. Available today, no change to this crate.**

Three 832-byte rows are 2.5 KB. The default page is 16 KB, so **84% of every
partition's page is empty**.

| page size | bytes/partition | at 2,000 partitions |
|---:|---:|---:|
| 16,384 (default) | 28,459 | 54.3 MB |
| **4,096** | **16,171** | **30.8 MB** |

`config: { page_size: 4096 }` on the declaration is a **43% cut**, 23 MB back,
and it is one line. This is the first thing to do.

**2. Cache the config in the caller.** A separate table, `S3Config`, is a
single row read from the event-generation and order-placement paths at the same
rate and written perhaps once a day. Measured at 20.16 ns a read for an 11-field
row. Caching it in the strategy struct and invalidating on
`upsert_configuration` takes it to approximately zero and needs nothing from
this crate.

**3. Direct addressing on a `u8` key.** Both `OrderBook` and `S3Config` are
keyed `u8`, which bounds them at 256 rows *in the type*, at compile time. For
such a key an index can be a 2 KB array rather than a radix tree.

| shape | ns/read | vs today |
|---|---:|---:|
| paged `worktable!`, owned row (today) | 20.16 | 1.00x |
| `vec: true`, borrowed row | 5.66 | 0.28x |
| direct `[Option<Row>; 256]` on a `u8` key | **0.56** | **0.03x** |

36x, decided from the declared key type, needing **no grammar and no runtime
machinery**. There is no row count at which a radix tree beats a 256-entry
array, so this is not a trade-off.

**4. A smaller apparatus for small partitions.** After the page, roughly 12 KB
per partition remains: index, lock map, registries, epoch domain. Nothing in
the grammar lets a caller say "this partition holds three rows". This is the
largest remaining number and the least designed.

### What this changes about the rest of this document

The earlier sections conclude that adaptive small-table indexing is a
micro-optimisation with no workload behind it. **The first half of that is
still right and the second half is not.** There is a workload. It is just not
the index that is costing it.

- The index at 1-3 rows is real but small here: ~10 ns of a 48 ns read, and a
  fraction of the 28 KB.
- The **page** is 12 KB of the 28, fixable today with one config key.
- The **rest of the table apparatus** is the other 12 KB, and is not addressable
  at all right now.

An adaptive index would have fixed the smallest of the three.

## What our benchmarks measure, and what they miss

Every scale constant in both suites, against the two lines:

| suite | arm | vs 32 rows (time) | vs ~1,000 rows (memory) |
|---|---:|---|---|
| perf-benchmarks | 200,000 | above | above |
| | 100,000 events | above | above |
| | 68,172 | above | above |
| | 4,000 documents | above | above |
| | 3,000 vocabulary | above | above |
| | 1,000 range rows | above | at the line |
| | 256 per partition | above | **below** |
| wt-benchmarks | 100,000 | above | above |
| | 20,000 | above | above |
| | 1,528 (MoE resident) | above | just above |
| | 256 | above | **below** |

**No arm anywhere is below the time crossover.** The lowest is 256.

**The one 256-row arm does not use an index at all.** `partition-route` routes
by arithmetic:

```rust
rows: (0..ROWS_PER_PARTITION).map(|id| (id, id as f64)).collect(),
let at = (id % ROWS_PER_PARTITION) as usize;   // a position, not a lookup
```

Sixty-four partitions of 256 rows, each a plain `Vec` addressed by position. So
the single place in either suite that sits in the wasteful band independently
arrived at the right answer for that size, in code written before any of this
was measured.

**The blind spot:** nothing measures a small *indexed* table. The suites either
go large, or go small and skip the index. So if an application declares a
forty-row lookup table with `worktable!`, it pays 25x the row size in memory
and roughly 2x the lookup a scan would have done for free, and **every
benchmark stays green**.

## What to do about it

In order.

1. **Add a small-table benchmark arm.** 8, 16, 32 and 64 rows, indexed, against
   the scan. It will not impress, which is the point: it makes the one regime
   where our defaults are wrong visible to the suite.
2. **Measure index memory more widely.** Only `moe-resident-memory-ab` measures
   index bytes, and only at 1,528 rows. Nothing demonstrates arctic's 16 B/row
   at 131,072, which is a real win over both std maps that we currently cannot
   show.
3. ~~Find out whether anything real is under 32 rows and indexed.~~ **Found:
   see the web3.trading section.** `OrderBook` is 2-3 rows per partition across
   up to 2,000 partitions, read and written 10k/s. The index is the smallest
   part of what it costs.
4. **Consider a hash-shaped backend** before considering adaptivity. Larger
   win, at every size, and no runtime machinery.
5. **Only then consider adaptivity**, and only for the single-writer table
   where it is a branch rather than a concurrency problem.

Until any of that happens, the practical advice for a caller is one line: **a
table under about thirty rows is better as a `Vec` and a scan than as a
`worktable!`**, and nothing in the tooling will tell you so.

## What is not measured here

Stated so nobody quotes these numbers past what they cover.

- **Single-threaded only.** Every figure is one thread. Contention changes the
  picture for any concurrent index, and none of this says anything about it.
- **One key type.** `u64` keys throughout. A `String` key changes both the scan
  (comparison cost) and the index (arctic's string path is a different shape).
- **Successful lookups only.** Every key looked up is present. A miss is a
  different cost, and for a scan it is the worst case: the whole table.
- **One machine.** Apple M4 Max, aarch64. Cache sizes decide where the scan
  stops being a cache-line walk, so the 32-row line is this machine's.
- **No deletes, no updates.** Build then query. A churning table amortises the
  index build differently and the crossover moves.

## Reproducing

The probes live outside the repository, in the session scratchpad, because they
measure alternatives rather than this crate. To rebuild them, the arms are:

- **crossover**: a `Vec<(u64,u64,u64)>` and an `ArcticIndex<u64,u64>` over the
  same rows, `iter().find()` against `get_value`, sizes from 4 to 131,072,
  passes repeated to `1_000_000 / n`, 9 rounds, median.
- **memory**: a counting `GlobalAlloc`, measuring live bytes across the build of
  each index alone, at 64 / 1,024 / 16,384 / 131,072 / 1,048,576 rows.
- **decomposition**: the same arms with the build and query phases timed
  separately, 15 rounds.
- **adaptive**: an `Option<ArcticIndex>` built once on crossing a threshold, and
  `select` branching on whether it exists.
