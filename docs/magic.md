# Historical design discussion

This document retains the original runtime proposal and measurements. It is not the released API contract. In particular, runtime selection does not change table lock types, profiles must match the declared backend/flavor, and scheduled selects finish with execute_async().await. See [the canonical guide](wt-user-guide.typ) and [the implementation contract](query-runtime-release-gate.md).

# `worktable!`: the whole DSL

What the macro accepts today, why the runtime work exists at all, and the syntax
proposed for it. The two halves are separated: under **Today** it compiles now,
under **Proposed** it does not.

---

# Why any of this exists

## One engine, several concurrency points, opposite answers

A WorkTable table is not one concurrent thing. It is five, and they want
different scheduling:

| point | where | shape |
|---|---|---|
| row-lock handoff | `src/lock/` — a writer parks, the releaser wakes it | a chain: the successor wants the cache lines just touched |
| in-place mutation | internal locking, no caller-visible lock | different contention entirely |
| persistence flush | `persistence/task.rs` — one long-lived queue drainer per table | never usefully suspends, just needs a thread |
| vacuum sweep | `table/vacuum/manager.rs` — background, already paced | must **not** disturb the foreground |
| query fan-out | does not exist yet | independent chunks, embarrassingly parallel |

A single table-wide setting cannot serve those. That is the whole motivation.

## The trade is real, and it was measured rather than assumed

Keeping a woken task on the worker that woke it is worth a lot to one workload
shape and costs a lot to another. Measured on YCSB at eight threads, same engine,
against a tokio-driven build:

| workload | `locality` | `spread` |
|---|---|---|
| 50% read / 50% update | **+6.4%** | −40.2% |
| read-modify-write | **+2.0%** | −38.4% |
| 95% read / 5% update | −19.2% | **+74.4%** |
| 95% read / 5% insert | +137.1% | **+321.6%** |

Four independent attempts were made to find a static rule that wins both columns:

1. route only *self*-wakes locally → the update workload went to −41.8%
2. route locally only when no worker is idle → −49.5%
3. let a thief take the LIFO slot on second sight → +97.7% one column, −34.5% the other
4. push local wakes onto the stealable deque → +113.4% one column, −41.2% the other

Four attempts, four times the same answer: one column or the other, never both.
**That is evidence there is no such rule**, and it is why this is a selection
mechanism rather than a better default.

## Nobody else exposes it, and everyone has it

tokio ships this exact switch as `disable_lifo_slot`: one boolean, no guidance on
when to flip it. The mechanism is not novel. What is novel here is that each
setting carries the measurement that produced it, so a schema author can choose on
evidence instead of folklore.

## No scheduler wins everywhere

Same engine, only the executor driving the clients changed:

| benchmark | winner |
|---|---|
| orderbook-arrival, p99 at 50% load | nagoya, 6,042 ns against tokio's 60,875 |
| orderbook-burst, makespan | nagoya, 5.8 ms against tokio's 7.0 |
| timer latency, 1 ms sleep | nagoya, 270 µs p50 against tokio's 1,538 |
| YCSB pure read, 16 threads | tokio, 18,850,424 against nagoya's 14,329,421 |
| YCSB A / B / F at 4-6 threads | thread-per-client, no async scheduler at all |
| p50 at any load | thread-per-client |

So the design goal is not "make nagoya win". It is "let the schema say which shape
this table's work has", and then be right for that shape.

## Why the DSL and not a config file

Because the choice **changes the generated type**. `runtime:` selects the
`RwLock`, `Notify` and `JoinHandle` that `LockMap` and `PersistenceTask` are built
from. That is the same reason `persist:` is in the macro rather than a setting:
it changes what is generated, not how it behaves at run time.

It also keeps the decision next to the columns and queries it governs, where a
reader can see it.

---

# Today

## The smallest table

```rust
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: Simple,
    columns: {
        id: u64 primary_key autoincrement,
        value: String,
    }
);
```

Generates `SimpleWorkTable`, `SimpleRow`, `SimplePrimaryKey` and the query methods
below. Every generated name derives from `name:`.

## The grammar

A **fixed, ordered prefix**, then a free-order section list.

| position | key | meaning |
|---|---|---|
| 1, required | `name:` | table name, CamelCase |
| 2, optional | `version:` | schema version, for migration |
| 3, optional | `persist:` | `true` writes to disk |
| 4, optional | `partition_by:` | partition key name and unsigned type |
| 5, required with 4 | `partition_max_size:` | rows per partition, as an index width |
| any order | `columns:` | the row and its primary key |
| any order | `indexes:` | secondary indexes |
| any order | `queries:` | generated `update` / `delete` / `in_place` |
| any order | `config:` | `page_size`, `row_derives` |

The prefix is genuinely ordered: `parse_name` reads the first token and errors if
it is not `name`, so nothing can precede it.

## Everything at once

```rust
worktable!(
    name: Test,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement,
        test: i64,
        another: u64,
        exchange: String
    },
    indexes: {
        test_idx: test unique,
        exchnage_idx: exchange,
        another_idx: another,
    },
    queries: {
        update: {
            AnotherByExchange(another) by exchange,
            AnotherByTest(another) by test,
            AnotherById(another) by id,
        },
        delete: {
            ByAnother() by another,
            ByExchange() by exchange,
            ByTest() by test,
        }
    }
);
```

## Columns

```rust
columns: {
    id: u64 primary_key autoincrement,   // the table generates the key
    other: u128 primary_key,             // the caller supplies it
    name: String,
    amount: u64,
    price: f64,
    flag: bool,
}
```

Exactly one column takes `primary_key`. `autoincrement` makes the table generate
it and adds `get_next_pk()`.

## Index backends: `using`

The mechanism the proposed `runtime:` selection copies, and the reason `runtime`
is a *separate* keyword rather than an overload of this one.

```rust
columns: {
    id: u64 primary_key using worktables_index,
    other: u64,
},
indexes: {
    other_idx: other using congee,
    name_idx: name unique using arctic,
}
```

| backend | notes |
|---|---|
| `arctic` | the default |
| `worktables_index` | the persisted page format earlier releases wrote |
| `congee` | requires explicit persistence |
| `indexset` | the upstream crate, behind the `vanilla-index` feature |

Omitting `using` gives `arctic` for in-memory lookups while persisted tables keep
the `worktables_index` page format, so an existing file still opens. `unique` is
independent of the backend and combines with it.

## Queries

Three kinds. CamelCase in the declaration, snake_case in the generated method.

```rust
queries: {
    update: {
        AmountById(amount) by id,
    },
    delete: {
        ByName() by name,
    },
    in_place: {
        SomeValueById(some_value) by id,
    }
}
```

**`update`** generates `update_amount_by_id(AmountByIdQuery { amount }, id)`. The
query struct is the name plus `Query`.

**`delete`** generates `delete_by_name(name)`. Empty parentheses because a delete
names no columns.

**`in_place`** generates `update_some_value_by_id_in_place(id, |value| ...)`, which
mutates without selecting first. Its locking is internal, so it is safe from
several threads without the caller holding anything — which is also why it is a
*different concurrency point* from `update`. **Only `by {pk_field}` is
supported.**

## Selects, which are not declared

Generated from the columns and indexes:

```rust
table.select(pk)                                    // by primary key
table.select_by_name("abc".to_string())             // by an indexed column
table.select_by_pk_range(start..=end).execute()?    // a range
table.select_all().execute()?
table.select_all()
     .order_on(TestRowFields::Test, Order::Desc)
     .limit(10)
     .execute()?
```

Note which of these return a **builder** — `select_all`, `select_by_pk_range` —
and which return a row directly. That distinction is load-bearing for the proposed
`.runtime()`, which can only exist on a builder.

## Persistence

```rust
worktable!(
    name: Orders,
    persist: true,
    columns: { id: u64 primary_key autoincrement, symbol: String },
);
```

Adds `load`, `wait_for_ops`, `close` and the persistence engine; files are opened
through `worktable::fsx`.

## Partitioning

```rust
worktable!(
    name: Price,
    partition_by: symbol_id: u16,
    partition_max_size: u8,
    columns: {
        exchange_id: u8 primary_key,
        bid: f64,
        ask: f64
    }
);
```

`partition_by: <name>: <unsigned type>`, and `partition_max_size: <width>` beside
it, which is required. It composes with everything else: indexes, queries and
config are untouched by it.

The size is a type rather than a count because it is an index width. `bool` is 2
rows, `u8` is 256, `u16` is 65,536, and `u32` or `u64` mean unbounded in practice
and generate a full table per partition. There is no `unbounded` keyword: the
widths run out of smallness, so `u64` is the escape.

A narrow width generates `<Name>DenseTable` as the partition payload: the primary
key *is* the row's position, so there is no primary index, no pages and no lock
map, and a lookup is a bounds check and a load. An empty dense partition costs
108 bytes against a full one's 28,404, which is the whole point of the key.

The width is a bound and not a reservation: the row vector grows to the highest
key used, so a `u16` partition holding three rows holds three slots.

## Versions and migration

```rust
mod v1 {
    worktable!(
        name: User,
        version: 1,
        persist: true,
        columns: { id: u64 primary_key autoincrement, name: String },
    );
}

mod v2 {
    worktable!(
        name: User,
        version: 2,
        persist: true,
        columns: { id: u64 primary_key autoincrement, name: String, email: String },
    );
}
```

`worktable_version!` declares a read-only view of an older layout, for opening
data written by a previous schema:

```rust
worktable_version!(
    name: UserV1,
    columns: {
        id: u64 primary_key autoincrement,
        name: String,
        email: String,
    },
    indexes: { name_idx: name },
);
```

## Config

```rust
config: {
    page_size: 16384,
    row_derives: Clone, Debug,
}
```

Tuning **values** live here. Anything that changes the generated *type* goes in
the prefix instead — which is the rule that puts `runtime:` beside `persist:`.

# Proposed, not implemented

Nothing below compiles yet. It is here for review before it is built.

The design follows `using <index>` as a *mechanism* — an enum in the DSL, a
codegen mapping to a concrete type, a trait the types satisfy — but deliberately
**does not reuse the `using` keyword**. `using` means index backend and only that.
Runtime selection uses `runtime`.

## The mapping is concurrency points, not tables

A table is the wrong unit, and so is a single query. A table contains several
concurrency points and they want opposite things:

| point | where | shape | measured |
|---|---|---|---|
| row-lock handoff | `src/lock/`, a writer parks and the releaser wakes it | a chain; the successor wants the lines just touched | `locality`: YCSB A +6.4%, F +2.0% vs tokio |
| in-place mutation | internal locking, no caller-visible lock | different contention entirely | not separately measured |
| persistence flush | `persistence/task.rs`, one long-lived queue drainer | never usefully suspends, just needs a thread | pool choice barely matters |
| vacuum sweep | `table/vacuum/manager.rs`, already paced | must not disturb the foreground | wants a budget, not a flavor |
| query fan-out | does not exist yet | independent chunks | 1.99x-2.84x on orderbook upsert/delete |

The `queries:` sections already group by concurrency point: everything in
`update:` goes through the same lock path, and `in_place:` is a different path by
design. So the annotation belongs on the section.

## Three positions, one keyword

| position | scope | cost |
|---|---|---|
| `runtime: nagoya(spread)` at the top level | the table's sync types and default pool | changes the generated type |
| `update runtime fast_local:` on a section | that concurrency point | compile time, free |
| `.runtime(wide)` on a builder | one call | runtime, opt-in |

## Named profiles

```rust
runtimes! {
    tokio_max:  tokio,
    fast_local: nagoya(locality),
    wide:       nagoya(spread),
}
```

## The whole thing together

```rust
worktable!(
    name: Orders,
    persist: true,
    runtime: nagoya,                                    // table default = nagoya(locality)
    columns: {
        id: u64 primary_key autoincrement using arctic, // `using` = index backend
        symbol: String,
        qty: u64,
    },
    indexes: {
        symbol_idx: symbol using congee,
    },
    queries: {
        update runtime fast_local: {                    // `runtime` = scheduler
            Fill(qty) by id,
            Cancel(qty) by symbol,
        },
        in_place runtime fast_local: {
            Bump(qty) by id,
        },
        delete runtime wide: {
            BySymbol() by symbol,
        },
    }
);
```

Omitting `runtime` anywhere falls back to the table default, and omitting the
table default gives `nagoya(locality)`.

## Call site

```rust
// point read: unchanged, no hop, no way to get it wrong
let row = table.select(pk);

// long scan: the builder already exists, `.runtime()` is one more link
let rows = table.select_all()
    .order_on(OrdersRowFields::Symbol, Order::Desc)
    .limit(10_000)
    .runtime(wide)
    .execute()?;
```

`.runtime()` lives only on the builder-returning selects, so it cannot be attached
to a point read. That is deliberate: **dispatching costs 21 ns to spawn
(`null-submit-cost`) plus ~2,250 ns to wake (`null-wake-latency`), against a
~400 ns point read.** The hop is larger than the operation.

| query | cost | hop as a share | verdict |
|---|---|---|---|
| point read | 400 ns | 560% | never |
| single update | 1.6 us | 140% | never |
| 16k-row scan | 6-17 ms | 0.02% | worth it |

## Why `runtime:` is top level and not in `config:`

| goes | what |
|---|---|
| top level | anything that changes the **generated type** |
| `config:` | tuning **values** |

`persist:` is top level because it changes the type. `page_size` is a number, so
it is in `config:`. `runtime:` selects the `RwLock`, `Notify` and `JoinHandle`
that `LockMap` and `PersistenceTask` are built from, so it goes beside `persist:`.
It also keeps the setting next to the columns and queries it governs.

PR #58 moved `columnar_slot_id` and `columnar_chunk_rows` into `config:`, which
was right for those: they are tuning values.

## The flavors, and what each measured

| flavor | for | vs tokio, YCSB at 8 threads |
|---|---|---|
| `locality` | wakes that are a chain | 50% update **+6.4%**, read-modify-write **+2.0%** |
| `spread` | wakes that are independent | 95% read / 5% update **+74.4%**, 95% read / 5% insert **+321.6%** |
| `throughput` | a firehose from outside the pool | the defaults before local wakes existed |

No setting wins both columns; four attempts to find one each reproduced a single
column exactly. `locality` is the default because 19% behind on one shape beats
40% behind on two.

## Two axes, not one

Research backends are **queue algorithms**, not runtimes. They swap st3's deque
and keep the facade above it.

| axis | what changes | cost | examples |
|---|---|---|---|
| runtime | sync types, spawn, timers, io | the `Runtime` trait, ~40 signatures | nagoya, tokio, smol |
| queue | the work-stealing algorithm only | one `Pool` impl | st3, BWoS, Chase-Lev |

Undecided: whether a queue choice is `nagoya(spread, bwos)` or a separate key.

## Parse-time rules

Taken from PR #58's review, which rejected inert declarations rather than
accepting them:

- a profile naming `tokio` cannot be referenced from a table whose `runtime:` is
  `nagoya`; the sync types are already fixed
- unimplemented backends must **fail to compile**, not be accepted and ignored
- `runtime` cannot appear before `name:` — the parser's fixed prefix is `name`,
  `version`, `persist`, `partition_by`, and everything after is free-order

## Open question the syntax does not settle

Whether a section annotation should select a **pool** or a **retry policy**.

Only the write path spins: the generated update code has a retry loop calling
`yield_now` with exponential backoff, and `yield_now` self-wakes, which is exactly
what `locality` optimises. The read path has no such loop. So "writes want
locality" may be about that loop rather than about pools, in which case the knob
is the backoff curve, which is inline and free.

The experiment: set `local_wakes: false` while changing the generated retry loop
from `yield_now` to `sleep`, and see whether the update workload's advantage
survives. Not yet run.

## Precedence: defining both is an error

| defined | result |
|---|---|
| section **and** call site | **compile error** |
| section only | the section's profile |
| call site only | the call's profile |
| neither | the table's `runtime:`, else `nagoya(locality)` |

Decided: an error rather than a silent override, so there is one answer to "which
runtime does this query use" and it is visible at the place you are reading.

**The cost of that choice**, recorded so it is not a surprise: adding a section
annotation becomes a breaking change for callers already using `.runtime()`. The
window is narrow today — `.runtime()` exists only on select builders, and selects
are not declared in `queries:`, so `update` / `delete` / `in_place` annotations
can never collide with it. It only bites if a `select` section annotation is added
later.

**Implement the error deliberately.** Omitting `.runtime()` from the generated
builder when the section pins one gives *"no method named `runtime` found for
struct `SelectQueryBuilder`"*, which points at the wrong thing. Generate the
method and make it unsatisfiable so the message can say what happened:

```rust
#[diagnostic::on_unimplemented(
    message = "`{Self}` already has a runtime pinned by the schema",
    label = "remove this `.runtime()`, or remove `runtime` from the `select` section",
)]
```

## The one error that is unconditional

A call site may only name a profile whose **backend matches the table's**. The
table-level `runtime:` selects types — the `RwLock`, `Notify` and `JoinHandle`
that `LockMap` and `PersistenceTask` are built from — so nothing downstream can
change it.

```rust
// table is `runtime: nagoya`
table.select_all().runtime(wide).execute()?;        // ok, wide is nagoya(spread)
table.select_all().runtime(tokio_max).execute()?;   // compile error, always
```

This is why `runtimes!` must give each profile a **backend marker type** rather
than a bare name: `.runtime()` takes `P: Profile<Backend = Self::Backend>`, so the
mismatch surfaces as a trait bound naming both backends. Retrofitting that later
is much more expensive than honouring it in the profile macro now.

**So: the flavor is selectable at the section or the call site, never both. The
backend is fixed at the table and only ever checked downstream.**

## No parameters, for now

`runtimes!` takes a backend and a flavor, nothing else. `.runtime()` takes a
profile name, nothing else. No worker counts, no tuning values, no
`.runtime(wide, 12, 100)`.

Decided. The reasons, so the decision can be revisited on evidence rather than
taste:

**Every distinct parameterisation is a distinct thread pool.** A profile is not a
value passed to an existing pool, it selects one. Free-form numbers at call sites
mean unbounded pool creation, and nobody reading the call site can see that. With
names only, every pool the process will ever create can be enumerated by reading
one macro.

**The knobs are counts, not durations.** Anything of the form "100ms stealing"
does not map onto this pool. `promote_every`'s own doc says it is "a count rather
than a clock because the pool has no clock it is willing to read on the hot path".
The real surface:

| knob | unit | default | where |
|---|---|---|---|
| workers | count | `available_parallelism` | `Pool::new` |
| `rounds_before_park` | empty rounds | 64 | `Tuning` |
| `backoff_spins` | spin-loop hints | 1024 | `Tuning` |
| `injector_batch` | jobs | 1 | `Tuning` |
| `promote_every` | jobs between heartbeats | 64 | `Tuning` |
| `local_wakes` | bool | true | `Tuning` |

**Positional numbers are unreadable**, and the DSL already settled on the named
form elsewhere: `columnar(chunk_rows(32_768), compression(none))`.

If parameters are wanted later, the shape that keeps the pool set finite is a
named profile carrying them, not a call-site tuple:

```rust
runtimes! {
    wide:    nagoya(spread),
    wide_12: nagoya(spread) { workers: 12, backoff_spins: 4096 },
}
```

That also needs a pool cache keyed by `(backend, workers, tuning)`, which does not
exist today: `nagoya::runtime::background()` is a single process-wide pool.

### Keeping parameters cheap to add later

Two things keep the door open, and both cost nothing now.

**Chain, do not widen.** When parameters arrive they go on as further builder
links, not as extra arguments:

```rust
.runtime(wide).workers(12)      // additive, existing calls unaffected
.runtime(wide, 12)              // arity change, breaks every existing call
```

Same reason `.limit()` and `.order_on()` are separate links.

**The API is the free part; the pool registry is not.** A parameterised profile
must resolve to a *cached* pool keyed by `(backend, workers, tuning)`. Today
`nagoya::runtime::background()` is a single process-wide pool with no such lookup,
so that registry is the real work — roughly a `OnceLock<HashMap>` and the logic to
start a pool on first use.

Build the profile as a struct with room to grow rather than a bare enum, so adding
fields later is a struct change and not a signature change.
