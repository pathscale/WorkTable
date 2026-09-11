# A hash index backend: what it would be worth, and who could use it

Asked and answered on 2026-09-11. `perf-benchmarks/benchmarks/fx-index.rs`
measured what a hash-shaped `using` backend would buy. This is the other half:
which declarations we already have could take one.

**The answer is none of them, today.** That is a useful result rather than a
disappointing one, because the reasons are structural and each of them names
the thing that would have to change first.

## What it would be worth

On the `vec: true` shape, a million rows, against the `ArcticIndex` it holds
today:

| | gain |
|---|---:|
| build | **7.2x to 9.4x** |
| lookup | **3.9x to 7.7x** |
| delete | 17x to 21x (measured before ghosting; see below) |

Reserving is most of the build win: an unreserved hash map is only 1.8x to 2.5x,
so `with_capacity` on the index is worth a further 3.1x to 5.1x. That matters
because it is the one place pre-allocation has any headroom at all —
`arctic-prealloc.rs` put the ceiling on pooling Arctic's node allocation at
**0.92x**, below one.

The delete figure is stale in the useful direction: `vec: true` now ghosts a
delete and the 21-millisecond path it was measured against is gone. Do not quote
it.

## The survey

Every `worktable!` in the repositories that have real declarations. Counted
from the checkouts in `~/code`, not from memory.

| repository | declarations | persisted | in-memory | `vec: true` | range sites | ordered scans |
|---|---:|---:|---:|---:|---:|---:|
| `web3.trading-backend` | 24 | 10 | 14 | **0** | 0 | 4 |
| `agencyzero` | 19 | 19 | 0 | **0** | 1 | 0 |
| `pays.online-backend` | 24 | 23 | 1 | **0** | 0 | 0 |
| `nofilter.io-backend` | 16 | 9 | 7 | **0** | 0 | 0 |
| `api.support.cafe` | 11 | 10 | 1 | **0** | 2 | 0 |
| `auth.honey.id-backend` | 14 | 9 | 5 | **0** | 0 | 0 |
| `api.honey.id-backend` | 8 | 7 | 1 | **0** | 0 | 0 |
| **total** | **116** | **87** | **29** | **0** | **3** | **4** |

`wt-benchmarks` is excluded from the total: it has 41 invocations, it is a
measurement suite rather than an application, and 18 of the repository's range
call sites are in it.

## Three filters, and what each one removes

**1. It only fits `vec: true`, and nothing is `vec: true` yet.** `UniqueIndex`
requires `range_values` and `range_links`, which a hash map cannot answer at any
price, so a hash backend cannot be a fifth arm of the existing trait. The
generator for `vec: true` is the one that never calls them. Zero of the 116
declarations use it, because it shipped in 1.9 and nothing has adopted it.

**2. Persistence is a hard exclusion, and it removes 87 of 116.** Verified in
`codegen/src/persist_index/generator.rs`: `from_persisted` rebuilds each index
with `attach_node` / `attach_nodes` / `attach_multi_nodes` from B-tree nodes read
off disk. The on-disk form of an index *is* sorted pages. A hash map has no node
structure to attach and no page form to write, so `persist: true` and a hash
index cannot both be true without a second on-disk index format.

**3. A shared table needs a lock, and the lock is the whole gain.**
`wt-vs-rustc-structures.rs` measured seven readers against writers: a
`RwLock<FxHashMap>` keeps **22%, 16% and 10%** of its read throughput at one,
two and four writers, where `ArcticIndex` keeps **97%, 88% and 66%** and
overtakes at two writers. Every one of the 29 in-memory declarations is held as
`Arc<...WorkTable>` and shared.

That leaves the read-only case, where a locked hash map still wins 4.6x because
nothing ever takes the write side. It does not occur here either. Counting call
sites against `web3.trading-backend`'s seven S5 tables:

| table | read sites | write sites |
|---|---:|---:|
| `signal_table` | 0 | 1 |
| `event_table` | 6 | 2 |
| `position_table` | 1 | 7 |
| `order_table` | 9 | 5 |
| `fill_table` | 0 | 1 |
| `key_table` | 7 | 5 |

Every table is written. None is the build-once-read-forever shape.

## What a candidate would look like

So the filter, stated as something that can be checked against a declaration
rather than argued about:

1. `vec: true`, or any table with a single writer — no `Arc` sharing with a
   writer on the other end.
2. Not `persist: true`.
3. No `select_by_*_range`, no `order_on`, no ordered iteration.
4. More than 20 rows (below that a scan wins outright), and searched more than
   about 150 times after each build (below that the build never earns itself
   back).

Points 3 and 4 are already measured; see `docs/small-tables.md`.

## What changes the answer

`vec: true` got ranges and ghosted deletes on 2026-09-11, which cuts both ways
and is worth stating plainly.

It makes the shape **more** likely to be adopted, so candidates may appear where
there are none now: a `vec: true` table is now a credible replacement for an
in-memory paged table that was only paged because nothing else could range.

It also makes a hash backend **less** attractive on that shape specifically. A
hash backend would have to give the range API back up, so `using fxhash` would
become a per-backend capability question — a table that declares a range cannot
take it — which is exactly what `using` is for, and exactly the kind of
conditional surface that needs sign-off before anything is built.
