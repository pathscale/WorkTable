#set document(title: "Why WorkTables", author: "PathScale")
#set page(paper: "a4", margin: (x: 2.2cm, y: 2cm), numbering: "1")
#set text(font: ("Helvetica", "Arial"), size: 10pt, fill: rgb("#172833"))
#set par(leading: 0.65em)
#show heading: set block(above: 1.3em, below: 0.6em)
#show heading.where(level: 2): set text(size: 16pt)
#show raw.where(block: true): it => block(fill: rgb("#eff4f5"), inset: 10pt, radius: 3pt, width: 100%, breakable: false, text(size: 8pt, it))
#show link: set text(fill: rgb("#176a7a"))

#text(size: 10pt, weight: "bold", fill: rgb("#176a7a"))[PATHSCALE / WORKTABLES]
#v(0.5cm)
#text(size: 32pt, weight: "bold")[Declare the table.
Keep control of the machine.]
#v(0.3cm)
#text(size: 15pt)[Typed storage for the working data inside your Rust application.]
#v(0.4cm)

A map is a good beginning. Then the application needs another lookup, a range,
a batch update, a memory budget and a way to reopen its state. WorkTable brings
those concerns into a declaration and generates a typed Rust API around them.

Your data stays in process. Its indexes, storage shape and lifecycle remain
choices you can see in code and measure on your own workload.

== From a declaration to useful operations

```rust
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: Snapshot,
    vec: true,
    columns: { id: u64 primary_key using fxhash, quantity: u64, }
);

let mut table = SnapshotWorkTable::with_capacity(1024);
table.insert(SnapshotRow { id: 7, quantity: 42 }).unwrap();
assert_eq!(table.select(&7).unwrap().quantity, 42);
```

This is the compact, exclusively mutated Vec shape: borrowed reads and
synchronous writes. The paged shape offers shared access, async mutations,
secondary indexes and declared update/delete queries. Their distinct Rust
callsites preserve the difference in ownership and behavior.

== Physical design belongs in the API

Choose ordered indexes for ranges, or a hash index for Vec point lookups.
Use dense partitions when small bounded keys describe the data. Add columnar
replicas and clustered indexes when projection and clustered access are useful.
Tune page size and columnar chunk size against the workload.

These choices affect memory use, write cost and access patterns. A single
declaration connects the logical table to the structures that implement it,
without hiding every physical decision behind one universal container.

#pagebreak()
#text(size: 10pt, weight: "bold", fill: rgb("#176a7a"))[WHY WORKTABLES / MEASURED BEHAVIOR]
== A small choice can change the cost of a table

The suite measures the generated code, alongside hand-written controls. On
one million rows, choosing `using fxhash` together with `with_capacity(rows)`
changed these per-row costs in the local full-suite run:

#table(
  columns: (1.5fr, 1fr, 1fr), inset: 8pt,
  stroke: rgb("#d1dcdf"),
  table.header([*Generated Vec table*], [*Build / row*], [*Point lookup*]),
  [Arctic, grown on demand], [34.90 ns], [42.64 ns],
  [FxHash, capacity reserved], [6.99 ns], [10.97 ns],
  [Measured ratio], [*4.99× faster*], [*3.89× faster*],
)

This is a physical-design result: both arms use the real `worktable!` macro.
The build comparison includes reservation as well as the backend change.
The hash table gives up ordered range methods and uses exclusive mutation.
The result supports choosing the right shape for a read-oriented snapshot;
it does not imply that a hash index replaces the concurrent paged table.

The reserved hand-written hash-map control recorded 8.89 ns per lookup in
the same run. Keeping that control visible helps separate the cost of the
generated table from the cost of the underlying index.

== Lifecycle is part of performance

Building a table is only the beginning. Inspect live rows and accounted
storage, delete data, compact Vec slots or pace paged vacuum work around
foreground operations. For persisted tables, observe the disk footprint
as well as memory; memory reclamation does not imply file truncation.

Persistence is opt-in for the paged shape, with local disk and an S3-backed
tier. Its completion boundaries are explicit. A successful mutation is
accepted and queued; orderly `close().await` drains and joins the engine.
The current alpha does not promise transaction journaling or fsync durability.
That makes it a fit for application-owned working state whose recovery
contract is designed deliberately.

== Put the application back in charge

WorkTable is useful when the hard part is maintaining indexed, typed working
data close to computation: routing state, snapshots, simulation state or
application caches. The declaration removes repetitive table plumbing while
leaving the consequential choices inspectable.

Start with the #link("wt-user-guide.pdf")[WorkTable user guide]: declarations,
every storage shape, queries, callsites, runtimes, persistence and lifecycle
examples. It describes 1.9.0-alpha1; use the reviewed checkout until publication.

#v(0.35cm)
#text(size: 8pt, fill: rgb("#526873"))[
  *Measurement note.* Apple M4 Max, macOS arm64, 11 September 2026.
  `fx-index`: 1,000,000 rows; 100 lookups per timed burst over 2,000 bursts;
  mean of three rounds after one discarded round. Values are amortized
  per operation, not individual request latency. One machine and one local
  full-suite run; no external database comparison is implied.
  #link("https://github.com/pathscale/perf-benchmarks/blob/fix/two-ps-st3-in-one-graph/data/apple-m4-max-darwin-arm64/2026-09-11-210249-full.md")[Report and provenance].
  #link("https://github.com/pathscale/perf-benchmarks/blob/fix/two-ps-st3-in-one-graph/benchmarks/fx-index.rs")[Benchmark and controls].
]
