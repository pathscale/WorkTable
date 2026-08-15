# WorkTable in One Page

## The big idea

Most databases are built like universal tools: they can accept new schemas and many kinds of queries while the program is running. That flexibility costs time. Values may be boxed, queries interpreted, rows converted to and from JSON, and broad locks used even when only one field changes.

WorkTable takes the opposite approach. A Rust macro reads a table's columns, indexes, and allowed query shapes during compilation. It then generates a purpose-built in-memory table engine for that exact schema. The result aims to combine the readable, typed interface of an ORM with performance closer to carefully hand-written Rust.

## The performance edge

The paper's main comparison stores 10,000 typed rows either directly in WorkTable or as JSON documents in two fast embedded key-value stores. These are reported median times in milliseconds; lower is better.

| Operation | WorkTable | redb + JSON | LMDB + JSON | WorkTable advantage |
|---|---:|---:|---:|---:|
| Insert | **12.6 ms** | 173.8 ms | 21.5 ms | **13.8x vs redb; 1.7x vs LMDB** |
| Point read | **2.10 ms** | 3.60 ms | 3.80 ms | **1.7x-1.8x** |
| Query a field | **1.50 ms** | 2.30 ms | 1.80 ms | **1.2x-1.5x** |
| Update a field | **6.7 ms** | 182.2 ms | 25.6 ms | **27.2x vs redb; 3.8x vs LMDB** |

The main reason is avoided work. WorkTable stores typed archived rows directly, so it does not repeatedly turn a Rust row into JSON and parse it back. Generated code already knows the row layout, index path, and fields being changed. Same-size fields can often be updated in place.

Compile-time backend selection helps too. On the same WorkTable storage and workload, the specialized Congee and Arctic indexes delivered **1.55x-1.66x faster point reads** and about **1.18x faster inserts** than the general WorkTablesIndex backend. WorkTablesIndex still had a small advantage on range scans, showing that the best backend depends on the workload.

The primary measurements used a 64-vCPU Intel Linux server, with the ordering cross-checked on AMD and two ARM systems. These numbers demonstrate this benchmark, not every possible application.

## Benefits beyond speed

- **Readable, typed calls:** applications use generated Rust methods instead of SQL strings or hand-written scans.
- **Mistakes become compile errors:** wrong value types and undeclared indexed queries cannot be expressed.
- **No surprise full-table scan:** if an indexed access path was not declared, the method is not generated. Explicit scans are still possible.
- **More useful concurrency:** generated updates lock only the columns they change, so independent changes to one row can proceed together.
- **Less repeated plumbing:** one declaration generates rows, indexes, locks, query methods, and optional persistence support.
- **Real-world use:** the paper reports production use in trading, authentication, payments, support software, and desktop agent storage.

## The honest trade-off

WorkTable is not a general SQL database. It is best for fixed-schema, single-process workloads that need fast inserts, point lookups, indexed access, and updates. It does not currently provide multi-operation or cross-table transactions, snapshot scans, runtime schema changes, or crash-safe durability. Persistence targets warm restart: there is no write-ahead journal or `fsync` guarantee yet. Safe overlapping reads and updates require the versioned-row-publication mode; the faster default mode expects the application to prevent that overlap. Specializing every table also adds compile-time and binary-size cost that still needs fuller measurement.

## Bottom line

WorkTable's speed does not come from making a universal database slightly faster. It comes from removing generality the application does not need. When schemas and access paths are known at compile time, WorkTable can replace JSON conversion, runtime query decisions, and broad locking with generated, typed code. For the workloads it targets, the paper shows that this can provide ORM-like readability while beating common embedded tabular-over-KV designs by roughly **1.2x to 27x**, depending on the operation and baseline.

*Source: "WorkTable: ORM Readability at Hand-Rolled Speed via Compile-Time Engine Specialization," especially Tables 1 and 2 and Sections 2, 3, 5, and 7.*
