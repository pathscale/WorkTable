# What a partition is, in WorkTable and in six other systems

WorkTable's partitioning is Postgres-shaped: a partition is a complete table
with its own storage, its own index and its own locks. That is a real choice
with a real cost, not an implementation detail, and this page exists so the
choice can be read next to the alternatives.

## Read this before the table

**The seven systems below do not mean the same thing by "partition."** A single
ranked cost column would compare incomparable units and imply an equivalence
that is not there:

| system | what the word names |
|---|---|
| PostgreSQL | a table |
| Kafka | a unit of ordering and parallelism |
| ClickHouse | a logical group of physical *parts* |
| Cassandra | one partition key's rows; you expect billions of them |
| HBase | a shard, called a region |
| Snowflake | a storage block, created for you |
| **WorkTable** | **a complete generated table** |

A Cassandra partition and a Postgres partition are four orders of magnitude
apart in expected count. Comparing their per-partition costs without saying so
is how a table like this misleads.

## The comparison

| system | a partition is | per-partition cost | what dominates it |
|---|---|---|---|
| PostgreSQL | an ordinary table with its own relfilenode, its own child indexes and its own statistics | high | catalog, planner, per-session metadata, lock manager |
| Kafka | a directory, whose every *log segment* carries its own `.log`, `.index` and `.timeindex` | high | file descriptors, page cache, replica fetchers, metadata |
| **WorkTable today** | **a complete generated table** | **~28 KB, measured** | **fixed apparatus allocated at creation** |
| ClickHouse | a logical group; the physical unit is a *part*, a directory of column files plus a sparse primary index | medium, and small parts are merged away | open files, and a hard cap on active parts |
| HBase | a key range, called a region | medium to high | memstore memory per region per column family |
| Cassandra | a hash token on a shared ring; one key's rows live inside shared SSTables | near zero | bloom filters and index summaries, which scale with partition *count* |
| Snowflake | a 50 to 500 MB uncompressed columnar block, created automatically | not a comparable concept | n/a |

### PostgreSQL

A partitioned table "is a 'virtual' table having no storage of its own. Instead,
the storage belongs to *partitions*, which are otherwise-ordinary tables." An
index declared on the parent is virtual in the same way, so N partitions and M
indexes are N x M physical index relations.

**There is no documented fixed byte overhead per partition, and nothing here
invents one.** What the documentation does commit to is that the planner
"is generally able to handle partition hierarchies with up to a few thousand
partitions fairly well, provided that typical queries allow the query planner to
prune all but a small number of partitions", and that "each partition requires
its metadata to be loaded into the local memory of each session that touches
it" — so the memory cost is per session times per partition, not paid once.

Two traps worth knowing. Autovacuum does **not** analyze the partitioned parent,
only its children, so parent-level statistics need a manual `ANALYZE`
(PostgreSQL 18 revisits this, adding an `ONLY` option and changing the recursion
default). And the sharpest practical limit is the lock manager rather than disk:
every partition and partition index touched takes a relation lock, fast-path
slots were fixed at 16 per backend before PostgreSQL 18 and are sized from
`max_locks_per_transaction` after it, and overflow spills to the shared lock
table and shows up as `LWLock:LockManager` waits.

### Kafka

A partition is a directory named `<topic>-<partitionId>`, and the file cost is
**per segment inside it**, not per partition: each log segment carries its own
`.log`, `.index` and `.timeindex`, and each index pair is an mmap. Segments roll
at `log.segment.bytes`, one gigabyte by default.

**The partition-count numbers most often quoted are ZooKeeper-era and should be
labelled as such.** The familiar "limit partitions per broker to `100 * b * r`,
roughly 2,000 to 4,000 per broker" guidance is from a 2015 Confluent post, and
those bounds came from controller failover and unclean-failure availability
rather than steady-state cost.

KRaft changed this substantially: Confluent's current documentation cites a
benchmark cluster running **two million partitions**, "10 times the maximum
number of partitions for a cluster running ZooKeeper". Note what is *not*
available: neither Apache nor Confluent publishes a current numeric supported
maximum per broker or per cluster under KRaft, only that "Kafka's scalability
still primarily depends on adding nodes". KRaft reached general availability in
3.3, parity in 3.9, and ZooKeeper was removed in 4.0.

### ClickHouse

**A partition is not a part, and the two words are not interchangeable.**
`PARTITION BY` defines a *logical* partition; a *part* is the physical on-disk
unit, a directory of column `.bin` files, `.mrk` mark files and `primary.idx`.
One partition contains many parts. Every insert creates at least one part per
affected partition, and parts in different partitions are never merged together.

The primary index is genuinely sparse: one entry, a "mark", per granule of rows
rather than one per row, with `index_granularity` defaulting to 8,192 rows and
adaptive granularity via `index_granularity_bytes`, default 10 MB.

Background merges do fold small parts together, and the limits that enforce it
are the clearest statement of what a partition costs there:
`parts_to_delay_insert` at 1,000 and `parts_to_throw_insert` at 3,000 active
parts **per partition**, plus `max_parts_in_total` at 100,000 per table. The
partition-count guidance is explicit: "you shouldn't make overly granular
partitions (more than about a thousand partitions)", because of "an
unreasonably large number of files in the file system and open file
descriptors".

### Cassandra and HBase are not one row

Pairing them was an error. They partition differently and cost differently.

**Cassandra does not use key ranges.** It "partitions data over storage nodes
using a special form of hashing called consistent hashing": the partition key is
hashed by `Murmur3Partitioner` into a 64-bit token, and what maps to nodes is a
token range, a range of *hashes*. Order-preserving partitioning exists and is
strongly discouraged.

Its per-partition cost is genuinely low, with no file or directory per
partition, but it is not zero: bloom filters and index summaries scale with
partition **count**, and a billion partitions at the default 1% false-positive
rate costs roughly 1.2 GB of off-heap bloom filter memory. The governing limit
there is partition *size* rather than count, around 100 MB.

**HBase regions are key ranges, and they are not cheap.** The documentation puts
"20-200 regions per RegionServer" as the reasonable range, with the maximum
"mostly determined by memstore memory usage": each region has a memstore per
column family, flush sizes typically 128 to 256 MB, and exceeding the budget
"can cause undesirable consequences such as unresponsive server or compaction
storms". A worked example on a 16 GB server lands near 51 regions.

### Snowflake

Verified, and the reason to keep it in this table is a naming one. "Each
micro-partition contains between 50 MB and 500 MB of uncompressed data", and
"micro-partitioning is automatically performed on all Snowflake tables". They
are never declared; the knob a user does control is the clustering key.

**So do not use "micro-partition" in WorkTable's user-facing text for a 23-row
partition.** The word already means something automatic and enormous to everyone
who has met it. The generated type here is `<Name>DenseTable` for this reason.

## What the cost actually buys, stated carefully

An earlier version of this comparison claimed that isolation is what the
per-partition cost buys, and that "2,000 independent tables never contend, where
a Cassandra-style shared index under 10,000 writes/sec would". **Both halves of
that are wrong and neither should be repeated.**

The Postgres half is false: partitions share the buffer pool, the WAL and the
lock manager, every partition and partition index touched takes a relation lock,
and unpruned partition scans under concurrency are a documented way to lose
throughput. Many partitions can contend *more* than few, which is the opposite
of the claim.

The Cassandra half is unsupported. No source was found for a shared LSM index
contending at 10,000 writes per second; Cassandra's write path is a sequential
commitlog append plus a memtable insert with no read-before-write, and per-node
rates well above that are unremarkable. Its real contention modes are hot
partitions and compaction backpressure, neither of which is a function of
aggregate write rate.

What per-partition cost genuinely buys is **independent physical objects**:
detaching or dropping one as a near-metadata operation, per-partition indexes,
compression, retention and statistics, and partition pruning. That is a real
benefit and it is worth paying for. It is not "never contends".

For WorkTable specifically, the isolation is stronger than Postgres's because
there is no shared lock manager to contend on: a partition is an independent
generated table behind its own handle. That is a claim about this system and it
should be made about this system rather than by analogy.

## What WorkTable's own partition costs, measured

Not recalled. `tests/dense_partition_memory.rs` counts what the allocator was
asked for, 200 partitions of 23 rows, one declaration at two widths:

| shape | bytes per partition |
|---|---:|
| full table, empty | 28,404 |
| **dense, empty** | **108** |
| full table, 23 rows of an 88-byte row | 32,900 |
| **dense, same** | **3,180** |

The empty row is the one to read. The saving is fixed apparatus allocated at
partition creation, so it is about 28 KB per partition whatever the rows weigh:
roughly 56 MB at 2,000 symbols.

`partition_max_size: u8` is how a declaration asks for the second shape. See
`docs/small-tables.md` for where the 28 KB goes, and the user guide's section 9
for the grammar.

## Provenance

Every claim above about another system was checked against that system's
current documentation before this page was written, because the version of this
comparison it replaces was written from memory and got ClickHouse's central term
backwards, merged two systems that partition differently, and asserted a
contention figure that does not appear to exist.

Three things are marked unverified above rather than smoothed over: a
byte-level per-partition overhead for PostgreSQL, a current official numeric
partition maximum for Kafka under KRaft, and the 10,000 writes per second
Cassandra figure, which was deleted rather than softened.

Sources: the PostgreSQL manual on declarative partitioning and `pg_class`,
the PostgreSQL 18 release notes, the Apache Kafka log implementation
documentation, Confluent's 2015 partition-count post and its current KRaft
documentation, the ClickHouse MergeTree, custom-partitioning-key and
sparse-primary-index pages, the Cassandra Dynamo-architecture and bloom-filter
pages, the HBase region-and-capacity guide, and Snowflake's table-clustering
and micro-partitions page.
