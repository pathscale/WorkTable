# Remote page stores and partial hydration

**Status:** accepted, 2026-09-12

**Scope:** DataBucket storage domains, WorkTable partial hydration, Upstash Redis,
Tigris object storage, a dual-write backend using both services, and measured
S3-compatible provider alternatives.

## Decision

DataBucket becomes the storage-facing API. It owns stable page identities, the
physical system catalog, mutation generations, page reads and writes, and the
backend contract. WorkTable remains the typed table and query layer above it.

WorkTable will no longer require every row page to be resident. A spillable
table starts fully resident and uses the same in-memory path while it remains
below its configured memory high-water mark. After it crosses that boundary,
it evicts eligible pages and faults them back from a local file or a configured
remote page store when a query needs them. Fully resident
tables keep their current synchronous API and hot path. Spillable tables use a
distinct generated wrapper with asynchronous, fallible query and mutation
callsites. This requires no DSL grammar change.

The three remote configurations are:

| Configuration | Primary purpose | Commit authority | Read shape |
|---|---|---|---|
| Upstash | Optional batched page cache and metadata service | Upstash generation head | Direct page keys, batched with `MGET` |
| Tigris | Default durable capacity and scans | Conditional Tigris generation head | Range reads from immutable page segments |
| Hybrid | Optional Upstash serving tier plus a Tigris durable copy | Upstash live head plus a Tigris commit marker | Upstash first for point reads, Tigris for scans and repair |

Hybrid means every acknowledged generation is written to both services. It is
not a cache with an optional backup.

The 2026-09-12 provider gate changed the implementation priority. Upstash's
temporary Redis service had a roughly 216 ms request floor from Fly Singapore
and did not scale independent page requests with concurrency. It is not on the
first durable write path, and the hybrid backend is deferred until a paid,
region-selected Upstash deployment passes the same gate. Tigris, Bunny Storage
and Cloudflare R2 all passed exact-read and conditional-head tests. Tigris and
Bunny passed the complete performance gate. Tigris is the first backend
because it had the stronger sustained write shape. Bunny is supported by the
same S3 adapter as a read-strong alternative. R2 remains adapter-compatible
but is excluded from the first production path by its range and segment
results.

## Current boundary

The current persisted WorkTable is restored completely into memory. Primary
and secondary indexes contain DataBucket `Link` values, and those links are
resolved against an owning in-memory page list. This gives point reads their
current inexpensive synchronous path, but it also makes available memory a
hard table-size limit.

The database-wide S3 engine runs above the local disk engine. After a persisted
batch it walks each table file on the private persistence runtime, hashes data
pages and stable 16 KiB index chunks, and submits only changes to one shared
storage domain. DataBucket stages immutable segments and WorkTable prepares one
generated catalog checkpoint for the database before a conditional head update.
The stateful adapter fixture measures 33,016 uploaded bytes for an isolated page
mutation with a small catalog and 49,544 bytes after the catalog grows beyond one
page. This removes the 4 MiB network floor. The full local scan remains a local
CPU and disk cost until exact dirty-page reporting is connected.

DataBucket already knows the affected `Space`, `PageId`, physical stride and
row extent at `persist_page`, `persist_pages_batch`, and `update_at`. It should
report mutations at that point so the S3 engine can skip the scan. A raw
`AsyncWrite` wrapper cannot recover the same meaning reliably from byte offsets.

## Dependency direction

The runtime relationship is deliberately two-way while the Cargo graph stays
one-way:

```text
application
   |-- generated WorkTable API
   |      `-- data_bucket storage-domain API
   `-- data_bucket API directly

worktable  --depends on--> data_bucket
    |                       |
    `-- generated catalog -'  DataBucket owns its write permit and commit order
```

DataBucket defines the bounded catalog records, generation transaction and a
`SystemCatalog` provider interface. WorkTable implements that interface with a
real generated `vec: true` WorkTable. DataBucket receives a private write
permit and publishes the prepared table only after the page objects, catalog
checkpoint and conditional generation head are durable. Applications receive
read-only typed views over the same generated table.

This does not create a Cargo cycle. WorkTable depends on DataBucket's protocol;
DataBucket never names the WorkTable crate. At runtime, WorkTable supplies the
catalog implementation that DataBucket owns and updates. The hosted S3 adapter
is an optional DataBucket module behind `std` and `s3-support`; the storage
domain records and catalog interface remain `no_std` plus `alloc`.

## Storage domain and bootstrap

A storage domain represents one database persistence root. It contains all
persisted table spaces plus reserved system spaces. A WorkTable generated type
registers its spaces with the domain when it opens.

Every backend has one deterministic bootstrap location:

```text
bootstrap head
      |
      v
generation manifest
      |
      +--> catalog root and catalog segments
      +--> table data pages or page segments
      +--> primary-index pages
      `--> secondary-index pages
```

The bootstrap record is intentionally small. It contains the storage-domain
identifier, format version, current generation, parent generation, manifest
identity, manifest checksum and writer epoch. It does not contain the full
catalog.

The catalog checkpoint cannot require the catalog to locate itself. The small
bootstrap head therefore names that immutable checkpoint directly. This is the
only raw bootstrap record. User data and index pages are located through rows
in the generated catalog.

The DataBucket v3 data-page format remains the unit validated after a fetch.
The new catalog has its own format version. It should not add fields to the
archived `SpaceInfoPage` shape merely to hold statistics, because that would
unnecessarily change the v3 page layout. This design should land before the v3
storage-domain contract is declared stable.

## Physical system catalog

The authoritative catalog is database-wide. It has one table row per logical
table, one page row per current logical page, one index row per logical index,
and replication rows when the hybrid backend is active.

Conceptually, its stable records are:

```rust
struct SystemTableRow {
    table_id: SpaceId,
    name: String,
    schema_version: u32,
    row_count: u64,
    live_row_bytes: u64,
    allocated_data_pages: u64,
    live_data_pages: u64,
    primary_index_entries: u64,
    secondary_index_entries: u64,
    applied_generation: Generation,
    durable_generation: Generation,
}

struct SystemPageRow {
    table_id: SpaceId,
    space_id: SpaceId,
    page_id: PageId,
    page_kind: PageKind,
    generation: Generation,
    object: ObjectId,
    object_offset: u64,
    encoded_length: u32,
    decoded_length: u32,
    checksum: Checksum,
    live_rows: u32,
    live_bytes: u32,
}

struct SystemReplicationRow {
    generation: Generation,
    upstash: ReplicaState,
    tigris: ReplicaState,
    last_error: Option<ReplicationErrorCode>,
}
```

These are logical shapes. Their persisted representation must use bounded
fields and DataBucket-owned types suitable for `no_std` plus `alloc`.
Human-readable error text belongs in process diagnostics, not the durable
catalog.

WorkTable exposes generated read-only views such as
`system_tables()`, `system_pages()` and `system_replication()`. The user can
filter and inspect them, but cannot insert, update, delete, vacuum, or define
indexes on them. Generation is automatic and does not add schema grammar.

### Maintained values

Exact values that would otherwise require loading or scanning all pages are
updated as part of each mutation generation:

- live row count;
- live archived-row bytes;
- allocated and live page counts;
- entry count for every primary and secondary index;
- current applied and durable generations;
- tombstone or ghost count when the table representation has that state; and
- per-backend replication state.

`count()` and `row_count()` read the maintained row count in O(1). They do not
derive it from the number of resident rows or walk the primary index. The
in-process value advances when a mutation is published. The durable value
advances only when that generation commits. Both generations are observable
so an operator can distinguish live state from remotely recoverable state.

A batch computes one aggregate delta and publishes it once. Failed unique
inserts, rolled-back index operations and abandoned generations do not change
the committed values. Recovery can verify or rebuild the aggregates offline
from v3 row directories and persisted indexes, but ordinary open and query
paths trust the checksummed committed catalog.

Values such as column minimum and maximum should not be included initially.
They are cheap on insert but can require an unbounded search when the current
extreme is deleted. A maintained statistic belongs here only when every
mutation can update it with bounded work or it is explicitly approximate.

Process-local cache statistics are exposed through runtime metrics rather than
persisted catalog rows. Cache hits, misses and current resident bytes are not
database facts.

## DataBucket generation contract

DataBucket collects physical changes into a generation before an adapter sees
them:

```rust
struct PageMutation {
    address: PageAddress,
    kind: MutationKind,
    image: PageImage,
    checksum: Checksum,
}

struct GenerationPlan {
    id: Generation,
    parent: Generation,
    writer_epoch: WriterEpoch,
    pages: Vec<PageMutation>,
    catalog_delta: CatalogDelta,
}
```

WorkTable opens a DataBucket generation before applying a logical operation
and passes that generation context through every data, primary-index,
secondary-index and catalog write. The context owns the changed page images
until `finish()` produces the plan:

```rust
let mut generation = domain.begin_generation(expected_parent)?;
data_space.persist_pages(&mut generation, data_pages).await?;
primary_space.persist_pages(&mut generation, primary_pages).await?;
secondary_spaces.persist_pages(&mut generation, secondary_pages).await?;
generation.apply_semantic_delta(index_delta)?;
let plan = generation.finish()?;
```

The real signatures may differ, but generation membership cannot be inferred
later from unrelated file writes. Existing low-level DataBucket callers may
use an explicit one-operation generation or a non-durable sink. WorkTable is
responsible for grouping all physical parts of one logical mutation.

DataBucket derives row-count, live-row-byte and page-count deltas by comparing
the validated old and new page directories. This keeps those facts correct for
direct DataBucket consumers as well as WorkTable. WorkTable supplies semantic
index-entry deltas because DataBucket does not understand every index
operation. DataBucket records those deltas only with the page generation they
describe.

`PageAddress` contains the storage domain, table, space and page identifiers.
It is stable across cache eviction. A physical object identity is assigned by
the backend and stored in the resulting catalog snapshot.

The adapter interface needs these operations:

```rust
trait PageStore {
    fn load_head(&self, domain: StorageDomainId) -> Result<Option<Head>, StoreError>;
    fn load_catalog(&self, head: &Head) -> Result<Vec<u8>, StoreError>;
    fn read_page(&self, page: &PageRef) -> Result<PageImage, StoreError>;
    fn read_pages(&self, pages: &[PageRef]) -> Result<Vec<PageImage>, StoreError>;
    fn stage(&self, plan: &GenerationPlan) -> Result<StagedGeneration, StoreError>;
    fn stage_catalog(&self, staged: &mut StagedGeneration, checkpoint: &[u8])
        -> Result<(), StoreError>;
    fn commit(&self, staged: StagedGeneration) -> Result<CommittedGeneration, StoreError>;
}
```

The exact Rust shape may use associated futures to preserve `no_std` and avoid
an `async_trait` allocation. The semantic split between `stage` and `commit`
is required.

Staging writes immutable page or segment objects and an immutable catalog
snapshot. Commit moves the small bootstrap head from the expected parent to
the new generation. A failed or repeated stage is idempotent. A commit with a
different current parent returns a conflict instead of applying last-writer
wins.

The first implementation supports one active writer for a storage domain.
The writer epoch makes stale processes detectable. Multi-writer coordination
is a separate protocol and must not be implied by an atomic object PUT or a
Redis transaction.

## Partial hydration

### Residency model

The unit of data residency is a complete validated DataBucket page, not an
individual row. A page frame moves through these states:

```text
Absent
  | fault
  v
Loading --> CleanResident --> Evicting --> Absent
                  |
                  | mutation
                  v
             DirtyResident --> Flushing --> CleanResident
```

Only one load may be in flight for a page. Concurrent faults share its result.
A query or mutation pins the page frame while it decodes or changes a row.
Pinned pages cannot be evicted. Dirty pages cannot be discarded until their
generation is committed or retained in a durable local write-ahead record.

The cache key includes logical page identity and the committed object checksum
or generation. This prevents a cached old page from satisfying a newer
catalog reference. Every fetched page is checked using DataBucket's v3 header,
page identity, row directory, bounds and checksum before publication.

The initial cache policy should be a segmented LRU or CLOCK variant with:

- an explicit byte budget rather than a page-count budget;
- separate data-page and index-page budgets;
- high and low watermarks so eviction is batched;
- pin and dirty-state awareness;
- negative caching only for catalog-proven absence; and
- bounded metadata per non-resident page.

The cache manager must account for page frames, decoded scratch buffers and
in-flight fetches. A range query cannot evade the budget by issuing thousands
of reads concurrently. Per-query prefetch concurrency and bytes are bounded.

### Spill mode

Spill is a residency transition, not a different persisted table format. A
spillable table has three runtime conditions:

```text
Resident:  every current page is in memory
Spilling:  resident bytes crossed the high watermark; eviction is active
Spilled:   at least one current page is absent and must be faulted on demand
```

Most tables remain `Resident` for their complete lifetime. They pay the
spillable wrapper's budget accounting and one predictable resident-page check,
but perform no storage read and run no eviction work. The ordinary resident
table type pays neither cost.

`SpillConfig` contains at least a soft byte budget, a lower target watermark,
a hard byte ceiling, data/index budget shares, maximum in-flight fetch bytes
and a backing `PageStore`. Crossing the soft high watermark schedules eviction
until resident bytes fall below the lower watermark. Hysteresis prevents a
table from oscillating around one exact byte value.

The hard ceiling is a correctness boundary. If pinned, dirty and in-flight
pages leave no eligible victim, an allocating query or mutation waits for
flush/eviction progress or returns a typed budget error according to its
deadline. It does not exceed the configured ceiling indefinitely or discard a
dirty page. Allocation failure is not used as the normal signal to start
spilling; eviction begins before that point.

Clean pages already named by the committed catalog can be dropped immediately.
A dirty page must first become part of a staged generation or a synchronously
durable local WAL record. Newly inserted and recently faulted pages enter the
hot segment. Sequential scans receive weak admission so one scan does not
replace the repeatedly accessed working set.

Spill works in both directions. A fault makes a page resident again, and a
small table can return to having every page resident after old rows are
deleted. The spill-capable Rust type does not change back into the synchronous
resident type because it may spill again on its next mutation.

Opening an existing store follows the same budget. If all current pages fit,
the table may hydrate completely and report `Resident`. If they do not, open
loads bootstrap metadata, catalog roots and the configured index working set,
then leaves remaining pages cold. It does not first load the entire table only
to evict it.

Runtime spill condition, cache hits and resident bytes are process facts and
are not persisted as authoritative table statistics. The system interface may
join them with durable catalog rows for observation. Exact row count, live
bytes, logical page count and index-entry counts remain maintained catalog
values, so they are correct even when almost every page is cold.

### Index residency

Partial hydration has two implementation stages, both covered by the target
design:

1. Keep primary and secondary indexes resident while data pages use the
   bounded cache. This removes the dominant row-byte requirement and provides
   the first useful release boundary.
2. Keep index roots and selected upper nodes resident, then fault lower WTI,
   ART and table-of-contents pages through a bounded index cache. This removes
   the remaining requirement that every index entry fit in memory.

Stage one is not the final claim that arbitrary tables fit in bounded memory.
Documentation must say that indexes still need to fit until stage two lands.

The pageable index interface cannot expose raw pointers into evictable nodes.
It resolves an equality or range lookup into stable DataBucket links while
holding node pins. The result owns its links before pins are released. Existing
fully resident WTI, ART, Arctic and Congee implementations keep their current
interfaces.

### Query execution

A spillable query plans page access before fetching rows:

1. Resolve the primary or secondary index to stable row links.
2. Group links by `(space_id, page_id)` and deduplicate page requests.
3. Visit resident pages immediately.
4. Fetch missing pages in bounded batches.
5. Validate and publish each fetched page once.
6. Decode all requested rows from that page while it is pinned.
7. Apply filters, ordering, offset and limit according to the existing query
   contract.

Point lookup faults at most the required index path and one data page. A range
lookup prefetches upcoming pages within a small byte window. A table scan uses
the catalog's ordered live-page list and streams pages through the cache. It
does not first create one future or buffer per page.

Ordering and early termination matter. When an index already supplies the
requested order, `limit` should stop hydration after enough matching rows are
produced. A sort on an unrelated field may require reading every candidate.
If its result cannot fit the query memory budget, the executor needs an
external merge path backed by temporary storage. Partial hydration alone does
not make an unbounded sort bounded.

Current WorkTable queries are not snapshot-isolated transactions. Partial
hydration preserves the existing concurrency contract. It must still avoid
combining object identities from different committed catalog generations.
Resident dirty pages take precedence over their prior committed backing page.

### Mutations

Updating or deleting a cold row first faults and pins its page exclusively.
The mutation updates the row page, all affected indexes and the aggregate
delta under one WorkTable operation. A relocation produces the new page image,
the old page image, every changed index page and one catalog delta in the same
generation.

An insert may use a resident page with free space or allocate a new logical
page. The free-space summary needed to choose that page is maintained in the
catalog. Choosing an insertion target must not scan or hydrate every page.

Vacuum follows the same rule. Its candidate summaries are catalog metadata.
It hydrates only selected source and destination pages, emits the complete set
of relocated links and page changes, then updates counts once. Vacuum may not
publish a reclaimed page before every index relocation and catalog change in
its generation is staged.

### Generated API

The resident generated type remains source-compatible:

```rust
let table = UserWorkTable::load(engine).await?;
let row = table.select(id);                 // Option<UserRow>
let rows = table.select_by_tenant(tenant).execute()?;
```

Every persisted declaration also generates a spillable wrapper without new
DSL:

```rust
let table = UserWorkTable::load_spillable(
    engine,
    SpillConfig::memory_limit(256 * 1024 * 1024),
).await?;

let row = table.select(id).await?;          // Result<Option<UserRow>, _>
let rows = table
    .select_by_tenant(tenant)
    .limit(100)
    .execute()
    .await?;
```

The concrete returned type is `UserSpillableWorkTable` unless code-generation
constraints require an opaque equivalent. The important constraint is that it
is a different Rust type. Calling a synchronous `select() -> Option<Row>` on a
table that may spill later would otherwise have only three bad choices: block
unexpectedly, report a storage failure as absence, or panic. The spillable
callsite is asynchronous from construction, but a resident hit completes
without scheduling storage I/O.

The existing `execute_async()` query option selects a runtime for CPU work. It
does not currently mean storage hydration and must not be repurposed silently.
The spillable builder's `execute().await` performs both asynchronous page
access and the selected CPU execution policy.

Spillable updates and deletes are asynchronous and fallible because they may
fault pages. The resident methods remain unchanged. This is a callsite
extension, not grammar.

## Upstash backend

Upstash stores complete encoded DataBucket pages as immutable values. The
default 16 KiB page is well below current record and request limits. Keys use
one Redis hash tag per storage domain so generation-head coordination and its
metadata share a locking domain.

This remains a defined adapter path, but it did not pass the first provider
gate. From a Fly Singapore Machine, SET and GET medians were both about 216 ms
and a one-page write plus Lua head compare-and-set was about 444 ms. Sending
128 pages in one MSET reached 213 page writes/s, but the generation still
needed a second request and completed only 1.67 times/s. The command API also
requires base64 for binary pages carried in JSON. An implementation must batch
behind the local WAL; it must not synchronously call Upstash for every row
mutation.

One possible key layout is:

```text
wt:{domain}:head
wt:{domain}:generation:<generation>:manifest:<part>
wt:{domain}:page:<content-hash>
wt:{domain}:catalog:<content-hash>
wt:{domain}:writer
```

Page values are content addressed. Staging uses `SET ... NX`; an existing key
is accepted only after its length and checksum match. The immutable generation
manifest maps logical page addresses to page hashes. Large manifests are
segmented so no transaction or request approaches the service request limit.

Cold point reads use `GET`. Queries group page hashes and use `MGET` or a REST
pipeline within a configured byte ceiling. Pipelines reduce round trips but
are not a commit primitive. Upstash's `/multi-exec` transaction endpoint can
atomically update bounded metadata. Because REST `WATCH` is unavailable, the
head compare-and-set uses a small Lua script or an equivalent supported atomic
conditional operation.

The script verifies the expected parent generation and writer epoch, then
publishes the new head. The head names the immutable catalog snapshot that
already contains the exact aggregate values, so this atomic operation remains
small. Retrying it with the same generation is idempotent.

Upstash configuration used as durable storage must not enable Redis eviction
or attach TTLs to WorkTable keys. Quota or command-limit failures are storage
errors and cannot be treated as cache misses. Command count and transferred
bytes are first-class backend metrics because they determine both latency and
cost.

Garbage collection retains every object reachable from the current head and
the configured recovery window. Unreachable staged generations are deleted
only after their writer lease expires and no retained manifest names them.

### Upstash transport and access security

A Fly Machine reaches the normal Upstash endpoint over the public network.
Fly's private 6PN does not extend to Upstash. The connection therefore relies
on all of these boundaries:

1. TLS with normal hostname and certificate validation protects page contents
   and credentials in transit.
2. A dedicated Upstash ACL user grants only the commands and key prefix needed
   by one WorkTable storage domain. The application must not use the default
   full-database token when an ACL token can express the smaller authority.
3. The ACL token is stored as a Fly app secret and sent in the HTTP
   `Authorization` header. It is never placed in a URL, image, `fly.toml`, log,
   trace field or error message.
4. A paid Upstash database enables an IPv4 allowlist containing only the
   app-scoped static egress IPv4 addresses allocated to the Fly app in every
   region where it runs.

Fly's default outbound addresses are not stable enough for an allowlist.
App-scoped egress addresses survive Machine recreation, but they are regional,
so every deployed region must be allocated and allowlisted. Upstash currently
documents IPv4-only allowlisting. Deployment validation must prove that the
client actually exits through an allowed IPv4 address before the public token
path is enabled.

The minimum writer ACL is expected to include page and manifest reads, bounded
page creation, the generation-head script and explicitly invoked garbage
collection. Its key pattern is limited to `wt:{domain}:*`. Administrative
commands, keyspace-wide scans, configuration changes, subscription commands
and unrelated key prefixes are denied. A read-only process receives a separate
read-only ACL token. Exact commands are fixed after the adapter prototype and
tested by proving required operations pass and forbidden operations fail.

Upstash advertises VPC peering and AWS PrivateLink, but a normal Fly Machine is
not inside that AWS VPC or PrivateLink endpoint. Using either would require a
separately operated private gateway or tunnel and is not the default design.
The standard production path is TLS plus least-privilege ACL plus static-egress
IP allowlisting.

Fly secrets protect the token at configuration and deployment time, but the
running application receives it and a person able to deploy arbitrary code or
obtain root access to the Machine can read it. Workloads that should not share
that authority must run as separate Fly apps with separate Upstash ACL users.
Rotation replaces the ACL token in Fly secrets, rolls Machines, verifies the
new credential, and then revokes the old credential.

The operational flow is:

1. An administrator creates the restricted ACL user in Upstash.
2. Upstash's `ACL RESTTOKEN <username> <password>` command issues the REST
   token carrying that user's permissions.
3. The operator imports the token into the target Fly app's secret vault. To
   keep the literal token out of shell history, it can arrive through a local
   environment variable and stdin:

   ```sh
   printf 'UPSTASH_REDIS_REST_TOKEN=%s\n' "$UPSTASH_TOKEN" |
       fly secrets import --app "$FLY_APP"
   ```

4. Fly restarts or updates the app's Machines and injects
   `UPSTASH_REDIS_REST_TOKEN` into their runtime environment at boot.
5. The WorkTable Upstash adapter reads the variable once at startup, wraps it
   in a redacted secret type, and configures the HTTP client to send:

   ```text
   Authorization: Bearer <UPSTASH_REDIS_REST_TOKEN>
   ```

6. The adapter never implements `Debug` or tracing output that reveals the
   header, token, signed request, or complete client configuration.

The endpoint URL is not an authentication credential and may be ordinary Fly
configuration. Keeping it beside the token as a secret is also acceptable.
The Upstash token never crosses the application's public API and is unrelated
to an end-user Honey login token. End-user authorization terminates at the
application; the application uses its own storage credential to reach
Upstash.

Fly app secrets are normally available to every Machine in that app. If only a
storage worker should have this authority, put that worker in a separate Fly
app with its own secret and expose a narrow service over Fly's private 6PN.
Changing Unix environment variables to a file does not protect the token from
root or arbitrary deployed code in the same Machine.

Upstash's service-side encryption at rest is plan dependent. WorkTable pages
are opaque Redis values, so an optional client-side authenticated-encryption
layer can protect sensitive page and catalog payloads without losing Redis
query features that this backend does not use. Encryption keys remain in a
separate Fly secret. Object identifiers and checksums must be designed so a
malicious substitution, replay or cross-domain page copy fails authentication.

Relevant provider constraints are documented at:

- <https://upstash.com/docs/redis/features/security>
- <https://upstash.com/docs/redis/howto/ipallowlist>
- <https://upstash.com/docs/redis/features/restapi>
- <https://fly.io/docs/networking/egress-ips/>
- <https://fly.io/docs/apps/secrets/>

## Tigris backend

Tigris stores immutable page segments rather than fixed 4 MiB slices of local
files. A segment is built directly from changed DataBucket page images. It may
contain one page when a flush must happen immediately or many pages when the
background writer coalesces mutations. A target segment size is a batching
goal, never a minimum write size.

One possible object layout is:

```text
<domain>/head
<domain>/generations/<generation>/manifest
<domain>/generations/<generation>/catalog/<part>
<domain>/segments/<content-hash>
<domain>/commits/<generation>
```

The page catalog records the segment hash, byte offset, encoded length and
page checksum. A cold point lookup issues a byte-range GET for the page. A
scan coalesces adjacent requested pages from the same segment. If measurement
shows that small range GETs are inefficient, the cache may fetch the complete
segment, but it still publishes pages individually and charges the fetched
bytes against its budget.

The initial target is 4 MiB of encoded pages per segment, with a 256 KiB read
window for cold faults. The segment target is not a correctness boundary. An
idle or pressured writer may flush a smaller segment, and adjacent requested
pages may expand a read window within the query budget.

The background writer may compress a segment when the codec allows bounded
independent page decoding. Compression metadata is stored per page or per
small frame so reading one page does not require expanding a large segment.
Already compressed archived values should be detected by measurement, not
assumed.

Commit uploads all segments, catalog parts and the immutable generation
manifest before updating `head`. The head update is conditional on the
expected parent and writer epoch. The Rust QA driver verified conditional
creation and replacement against Tigris: stale `If-None-Match` and `If-Match`
requests were rejected with HTTP 412, while the current ETag replacement
succeeded. The first implementation still supports one writer guarded by a
renewable lease; conditional publication makes stale writers fail instead of
silently replacing the head.

This removes the current full-file scan and 4 MiB mutation floor. A single
changed page stages roughly one page image plus manifest and catalog metadata.
Batching can improve request efficiency without increasing the correctness
unit.

Garbage collection is manifest based. It computes reachability across every
retained generation and active reader lease before deleting immutable
segments. It never deletes an object merely because the current generation no
longer references it.

### S3-compatible provider selection

The same Rust executable ran from Fly Singapore against Tigris, Bunny Storage
and an R2 bucket with the APAC placement hint. Every page and range was checked
before it counted as a result.

| Measurement | Tigris | Bunny Singapore | R2 APAC hint |
|---|---:|---:|---:|
| 16 KiB PUT p50 | 34.93 ms | 44.57 ms | 170.83 ms |
| 16 KiB GET p50 | 18.22 ms | 4.93 ms | 50.17 ms |
| HEAD p50 | 4.70 ms | 4.35 ms | 38.82 ms |
| 256 KiB range GET p50 | 24.94 ms | 5.14 ms | 49.73 ms |
| 16 KiB writes/s at concurrency 16 | 63.28 | 48.11 | 68.14 |
| 4 MiB PUT | 303.00 Mbit/s | 180.33 Mbit/s | 90.90 Mbit/s |
| 4 MiB GET | 455.59 Mbit/s | 653.57 Mbit/s | 202.37 Mbit/s |

Bunny significantly outperformed Tigris for colocated reads. Its very high
hot-key concurrent read result is treated as cache-assisted. The lower
concurrency range median is the planning value, but that measurement also
reused one object and is not a cold-store result. The adapter-level gate must
add unique-object cold faults. Tigris had stronger sustained page and segment
writes. This selects Tigris as the first durable backend while preserving
Bunny as a supported alternative through the same S3 contract.

Bunny replication is not part of the measured or selected protocol. The tested
zone had Singapore as its primary and no replication regions. If a deployment
later enables Bunny geo-replication, the authoritative conditional head must
still be read and written at the primary; asynchronously replicated copies
cannot coordinate writers.

Cloudflare R2 returned the expected `200/412/412/200` conditional-write
sequence and all 7,888 verified page reads were exact. Its performance misses
the first backend gate: the 256 KiB range median is 49.73 ms, a 4 MiB PUT takes
369.15 ms on average, and that PUT sustains 90.90 Mbit/s. Its concurrent small
writes scale, but its application-facing request and segment shapes are weaker
than Tigris and Bunny from this Fly Singapore client. R2 remains a compatible
configuration of the S3 adapter rather than the first production default.
This provider choice does not alter the durable catalog or WorkTable grammar.

Cloudflare Pipelines addresses a different boundary. It can durably buffer
HTTP ingestion and deliver records exactly once into an R2 sink. It does not
provide page-key reads, range hydration or conditional generation-head
publication. The current 5 MB/s per-stream ingestion limit and minimum
10-second R2 roll interval also make it unsuitable as the interactive page
store. A later adapter may measure it as an asynchronous mutation or WAL
export channel, with recovery consuming the materialized R2 records. It does
not replace the `PageStore` contract or repair R2's measured fault latency.

## Hybrid dual-write backend

The hybrid backend uses the same generation identifier, logical page images,
checksums and catalog contents in both services. Upstash is the live commit
coordinator and preferred point-read source. Tigris is the durable capacity
copy, scan source and repair source.

No transaction can atomically commit Redis and S3 together. The backend uses
an idempotent state machine instead of claiming cross-service atomicity:

1. Allocate generation `G` with expected parent `P` and a unique writer epoch.
2. Stage every changed page and catalog part in Upstash.
3. Stage every changed page segment and catalog part in Tigris.
4. Write the immutable generation manifest to both services.
5. Record `G` as prepared in Upstash.
6. Atomically compare `P` and the writer epoch, then move the Upstash live head
   to `G` and mark it committed.
7. Write the immutable Tigris commit marker for `G`, then update its advisory
   head.
8. Mark the Upstash replication row complete and acknowledge the generation.

Every step is safe to retry using `G`. A crash before step 6 leaves only
unreachable staged objects. A crash after step 6 resumes steps 7 and 8. It
does not roll the live database back. An ambiguous response is resolved by
reading both commit records and their checksums.

The default policy is `BothRequired`: a mutation generation is remotely
acknowledged only after both services contain it and the Tigris commit marker
exists. An optional degraded policy may keep serving when one backend is down,
but it must report a degraded generation and cannot claim dual durability.
The policy is an engine configuration, not schema grammar.

Under `BothRequired`, head commits remain ordered and a later generation may
be staged but cannot advance the live head until its parent has completed
steps 7 and 8. Degraded operation uses the same linear generation chain and
records the missing replica work durably before accepting a child.

When Upstash is available, reads pin its current committed generation. A page
miss or checksum failure may be repaired from the identical Tigris generation.
Large scans may read Tigris directly. The executor may mix page sources only
when every page reference comes from the same manifest and the returned hashes
match that manifest.

When Upstash is unavailable, disaster recovery chooses the newest Tigris
generation with a valid hybrid commit marker. Because `BothRequired` writes
the marker before acknowledging, this preserves acknowledged generations.
Prepared manifests without a marker are not promoted automatically.

Reconciliation walks generation metadata, not user rows. It copies missing
content-addressed objects, validates hashes and advances replication state.
Conflicting bytes under the same hash are corruption and stop repair.

## Local disk and write-ahead staging

Partial hydration must work against local files before a remote adapter is
trusted. The local DataBucket store uses `pread`-shaped page access where the
platform adapter permits it, avoiding one shared seek cursor. This provides a
deterministic correctness and performance baseline for cache faults.

A local write-ahead staging area is the default for every remote backend. It
is required whenever the process acknowledges before the remote generation
commits, and it is what lets Tigris or Bunny coalesce writes without exposing
their request latency to each mutation. It stores complete generation plans
with checksums. Truncation happens only after the configured remote commit
condition is satisfied. A configuration that waits synchronously for the
remote generation commit may omit it, but inherits the measured provider
latency.

If no synchronously durable local WAL is configured, an enqueue acknowledgment
retains WorkTable's existing best-effort boundary. The API and system catalog
must distinguish:

- applied in this process;
- staged locally;
- committed to Upstash;
- committed to Tigris; and
- committed to both.

`wait_for_ops()` waits for the configured commit policy. `close()` stops
intake, drains to that policy and joins the worker. Neither method should use
the vague word "synced" without naming the reached state.

## Failure and correctness rules

- A missing remote object named by a committed manifest is corruption, not an
  empty page.
- A backend timeout is an availability error, not `None` from a select.
- A query never returns a row before its fetched page passes v3 validation.
- A catalog aggregate and the data/index changes it describes commit in the
  same generation.
- A stale writer cannot advance the head after losing its epoch or lease.
- A query pins object identities from one catalog generation even if a newer
  generation commits while it runs.
- Dirty or pinned pages are never selected for eviction.
- A failed unique insert and every rollback leave both catalog counts and page
  mappings unchanged.
- Vacuum relocation publishes old-page, new-page and index-link changes as one
  generation.
- Hybrid recovery never treats an unmarked Tigris prepared generation as
  acknowledged.
- Garbage collection is generation-aware and reader-aware.

## `no_std` boundary

DataBucket core keeps page identities, catalog record codecs, mutation plans,
page validation, cache state and backend traits available under `no_std` plus
`alloc`. WorkTable's resident core remains available without `std`, and its
spillable core should also compile without `std` when the caller supplies
storage, time and task-wakeup implementations.

The provided local-file, HTTP, TLS, Redis, S3 and background-thread adapters
live behind `std` features or separate crates. A `no_std` target may implement
the same traits with libc, platform I/O or its own runtime. The core contracts
must not name `std::fs`, Tokio, or a specific HTTP client.

CI continues to build WorkTable and DataBucket without default features. A
remote adapter is never pulled into that dependency graph accidentally.

## Observability

Expose at least these per-domain and per-table metrics:

- resident, pinned, dirty and in-flight bytes;
- data and index cache hit ratio;
- coalesced fault count;
- fetch latency and bytes by backend;
- pages and bytes staged per generation;
- catalog and manifest bytes per generation;
- Upstash command count and REST request count;
- Tigris GET, range GET and PUT count;
- applied, locally staged and remotely committed generation lag;
- hybrid replication lag and repair count; and
- eviction scans, successful evictions and budget stalls.

The read-only system views expose durable database facts and generation state.
High-rate cache metrics should use counters and tracing rather than mutating a
persisted system page on every read.

## Performance and release gates

Resident tables use their existing type, so this work should add no branch,
lock or page-cache lookup to their point-read path. That claim must be measured
against the existing full suite.

The spillable release requires:

- hot point reads measured against resident point reads;
- cold primary-key and secondary-index reads for local disk, Upstash, Tigris
  and hybrid;
- bounded-memory scans over a table several times larger than the cache;
- repeated skewed reads proving that hot pages remain resident;
- range queries with useful index order proving early `limit` termination;
- updates, deletes, relocation and vacuum against cold pages;
- O(1) exact `count()` with most data pages absent;
- a single-row remote write showing no 4 MiB file-chunk upload;
- request, command and byte accounting for each backend;
- forced failures between every stage and commit step;
- restart, repair, another mutation and a second restart;
- checksum, missing-object and stale-writer rejection; and
- unchanged `no_std` builds.

Benchmarks on Apple silicon should run only after competing compiler work is
quiet and use the repository's `taskpolicy` benchmark wrapper. Report the
observed core scheduling conditions with the result. Remote benchmarks report
service region, client region, page stride, cache size, batching window and
resolved dependency versions.

The first useful performance targets are structural:

- one cold point data lookup causes at most one data-page fetch after its index
  path is resolved;
- concurrent misses for one page cause one backend fetch;
- a scan's resident memory stays within cache and bounded query overhead;
- `count()` performs no page fetch;
- a one-page mutation sends one page image per backend plus bounded metadata;
  and
- the fully resident suite has no statistically meaningful regression.

The first remote provider gate is now measured:

- conditional generation-head creation and replacement must reject stale
  expectations;
- a colocated 256 KiB range GET has a p50 no higher than 25 ms;
- a 4 MiB PUT averages no more than 250 ms and sustains at least 150 Mbit/s of
  logical payload; and
- every returned page passes exact byte verification.

Tigris and Bunny pass this gate. R2 passes the exact-read and conditional-head
checks but misses all three performance thresholds. Upstash does not pass the
synchronous request shape, although large command batches may support a later
serving tier. The committed evidence is in
`perf-benchmarks/data/fly-sin-shared-cpu-1x/2026-09-12-remote-store-gate.md`.
These are transport gates, not application latency promises. The adapter must
still pass WAL acknowledgment, restart, partial hydration and repair tests.

## Implementation status and order

1. Done: DataBucket storage-domain identifiers, generation plans, private
   catalog write capability and a stateful page-store fixture. The core remains
   `no_std` plus `alloc`.
2. Done: Tigris-compatible immutable page segments, range reads, conditional
   head publication, restart restore, and a chunked generated-catalog
   checkpoint. WorkTable supplies one real generated system table per database.
3. Done as a transition: the database-wide WorkTable engine runs catalog and
   page accounting on its private persistence runtime and restores tables from
   catalog mappings. It still discovers dirty pages by scanning the local files.
4. Next: feed exact mutations from DataBucket page persistence into generation
   plans and remove the remaining local file scan.
5. Add the local bounded data-page cache, spill state machine and generated
   `UserSpillableWorkTable` shape. Keep indexes resident for this milestone.
6. Move `count()`, row bytes, page counts and index-entry counts onto maintained
   catalog aggregates. Validate them against full offline scans in tests.
7. Run the complete Tigris application-level crash and performance gates.
8. Implement Upstash staging, batching, head compare-and-set, recovery and
   garbage collection after a region-selected service passes the gate.
9. Compose both adapters into the hybrid state machine and repair worker.
10. Add pageable lower index nodes and bounded-memory index scans.
11. Run the complete release gates and retire the per-table compatibility
   engine.

The remote adapters share the same catalog and generation fixtures so their
differences remain transport and commit-policy differences. Partial hydration
is still a release blocker: the new catalog and bounded `read_page` path are its
foundation, but generated queries do not yet evict or fault row pages.

## External constraints to verify during implementation

- Upstash documents REST pipelines as ordered but non-atomic, `/multi-exec` as
  atomic, Lua scripting as available, and REST `WATCH` as unavailable. The
  implementation therefore uses pipelines for reads and a bounded atomic
  operation for the generation head:
  <https://upstash.com/docs/redis/features/restapi>
- Upstash service limits and billing vary by plan. Batch ceilings must be
  configuration bounded and command/byte metrics must be retained:
  <https://upstash.com/pricing/redis>
- Tigris, Bunny and R2 passed conditional-write and exact-read checks with the
  exact Rust client. Only Tigris and Bunny passed the full performance gate.
  Those operations remain release checks because provider behavior and
  configuration can change: <https://fly.io/docs/tigris/>,
  <https://bunny.net/storage/> and
  <https://developers.cloudflare.com/r2/api/s3/>.
- Cloudflare Pipelines currently guarantees exactly-once delivery to its sink,
  caps each stream at 5 MB/s and rolls R2 files no faster than every 10
  seconds. It remains an optional asynchronous ingestion investigation:
  <https://developers.cloudflare.com/pipelines/>.

## Deferred decisions

These choices need measurements or adapter prototypes, but they do not block
the architecture:

- exact cache policy and data/index budget split;
- exact coalescing interval around the initial 4 MiB segment target;
- whether point faults fetch one 256 KiB range or a complete small segment;
- local WAL acknowledgment policy defaults;
- retained-generation count and reader-lease duration;
- when pageable indexes become the default rather than an explicit mode.

None of these require new WorkTable grammar.
