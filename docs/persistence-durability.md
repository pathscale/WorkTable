# Persistence durability and recovery

WorkTable is an embedded in-memory table with optional **best-effort persistence**.
Its current local-disk and S3 paths are not a crash-atomic database: there is no
transaction journal spanning data, primary-index, and secondary-index files, and
ordinary disk batches are flushed but not synchronously committed to stable media.

This is an explicit product boundary, not an implied durability guarantee.

## Guarantee matrix

| Boundary | Guaranteed | Not guaranteed |
|---|---|---|
| Mutation returns `Ok` | The in-memory mutation completed and its persistence operation was accepted by the running queue. | The bytes have reached the OS, disk, or S3. |
| `wait_for_ops()` returns `Ok` | The local persistence task reached an idle point: its queue and analyzer are empty and no batch is in flight. Errors observed by the worker are surfaced. | Intake is not closed; concurrent or later writers may queue more work. File `flush` is not `fsync`, and a multi-file batch is not crash-atomic. |
| `close()` returns `Ok` | Intake is closed, queued work is drained, and the persistence worker has joined without a reported error. | Power-loss durability or atomicity across data and index files. |
| Graceful process exit after `close()` | The WorkTable worker completed all writes it reported. | Survival of a subsequent power loss before the operating system commits buffered writes. |
| Process crash or `SIGKILL` | No row-fidelity guarantee for an interrupted batch. The next load either returns a state whose primary links and rows validate, or returns `PersistenceLoadError`. | Preservation of the latest acknowledged changes. |
| Power loss | The next load applies the same validation/refusal boundary. | Any acknowledged-change retention window; current batches do not call `fsync`. |
| S3 synchronization | A successful persistence operation has uploaded every new immutable chunk and then committed one checksummed manifest covering the data, primary index, and secondary indexes. Restore validates all referenced chunks before atomically installing the local directory. | S3 makes the local disk engine's completed state remotely recoverable; it does not make the local multi-file update power-loss atomic, call `fsync`, or provide multi-writer coordination between processes. |

Call `close()` during orderly shutdown. If `wait_for_ops()` is used before a
non-consuming shutdown path, stop application writers first; otherwise a writer can
enqueue new work after the task appears idle.

## Load validation

The default `PersistedWorkTable::load()` path uses `LoadMode::Strict` and performs
an additional startup-only audit before the persistence worker starts:

- persisted archived rows referenced by the primary index must pass rkyv validation;
- each physical link must be in the initialized part of its page;
- each decoded row's primary key must equal the primary-index key;
- no two primary keys may reference the same physical link; and
- forward and reverse primary indexes must agree; and
- every secondary index must contain exactly one correct entry for each loaded row.

Parsing failures and audit failures are returned as `PersistenceLoadError`. The public
`PersistedWorkTable::load` API still returns `eyre::Result`, so callers can identify the
typed outcome without string matching:

```rust
match MyWorkTable::load(engine).await {
    Ok(table) => use_table(table),
    Err(report) => {
        if let Some(corruption) = report.downcast_ref::<PersistenceLoadError>() {
            eprintln!("refusing {}: {}", corruption.path().display(), corruption.reason());
            restore_or_rebuild(corruption.path());
        } else {
            return Err(report);
        }
    }
}
```

The strict audit is proportional to the number of primary-index entries. It runs only
during `load()` and adds no branch, lock, or scan to steady-state insert, select,
update, or delete paths.

## S3 generation protocol

The S3 engine divides each table file into fixed 4 MiB chunks and names each chunk by
its BLAKE3 content hash. A mutation still scans and hashes the local table files after
the disk engine completes, but it uploads only content absent from the preceding
committed generation. For example, a change confined to one chunk of a 10 MiB file
uploads 4 MiB of file data, plus the small manifest, instead of re-uploading 10 MiB.
Dirty-range reporting from the disk spaces can remove the remaining local scan in a
future compatible optimization.

The mutable `manifest.v1` object is the only remote commit point. It is written after
all referenced immutable chunks. A failed manifest PUT leaves the preceding generation
visible; a failed response is resolved by reading the manifest back and comparing its
exact bytes. Startup refuses a corrupt manifest, a missing chunk, a length mismatch, or
a hash mismatch. It restores into a sibling staging directory and renames that directory
into place only after every table file validates, so a failed remote restore leaves the
existing local table untouched.

Chunks no longer referenced by the current manifest are retained. This prevents a
concurrent restore that already read the prior manifest from losing a chunk underneath
it. Object reclamation therefore belongs in an explicit offline or lease-aware garbage
collector; the alpha engine does not delete remote chunks automatically.

When `manifest.v1` is absent, startup lists and restores the former whole-file layout.
The next successful mutation uploads chunks and establishes the first manifest. Once a
manifest exists, its failure is fatal; WorkTable will not silently continue from stale
local files and overwrite a newer remote generation.

## Offline index recovery

`PersistedWorkTable::load_with(engine, LoadMode::Recovery)` is a low-level
escape hatch for an offline recovery program. It exists for a specific case: a
private copy has an index that cannot be trusted, while another index and the data
pages may still contain valid rows that can be copied into a fresh table.

Recovery mode relaxes only cross-index completeness and equality. It still:

- parses the persisted files normally;
- validates every surviving primary-index entry, including its forward/reverse
  mapping and decoded primary key; and
- validates every surviving secondary-index entry by checked row decoding and by
  comparing the index key with the referenced row.

For example, a recovery tool may preserve the rejected directory, copy it to a
scratch location, move a damaged primary-index file aside in that scratch copy, let
the engine create an empty primary index, and then read valid rows through a surviving
secondary index:

```rust
let scratch = RecoveryWorkTable::load_with(engine, LoadMode::Recovery).await?;
for row in scratch.select_by_tenant(tenant).execute()? {
    clean_table.insert(row)?;
}
scratch.close().await?;
clean_table.close().await?;

// Reopen the rebuilt destination using the strict default before publishing it.
let clean_table = RecoveryWorkTable::load(clean_engine).await?;
```

Never point recovery mode at a live or only copy, serve traffic from the returned
table, or treat it as an in-place repair. Do not insert, update, or delete through the
recovery table. A malformed surviving entry is still rejected; recovery mode does not
turn arbitrary bytes into rows.

## Supported recovery procedure

`PersistenceLoadError` is a refusal boundary. Do not continue writing to the rejected
directory and do not replace individual index or data files in place: the files are one
logical generation even though the format cannot commit them atomically.

1. Stop every process that can write the table.
2. Preserve the rejected table directory for diagnosis.
3. Restore the **entire** table directory from one application-managed snapshot;
   create a new empty table directory and replay rows from an external authoritative
   source or event log; or use the offline recovery mode above to copy individually
   validated rows from a scratch copy into a new table.
4. Open the restored/rebuilt directory and require `load()` to pass before serving it.

WorkTable does not provide automatic or in-place salvage and cannot prove which side
of a torn multi-file batch is authoritative. Full-directory restore, clean replay, or
explicit row-by-row rebuilding from a checked scratch copy are the supported recovery
paths. If none is possible, acknowledged data may be unrecoverable under this
best-effort contract.

## When stronger durability is required

Use a durable database or place WorkTable behind an authoritative log/snapshot system
when acknowledged writes must survive process or power loss. Making WorkTable itself
crash-atomic would require a separately designed and tested journal, shadow-page, or
generation-manifest protocol; it is not claimed by this contract.
