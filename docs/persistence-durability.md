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
| S3 synchronization | Successful calls report completion of the configured upload path. | A transactionally consistent multi-file snapshot. Treat independently uploaded objects as best-effort unless an application-managed snapshot generation protects them. |

Call `close()` during orderly shutdown. If `wait_for_ops()` is used before a
non-consuming shutdown path, stop application writers first; otherwise a writer can
enqueue new work after the task appears idle.

## Load validation

Loading an existing table performs an additional startup-only audit before the
persistence worker starts:

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

The audit is proportional to the number of primary-index entries. It runs only during
`load()` and adds no branch, lock, or scan to steady-state insert, select, update, or
delete paths.

## Supported recovery procedure

`PersistenceLoadError` is a refusal boundary. Do not continue writing to the rejected
directory and do not replace individual index or data files in place: the files are one
logical generation even though the format cannot commit them atomically.

1. Stop every process that can write the table.
2. Preserve the rejected table directory for diagnosis.
3. Restore the **entire** table directory from one application-managed snapshot; or
   create a new empty table directory and replay rows from an external authoritative
   source or event log.
4. Open the restored/rebuilt directory and require `load()` to pass before serving it.

WorkTable does not currently provide an in-place salvage tool that can prove which
side of a torn multi-file batch is authoritative. Full-directory restore or clean
replay is the supported recovery path. If neither exists, acknowledged data may be
unrecoverable under this best-effort contract.

## When stronger durability is required

Use a durable database or place WorkTable behind an authoritative log/snapshot system
when acknowledged writes must survive process or power loss. Making WorkTable itself
crash-atomic would require a separately designed and tested journal, shadow-page, or
generation-manifest protocol; it is not claimed by this contract.
