# Knowing when a write is actually durable

**Status:** proposal. Nothing here is implemented.

Today a mutation returns `Ok` when the in-memory change is done and its
persistence operation was accepted by the queue. That is the top row of the
guarantee matrix in [`persistence-durability.md`](persistence-durability.md),
and it is deliberately weak: **it explicitly does not mean the bytes reached the
OS, the disk, or S3.**

The gap is not that the boundary is undocumented. It is that a caller who needs
a stronger guarantee has no way to ask for one, and no way to observe when it
arrives. The only signal available is `wait_for_ops()`, which is whole-queue
rather than per-operation, polls on a one second timer, and answers a question
almost nobody asked ("is the worker idle right now?") rather than the one they
did ("is *my* write safe yet?").

This proposes the ladder, the API to observe it, and the background worker that
advances it.

## 1. The ladder

Four stages. A write climbs them in order, and each one survives strictly more
than the last.

| Stage | Reached when | Survives | Today |
| --- | --- | --- | --- |
| **Accepted** | `apply_operation` returned `Ok`; the op is in the in-process queue | nothing; a `SIGKILL` here loses the write | observable, it is the return value |
| **Written** | the engine issued this op's writes and `flush()` returned | process crash, `SIGKILL`, panic | **reached but not observable per operation** |
| **Synced** | `sync_data` (or `F_FULLFSYNC`) returned for every file the op touched | power loss, OS crash | **does not exist for the data path** |
| **Uploaded** | the configured S3 path reported success | loss of the machine | partially, and not per operation |

Two things in that table are worth saying plainly.

**"Written" is already reached, just invisible.** `save_data`, `save_batch_data`
and `save_info` each end in `self.data_file.flush().await?`. For
`tokio::fs::File` that pushes the userspace buffer into the OS page cache
through the blocking pool. So the bytes genuinely do survive a process crash
once the engine has drained that operation. Nobody can currently find out when
that happened for a given write.

**"Synced" does not exist.** `sync_data()` appears exactly once in the entire
crate, in `art_index.rs`, on an index file *creation* path. No data write, no
batch, and no info-page write ever syncs. Flushing to the page cache is not
durability against power loss, and the guarantee matrix already says so. So this
feature cannot honestly report "on disk" until the sync is added. Reporting is
the smaller half of the work.

## 2. Watermarks, not per-operation futures

Every operation already carries an `OperationId`, which is a UUID v7 and
therefore time-ordered, and the engine drains the queue in that order. So the
state of the whole system at any moment is two monotonically increasing ids:
the highest operation that has been written, and the highest that has been
synced. Anything at or below a watermark has reached that stage.

That matters because it makes the mechanism O(1) rather than O(pending). No
per-operation registry, no map of waiting futures keyed by id, no bookkeeping
that grows with queue depth. Two atomics and a `Notify`.

```rust
/// How far a write has got. Ordered: each stage implies the ones before it.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum DurabilityStage {
    Accepted,
    Written,
    Synced,
    Uploaded,
}

/// The highest operation id to have reached each stage.
#[derive(Copy, Clone, Debug)]
pub struct Watermarks {
    pub written: Option<OperationId>,
    pub synced: Option<OperationId>,
    pub uploaded: Option<OperationId>,
}
```

## 3. The API

The important design decision is that **no existing signature changes**.

Because ids are time-ordered, a caller does not need to know their own operation
id. They need a mark taken *after* their write, and to wait for that mark. Waiting
for a slightly later mark than strictly necessary is always safe: it can wait a
little longer, never report durability too early.

```rust
let pk = table.insert(row)?;             // unchanged, still returns immediately
let mark = table.durability_mark();      // "everything queued up to now"

// Block until it is on the device, or the worker fails.
mark.wait(DurabilityStage::Synced).await?;
```

For callers that want to react rather than await:

```rust
let mut watch = table.durability_watch();
while watch.changed().await? {
    let marks = watch.watermarks();
    metrics.gauge("wt.unsynced_ops", pending_below(marks.synced));
}
```

And the cheap non-blocking question, for a status endpoint or a health check:

```rust
match table.stage_of(mark) {
    DurabilityStage::Accepted => /* still only in the queue */,
    DurabilityStage::Written  => /* safe against a process crash */,
    DurabilityStage::Synced   => /* safe against power loss */,
    DurabilityStage::Uploaded => /* safe against losing the machine */,
}
```

A `Mark` is a plain `OperationId` plus a handle to the watermarks. It is `Copy`,
cheap to take, and can be stored, sent across threads, or persisted in an
application's own record of what it has acknowledged to *its* callers.

For the batch case, one mark after a batch covers the whole batch, which is the
natural granularity anyway.

## 4. The background worker

`Written` needs no new thread. The engine task already knows when it finished an
operation; it publishes the id and notifies. That is a store and a `notify_waiters`
per drained batch, not per operation.

`Synced` needs the new worker, and its whole job is **group commit**.

An `fsync` costs on the order of a millisecond, against a 761 ns insert. Syncing
per operation would make writes three orders of magnitude slower, so the syncer
must batch: one `fsync` covers every write issued before it, so a hundred waiters
arriving during one sync are all satisfied by that single sync.

```rust
pub enum SyncPolicy {
    /// Today's behaviour, and the default. Never syncs; `Synced` never advances
    /// and waiting on it returns an error rather than hanging.
    Never,
    /// Sync when someone is waiting, coalescing all current waiters into one.
    OnDemand,
    /// Sync at most this often, whether or not anyone is waiting. Bounds the
    /// window of writes that a power loss can take.
    Interval(Duration),
    /// Sync after every drained batch. Slowest and strongest.
    EveryBatch,
}
```

The loop is small:

1. wait for either a sync request or the interval tick;
2. read the current `written` watermark, call it `w`;
3. `sync_data()` every open file (data, info, primary index, secondary indexes);
4. publish `synced = w` and notify.

Step 2 before step 3 is the part to get right: the watermark captured is the one
from *before* the sync started, because writes landing during the sync are not
covered by it. Publishing the later value would report durability that was not
achieved, which is the only genuinely dangerous bug this feature can have.

**On macOS, `fsync` is not enough.** It returns once the data reaches the drive,
without waiting for the drive to flush its own write cache. Only
`fcntl(F_FULLFSYNC)` does that, and it is considerably slower. If `Synced` is to
mean what it says on a developer's laptop, the syncer needs `F_FULLFSYNC` there,
and the policy should probably let a caller choose the weaker one knowingly.

## 5. What this does not do

It does not make WorkTable crash-atomic, and it must not be described as if it
did. A batch still spans data, primary-index and secondary-index files with no
journal, so a power loss mid-batch can still tear across them. `Synced`
answers "are these bytes on the device", not "is this table a consistent
generation". The load-time audit in `persistence-durability.md` remains the
thing that catches a torn batch, and full-directory restore remains the
recovery path.

Being able to observe durability makes the existing boundary usable. It does not
move it.

## 6. Order of work

1. **Publish the `written` watermark.** No new thread, no format change, no cost
   on the write path. Delivers the process-crash guarantee that is already being
   met but cannot be observed. This is most of the value for the least risk.
2. **`Mark`, `stage_of`, `wait`, and the watch.** Pure API over step 1.
3. **Add `sync_data` to the space layer** and the syncer task with `SyncPolicy`,
   defaulting to `Never` so nothing existing changes speed.
4. **Measure it.** Insert throughput under each policy, and the latency of
   `wait(Synced)` under concurrent load, on the wt-benchmarks harness. `EveryBatch`
   is expected to be dramatically slower; the number should be published rather
   than guessed at, because it is the number that tells someone which policy to
   pick.
5. **`F_FULLFSYNC` on macOS**, and a documented statement of what `Synced` means
   per platform.
6. **`Uploaded`**, last, and only if S3 users ask. It is a different failure
   domain and a much weaker ordering story.

Steps 1 and 2 are additive and could ship in a patch release. Step 3 changes
what the crate does to your disk and wants its own release and its own tests.
