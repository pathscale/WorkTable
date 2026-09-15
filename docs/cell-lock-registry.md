# Archived-row lock stripes

Each 16 KiB data page keeps 256 fixed reader/writer states outside its archived
image. Every archived-row offset is mixed across all of its bits before it is
assigned a stripe. Readers increment the stripe's reader count. A writer sets
its writer bit, which stops new readers, and waits for existing readers to
leave.

Rows that collide may read concurrently because neither mutates bytes. A write
waits for every reader or writer on the same stripe, including an unrelated
row that happens to collide. This is conservative exclusion: it can delay an
operation, but it cannot let a reader overlap a write to the same row.

Each stripe records the operating-system identity of its active writer. If a
generated in-place callback synchronously re-enters the table and its target
maps to the same stripe, a read of a different row borrows the exclusion the
callback already holds instead of waiting for itself. A read of the row being
mutated, or a nested write, returns `CellLockReentry` because lending either
would create overlapping mutable access. Other threads still wait normally.
The owner lookup runs only on write acquisition or after a reader observes the
writer bit, so uncontended reads keep the single-atomic path. Unix uses
`pthread_self` and Windows uses `GetCurrentThreadId`; both remain available
without `std`.

## Released-collision bug

Review on 12 September 2026 reproduced this interleaving on commit 85113ce:

1. A reader of row A occupies the home slot.
2. Row B hashes to the same home slot, so its reader occupies the next slot.
3. A's last reader releases the home slot.
4. A new reader of B sees the empty home slot and claims it, without finding
   B's existing reader in the next slot.

The two readers then protect one row with different atomic states. A writer
can acquire one state while the other still has readers. This violates the
archived-byte synchronization contract.

## Why stripes replace registration

The first repair assigned vacant exact-row entries under a per-page mutex. A
random lookup normally outlived its entry for only one guard, so almost every
read needed that mutex. On the twelve-client read control, throughput fell
from roughly 140 to 147 million operations per second to roughly 48 million.

Fixed stripes have no key assignment, reclamation, scan or registration lock.
The stripe is a pure function of the row offset, so all access to one row
always reaches one atomic state. Mixing matters because archived row starts
are aligned: masking the low offset bits directly would collapse common row
sizes into only a few stripes.

The states occupy 1 KiB per 16 KiB data page; the owner identities use another
2 KiB on a 64-bit host. Keeping them in separate arrays leaves the normal read
cache path on the compact state array. There is no heap allocation on
acquisition. All coordination remains runtime-only, so no lock state is
serialized and the binary format and grammar do not change.

## Verification

Native regressions cover stable mapping for colliding offsets, mixing of offsets
that share low bits, page serialization and reset. The ordinary workspace CI
sequence also exercises concurrent publication, updates, deletion, vacuum and
reopen.

The production acquisition/drop code substitutes Loom atomics under `wt_loom`.
Two bounded models check same-row read/write exclusion and colliding-offset
exclusion against a Loom-tracked payload, with two preemptions. These are
bounded safety checks, not an exhaustive liveness proof or a claim about all
possible workloads.

Run from the WorkTable checkout, with the matching release dependencies:

```sh
cargo test --lib in_memory::data::tests
RUSTFLAGS='--cfg wt_loom' cargo test --release --lib cell_lock_models
scripts/ci-local.sh
```

The corrected stripe implementation must pass the companion performance
suite's same-source lock comparison before release. The report records both
executable hashes and the exact WorkTable revision so results cannot be mixed
with an older scheduler or dependency graph.

## Point reads without a reader CAS

A point read does not take the stripe's reader count. It snapshots the stripe,
copies or reads the archived cell, and then checks the stripe again; if a writer
arrived in between, it retries. Incrementing the reader count instead made every
concurrent reader write to the same atomic, and shared `select` scaled 2.4x
across eight workers while the same read on a private table scaled 7.4x. Stripe
states are padded to a cache line each for the same reason: sixteen of them
previously shared one line, so unrelated rows bounced it between cores.

The snapshot is a monotonic counter, not the stripe's reader/writer word. A
writer releases by storing zero, which is exactly what an idle stripe reads, so
a write that began and finished inside one reader's window would otherwise be
invisible and the reader would keep bytes copied from the middle of it. The
counter advances once per completed write, before the writer bit clears.

Two read shapes follow from that check.

`select` and `select_ref` copy the cell into private memory and validate the
copy, so they work for any row. `select_with` instead runs the caller's closure
directly on the page, with no copy, and validates afterwards. That is faster,
about 1.8x at one worker, and it means the closure can observe a value a writer
is currently changing.

For a fixed-size scalar that is recoverable: the closure reads a torn number and
the retry throws away whatever it computed. For a relative pointer, which is
what an archived `String` or `Vec` field is, it is not: the closure would
dereference the torn pointer before the check runs. So `select_with` is
generated only for tables whose columns are all inline scalars, gated on the
`InlineArchived` marker the macro emits for exactly those. A table with a
`String` column has no `select_with` at all, and a call to it is a compile
error naming the missing method rather than a silent copy or a torn read. Those
tables use the owned `select`.

The macro refuses opaque user types here, because it cannot inspect a user
type's `Archive::Archived` layout. That costs such a table the zero-copy path;
it never grants one unsoundly.
