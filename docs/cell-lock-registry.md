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

The states occupy 1 KiB per 16 KiB data page. There is no heap allocation on
acquisition. They remain runtime-only, so no lock state is serialized and the
binary format and grammar do not change.

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

The corrected stripe implementation restores single-client reads to the
pre-bug baseline. At twelve clients, two reversed-order repetitions measured
about 157 to 167 million shared-table reads per second across balanced,
almost_tokio and Tokio, roughly 10 to 16 percent above the pre-bug baseline.
The exact report and source hashes live in the companion performance suite.
