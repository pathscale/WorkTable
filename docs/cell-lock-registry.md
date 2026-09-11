# Archived-row lock registry

The page keeps up to 64 active exact-cell lock entries outside its archived
image. Different rows that hash to the same initial slot still receive
independent lock states. Readers share a row's state; a writer reserves its
writer bit and waits for existing readers to leave. No new reader may join
while that bit is set.

## Released-collision bug

Review on 12 September 2026 reproduced this interleaving on commit 85113ce:

1. A reader of row A occupies the home slot.
2. Row B hashes to the same home slot, so its reader occupies the next slot.
3. A's last reader releases the home slot.
4. A new reader of B sees the empty home slot and claims it, without finding
   B's existing reader in the next slot.

The two readers then protect one row with different atomic states. A writer
can acquire one state while the other still has readers. This violates the
archived-byte synchronization contract. The regression test fails on the old
implementation without performing an unsafe concurrent payload access.

## Assignment and reclamation

Only a short per-page registration critical section may assign a vacant slot
a new key. It searches for an existing matching key before using a vacancy,
including vacancies before an occupied matching slot. Existing home entries
can acquire another guard through their atomic state without registration.

A displaced-entry counter provides the common-case shortcut. It increments
before a new non-home entry is published, and decrements only after that entry
becomes vacant. A zero count proves that no matching key can be hidden beyond
a released collision. Registration serializes publishers; guard drops can
only make this count conservatively high during cleanup, never too low.

Registration is released before waiting for current readers or a writer.
Callbacks therefore retain independent locks for colliding rows; replacing
this registry with fixed hashed lock stripes would change that behavior.
There is no heap allocation on acquisition. The registry remains runtime-only:
no lock state, mutex or counter is serialized, and no grammar changes.

## Verification

Native regressions cover a released preceding collision and two simultaneously
held write guards for different colliding rows. Page serialization and reset
checks cover the unchanged archived layout. The ordinary workspace CI sequence
also exercises concurrent publication, updates, deletion, vacuum and reopen.

The production acquisition/drop code substitutes Loom atomics and the
registration mutex under `wt_loom`. Two bounded models check same-row
read/write exclusion and the released-collision interleaving against a
Loom-tracked payload, with two preemptions. These are bounded safety checks,
not an exhaustive liveness proof or a claim about all possible workloads.

Run from the WorkTable checkout, with the matching release dependencies:

```sh
cargo test --lib in_memory::data::tests
RUSTFLAGS='--cfg wt_loom' cargo test --release --lib cell_lock_models
scripts/ci-local.sh
```

Performance reports from before this fix remain historical observations at
their recorded source revisions. Release comparisons must also measure the
corrected registry; correctness cannot be traded for a faster unsound path.
