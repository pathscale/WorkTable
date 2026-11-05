# Deferred follow-ups from the patch queue

The old patch-queue README was the only tracker for these requirements. Its
dispositions are captured here so the scratch queue can be removed. Recheck
each item against current `master` before implementation; this transfer did
not revalidate the source behavior.

## Open engineering requirements

- **Issue #168 — persistence lifecycle:** define `close()`, a terminal
  `Failed` state, and `wait_for_ops` returning `Result`. The queue README says
  this remains deferred; it distinguishes the narrower idle-only `Drop`
  abort-and-log behavior already landed from the requested lifecycle.
- **Issue #163 — persisted vacuum:** reclaim on-disk space when persisted
  tables are vacuumed.
- **Issue #164 — upsert:** provide single-lock linearization for upsert.
- **Issue #165 — lock-map retention:** resolve retention when an acquirer is
  cancelled while waiting.
- **Issue #167 — CDC event IDs:** prevent non-CDC mutations from consuming
  CDC event IDs.
- **Issue #169 — lock-free insert and locked mutations:** resolve the race
  under same-key churn that the queue README describes as exposing ghost rows,
  delete panic, or stalls.

## Release decision

The queue README also records a parked `0.9.0` version change in
`Cargo.toml` and `README.md`. Confirm release policy and the current manifest
and README before deciding whether that version is still appropriate.

These are requirements to investigate, not claims that current `master` has
the listed defects. Record source evidence and focused validation before
closing each item.
