

## Examples for WorkTable 


Minimum code for WorkTable usage. Uses async-std for futures. 




## Real S3 acceptance

The s3_acceptance example sends WorkTable's normal signed S3 requests to the configured service. It does not use an in-process object-service substitute and never deletes remote objects. Each run appends a unique child prefix below the caller-provided base prefix and leaves those objects in place for the service owner's retention policy.

The per-table generation case writes a row through the normal disk persistence engine, checks that the resulting `.wt.data` and `primary.wt.idx` files are present and non-empty, publishes those exact files as a WTS3G001 generation, restores them to a fresh directory, and cold-opens the restored WorkTable to verify the row. The remaining cases separately exercise the database-wide S3 catalog's under-filled segments, a concurrent database-generation CAS conflict, and a local after-write failure hook followed by a cold catalog reopen.

The existing `S3Config` struct remains unchanged. Per-table S3 APIs accept a separate `S3TransportOptions` value for an optional session token and virtual-hosted-style requests; omitting it preserves path-style requests with static access-key/secret-key credentials. The acceptance fixture forwards the same settings to its publisher and reader, matching the database-wide DataBucket adapter.

Use a dedicated, isolated base prefix approved for this acceptance run. Set WORKTABLE_S3_ACCEPTANCE_APPROVED=YES only after verifying that the prefix is safe for these writes. The fixture refuses to run without both that approval flag and the base prefix.

Provide these variables in the process environment without printing or recording their values:

- WORKTABLE_S3_ENDPOINT
- WORKTABLE_S3_BUCKET
- WORKTABLE_S3_ACCESS_KEY
- WORKTABLE_S3_SECRET_KEY
- WORKTABLE_S3_REGION
- WORKTABLE_S3_ACCEPTANCE_PREFIX
- WORKTABLE_S3_ACCEPTANCE_APPROVED=YES

Set `WORKTABLE_S3_SESSION_TOKEN` only when the key pair is temporary, and set `WORKTABLE_S3_VIRTUAL_HOST_STYLE=true` only when the configured endpoint expects bucket-name DNS routing. Both settings are now passed through the per-table signing and URL construction paths; leaving them unset preserves the old default.

From the WorkTable repository root, run:

~~~sh
cargo run --features s3-support --example s3_acceptance
~~~

The local after-write hook reports an indeterminate result only after the actual S3 generation commit has returned successfully; it does not replace or forge any S3 transport response. This checks recovery from an ambiguous post-commit acknowledgment at the WorkTable call boundary. A provider-side dropped HTTP response remains a separate transport-level integration gate.
