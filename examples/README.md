

## Examples for WorkTable 


Minimum code for WorkTable usage. Uses async-std for futures. 




## Real S3 acceptance

The s3_acceptance example sends WorkTable's normal signed S3 requests to the configured service. It does not use an in-process object-service substitute and never deletes remote objects. Each run appends a unique child prefix below the caller-provided base prefix and leaves those objects in place for the service owner's retention policy.

The per-table generation case writes a row through the normal disk persistence engine, checks that the resulting `.wt.data` and `primary.wt.idx` files are present and non-empty, publishes those exact files as a WTS3G001 generation, restores them to a fresh directory, and cold-opens the restored WorkTable to verify the row. The remaining cases separately exercise the database-wide S3 catalog's under-filled segments, a concurrent database-generation CAS conflict, and a local after-write failure hook followed by a cold catalog reopen.

The per-table generation transport currently uses path-style S3 requests and static access-key/secret-key credentials. The combined example refuses virtual-host style or a session token rather than dropping either setting; although the database-wide adapter accepts those settings, this fixture has no database-only mode.

Use a dedicated, isolated base prefix approved for this acceptance run. Set WORKTABLE_S3_ACCEPTANCE_APPROVED=YES only after verifying that the prefix is safe for these writes. The fixture refuses to run without both that approval flag and the base prefix.

Provide these variables in the process environment without printing or recording their values:

- WORKTABLE_S3_ENDPOINT
- WORKTABLE_S3_BUCKET
- WORKTABLE_S3_ACCESS_KEY
- WORKTABLE_S3_SECRET_KEY
- WORKTABLE_S3_REGION
- WORKTABLE_S3_ACCEPTANCE_PREFIX
- WORKTABLE_S3_ACCEPTANCE_APPROVED=YES

For this combined acceptance, leave WORKTABLE_S3_SESSION_TOKEN unset and WORKTABLE_S3_VIRTUAL_HOST_STYLE unset or false. The per-table generation API does not yet carry a session token or virtual-host-style setting, and the fixture fails before writing if either is enabled.

From the WorkTable repository root, run:

~~~sh
cargo run --features s3-support --example s3_acceptance
~~~

The local after-write hook reports an indeterminate result only after the actual S3 generation commit has returned successfully; it does not replace or forge any S3 transport response. This checks recovery from an ambiguous post-commit acknowledgment at the WorkTable call boundary. A provider-side dropped HTTP response remains a separate transport-level integration gate.
