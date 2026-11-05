# Detailed follow-ups

## Resolve the legacy release-yank decision

The parent of commit `41ca5ce` recorded an unresolved decision under
`docs/TODO.md`, “Decide what happens to beta.16 on crates.io.” Neither
[the consolidated plan](open-work.md) nor its linked durable plans carries
that release-administration requirement forward.

Decide whether to yank `ps-reclaim 0.1.0` and whether to yank
`worktable 1.0.0-beta.16` after its replacement release. Record the decision,
its rationale, and verified registry status before closing this follow-up.
This review did not query the registry; the historical question does not
establish that either version remains unyanked today.

The safety context from the removed TODO is necessary for that decision:

- `ps-reclaim 0.1.0` allowed its `Guard` to be `Send`. Dropping a guard on
  another thread could clear the originating thread's pin while it was still
  reading and decrement the wrong thread's nesting depth, creating
  use-after-free windows. The TODO records `0.1.1` as the fix, making the guard
  `!Send` with a compile-fail doctest.
- WorkTable beta.16 required `ps-reclaim ^0.1.0`. That requirement permits the
  fixed release, but a pre-existing lockfile can retain `0.1.0`. A caret
  requirement alone does not repair those lockfiles.
- Evaluate yanking the unsound dependency separately from yanking the
  superseded WorkTable release. Document guidance for affected locked
  consumers; yanking does not remove a published version or update existing
  lockfiles.

This is a preserved decision task, not authorization to change the registry.

## Review disposition of the other removed sections

The former formatting-gap item is addressed in the current repository:
[the Rust workflow](../../.github/workflows/rust.yml) has a formatting job
running `cargo fmt --all --check`, the same command present in
[the local CI script](../../scripts/ci-local.sh). This is source inspection,
not evidence that a workflow run passed or that automatic triggers are enabled.

The other removed sections describe completed releases, accepted historical
measurements, the completed reclamation port, and persistence event leaks
explicitly marked fixed. They are not restored as open requirements. This
disposition does not independently revalidate those historical fixes.
