# Compile-fail drafts for `runtimes!` and `.runtime()`

Case bodies only. Wiring them into `trybuild` (and capturing the `.stderr`
files) belongs to the trybuild lane; nothing here is compiled by `cargo test`
today, because `tests/ui-drafts` is a directory rather than a test target.

Each file names, in a header comment, the message content that must appear. The
messages were produced by rustc 1.97.1 against this branch, so the `.stderr`
files can be generated with `TRYBUILD=overwrite` and then read rather than
guessed at.

| file | what it proves |
|---|---|
| `backend-mismatch.rs` | a `tokio` profile on a `nagoya` table, named at a call site |
| `pinned-by-schema.rs` | a section annotation and a `.runtime()` both present |
| `no-runtime-on-point-select.rs` | `select(pk)` has no `.runtime()` at all |
| `not-implemented-backend.rs` | `forte`, `blocking` and `bwos` are rejected, not accepted inert |
| `unknown-backend.rs` | an unknown backend names what does exist |
| `unknown-flavor.rs` | an unknown flavor lists the three |
| `tokio-has-no-flavor.rs` | `tokio(spread)` |
| `duplicate-profile.rs` | two profiles with one name |
| `runtime-takes-one-argument.rs` | `.runtime()` does not widen |

Eight of the nine were checked against this branch and produce the message
their header claims. `no-runtime-on-point-select.rs` is the exception: it uses
`worktable!`, so it needs the generated table's `TableRuntime` impl, which is
the codegen lane's. Run it once that lands.
