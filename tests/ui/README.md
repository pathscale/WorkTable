# Compile-fail tests

Every file here is a program that must **not** compile, paired with a
`.stderr` holding the diagnostic it must produce. `tests/ui.rs` runs them
through `trybuild`.

Roughly half of what `worktable!` promises is a refusal. A test in `tests/`
cannot express one, because a test only runs once its crate has compiled, so
every rule of that kind was unverified until this directory existed.

The assertion is the **message**, not the failure. A case that only checked
"this did not compile" stays green while its diagnostic decays into one that
points at the wrong line, which is the failure these rules exist to prevent.

```sh
cargo test --test ui
```

## Adding a case

1. Write `tests/ui/<rule>.rs`. Keep it minimal: one `worktable!` invocation
   breaking one rule, plus `fn main() {}`. Open with a comment saying which
   rule it pins and why that rule exists, not what the code does.
2. Import only `use worktable::worktable;`. Do **not** add
   `use worktable::prelude::*;` unless the case actually needs it: the macro
   errors before the import is used, so the prelude lands an
   `unused_imports` warning in the `.stderr` and that warning's wording moves
   between compiler releases.
3. Add a `t.compile_fail("tests/ui/<rule>.rs");` line to `tests/ui.rs`. Cases
   are listed one by one on purpose, not globbed. See "Drafts" below.
4. Generate the expectation, read it, commit it:

   ```sh
   TRYBUILD=overwrite cargo test --test ui
   ```

Read the generated `.stderr` before committing it. `TRYBUILD=overwrite`
records whatever the compiler said, including a message that is wrong, so
blessing without reading turns the harness into a transcript of current
behaviour rather than a check on it.

## Regenerating expectations

```sh
TRYBUILD=overwrite cargo test --test ui
```

That rewrites every `.stderr` in place. Diff them afterwards. A change you did
not intend is the finding.

## The `.stderr` files are compiler-version sensitive

They are the compiler's output verbatim: message text, line and column
numbers, the underline, the trailing notes. Anything rustc changes about how
it renders a diagnostic changes these files, on code nobody touched.

The cases here are all `syn::Error` text emitted by `worktable!` through
`compile_error!`, which is the least fragile shape available: the message is
ours, and rustc contributes only the span rendering. Cases that lean on
rustc's own diagnostics, such as the trait-bound and
`#[diagnostic::on_unimplemented]` cases the runtime lane will add, are more
exposed.

Generated with **rustc 1.97.1 (8bab26f4f 2026-07-14)**. If CI runs a newer
stable than your toolchain, expect the first mismatch to come from CI, not
from your terminal. `scripts/ci-local.sh` prints both versions.

## Drafts

`runtime_*.rs` are written but **not** listed in `tests/ui.rs`. They pin the
rules in section 7 of the runtime-backend contract, and the parser has no
`runtime:` arm yet, so today they fail with

```
Unexpected token `runtime`; expected one of `columns`, `indexes`, `queries`, `config`
```

which is the right verdict for the wrong reason. Wiring them up now would
commit a `.stderr` asserting that the feature is missing, and that file would
pass right up until the feature landed and then have to be rewritten.

Each draft carries a comment saying which lane enables it. That lane adds its
`t.compile_fail(...)` line and blesses its `.stderr` in the same commit that
lands the rule.
