//! Compile-fail tests for `worktable!`.
//!
//! Roughly half of what the macro promises is a refusal: a declaration that is
//! grammatical but wrong has to be rejected, with a message that says which
//! rule it broke. Nothing in `tests/` could express that, because a test only
//! runs once its crate has compiled, so every one of those rules was
//! unverified. `trybuild` compiles each case in `tests/ui/` on its own and
//! diffs the compiler's output against a committed `.stderr`.
//!
//! The assertion is the message, not the failure. A case that only checked
//! "this did not compile" would stay green while the diagnostic decayed into
//! something that points at the wrong line, which is the failure mode these
//! rules exist to prevent.
//!
//! Cases are listed one by one rather than globbed. `tests/ui/runtime_*.rs`
//! are drafts for the runtime-backend work and describe a feature the parser
//! does not have yet, so a glob would fail them for the wrong reason. The lane
//! that lands `runtime:` adds them here; see `tests/ui/README.md`.

#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();

    // Grammar and shape.
    t.compile_fail("tests/ui/no_primary_key.rs");
    t.compile_fail("tests/ui/unknown_index_backend.rs");
    t.compile_fail("tests/ui/query_over_unknown_column.rs");

    // Index backend rules.
    t.compile_fail("tests/ui/indexset_unsized_key.rs");
    t.compile_fail("tests/ui/nonunique_congee_index.rs");
    t.compile_fail("tests/ui/congee_unsupported_key_type.rs");
    t.compile_fail("tests/ui/congee_without_persist.rs");

    // Query rules.
    t.compile_fail("tests/ui/autoincrement_unsupported_key.rs");
    t.compile_fail("tests/ui/in_place_over_indexed_column.rs");
}
