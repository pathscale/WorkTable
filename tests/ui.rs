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
    t.compile_fail("tests/ui/duplicate_index_on_column.rs");

    // Query rules.
    t.compile_fail("tests/ui/autoincrement_unsupported_key.rs");
    t.compile_fail("tests/ui/in_place_over_indexed_column.rs");
    t.compile_fail("tests/ui/update_primary_key_column.rs");
    t.compile_fail("tests/ui/update_empty_columns.rs");
    t.compile_fail("tests/ui/delete_with_columns.rs");
    t.compile_fail("tests/ui/update_selector_wrong_value.rs");
    t.compile_fail("tests/ui/update_selector_collision.rs");
    t.compile_fail("tests/ui/delete_by_unindexed_column.rs");
    t.compile_fail("tests/ui/delete_by_unindexed_column_persist.rs");
    t.compile_fail("tests/ui/update_by_unindexed_column.rs");
    t.compile_fail("tests/ui/in_place_by_unindexed_column.rs");
    t.compile_fail("tests/ui/in_place_by_unique_index.rs");
    t.compile_fail("tests/ui/in_place_by_nonunique_index.rs");
    t.compile_fail("tests/ui/delete_by_composite_pk.rs");
    t.compile_fail("tests/ui/update_by_composite_pk.rs");
    t.compile_fail("tests/ui/in_place_by_composite_pk.rs");

    // Read-path safety rules.
    t.compile_fail("tests/ui/select_with_needs_inline_archived.rs");

    // Malformed macro and derive input rules.
    t.compile_fail("tests/ui/worktable_without_columns.rs");
    t.compile_fail("tests/ui/worktable_version_without_columns.rs");
    t.compile_fail("tests/ui/migration_engine_bad_current.rs");
    t.compile_fail("tests/ui/migration_engine_bad_version_table.rs");
    t.compile_fail("tests/ui/persist_index_bad_name.rs");
    t.compile_fail("tests/ui/persist_index_tuple_struct.rs");
    t.compile_fail("tests/ui/persist_index_non_path_field.rs");
    t.compile_fail("tests/ui/persist_index_missing_type_args.rs");
    t.compile_fail("tests/ui/persist_index_no_type_arg.rs");
    t.compile_fail("tests/ui/persist_index_malformed_attr.rs");
    t.compile_fail("tests/ui/persist_table_bad_name.rs");
    t.compile_fail("tests/ui/persist_table_no_fields.rs");
    t.compile_fail("tests/ui/persist_table_too_few_generics.rs");
    t.compile_fail("tests/ui/persist_table_malformed_attr.rs");
}
