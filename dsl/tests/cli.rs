//! The `wt-dsl` binary's contract.
//!
//! Exercised through the library rather than by spawning the binary, because
//! the binary is a thin shell over these calls and a test that spawns it
//! measures cargo more than it measures the language. What matters is the
//! contract the shell depends on: `parse` round-trips byte-exactly, `check`
//! separates a grammar failure from a rule failure, and a scan reports what it
//! could not read rather than dropping it.

/// `wt-dsl parse` is a round trip, not a bare parse.
///
/// A second implementation compares its output against this text, so the
/// property is byte equality after a second pass, not "it parsed".
#[test]
fn parse_output_is_byte_stable() {
    let source = "name: Account, columns: { id: u64 primary_key autoincrement, email: String }, indexes: { email_idx: email unique },";
    let once = worktable_dsl::Schema::parse(source).expect("parse").to_dsl();
    let twice = worktable_dsl::Schema::parse(&once).expect("re-parse").to_dsl();
    assert_eq!(
        once, twice,
        "emitting is not a fixed point, so no byte comparison is possible"
    );
}

/// `wt-dsl check` exits non-zero exactly when the macro would refuse.
#[test]
fn check_accepts_what_the_macro_accepts_and_refuses_what_it_refuses() {
    assert!(worktable_dsl::check("name: T, columns: { id: u64 primary_key },").is_acceptable());

    let bad = worktable_dsl::check("name: T, columns: { id: u64 primary_key }, indexes: { nope: missing unique },");
    assert!(!bad.is_acceptable(), "a dangling index must be refused");
    assert_eq!(
        bad.diagnostics.len(),
        1,
        "one problem, one diagnostic: {:?}",
        bad.diagnostics
    );
}

/// A grammar failure and a rule failure are different things to an editor: one
/// has nothing to draw, the other has a schema that can be rendered.
#[test]
fn check_separates_a_grammar_failure_from_a_rule_failure() {
    let broken = worktable_dsl::check("name: Oops, columns: { id: u64 primary_key ");
    assert!(broken.schema.is_none(), "a grammar failure produces no tree");
    assert_eq!(broken.diagnostics[0].stage, worktable_dsl::Stage::Grammar);

    let rule = worktable_dsl::check("name: T, columns: { id: u64 primary_key }, indexes: { nope: missing unique },");
    assert!(rule.schema.is_some(), "a rule failure still has a schema to draw");
    assert_eq!(rule.diagnostics[0].stage, worktable_dsl::Stage::Rules);
}

/// `wt-dsl scan` finds declarations inside function bodies, which an item walk
/// would miss, and reports what it could not read instead of dropping it.
#[test]
fn scan_finds_declarations_in_a_function_body() {
    let found =
        worktable_dsl::declarations_in_source("fn f() { worktable!(name: Inner, columns: { id: u64 primary_key },); }")
            .expect("the file tokenises");

    assert_eq!(
        found.schemas.len(),
        1,
        "a declaration inside a function body was missed"
    );
    assert_eq!(found.schemas[0].name, "Inner");
    assert!(found.is_complete());
}

/// `wt-dsl parse` must answer "would the macro accept this", not "is this a
/// declaration".
///
/// The difference is not academic and this test exists because the first
/// version of the binary got it wrong. `page_size: 4096` beside
/// `persist: true` parses perfectly: it is a well-formed declaration. The
/// macro refuses it, because the on-disk layer hardcodes 16384-byte pages and
/// any other value reads and writes the wrong file offsets.
///
/// A round trip built on `Schema::parse` therefore reports success for output
/// that does not compile, which is precisely the mistake a second
/// implementation would then ship, and the cross-implementation check exists
/// to stop.
#[test]
fn a_declaration_the_macro_refuses_is_not_a_successful_round_trip() {
    let refused = "name: T, persist: true, columns: { id: u64 primary_key }, config: { page_size: 4096 },";

    // It parses. That is the trap.
    assert!(
        worktable_dsl::Schema::parse(refused).is_ok(),
        "the fixture must be a well-formed declaration, or it tests nothing"
    );

    // And the macro would refuse it, which is what the binary reports.
    let checked = worktable_dsl::check(refused);
    assert!(
        !checked.is_acceptable(),
        "the macro refuses this and so must the binary"
    );
    assert!(
        checked.diagnostics.iter().any(|d| d.message.contains("page_size")),
        "the diagnostic must name the offending option: {:?}",
        checked.diagnostics
    );
}
