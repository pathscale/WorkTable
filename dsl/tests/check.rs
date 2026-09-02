//! The question `worktable_dsl` could not answer before `check`: would the
//! macro accept this?
//!
//! Every case here was run against the parser before `check` existed. The
//! rule failures came back as a `Schema` that parsed cleanly, because
//! `Schema::parse` runs the parser and not the validator, so a designer had no
//! way to know the declaration would not compile short of compiling it.

use worktable_dsl::{Stage, check};

/// A declaration that is both grammatical and acceptable.
#[test]
fn a_good_declaration_has_nothing_to_report() {
    let checked = check("name: Good, columns: { id: u64 primary_key, label: String }");

    assert!(checked.is_acceptable(), "unexpected: {:?}", checked.diagnostics);
    assert_eq!(checked.schema.expect("parsed").name, "Good");
}

/// The case the module exists for: parses, would not compile.
///
/// `Schema::parse` returns `Ok` here, which is correct and is the documented
/// contract: the IR holds declarations the macro refuses, so an editor can
/// render what somebody is halfway through typing. It is also useless on its
/// own, because nothing then says the thing on screen will not build.
#[test]
fn a_rule_failure_still_yields_a_drawable_schema() {
    // `persist: false` is required, and stated first: `congee` refuses to be
    // used at all until the declaration commits either way, and that rule
    // fires before the key type is looked at. Leaving it out tests the
    // persistence rule while claiming to test the key-type one.
    let checked = check(
        "name: Bad,
         persist: false,
         columns: { id: u64 primary_key, label: String },
         indexes: { label_idx: label unique using congee }",
    );

    assert!(checked.schema.is_some(), "a rule failure must still be drawable");
    assert!(!checked.is_acceptable());
    assert_eq!(checked.diagnostics.len(), 1);
    assert_eq!(checked.diagnostics[0].stage, Stage::Rules);
    assert!(
        checked.diagnostics[0].message.contains("does not support key type"),
        "got: {}",
        checked.diagnostics[0].message
    );
}

/// Every problem at once, not the first one.
///
/// The macro stops at the first because it cannot generate code either way.
/// An editor has the opposite economics: fix, recompile, find the next is the
/// loop a live checker removes.
#[test]
fn every_broken_rule_is_reported() {
    let checked = check(
        "name: Several,
         persist: true,
         columns: { id: u64 primary_key, label: String },
         indexes: { label_idx: label unique using congee },
         config: { page_size: 4096 }",
    );

    assert!(checked.schema.is_some());
    assert!(
        checked.diagnostics.len() >= 2,
        "expected the backend and the page size, got: {:?}",
        checked.diagnostics.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
    assert!(
        checked
            .diagnostics
            .iter()
            .any(|d| d.message.contains("does not support key type"))
    );
    assert!(checked.diagnostics.iter().any(|d| d.message.contains("page_size")));
}

/// A grammar failure says so, and produces nothing.
#[test]
fn a_grammar_failure_is_distinguished_from_a_rule_failure() {
    let checked = check("name: Half, columns: { id: u64 primary_key, x: }");

    assert!(checked.schema.is_none(), "there is no schema to draw");
    assert_eq!(checked.diagnostics.len(), 1);
    assert_eq!(checked.diagnostics[0].stage, Stage::Grammar);
}

/// An unbalanced brace, which is the state a declaration is in for most of the
/// time somebody is typing one, is reported rather than panicking.
#[test]
fn an_unclosed_brace_is_a_diagnostic() {
    let checked = check("name: Typing, columns: { id: u64 primary_key");

    assert!(checked.schema.is_none());
    assert_eq!(checked.diagnostics[0].stage, Stage::Grammar);
}

/// Without the `spans` feature the location is absent, never wrong.
#[cfg(not(feature = "spans"))]
#[test]
fn a_diagnostic_without_the_spans_feature_carries_no_location() {
    let checked = check("name: Bad, columns: { id: u64 primary_key, x: }");
    assert!(checked.diagnostics[0].span.is_none());
}

/// With `spans`, the range points at the offending text in the input.
///
/// Asserted by slicing the source with the range rather than by comparing
/// offsets: an off-by-one in either direction produces a plausible-looking
/// number and a wrong underline, and only the slice catches that.
#[cfg(feature = "spans")]
#[test]
fn a_diagnostic_points_at_the_offending_text() {
    let source = "name: Bad,
         persist: false,
         columns: { id: u64 primary_key, label: String },
         indexes: { label_idx: label unique using congee }";
    let checked = check(source);

    let span = checked.diagnostics[0].span.expect("the spans feature is on");
    assert_eq!(&source[span.start..span.end], "label_idx");
}
