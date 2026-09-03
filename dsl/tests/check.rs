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

/// An index over a column that does not exist.
///
/// The macro never reaches this rule: its own parse fails first, so the
/// validator was written with an `expect` on the column being present.
/// `check` does reach it, because it runs the same rules over whatever a
/// person has typed, and an index naming a column that is not there yet is
/// exactly what half-finished input looks like. It panicked, which for an
/// editor calling `check` on every keystroke is a crash rather than a squiggle.
#[test]
fn an_index_over_a_missing_column_is_reported_rather_than_panicking() {
    let checked = worktable_dsl::check(
        r#"
        name: Broken,
        columns: { id: u64 primary_key, email: String },
        indexes: { nope_idx: does_not_exist unique },
        "#,
    );

    assert!(!checked.is_acceptable(), "a dangling index must not be accepted");
    assert!(
        checked
            .diagnostics
            .iter()
            .any(|d| d.message.contains("nope_idx") && d.message.contains("not a column")),
        "the diagnostic must name the index and say what is wrong: {:?}",
        checked.diagnostics
    );
}

/// The same schema with the column present is accepted, so the rule above is
/// rejecting the dangling reference and not the shape of the declaration.
#[test]
fn the_same_index_is_accepted_once_its_column_exists() {
    let checked = worktable_dsl::check(
        r#"
        name: Fixed,
        columns: { id: u64 primary_key, does_not_exist: String },
        indexes: { nope_idx: does_not_exist unique },
        "#,
    );
    assert!(checked.is_acceptable(), "unexpected: {:?}", checked.diagnostics);
}

/// `page_size` values a person can actually type that are not a `u32`.
///
/// `Literal` accepts all of these; `u32::from_str` accepts none of them. The
/// parser used to unwrap, which inside the macro was a poor error and in the
/// public `check` API is a crash: an editor calling it per keystroke takes the
/// panic rather than showing a squiggle.
#[test]
fn a_page_size_that_is_not_a_u32_is_a_diagnostic_not_a_panic() {
    for bad in ["16384u32", "4294967296", "1.5", "0x4000"] {
        let checked = worktable_dsl::check(&format!(
            "name: T, columns: {{ id: u64 primary_key }}, config: {{ page_size: {bad} }},"
        ));
        assert!(!checked.is_acceptable(), "`{bad}` must be rejected");
        assert!(
            !checked.diagnostics.is_empty(),
            "`{bad}` must produce a diagnostic rather than nothing"
        );
    }
}

/// The ordinary value still works, so the rule above rejects the literal form
/// and not the option.
#[test]
fn a_plain_page_size_is_still_accepted() {
    let checked = worktable_dsl::check("name: T, columns: { id: u64 primary_key }, config: { page_size: 16384 },");
    assert!(checked.is_acceptable(), "unexpected: {:?}", checked.diagnostics);
}

/// Several indexes breaking the same rule must all be reported.
///
/// `index_backends_into` used to pick one offender: the primary if it
/// qualified, otherwise the first secondary found. A declaration with three
/// was reported as having one problem, which is the edit-check-fix-one loop
/// that collecting every failure exists to remove.
#[test]
fn every_index_that_needs_explicit_persistence_is_reported() {
    let checked = worktable_dsl::check(
        r#"
        name: Several,
        columns: {
            id: u64 primary_key,
            a: u64,
            b: u64,
            c: u64,
        },
        indexes: {
            a_idx: a unique using congee,
            b_idx: b unique using congee,
            c_idx: c unique using congee,
        },
        "#,
    );

    assert!(!checked.is_acceptable());
    let reported = checked
        .diagnostics
        .iter()
        .filter(|d| d.message.contains("persist"))
        .count();
    assert_eq!(
        reported, 3,
        "all three indexes break the same rule; got {reported}: {:?}",
        checked.diagnostics
    );
}

/// `check` must refuse a primary key its backend cannot hold.
///
/// It used to accept `id: String primary_key using congee` with no diagnostic
/// at all, and the macro then refused it at expansion. The key-type rule was
/// applied in a loop over `columns.indexes`, which holds the *secondary*
/// indexes; the primary backend was only ever checked against the persistence
/// rule. So an editor showed nothing and the build failed.
#[test]
fn a_primary_key_backend_is_checked_against_its_key_type() {
    let refused = worktable_dsl::check(
        "name: Probe, persist: false, columns: { id: String primary_key using congee, label: String },",
    );
    assert!(!refused.is_acceptable(), "congee cannot hold a String primary key");
    assert!(
        refused
            .diagnostics
            .iter()
            .any(|d| d.message.contains("congee") && d.message.contains("String")),
        "the diagnostic must name the backend and the key type: {:?}",
        refused.diagnostics
    );

    // arctic has the same shape with a different list.
    assert!(
        !worktable_dsl::check("name: P, persist: false, columns: { id: u8 primary_key using arctic },").is_acceptable(),
        "arctic does not hold u8"
    );

    // And a key type the backend does hold is still accepted.
    assert!(
        worktable_dsl::check("name: P, persist: false, columns: { id: u32 primary_key using congee },").is_acceptable()
    );
}

/// `check` must refuse an `autoincrement` key type that cannot be generated.
///
/// `usize` is the case worth naming: it is an integer, it reads like one of
/// the accepted set, and there is no `AtomicUsize` in codegen's mapping. This
/// also passed `check` and failed to build.
#[test]
fn autoincrement_is_checked_against_its_key_type() {
    for bad in ["usize", "String", "u128", "i128", "isize", "bool"] {
        let checked = worktable_dsl::check(&format!(
            "name: Probe, persist: false, columns: {{ id: {bad} primary_key autoincrement }},"
        ));
        assert!(!checked.is_acceptable(), "`{bad}` cannot be autoincremented");
    }
}

/// The two lists are one list.
///
/// `worktable_codegen` maps exactly these types to atomics. If the set ever
/// grows on one side only, `check` starts accepting declarations that do not
/// build, which is the failure this module exists to prevent.
#[test]
fn every_autoincrement_type_is_accepted_by_check() {
    for good in worktable_dsl::AUTOINCREMENT_TYPES {
        let checked = worktable_dsl::check(&format!(
            "name: Probe, persist: false, columns: {{ id: {good} primary_key autoincrement }},"
        ));
        assert!(
            checked.is_acceptable(),
            "`{good}` is in AUTOINCREMENT_TYPES but check refuses it: {:?}",
            checked.diagnostics
        );
    }
}
