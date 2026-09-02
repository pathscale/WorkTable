//! Every block accepts a trailing comma, and every block accepts its absence.
//!
//! Three of the six block parsers consumed the comma that may follow their
//! block and three did not, so `config: { .. },` reached the top-level
//! dispatch as a `,` token and died as "Unexpected identifier". `config`,
//! `delete` and `in_place` happen to be written last in every declaration in
//! this repository, which is the only reason it had not been hit.
//!
//! It matters for the designer specifically: the emitter writes a declaration
//! and the parser reads it back, so an asymmetry here is a round trip that
//! fails on the tool's own output the moment block order changes.
//!
//! Each case below fails on the pre-fix parser. That was checked by reverting
//! the three `try_parse_comma()` calls and watching this file go red, rather
//! than by assuming a new test tests something.

use worktable_dsl::Schema;

/// The block that must not be last: everything after `config` was unreachable.
#[test]
fn a_comma_after_config_is_accepted() {
    let schema = Schema::parse(
        "name: Trailing,
         columns: { id: u64 primary_key, payload: String },
         config: { page_size: 4096 },",
    )
    .expect("a comma after the `config` block is a comma, not an identifier");

    assert_eq!(schema.name, "Trailing");
    assert_eq!(schema.config.page_size, Some(4096));
}

/// `config` written before `queries`, which the comma asymmetry forbade.
#[test]
fn config_does_not_have_to_be_written_last() {
    let schema = Schema::parse(
        "name: Ordered,
         columns: { id: u64 primary_key, name: String },
         config: { page_size: 8192 },
         queries: { update: { Renamed(name) by id, } }",
    )
    .expect("block order should not depend on which parser eats a comma");

    assert_eq!(schema.config.page_size, Some(8192));
    assert_eq!(schema.queries.updates.len(), 1);
}

/// The same asymmetry inside `queries`, where `delete` and `in_place` sat.
#[test]
fn a_comma_after_delete_or_in_place_is_accepted() {
    let schema = Schema::parse(
        "name: Inner,
         columns: { id: u64 primary_key, name: String },
         queries: {
             delete: { ByName() by name, },
             in_place: { SetName(name) by id, },
             update: { Renamed(name) by id, }
         }",
    )
    .expect("`delete` and `in_place` should not have to be written last either");

    assert_eq!(schema.queries.deletes.len(), 1);
    assert_eq!(schema.queries.in_place.len(), 1);
    assert_eq!(schema.queries.updates.len(), 1);
}

/// Omitting the comma stays valid. The fix is permissive, not a new rule.
#[test]
fn omitting_the_comma_is_still_accepted() {
    let schema = Schema::parse(
        "name: NoComma,
         columns: { id: u64 primary_key },
         config: { page_size: 4096 }",
    )
    .expect("the form the emitter writes must keep parsing");

    assert_eq!(schema.config.page_size, Some(4096));
}

/// A genuinely unexpected token names itself now.
///
/// The old message said "Unexpected identifier" for a `,`, which is what made
/// this class of bug cost an afternoon: the text names the wrong category of
/// token and sends you looking for a misspelled keyword.
#[test]
fn an_unexpected_token_is_named() {
    let error =
        Schema::parse("name: Bad, columns: { id: u64 primary_key }, wat: { x: 1 }").expect_err("`wat` is not a block");

    let message = error.to_string();
    assert!(
        message.contains("wat"),
        "the error should name the token it saw, got: {message}"
    );
}
