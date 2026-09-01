//! A schema can be read by a crate that is not the macro.
//!
//! This is the whole point of the extraction, so it gets an explicit test
//! rather than trusting that the code moved. Before it, every type here lived
//! in a `proc-macro = true` crate, which can export nothing but macros: the
//! parser existed, understood the grammar exactly as the compiler does, and was
//! unreachable. Anything wanting to read a declaration — a diagram, a migration
//! tool, a documentation generator — had to re-implement the grammar and drift
//! from it.
//!
//! An integration test is the right shape for that claim, because it compiles
//! as a separate crate. If `worktable_dsl` ever became a proc-macro crate
//! again, or stopped exporting these types, this file would fail to build,
//! which is a louder failure than an assertion.

use worktable_dsl::Parser;

/// Parse the shape of a real declaration, from outside.
#[test]
fn a_declaration_parses_into_a_model() {
    let tokens: proc_macro2::TokenStream = r#"
        name: Question,
        columns: {
            id: String primary_key,
            project_id: String,
            answered: bool,
        }
    "#
    .parse()
    .expect("the fixture is valid tokens");

    let mut parser = Parser::new(tokens);

    let name = parser.parse_name().expect("a name is declared");
    assert_eq!(name.to_string(), "Question");

    let columns = parser.parse_columns().expect("columns are declared");

    let mut declared: Vec<String> = columns.columns_map.keys().map(ToString::to_string).collect();
    declared.sort();
    assert_eq!(declared, ["answered", "id", "project_id"]);

    // The primary key is recognised as one rather than read as part of the
    // type, which is the parse most likely to be silently wrong.
    assert_eq!(
        columns.primary_keys.first().map(ToString::to_string),
        Some("id".to_owned()),
        "the primary key should be identified: {:?}",
        columns.primary_keys
    );
}

/// Declaration order comes from `field_positions`, never from `columns_map`.
///
/// `columns_map` is a `std::collections::HashMap`, whose iteration order Rust
/// randomises per process. Running the suite twice produced
/// `["answered", "project_id", "id"]` and then
/// `["project_id", "answered", "id"]` from the same input, so a consumer that
/// iterates it renders a different table on every run.
///
/// Nothing in this repository had noticed, and nothing needed to: the macro
/// does not care what order it sees columns in, and the parser's own tests
/// collect `columns_map` into another `HashMap` and assert membership. The
/// property was never specified because no caller existed to depend on it.
///
/// `field_positions` is the answer and is already there — it maps each column
/// to its position in the declaration. A diagram, a documentation page, or an
/// editor should sort by it. This test exists so the next consumer finds that
/// out here rather than by shipping a table that reorders itself.
#[test]
fn declaration_order_is_recovered_from_field_positions() {
    let tokens: proc_macro2::TokenStream = r#"
        name: Question,
        columns: {
            id: String primary_key,
            project_id: String,
            answered: bool,
        }
    "#
    .parse()
    .expect("the fixture is valid tokens");

    let mut parser = Parser::new(tokens);
    parser.parse_name().expect("a name is declared");
    let columns = parser.parse_columns().expect("columns are declared");

    let mut ordered: Vec<(usize, String)> = columns
        .field_positions
        .iter()
        .map(|(name, position)| (*position, name.to_string()))
        .collect();
    ordered.sort();

    let names: Vec<String> = ordered.into_iter().map(|(_, name)| name).collect();
    assert_eq!(
        names,
        ["id", "project_id", "answered"],
        "field_positions should recover the order the columns were declared in"
    );
}
