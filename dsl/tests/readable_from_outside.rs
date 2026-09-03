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

/// Both public views retain declaration order. `columns_map` is directly
/// iterable by consumers; `field_positions` remains the explicit numeric view.
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

    let map_names: Vec<String> = columns.columns_map.keys().map(ToString::to_string).collect();
    assert_eq!(map_names, ["id", "project_id", "answered"]);

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
