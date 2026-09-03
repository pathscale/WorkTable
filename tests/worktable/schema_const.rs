//! Is the embedded schema const actually usable, or just emitted?
//!
//! Each generated table carries its own declaration as text. The point is that
//! a migration tool or designer can read what a compiled binary was built
//! from, without the source. That only works if the text parses back into the
//! schema it came from.

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: Embedded,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement,
        owner: u64,
        label: String,
    },
    indexes: {
        owner_idx: owner,
        label_idx: label unique,
    },
);

#[test]
fn the_embedded_declaration_parses_back_into_the_same_schema() {
    // The const the macro baked in.
    let text: &str = EMBEDDED_SCHEMA;
    assert!(!text.is_empty(), "the table carries no declaration");

    let parsed = worktable::worktable_dsl::Schema::parse(text).expect("the embedded text must parse");

    assert_eq!(parsed.name, "Embedded");
    let cols: Vec<&str> = parsed.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(cols, ["id", "owner", "label"], "columns lost in the round trip");

    let idx: Vec<(&str, bool)> = parsed.indexes.iter().map(|i| (i.name.as_str(), i.unique)).collect();
    assert_eq!(idx, [("owner_idx", false), ("label_idx", true)], "indexes lost");

    // And it survives a second round trip, which is what a migration tool does
    // when it compares a checkout against what a binary was built from.
    let again = worktable::worktable_dsl::Schema::parse(&parsed.to_dsl()).expect("re-emitted text must parse");
    assert_eq!(again, parsed, "schema is not stable across emit/parse");
}
