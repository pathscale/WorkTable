//! The Mermaid emitter and the relation guessing it rests on.
//!
//! Separate from `schema.rs` because this is the one part of `worktable_dsl`
//! that is not about the `worktable!` grammar. The language has no foreign
//! keys, so `infer_relations` guesses links from a naming convention that
//! WorkTable does not enforce and has no opinion about; that mapping belongs
//! to whatever application draws the diagram. Hence the `uml` feature, and
//! hence these tests living behind it rather than beside the ones that test
//! what the compiler actually enforces.
#![cfg(feature = "uml")]

use worktable_dsl::{Schema, infer_relations, schemas_to_mermaid};

fn parse(source: &str) -> Schema {
    Schema::parse(source).unwrap_or_else(|error| panic!("{error}\n{source}"))
}
#[test]
fn mermaid_marks_the_key_the_generator_and_the_indexes() {
    let schema = parse(
        "
        name: Account,
        version: 3,
        persist: true,
        columns: {
            id: u64 primary_key autoincrement,
            email: String,
            tenant: u64,
            nickname: String optional,
        },
        indexes: {
            email_idx: email unique,
            tenant_idx: tenant,
        }
        ",
    );
    let diagram = schema.to_mermaid();

    assert!(diagram.starts_with("classDiagram\n"));
    assert!(diagram.contains("class Account {"));
    assert!(diagram.contains("<<v3 persisted>>"));
    assert!(diagram.contains("+id : u64 [PK, autoincrement]"));
    assert!(diagram.contains("+email : String [UK email_idx]"));
    assert!(diagram.contains("+tenant : u64 [IX tenant_idx]"));
    // Mermaid spells a generic with tildes.
    assert!(diagram.contains("+nickname : Option~String~"));
}

#[test]
fn mermaid_draws_queries_as_operations() {
    let schema = parse(
        "
        name: Ledger,
        columns: { id: u64 primary_key, balance: f64, note: String },
        queries: {
            update: { Balance(balance) by id }
            delete: { ById() by id }
        }
        ",
    );
    let diagram = schema.to_mermaid();
    assert!(diagram.contains("+update_Balance(balance) by_id"));
    assert!(diagram.contains("+delete_ById() by_id"));
}

#[test]
fn mermaid_puts_the_partition_key_in_a_note_not_a_column() {
    // The routing key is stored once per partition rather than once per row,
    // and no query can name it, so drawing it as an attribute would be a lie
    // about where the data lives.
    let schema = parse(
        "
        name: Price,
        partition_by: symbol_id: u16,
        columns: { exchange_id: u8 primary_key, bid: f64 }
        ",
    );
    let diagram = schema.to_mermaid();
    assert!(diagram.contains("note for Price \"partitioned by symbol_id: u16\""));
    assert!(!diagram.contains("symbol_id : u16"));
}

fn related() -> Vec<Schema> {
    vec![
        parse("name: Project, columns: { id: u64 primary_key autoincrement, title: String }"),
        parse(
            "
            name: Answer,
            columns: {
                id: u64 primary_key autoincrement,
                project_id: u64,
                body: String,
            }
            ",
        ),
    ]
}

#[test]
fn a_reference_is_inferred_from_the_naming_convention() {
    let relations = infer_relations(&related());
    assert_eq!(relations.len(), 1);
    assert_eq!(relations[0].from, "Answer");
    assert_eq!(relations[0].column, "project_id");
    assert_eq!(relations[0].to, "Project");
    assert_eq!(relations[0].to_column, "id");
}

#[test]
fn an_inferred_reference_is_drawn_as_a_dependency() {
    // Dashed, because the declaration does not say this. A solid association
    // would claim the schema language has foreign keys, and it does not.
    let diagram = schemas_to_mermaid(&related());
    assert!(diagram.contains("Answer ..> Project : project_id"));
}

#[test]
fn a_name_collision_on_a_different_type_is_not_a_reference() {
    let schemas = vec![
        parse("name: Project, columns: { id: u64 primary_key, title: String }"),
        parse("name: Answer, columns: { id: u64 primary_key, project_id: String }"),
    ];
    assert!(infer_relations(&schemas).is_empty());
}

#[test]
fn a_composite_key_is_not_guessed_at() {
    // There is no single column to point the arrow at, and picking one part of
    // the key would be worse than drawing nothing.
    let schemas = vec![
        parse("name: Project, columns: { tenant_id: u64 primary_key, id: u64 primary_key }"),
        parse("name: Answer, columns: { id: u64 primary_key, project_id: u64 }"),
    ];
    assert!(infer_relations(&schemas).is_empty());
}

#[test]
fn a_key_column_is_not_read_as_a_reference() {
    // `project_id` here is half of this table's own identity, not a link out.
    let schemas = vec![
        parse("name: Project, columns: { id: u64 primary_key, title: String }"),
        parse("name: Answer, columns: { project_id: u64 primary_key, seq: u64 primary_key }"),
    ];
    assert!(infer_relations(&schemas).is_empty());
}
