//! What the IR and the emitters promise, stated one claim per test.

use worktable_dsl::{Schema, declarations_in_source};

fn parse(source: &str) -> Schema {
    Schema::parse(source).unwrap_or_else(|error| panic!("{error}\n{source}"))
}

#[test]
fn columns_are_in_declaration_order() {
    // Declaration order is part of the public model contract. A consumer
    // walking the model or schema must see the same layout the author wrote.
    let schema = parse(
        "
        name: Answer,
        columns: {
            id: u64 primary_key autoincrement,
            project_id: u64,
            answered: bool,
        }
        ",
    );
    let names: Vec<&str> = schema.columns.iter().map(|column| column.name.as_str()).collect();
    assert_eq!(names, ["id", "project_id", "answered"]);
}

#[test]
fn queries_are_in_declaration_order() {
    // Query order is preserved as well, so schema tools and generated macro
    // output are both deterministic without rewriting the author's order.
    let schema = parse(
        "
        name: Sorted,
        columns: { id: u64 primary_key, a: u64, b: u64, c: u64 },
        queries: { update: { Charlie(c) by id, Alpha(a) by id, Bravo(b) by id } }
        ",
    );
    let names: Vec<&str> = schema.queries.updates.iter().map(|q| q.name.as_str()).collect();
    assert_eq!(names, ["Charlie", "Alpha", "Bravo"]);
}

#[test]
fn optional_is_recovered_from_the_type_the_model_stores() {
    // `optional` is not kept as a flag past the parser: `try_from_rows` folds
    // it into the type, which becomes `core::option::Option<T>`. Reading it
    // back out is the only way the emitter can write the keyword again.
    let schema = parse(
        "
        name: Optionals,
        columns: {
            id: u64 primary_key,
            nickname: String optional,
            age: u8,
        }
        ",
    );
    let nickname = schema.column("nickname").expect("declared");
    assert_eq!(nickname.ty, "String");
    assert!(nickname.optional);
    assert!(!schema.column("age").expect("declared").optional);
    assert!(schema.to_dsl().contains("nickname: String optional,"));
}

#[test]
fn an_omitted_persist_is_not_written_back() {
    // `Omitted` and `MemoryOnly` are different answers. The macro requires an
    // explicit `persist: false` before it will accept an index backend that
    // cannot persist, so writing one in would answer a question the author
    // deliberately left open.
    let omitted = parse("name: Omitted, columns: { id: u64 primary_key }");
    assert!(!omitted.to_dsl().contains("persist"));

    let explicit = parse("name: Explicit, persist: false, columns: { id: u64 primary_key }");
    assert!(explicit.to_dsl().contains("persist: false,"));
}

#[test]
fn only_a_deliberate_backend_choice_is_written_back() {
    // A primary key always carries a backend once parsed, because the model
    // fills the default in. Emitting `using worktables_index` everywhere would
    // round-trip correctly and read like noise.
    let default = parse("name: Default, columns: { id: u64 primary_key }");
    assert!(!default.to_dsl().contains("using"));

    let chosen = parse("name: Chosen, persist: false, columns: { id: u64 primary_key using congee }");
    assert!(chosen.to_dsl().contains("id: u64 primary_key using congee,"));
}

#[test]
fn the_emitted_body_wraps_into_an_invocation() {
    let schema = parse("name: Wrapped, columns: { id: u64 primary_key }");
    let invocation = schema.to_macro_invocation();
    assert!(invocation.starts_with("worktable! {\n"));
    assert!(invocation.ends_with("}\n"));
    assert!(invocation.contains("    name: Wrapped,"));
}

#[cfg(feature = "serde")]
#[test]
fn a_schema_survives_a_trip_through_serde() {
    // This is the property the migration planner rests on: a schema written
    // beside the data it describes has to come back the same, in a process
    // that never saw the Rust type it was generated from.
    let schema = parse(
        "
        name: Stored,
        version: 4,
        persist: true,
        partition_by: shard: u32,
        columns: {
            id: u64 primary_key autoincrement using congee,
            payload: String optional,
        },
        indexes: { payload_idx: payload unique },
        queries: { update: { Payload(payload) by id } },
        config: { page_size: 16384, row_derives: Clone, Debug }
        ",
    );
    let encoded = serde_json::to_string(&schema).expect("serialises");
    let decoded: Schema = serde_json::from_str(&encoded).expect("deserialises");
    assert_eq!(schema, decoded);
    assert_eq!(schema.to_dsl(), decoded.to_dsl());
}

#[test]
fn the_scanner_reads_both_delimiter_forms_and_nested_invocations() {
    // An invocation inside a function body is not an item, and real code puts
    // them there, so an item walk would quietly miss a table.
    let source = r#"
        worktable!(
            name: Braced,
            columns: { id: u64 primary_key },
        );

        mod inner {
            worktable! {
                name: Nested,
                columns: { id: u64 primary_key },
            }
        }

        fn in_a_body() {
            worktable! {
                name: InABody,
                columns: { id: u64 primary_key },
            }
        }
    "#;

    let found = declarations_in_source(source).expect("the source tokenises");
    let names: Vec<&str> = found.schemas.iter().map(|schema| schema.name.as_str()).collect();
    assert_eq!(names, ["Braced", "Nested", "InABody"]);
    assert!(found.is_complete());
    assert_eq!(found.found(), 3);
}

#[test]
fn the_scanner_sets_templates_aside_and_reports_real_failures() {
    // A `macro_rules!` body is not a declaration and its metavariables are not
    // mistakes. A declaration the compiler would accept but this cannot read is
    // a different matter, and dropping it silently would be worse than saying
    // so.
    let source = r#"
        macro_rules! table_for {
            ($name:ident, $backend:ident) => {
                worktable! {
                    name: $name,
                    columns: { id: u64 primary_key using $backend },
                }
            };
        }

        worktable! {
            name: Broken,
            columns: { id: u64 primary_key },
            nonsense: { whatever: 1 },
        }
    "#;

    let found = declarations_in_source(source).expect("the source tokenises");
    assert!(found.schemas.is_empty());
    assert_eq!(found.templates.len(), 1);
    assert_eq!(found.rejected.len(), 1);
    assert!(!found.is_complete());
    // The rejection names the token it tripped on. The old text said
    // "Unexpected identifier" for anything at all, including a `,`, which
    // named the wrong category of token and sent the reader hunting for a
    // misspelled keyword.
    let reason = found.rejected[0].1.to_string();
    assert!(
        reason.contains("nonsense"),
        "the rejection should name the block it did not recognise, got: {reason}"
    );
}

/// A primary key's generator changing is a migration, not a no-op.
///
/// `ColumnSpec` has always recorded the generator, but `diff_columns` compared
/// type, optionality and position only. Moving a key between `autoincrement`
/// and caller-supplied therefore planned as no change at all, while it changes
/// who is responsible for uniqueness and what generator state a persisted
/// table has to carry. A migration tool would have reported nothing to do.
#[test]
fn a_primary_key_generator_change_is_planned() {
    let with_gen = worktable_dsl::Schema::parse("name: T, columns: { id: u64 primary_key autoincrement },")
        .expect("schema with a generator");
    let without =
        worktable_dsl::Schema::parse("name: T, columns: { id: u64 primary_key },").expect("schema without one");

    let changes = worktable_dsl::plan(std::slice::from_ref(&with_gen), std::slice::from_ref(&without));
    assert!(
        !changes.is_empty(),
        "dropping the generator planned as no change at all"
    );

    let described = format!("{changes:?}");
    assert!(
        described.contains("PrimaryKeyGeneratorChanged"),
        "the change must name the generator: {described}"
    );

    // And it needs a human decision rather than being applied silently.
    let worktable_dsl::TableChange::Changed(diff) = &changes[0] else {
        panic!("expected a changed table, got {:?}", changes[0]);
    };
    assert_eq!(
        diff.cost(),
        worktable_dsl::Cost::NeedsIntent,
        "changing how identities are assigned cannot be a mechanical migration"
    );

    // Symmetric: adding one is equally a change.
    assert!(
        !worktable_dsl::plan(std::slice::from_ref(&without), std::slice::from_ref(&with_gen)).is_empty(),
        "adding a generator planned as no change either"
    );
}
