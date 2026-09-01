//! The round trip, checked against every schema the project actually declares.
//!
//! A hand-written fixture proves the emitter handles the cases its author
//! thought of. The repository already contains 128 `worktable!` invocations
//! written by people who were not thinking about this crate at all, which is a
//! better corpus than anything written here would be: they use the grammar the
//! way it is really used, including the corners.
//!
//! Both delimiter forms appear in the corpus (`worktable! { .. }` and
//! `worktable!( .. )`), so this accepts either.
//!
//! The property is `parse(emit(parse(source))) == parse(source)`. It is stated
//! on the parsed form rather than the text because the emitter does not
//! reproduce formatting or comments and is not trying to: what has to survive
//! is the meaning. Comparing text would fail on whitespace and would say
//! nothing about whether anything was lost.

use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr as _;

use proc_macro2::{Delimiter, TokenStream, TokenTree};
use worktable_dsl::Schema;

/// Pull every `worktable! { .. }` body out of a token stream, including the
/// ones nested inside modules, functions and other macros.
///
/// This walks tokens rather than using `syn`'s item tree because an invocation
/// inside a function body is not an item, and several of the corpus files put
/// one there.
fn collect_invocations(tokens: TokenStream, found: &mut Vec<TokenStream>) {
    let trees: Vec<TokenTree> = tokens.into_iter().collect();
    let mut index = 0;
    while index < trees.len() {
        if let TokenTree::Ident(ident) = &trees[index]
            && ident == "worktable"
            && let Some(TokenTree::Punct(bang)) = trees.get(index + 1)
            && bang.as_char() == '!'
            && let Some(TokenTree::Group(body)) = trees.get(index + 2)
            && body.delimiter() != Delimiter::None
        {
            found.push(body.stream());
            index += 3;
            continue;
        }
        if let TokenTree::Group(group) = &trees[index] {
            collect_invocations(group.stream(), found);
        }
        index += 1;
    }
}

/// Whether a body is a `macro_rules!` template rather than a declaration.
///
/// A dozen of the corpus's invocations sit inside `macro_rules!` and read
/// `name: $name, ... using $backend`. Those are not schemas: the metavariables
/// stand for text that only exists once the outer macro expands, and no parser
/// for this grammar can or should accept them.
fn is_macro_template(tokens: &TokenStream) -> bool {
    tokens.clone().into_iter().any(|tree| match tree {
        TokenTree::Punct(punct) => punct.as_char() == '$',
        TokenTree::Group(group) => is_macro_template(&group.stream()),
        _ => false,
    })
}
fn rust_files(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn every_declaration_in_the_repository_survives_a_round_trip() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate is in the workspace");

    let mut files = Vec::new();
    for directory in ["src", "tests", "benches", "examples", "codegen"] {
        rust_files(&repository.join(directory), &mut files);
    }
    files.sort();
    assert!(!files.is_empty(), "found no sources to read");

    let mut declarations = Vec::new();
    let mut templates = 0;
    for file in &files {
        let Ok(contents) = fs::read_to_string(file) else {
            continue;
        };
        if !contents.contains("worktable!") {
            continue;
        }
        let Ok(tokens) = TokenStream::from_str(&contents) else {
            continue;
        };
        let mut found = Vec::new();
        collect_invocations(tokens, &mut found);
        for body in found {
            if is_macro_template(&body) {
                templates += 1;
                continue;
            }
            declarations.push((file.clone(), body));
        }
    }

    assert!(
        declarations.len() >= 100,
        "expected the repository's declarations to be found, got {}",
        declarations.len()
    );

    let mut unparsed = Vec::new();
    let mut checked = 0;
    for (file, body) in declarations {
        let source = body.to_string();
        let Ok(schema) = Schema::from_tokens(body) else {
            unparsed.push((file, source));
            continue;
        };

        let emitted = schema.to_dsl();
        let reparsed = Schema::parse(&emitted).unwrap_or_else(|error| {
            panic!(
                "emitted declaration for `{}` from {} does not parse: {error}\n{emitted}",
                schema.name,
                file.display()
            )
        });
        assert_eq!(
            schema,
            reparsed,
            "round trip changed `{}` from {}\n{emitted}",
            schema.name,
            file.display()
        );
        checked += 1;
    }

    assert!(
        unparsed.is_empty(),
        "{} declaration(s) the parser rejected:\n{}",
        unparsed.len(),
        unparsed
            .iter()
            .map(|(file, source)| format!("  {}: {source}", file.display()))
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert!(checked >= 100, "only {checked} declarations were checked");
    assert!(
        templates >= 12,
        "the `macro_rules!` templates stopped being found, so the filter is now hiding something else: {templates}"
    );
}

#[test]
fn reading_the_same_declaration_twice_gives_the_same_schema() {
    // `Columns::columns_map` and the query maps are `HashMap`s, whose iteration
    // order Rust randomises per process. Within one process that randomisation
    // is fixed, so this catches an ordering mistake only if the schema is built
    // from two independently-hashed maps; the ordering guarantee that matters
    // across processes is the one `columns_are_in_declaration_order` states.
    let source = "
        name: Repeatable,
        columns: { id: u64 primary_key, a: u64, b: u64, c: String },
        queries: { update: { A(a) by id, B(b) by id, C(c) by id } }
    ";
    let first = Schema::parse(source).expect("parses");
    let second = Schema::parse(source).expect("parses");
    assert_eq!(first, second);
    assert_eq!(first.to_dsl(), second.to_dsl());
}
