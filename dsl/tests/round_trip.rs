//! The round trip, checked against every schema the project actually declares.
//!
//! A hand-written fixture proves the emitter handles the cases its author
//! thought of. The repository already contains 128 `worktable!` invocations
//! written by people who were not thinking about this crate at all, which is a
//! better corpus than anything written here would be: they use the grammar the
//! way it is really used, including the corners.
//!
//! The property is `parse(emit(parse(source))) == parse(source)`. It is stated
//! on the parsed form rather than the text because the emitter does not
//! reproduce formatting or comments and is not trying to: what has to survive
//! is the meaning. Comparing text would fail on whitespace and would say
//! nothing about whether anything was lost.
//!
//! This runs through `declarations_in_source`, so it is also the evidence that
//! the scanner finds what is there.

use std::fs;
use std::path::{Path, PathBuf};

use worktable_dsl::{Schema, declarations_in_source};

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

    let mut checked = 0;
    let mut templates = 0;
    let mut rejected = Vec::new();

    for file in &files {
        let Ok(contents) = fs::read_to_string(file) else {
            continue;
        };
        if !contents.contains("worktable!") {
            continue;
        }
        let Ok(found) = declarations_in_source(&contents) else {
            continue;
        };

        templates += found.templates.len();
        for (source, error) in found.rejected {
            rejected.push(format!("  {}: {error}\n    {source}", file.display()));
        }

        for schema in found.schemas {
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
    }

    assert!(
        rejected.is_empty(),
        "{} declaration(s) the parser rejected:\n{}",
        rejected.len(),
        rejected.join("\n")
    );
    assert!(checked >= 100, "only {checked} declarations were checked");
    assert!(
        templates >= 12,
        "the `macro_rules!` templates stopped being found, so the filter is now hiding \
         something else: {templates}"
    );
}

#[test]
fn reading_the_same_declaration_twice_gives_the_same_schema() {
    // Columns and query maps retain declaration order, so independently parsed
    // schemas and their emitted text must agree exactly.
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
