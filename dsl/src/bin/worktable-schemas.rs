//! `worktable-schemas` — every `worktable!` declaration under a directory, as JSON.
//!
//! Each entry carries the schema and the canonical text this crate emits for it, so a consumer
//! written in another language can emit from the same model and compare bytes without needing
//! to reproduce the scan or the parse.
//!
//! It exists to make the cross-implementation check run against *real* declarations rather than
//! against the handful anybody thinks to invent. A hand-written corpus tests the cases its
//! author already understood; this repository's own tables are the ones people actually wrote,
//! and they are where an emitter's unexamined assumption shows up.
//!
//! Requires the `serde` feature, which is off by default because `worktable_dsl` is compiled
//! for the host as part of `worktable_codegen` before anything else in a dependent's build.

use std::io::Write;
use std::path::Path;

fn main() {
    let root = std::env::args().nth(1).unwrap_or_else(|| ".".to_string());

    let mut entries: Vec<String> = Vec::new();
    let mut templates = 0usize;
    let mut rejected = 0usize;
    walk(Path::new(&root), &mut entries, &mut templates, &mut rejected);

    // Sorted, so the output is a function of the tree's contents rather than of the order a
    // directory happened to be read in. A consumer diffing two runs should see only real change.
    entries.sort();

    let body = entries.join(",\n    ");
    let out = format!(
        "{{\n  \"templates\": {templates},\n  \"rejected\": {rejected},\n  \"schemas\": [\n    {body}\n  ]\n}}\n"
    );
    if std::io::stdout().write_all(out.as_bytes()).is_err() {
        std::process::exit(2);
    }
}

fn walk(dir: &Path, entries: &mut Vec<String>, templates: &mut usize, rejected: &mut usize) {
    let Ok(read) = std::fs::read_dir(dir) else { return };
    for entry in read.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if path.is_dir() {
            // `target` is build output and would multiply the scan by every vendored crate.
            if name == "target" || name == ".git" || name == "node_modules" {
                continue;
            }
            walk(&path, entries, templates, rejected);
            continue;
        }
        if path.extension().is_none_or(|e| e != "rs") {
            continue;
        }
        let Ok(source) = std::fs::read_to_string(&path) else { continue };
        if !source.contains("worktable!") {
            continue;
        }
        let Ok(found) = worktable_dsl::declarations_in_source(&source) else { continue };
        *templates += found.templates.len();
        *rejected += found.rejected.len();
        for schema in found.schemas {
            let dsl = serde_json::to_string(&schema.to_dsl()).expect("a string serialises");
            let model = serde_json::to_string(&schema).expect("the schema serialises");
            entries.push(format!(
                "{{ \"file\": {}, \"dsl\": {dsl}, \"schema\": {model} }}",
                serde_json::to_string(&path.display().to_string()).expect("a path serialises")
            ));
        }
    }
}
