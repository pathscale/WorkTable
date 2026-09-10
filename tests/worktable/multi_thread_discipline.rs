//! A concurrency test on a current-thread runtime is not a concurrency test.
//!
//! `#[tokio::test]` with no arguments builds a **current-thread** runtime.
//! Tasks spawned inside it interleave only at await points, on one thread, so
//! two writers never overlap and no data race between them can be observed.
//! The test still passes. It simply stops proving what its name says.
//!
//! This repo has been bitten by it. Commit 2702c06 records two tests that
//! shared one table directory and only started failing once the persistence
//! worker moved off `tokio::spawn`: while that worker ran on the test's own
//! current-thread runtime, the two tables' writes never overlapped in time and
//! the corruption stayed invisible. The single-threaded harness was hiding a
//! real defect, and the runtime-backend work is exactly the kind of change
//! that moves work between runtimes again.
//!
//! So this file is the verifier rather than a note in a doc comment: a rule
//! nothing checks is a rule that is quietly false. It scans the test sources
//! for `#[tokio::test]` bodies that call `tokio::spawn` and fails on any that
//! is not already on the list below.
//!
//! `std::thread::spawn` is deliberately not flagged. An OS thread is genuinely
//! parallel whatever the harness runtime is doing, which is why
//! `tests/worktable/concurrency.rs` and `tests/worktable/partitioned.rs` are
//! honest despite their bare `#[tokio::test]` attributes.
//!
//! The fix for a flagged test is one line:
//!
//! ```text
//! #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//! ```

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Tests that spawn tokio tasks from a current-thread runtime today.
///
/// Empty, and meant to stay that way. Every entry was coverage weaker than its
/// name suggested: six had "concurrent" or "races" in the name or doc comment
/// and proved no such thing, and the two `base.rs` ones asserted only that a
/// spawned mutation future is `Send` and joins.
///
/// The list is retained rather than deleted because the check is two-sided.
/// A new offender fails against the empty list, which is the point, and an
/// entry that stops offending also fails, so re-adding one to silence a
/// failure cannot be done quietly.
const KNOWN_CURRENT_THREAD_SPAWNERS: &[(&str, &str)] = &[];

fn tests_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests")
}

fn rust_sources(dir: &Path, into: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("the tests directory is readable") {
        let path = entry.expect("a readable directory entry").path();
        if path.is_dir() {
            rust_sources(&path, into);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            into.push(path);
        }
    }
}

/// The body of the item that follows `lines[start]`, by brace balance.
///
/// Crude on purpose. A brace inside a string literal would confuse it, and the
/// alternative is a parser dependency for a lint that has to stay cheap enough
/// that nobody is tempted to delete it. A miscount can only mis-scope a body,
/// which shows up as a name this file cannot explain rather than as silence.
fn body_after(lines: &[&str], start: usize) -> String {
    let mut depth = 0i32;
    let mut opened = false;
    let mut body = Vec::new();
    for line in &lines[start + 1..] {
        body.push(*line);
        depth += line.matches('{').count() as i32;
        depth -= line.matches('}').count() as i32;
        if line.contains('{') {
            opened = true;
        }
        if opened && depth <= 0 {
            break;
        }
    }
    body.join("\n")
}

fn fn_name(body: &str) -> Option<String> {
    let (_, after) = body.split_once("fn ")?;
    let name: String = after.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
    (!name.is_empty()).then_some(name)
}

/// Every `#[tokio::test]` body in the tree that reaches for `tokio::spawn`,
/// keyed by path relative to `tests/` so the entries read like the allowlist.
fn current_thread_spawners() -> BTreeSet<(String, String)> {
    let root = tests_root();
    let mut sources = Vec::new();
    rust_sources(&root, &mut sources);
    sources.sort();

    let mut found = BTreeSet::new();
    for path in sources {
        let source = std::fs::read_to_string(&path).expect("a readable test source");
        let lines: Vec<&str> = source.lines().collect();
        let relative = path
            .strip_prefix(&root)
            .expect("every source is under tests/")
            .to_string_lossy()
            .replace('\\', "/");

        for (index, line) in lines.iter().enumerate() {
            // Exactly the bare attribute. Anything carrying arguments has
            // already said what runtime it wants.
            if line.trim() != "#[tokio::test]" {
                continue;
            }
            let body = body_after(&lines, index);
            if !body.contains("tokio::spawn") {
                continue;
            }
            if let Some(name) = fn_name(&body) {
                found.insert((relative.clone(), name));
            }
        }
    }
    found
}

#[test]
fn no_new_test_spawns_tokio_tasks_from_a_current_thread_runtime() {
    let found = current_thread_spawners();
    let known: BTreeSet<(String, String)> = KNOWN_CURRENT_THREAD_SPAWNERS
        .iter()
        .map(|(file, name)| ((*file).to_owned(), (*name).to_owned()))
        .collect();

    let new: Vec<_> = found.difference(&known).collect();
    assert!(
        new.is_empty(),
        "these tests spawn tokio tasks on a current-thread runtime, so their tasks \
         never actually overlap and the concurrency they claim to test is not tested: \
         {new:#?}\nUse #[tokio::test(flavor = \"multi_thread\", worker_threads = 4)]."
    );

    let fixed: Vec<_> = known.difference(&found).collect();
    assert!(
        fixed.is_empty(),
        "these entries no longer spawn from a current-thread runtime, so remove them \
         from KNOWN_CURRENT_THREAD_SPAWNERS: {fixed:#?}"
    );
}
