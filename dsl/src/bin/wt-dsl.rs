//! `wt-dsl`: the schema language from a command line.
//!
//! Everything here already existed as a library function with no way to reach
//! it from a shell. That gap is what blocks a second implementation: a
//! TypeScript emitter cannot be compared against the Rust one without a
//! process to pipe text through, so the two would drift with nothing to say so.
//!
//! Reads stdin, writes stdout, exits non-zero on refusal.
//!
//! ```sh
//! wt-dsl parse  < schema.wt     # canonical text: the byte-exact target
//! wt-dsl check  < schema.wt     # diagnostics with byte ranges
//! wt-dsl scan   < lib.rs        # find worktable! declarations in Rust source
//! wt-dsl diff   old.wt new.wt   # what changed, and what it costs
//! ```

use std::io::{Read, Write};
use std::process::ExitCode;

fn read_stdin() -> String {
    let mut buf = String::new();
    std::io::stdin().read_to_string(&mut buf).expect("stdin is readable");
    buf
}

fn fail(message: &str) -> ExitCode {
    let _ = writeln!(std::io::stderr(), "{message}");
    ExitCode::FAILURE
}

/// Parse, then emit the canonical text.
///
/// The round trip rather than a bare parse: a second implementation needs
/// bytes to compare against, and `to_dsl` is that target.
fn parse() -> ExitCode {
    match worktable_dsl::Schema::parse(&read_stdin()) {
        Ok(schema) => {
            print!("{}", schema.to_dsl());
            ExitCode::SUCCESS
        }
        Err(error) => fail(&format!("parse failed: {error}")),
    }
}

/// Every rule violation, each with the byte range it applies to.
///
/// Exits zero only when the macro would accept the declaration, so this works
/// as a lint in CI and not only inside an editor.
fn check() -> ExitCode {
    let checked = worktable_dsl::check(&read_stdin());
    for diagnostic in &checked.diagnostics {
        let at = match &diagnostic.span {
            Some(span) => format!("{}..{}", span.start, span.end),
            None => "-".to_string(),
        };
        let _ = writeln!(std::io::stderr(), "{:?} {at}: {}", diagnostic.stage, diagnostic.message);
    }
    if checked.is_acceptable() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Find `worktable!` declarations in Rust source, including inside function
/// bodies, which an item walk would miss.
///
/// A declaration that does not parse is reported rather than dropped: silently
/// losing a table the compiler accepts is worse than saying it could not be
/// read.
fn scan() -> ExitCode {
    match worktable_dsl::declarations_in_source(&read_stdin()) {
        Ok(found) => {
            for schema in &found.schemas {
                println!("{}", schema.name);
            }
            for template in &found.templates {
                let _ = writeln!(std::io::stderr(), "note: skipped a macro_rules! template: {template}");
            }
            for (text, error) in &found.rejected {
                let _ = writeln!(std::io::stderr(), "warning: unreadable declaration: {error}\n{text}");
            }
            if found.is_complete() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(error) => fail(&format!("scan failed: {error}")),
    }
}

/// What changed between two schemas, and what applying it costs.
fn diff(old_path: &str, new_path: &str) -> ExitCode {
    let read = |path: &str| -> Result<worktable_dsl::Schema, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
        worktable_dsl::Schema::parse(&text).map_err(|e| format!("{path}: {e}"))
    };
    let (old, new) = match (read(old_path), read(new_path)) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(error), _) | (_, Err(error)) => return fail(&error),
    };
    let changes = worktable_dsl::plan(std::slice::from_ref(&old), std::slice::from_ref(&new));
    if changes.is_empty() {
        println!("no change");
        return ExitCode::SUCCESS;
    }
    for change in &changes {
        println!("{change:?}");
    }
    ExitCode::SUCCESS
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
    match borrowed.as_slice() {
        ["parse"] => parse(),
        ["check"] => check(),
        ["scan"] => scan(),
        ["diff", old, new] => diff(old, new),
        _ => fail(
            "usage:\n  wt-dsl parse  < schema.wt\n  wt-dsl check  < schema.wt\n  \
             wt-dsl scan   < source.rs\n  wt-dsl diff   <old> <new>",
        ),
    }
}
