//! `worktable-parse` — read a declaration on stdin, write its canonical text to stdout.
//!
//! The input is the macro **body**: `name: Foo, columns: { .. }`, without `worktable!` or the
//! surrounding braces, which is what [`worktable_dsl::Schema::parse`] accepts and what
//! [`worktable_dsl::Schema::to_dsl`] produces.
//!
//! It exists so that an emitter written in another language can be checked against this one.
//! Two implementations of a language drift unless something compares them, and the comparison
//! has to be byte-exact: they can agree on every meaning and still disagree on every character
//! a person reads.
//!
//! It runs [`worktable_dsl::check`] rather than `Schema::parse`, and that is the whole point.
//! `parse` answers "is this a declaration"; `check` answers "would the macro accept it", which
//! is the question an emitter has to get right. `page_size: 4096` beside `persist: true` parses
//! perfectly and the macro refuses it, so a parse-only binary would report green for output
//! that does not compile.

use std::io::{Read, Write};

fn main() {
    let mut source = String::new();
    if std::io::stdin().read_to_string(&mut source).is_err() {
        eprintln!("worktable-parse: could not read stdin");
        std::process::exit(2);
    }

    let checked = worktable_dsl::check(&source);

    for d in &checked.diagnostics {
        let stage = match d.stage {
            worktable_dsl::Stage::Grammar => "grammar",
            worktable_dsl::Stage::Rules => "rules",
        };
        match d.span {
            // Byte ranges need the `spans` feature. Absence of a location is never absence of
            // a problem, so the message is printed either way.
            Some(s) => eprintln!("{stage} [{}..{}]: {}", s.start, s.end, d.message),
            None => eprintln!("{stage}: {}", d.message),
        }
    }

    let Some(schema) = checked.schema else {
        std::process::exit(1);
    };
    if !checked.diagnostics.is_empty() {
        // A rule violation still yields a schema — an editor has to draw it so somebody can
        // fix it — but it is not text the macro would accept, so it is not success.
        std::process::exit(1);
    }

    if std::io::stdout().write_all(schema.to_dsl().as_bytes()).is_err() {
        std::process::exit(2);
    }
}
