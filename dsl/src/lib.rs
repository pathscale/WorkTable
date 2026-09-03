//! The `worktable!` schema language: its model, and the parser that reads it.
//!
//! # Why this is its own crate
//!
//! This was `codegen/src/common`, inside `worktable_codegen`, which is declared
//! `proc-macro = true`. A proc-macro crate can export nothing but macros, so
//! every type here — the columns, the primary key, the indexes, the queries —
//! was unreachable from any other crate no matter how public it was declared.
//! `mod common` was not even public at that crate's root.
//!
//! The consequence was not theoretical. A schema is written down exactly once,
//! in a `worktable!` invocation, and anything that wants to *read* one — a
//! diagram, a migration tool, a documentation generator, an editor — could not
//! reach the parser that already understood it. The available options were to
//! re-implement the grammar and drift from it, or to do without.
//!
//! Nothing here changed in the move. The model and the parser are the ones the
//! macro has always used, and the macro still uses these: `worktable_codegen`
//! depends on this crate, so there is one grammar rather than a copy that can
//! disagree with the compiler about what a schema means.
//!
//! # Reading a declaration
//!
//! ```ignore
//! use worktable_dsl::Parser;
//! use syn::parse_str;
//!
//! let tokens: proc_macro2::TokenStream = parse_str(source)?;
//! let mut parser = Parser::new(tokens);
//! let name = parser.parse_name()?;
//! let columns = parser.parse_columns()?;
//! ```
//!
//! The parser is token-based rather than textual, so comments and string
//! literals are handled by `proc_macro2` rather than by hand. The schema files
//! this reads are more comment than code, which makes that difference matter.
//!
//! # What is in scope here, and what is not
//!
//! This crate is the WorkTable universe: one `worktable!` declaration, the
//! grammar it must satisfy, and the rules the macro enforces over it. That is
//! [`Parser`] and [`model`], [`Schema`] and its text emitter, [`validate`] and
//! [`check`], and [`schema::Diff`], which prices a schema change in terms of
//! what the storage engine has to do about it. All of those have a single
//! correct answer that WorkTable owns.
//!
//! The mapping *between* tables is a different layer and it is not in scope.
//! The language has no foreign keys: a column called `project_id` is a `u64`
//! like any other, and nothing in WorkTable enforces, records, or checks a
//! relationship between two tables. `infer_relations` guesses those links from
//! a naming convention, and the guess belongs next to whatever application
//! does enforce the convention. It is behind the off-by-default `uml` feature
//! for that reason: available, tested, and not something this crate asserts.
//!
//! An application building on this should treat a `Relation` as a suggestion
//! to confirm, never as a fact recovered from the schema, because there is no
//! fact there to recover.
//!
//! # Features
//!
//! All off by default. `worktable_codegen` depends on this crate and is a proc
//! macro, so it is compiled for the host before anything else in a dependent's
//! build: anything unconditional here is added to every WorkTable user's first
//! compile, to serve consumers who are not the compiler.
//!
//! - `serde` — `Serialize`/`Deserialize` on the IR, for storing a schema next
//!   to the data it describes or sending one over a socket.
//! - `spans` — byte ranges on [`check`] diagnostics, for an editor that wants
//!   to underline the offending token.
//! - `uml` — Mermaid class diagrams and the relation guessing above.

pub mod check;
pub mod model;
pub mod parser;
pub mod schema;
pub mod validate;

pub use check::{Checked, Diagnostic, SourceSpan, Stage, check};
#[allow(unused_imports)]
pub use model::*;
pub use parser::Parser;
pub use schema::{
    Change, ColumnSpec, ConfigSpec, Cost, Declarations, Diff, IndexSpec, OperationSpec, PartitionKeySpec, QueriesSpec,
    Schema, TableChange, TransformReason, TransformRequest, declarations_in_source, declarations_in_tokens, plan,
};
#[cfg(feature = "uml")]
pub use schema::{Relation, infer_relations, schemas_to_mermaid};
/// The key types `autoincrement` can generate, and the key types each index
/// backend can hold. Exported so `worktable_codegen` uses the same lists
/// `check` does, rather than a second copy that can drift.
pub use validate::{AUTOINCREMENT_TYPES, supported_key_types};
