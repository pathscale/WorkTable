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

pub mod model;
pub mod parser;
pub mod schema;

#[allow(unused_imports)]
pub use model::*;
pub use parser::Parser;
pub use schema::{Change, Cost, Diff, TableChange, TransformReason, TransformRequest, plan};
pub use schema::{
    ColumnSpec, ConfigSpec, IndexSpec, OperationSpec, PartitionKeySpec, QueriesSpec, Relation, Schema, infer_relations,
    schemas_to_mermaid,
};
