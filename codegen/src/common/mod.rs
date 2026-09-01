//! What stayed behind when the schema language moved out.
//!
//! `model` and `parser` are `worktable_dsl` now, so anything can read a
//! declaration. `name_generator` is not part of that language: it invents Rust
//! identifiers for generated code, which is this crate's concern and nobody
//! else's.
//!
//! It also could not have gone. Generators here define inherent `impl`s on
//! `WorktableNameGenerator`, and the orphan rule forbids that for a type owned
//! by another crate. The compiler makes the same argument the design does.
pub mod name_generator;

pub use worktable_dsl::{Parser, *};
pub use worktable_dsl::{model, parser};
