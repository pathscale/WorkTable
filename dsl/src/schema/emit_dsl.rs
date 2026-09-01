//! Rendering a [`Schema`] back into the declaration text it came from.
//!
//! This is the half of the round trip that makes a designer possible. Reading
//! a schema is enough to draw it; writing one back is what lets the drawing be
//! edited and the result be a file the compiler accepts.
//!
//! The output is the macro body, not the invocation: the caller decides whether
//! it is going inside `worktable! { .. }`, into a `.wt` file, or into a diff.
//! [`Schema::to_macro_invocation`] wraps it when the invocation is what is
//! wanted.
//!
//! Nothing here tries to preserve the input's formatting or its comments. It
//! cannot: the parser is token-based, so comments never reach the model, and a
//! `Schema` is a description of a schema rather than of a file. What is
//! preserved is meaning, and the round-trip test asserts exactly that and
//! nothing more.

use std::fmt::Write as _;

use super::{ColumnSpec, IndexSpec, OperationSpec, Schema};
use crate::model::{GeneratorType, IndexBackend, Persistence};

const INDENT: &str = "    ";

impl Schema {
    /// Render the declaration body.
    pub fn to_dsl(&self) -> String {
        let mut out = String::new();

        let _ = writeln!(out, "name: {},", self.name);
        let _ = writeln!(out, "version: {},", self.version);

        match self.persist {
            // An omitted `persist` is not the same as `persist: false`: the
            // macro requires the acknowledgement before it will accept an
            // index backend that cannot be persisted, so writing one in would
            // silently answer a question the author left open.
            Persistence::Omitted => {}
            Persistence::MemoryOnly => {
                let _ = writeln!(out, "persist: false,");
            }
            Persistence::Persisted => {
                let _ = writeln!(out, "persist: true,");
            }
        }

        if let Some(key) = &self.partition_by {
            let _ = writeln!(out, "partition_by: {}: {},", key.name, key.ty);
        }

        let _ = writeln!(out, "columns: {{");
        for column in &self.columns {
            let _ = writeln!(out, "{INDENT}{},", column_to_dsl(column));
        }
        let _ = writeln!(out, "}},");

        if !self.indexes.is_empty() {
            let _ = writeln!(out, "indexes: {{");
            for index in &self.indexes {
                let _ = writeln!(out, "{INDENT}{},", index_to_dsl(index));
            }
            let _ = writeln!(out, "}},");
        }

        if !self.queries.is_empty() {
            let _ = writeln!(out, "queries: {{");
            write_query_block(&mut out, "update", &self.queries.updates);
            write_query_block(&mut out, "delete", &self.queries.deletes);
            write_query_block(&mut out, "in_place", &self.queries.in_place);
            let _ = writeln!(out, "}},");
        }

        if !self.config.is_empty() {
            let _ = writeln!(out, "config: {{");
            if let Some(page_size) = self.config.page_size {
                let _ = writeln!(out, "{INDENT}page_size: {page_size},");
            }
            if !self.config.row_derives.is_empty() {
                // `row_derives` reads identifiers until it meets another config
                // key, so it has to be written last of the two.
                let _ = writeln!(out, "{INDENT}row_derives: {},", self.config.row_derives.join(", "));
            }
            // No comma: `parse_configs` does not consume one after its block, so a
            // trailing comma here reaches the top-level dispatch as a `,` token.
            // `config` is emitted last, so nothing needs to follow it.
            let _ = writeln!(out, "}}");
        }

        out
    }

    /// Render the declaration as a complete `worktable!` invocation, ready to
    /// be written into a Rust file.
    pub fn to_macro_invocation(&self) -> String {
        let mut out = String::from("worktable! {\n");
        for line in self.to_dsl().lines() {
            if line.is_empty() {
                out.push('\n');
            } else {
                let _ = writeln!(out, "{INDENT}{line}");
            }
        }
        out.push_str("}\n");
        out
    }
}

fn column_to_dsl(column: &ColumnSpec) -> String {
    let mut out = format!("{}: {}", column.name, column.ty);

    if column.primary_key {
        out.push_str(" primary_key");
        match column.generator {
            GeneratorType::None => {}
            GeneratorType::Autoincrement => out.push_str(" autoincrement"),
            GeneratorType::Custom => out.push_str(" custom"),
        }
    }

    if column.optional {
        out.push_str(" optional");
    }

    // A primary-key column always carries a backend once parsed, because the
    // model fills the default in. Writing the default back out would be
    // correct but noisy, and the point of this emitter is text a person will
    // read, so only a deliberate choice is written.
    if let Some(backend) = column.index_backend
        && backend != IndexBackend::default()
    {
        let _ = write!(out, " using {}", backend.name());
    }

    out
}

fn index_to_dsl(index: &IndexSpec) -> String {
    let mut out = format!("{}: {}", index.name, index.column);
    if index.unique {
        out.push_str(" unique");
    }
    if index.backend != IndexBackend::default() {
        let _ = write!(out, " using {}", index.backend.name());
    }
    out
}

fn write_query_block(out: &mut String, kind: &str, operations: &[OperationSpec]) {
    if operations.is_empty() {
        return;
    }
    let _ = writeln!(out, "{INDENT}{kind}: {{");
    for operation in operations {
        let _ = writeln!(
            out,
            "{INDENT}{INDENT}{}({}) by {},",
            operation.name,
            operation.columns.join(", "),
            operation.by
        );
    }
    // No comma after the closing brace. `parse_updates` consumes one if it is
    // there, but `parse_deletes` and `parse_in_place` do not, so a comma after
    // either of those blocks reaches the `queries` dispatch loop as a `,`
    // token and dies as "Unexpected identifier". Omitting it is accepted by
    // all three, which makes it the only form that is always valid.
    let _ = writeln!(out, "{INDENT}}}");
}
