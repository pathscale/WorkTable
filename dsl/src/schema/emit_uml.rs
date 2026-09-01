//! Rendering schemas as UML, for a designer to draw.
//!
//! The target is Mermaid's `classDiagram`, which is UML class notation and
//! renders anywhere Markdown does: a GitHub comment, a docs page, an editor
//! preview, and the designer itself. Emitting text rather than a picture keeps
//! this crate free of a rendering dependency and keeps the output diffable,
//! which matters when the diagram is generated from a schema in version
//! control.
//!
//! # The mapping
//!
//! A table is a class. Columns are attributes, carrying their markers in
//! brackets: `[PK]`, `[UK <index>]` for a unique index, `[IX <index>]` for a
//! non-unique one, and the backend name when a non-default one was selected.
//! Queries are operations, since that is what they are: a named thing the
//! table can be asked to do, with the columns it touches as parameters and the
//! column it selects by as the qualifier. An `optional` column is written
//! `Option~T~`, Mermaid's spelling of a generic.
//!
//! The partition key is not a column and is not drawn as one. It appears in a
//! note, because it describes the table rather than a row: it is stored once
//! per partition and no query can reference it.
//!
//! # Relations
//!
//! The schema language has no foreign keys, so there is nothing to draw an
//! association from. [`infer_relations`] guesses instead, by a single rule
//! stated in its own documentation, and returns what it guessed so a caller
//! can show the user rather than assert it. [`schemas_to_mermaid`] draws the
//! guesses as dependencies (`..>`) rather than associations, because a dashed
//! arrow is the honest notation for a link the declaration does not make.

use std::fmt::Write as _;

use convert_case::{Case, Casing as _};

use super::{ColumnSpec, Schema};
use crate::model::{GeneratorType, IndexBackend, Persistence};

impl Schema {
    /// Render this schema as a Mermaid `classDiagram`.
    pub fn to_mermaid(&self) -> String {
        let mut out = String::from("classDiagram\n");
        self.write_mermaid_class(&mut out);
        out
    }

    fn write_mermaid_class(&self, out: &mut String) {
        let _ = writeln!(out, "    class {} {{", self.name);
        let _ = writeln!(out, "        <<{}>>", self.stereotype());

        for column in &self.columns {
            let _ = writeln!(out, "        +{}", self.column_member(column));
        }

        for (kind, operations) in [
            ("update", &self.queries.updates),
            ("delete", &self.queries.deletes),
            ("in_place", &self.queries.in_place),
        ] {
            for operation in operations {
                let _ = writeln!(
                    out,
                    "        +{kind}_{}({}) by_{}",
                    operation.name,
                    operation.columns.join(", "),
                    operation.by
                );
            }
        }

        let _ = writeln!(out, "    }}");

        if let Some(key) = &self.partition_by {
            let _ = writeln!(
                out,
                "    note for {} \"partitioned by {}: {}\"",
                self.name, key.name, key.ty
            );
        }
    }

    fn stereotype(&self) -> String {
        let persistence = match self.persist {
            Persistence::Persisted => "persisted",
            Persistence::MemoryOnly => "in-memory",
            Persistence::Omitted => "in-memory by default",
        };
        format!("v{} {persistence}", self.version)
    }

    fn column_member(&self, column: &ColumnSpec) -> String {
        let ty = if column.optional {
            format!("Option~{}~", column.ty)
        } else {
            column.ty.clone()
        };
        let mut member = format!("{} : {ty}", column.name);

        let mut markers = Vec::new();
        if column.primary_key {
            markers.push("PK".to_string());
            match column.generator {
                GeneratorType::None => {}
                GeneratorType::Autoincrement => markers.push("autoincrement".to_string()),
                GeneratorType::Custom => markers.push("custom".to_string()),
            }
            if let Some(backend) = column.index_backend
                && backend != IndexBackend::default()
            {
                markers.push(backend.name().to_string());
            }
        }
        for index in self.indexes.iter().filter(|index| index.column == column.name) {
            let kind = if index.unique { "UK" } else { "IX" };
            let mut marker = format!("{kind} {}", index.name);
            if index.backend != IndexBackend::default() {
                let _ = write!(marker, " {}", index.backend.name());
            }
            markers.push(marker);
        }

        if !markers.is_empty() {
            let _ = write!(member, " [{}]", markers.join(", "));
        }
        member
    }
}

/// A link one schema appears to make to another.
///
/// Appears because nothing in the declaration says so: see [`infer_relations`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Relation {
    /// The table holding the referring column.
    pub from: String,
    /// The referring column.
    pub column: String,
    /// The table it appears to refer to.
    pub to: String,
    /// The primary-key column it appears to match.
    pub to_column: String,
}

/// Guess the links between schemas from their column names.
///
/// The schema language has no foreign keys, so there is no declared answer to
/// recover and this is a heuristic, deliberately a narrow one. A column of
/// table `A` is taken to refer to table `B` when all of the following hold:
///
/// - `B` has exactly one primary-key column. A composite key has no single
///   column to point at, and guessing which part was meant is worse than
///   drawing nothing.
/// - The column is named `<b>_<pk>`, where `<b>` is `B`'s name in snake_case:
///   `project_id` for `Project { id }`.
/// - The two types are identical, ignoring `optional`. A `String project_id`
///   against a `u64 Project::id` is a name collision, not a reference.
/// - The column is not itself part of `A`'s primary key. Those are usually a
///   composite key's own parts rather than a reference outward, and drawing
///   them as references clutters the diagram where it is already busiest.
///
/// It will miss links written under any other convention, and it can be wrong.
/// Callers showing this to a user should show it as a suggestion.
pub fn infer_relations(schemas: &[Schema]) -> Vec<Relation> {
    let targets: Vec<(&Schema, &ColumnSpec)> = schemas
        .iter()
        .filter_map(|schema| {
            let key = schema.primary_key();
            match key.as_slice() {
                [single] => Some((schema, *single)),
                _ => None,
            }
        })
        .collect();

    let mut relations = Vec::new();
    for schema in schemas {
        for column in &schema.columns {
            if column.primary_key {
                continue;
            }
            for (target, key) in &targets {
                if target.name == schema.name {
                    continue;
                }
                let expected = format!("{}_{}", target.name.to_case(Case::Snake), key.name);
                if column.name == expected && column.ty == key.ty {
                    relations.push(Relation {
                        from: schema.name.clone(),
                        column: column.name.clone(),
                        to: target.name.clone(),
                        to_column: key.name.clone(),
                    });
                }
            }
        }
    }
    relations
}

/// Render several schemas as one Mermaid `classDiagram`, with the links from
/// [`infer_relations`] drawn as dependencies.
///
/// The arrow is `..>` rather than an association because the link is inferred.
/// A solid line would claim the declaration says something it does not.
pub fn schemas_to_mermaid(schemas: &[Schema]) -> String {
    let mut out = String::from("classDiagram\n");
    for schema in schemas {
        schema.write_mermaid_class(&mut out);
    }
    for relation in infer_relations(schemas) {
        let _ = writeln!(out, "    {} ..> {} : {}", relation.from, relation.to, relation.column);
    }
    out
}
