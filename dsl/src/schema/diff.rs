//! What changed between two schemas, and what that costs.
//!
//! # The question this answers
//!
//! A table opens, and the version on disk does not match the version the
//! binary was compiled with. Something has to decide what to do about it, and
//! today that decision is made by a human who wrote `version_tables: { 1 =>
//! v1::UserV1WorkTable }` and kept the old table definition by hand, forever,
//! for every version that ever existed. That hand-maintenance is the whole
//! reason migrations get put off.
//!
//! With both schemas as data, the decision can be computed. [`Diff::between`]
//! says what changed; [`Cost`] says what it costs to apply; and
//! [`Diff::transforms_required`] says which parts a human still has to write,
//! because those are the parts that need intent rather than mechanism.
//!
//! # Cost is about links, not about fields
//!
//! A row is addressed by a `Link { page_id, offset, length }`, and every index
//! holds links. So the question that decides the cost of a change is not "how
//! many columns moved" but "is a row still where it was". A change to the row's
//! archived layout invalidates every link in the table at once, and the only
//! way through is to write every row somewhere else. A change to an index
//! invalidates nothing: the rows have not moved, and the index can be rebuilt
//! from them. That is why [`Cost`] has the shape it does, and why adding a
//! column is expensive while adding an index is not.
//!
//! # What it cannot tell you
//!
//! A rename is a drop and an add. Nothing in a declaration distinguishes
//! `email` becoming `email_address` from `email` being deleted while an
//! unrelated `email_address` appears, and guessing from type equality would be
//! wrong exactly when it mattered. The diff reports both changes and asks for a
//! transform, which is where the intent belongs.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use super::{ColumnSpec, IndexSpec, PartitionKeySpec, Schema};
use crate::model::{IndexBackend, Persistence};

/// What applying a change costs.
///
/// Ordered from cheapest to most expensive, so the cost of a whole diff is the
/// maximum over its changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Cost {
    /// Nothing on disk changes. The generated code differs and the data does
    /// not: new queries, different row derives, a version bump on its own.
    Nothing,
    /// Indexes are rebuilt from rows that stay where they are. No link is
    /// invalidated, so this can be done in place.
    RebuildIndexes,
    /// Every row is written somewhere else, because its archived layout
    /// changed. Every link in the table is invalidated at once, which is why
    /// there is no cheaper version of this: it is a copy-forward into a new
    /// space, with the old one left untouched until it succeeds.
    RewriteRows,
    /// Cannot be planned. A person has to say what they meant before anything
    /// can be applied.
    NeedsIntent,
}

impl Cost {
    /// A short explanation, for a report or an error message.
    pub fn describe(self) -> &'static str {
        match self {
            Self::Nothing => "no change on disk",
            Self::RebuildIndexes => "indexes rebuilt in place; rows are not moved",
            Self::RewriteRows => "every row is copied forward into a new space",
            Self::NeedsIntent => "cannot be planned automatically",
        }
    }
}

/// One difference between two schemas.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Change {
    /// The declared version changed. This is the trigger for a migration
    /// rather than a cost of one.
    Version {
        /// The version on disk.
        from: u32,
        /// The version the binary declares.
        to: u32,
    },
    /// The table is named differently, which means it is a different space on
    /// disk and nothing links the two but a person saying so.
    Renamed {
        /// The stored name.
        from: String,
        /// The declared name.
        to: String,
    },
    /// `persist` changed.
    PersistenceChanged {
        /// What was stored.
        from: Persistence,
        /// What is declared.
        to: Persistence,
    },
    /// The routing key changed. The key is not in the row, so it cannot be
    /// recomputed from the data: a row's partition is only knowable from where
    /// it already is.
    PartitionKeyChanged {
        /// What was stored.
        from: Option<PartitionKeySpec>,
        /// What is declared.
        to: Option<PartitionKeySpec>,
    },
    /// The primary key's columns changed, in membership or in order. Order
    /// counts: it decides the layout of the generated key type.
    PrimaryKeyChanged {
        /// The stored key columns, in order.
        from: Vec<String>,
        /// The declared key columns, in order.
        to: Vec<String>,
    },
    /// A column appeared.
    ColumnAdded(ColumnSpec),
    /// A column is gone, and its values with it.
    ColumnDropped(ColumnSpec),
    /// A column kept its name and changed its type.
    ColumnTypeChanged {
        /// Column name.
        name: String,
        /// The stored type.
        from: String,
        /// The declared type.
        to: String,
    },
    /// A column gained or lost `optional`.
    ColumnOptionalityChanged {
        /// Column name.
        name: String,
        /// Whether it is optional now.
        now_optional: bool,
    },
    /// A column moved. Declaration order is the row struct's field order, so
    /// moving one changes the archived layout as surely as changing its type.
    ColumnMoved {
        /// Column name.
        name: String,
        /// Its stored position.
        from: usize,
        /// Its declared position.
        to: usize,
    },
    /// A secondary index appeared.
    IndexAdded(IndexSpec),
    /// A secondary index is gone.
    IndexDropped(IndexSpec),
    /// An index of the same name is now built over a different column.
    IndexColumnChanged {
        /// Index name.
        name: String,
        /// The stored column.
        from: String,
        /// The declared column.
        to: String,
    },
    /// An index gained or lost `unique`.
    IndexUniquenessChanged {
        /// Index name.
        name: String,
        /// Whether it is unique now.
        now_unique: bool,
    },
    /// An index kept its shape and changed its implementation.
    IndexBackendChanged {
        /// Index name.
        name: String,
        /// The stored backend.
        from: IndexBackend,
        /// The declared backend.
        to: IndexBackend,
    },
    /// The primary index's implementation changed.
    PrimaryIndexBackendChanged {
        /// The stored backend.
        from: IndexBackend,
        /// The declared backend.
        to: IndexBackend,
    },
    /// The generated queries differ. Nothing on disk depends on them.
    QueriesChanged,
    /// The `config` block differs. `page_size` is pinned to the on-disk page
    /// size for persisted tables, so what is left here cannot reach the data.
    ConfigChanged,
}

impl Change {
    /// What applying this change costs.
    pub fn cost(&self) -> Cost {
        match self {
            Self::Version { .. } | Self::QueriesChanged | Self::ConfigChanged => Cost::Nothing,

            Self::IndexAdded(_)
            | Self::IndexDropped(_)
            | Self::IndexColumnChanged { .. }
            | Self::IndexUniquenessChanged { .. }
            | Self::IndexBackendChanged { .. }
            | Self::PrimaryIndexBackendChanged { .. } => Cost::RebuildIndexes,

            Self::ColumnAdded(_)
            | Self::ColumnDropped(_)
            | Self::ColumnTypeChanged { .. }
            | Self::ColumnOptionalityChanged { .. }
            | Self::ColumnMoved { .. } => Cost::RewriteRows,

            Self::Renamed { .. }
            | Self::PersistenceChanged { .. }
            | Self::PartitionKeyChanged { .. }
            | Self::PrimaryKeyChanged { .. } => Cost::NeedsIntent,
        }
    }

    /// What a person has to supply before this change can be applied, if
    /// anything.
    ///
    /// The rule is that the planner can invent a value only when there is
    /// exactly one it could be. Widening a column to `optional` has one answer,
    /// `Some(old)`. Narrowing it does not: what a `None` should become is a
    /// question about the data, not about the schema.
    pub fn transform_required(&self) -> Option<TransformRequest> {
        match self {
            Self::ColumnAdded(column) if !column.optional => Some(TransformRequest {
                column: column.name.clone(),
                reason: TransformReason::NoValueToFillItWith { ty: column.ty.clone() },
            }),
            Self::ColumnTypeChanged { name, from, to } => Some(TransformRequest {
                column: name.clone(),
                reason: TransformReason::NoConversionExists {
                    from: from.clone(),
                    to: to.clone(),
                },
            }),
            Self::ColumnOptionalityChanged {
                name,
                now_optional: false,
            } => Some(TransformRequest {
                column: name.clone(),
                reason: TransformReason::NothingToPutWhereNoneWas,
            }),
            _ => None,
        }
    }

    /// Something true about this change that its cost does not say.
    pub fn warning(&self) -> Option<String> {
        match self {
            Self::IndexUniquenessChanged { name, now_unique: true } => Some(format!(
                "index `{name}` becomes unique: rebuilding it fails if the existing rows already \
                 hold a duplicate, and that is only knowable by reading them"
            )),
            Self::ColumnDropped(column) => Some(format!(
                "column `{}` is dropped: its values are not carried anywhere and are gone once the \
                 old space is removed",
                column.name
            )),
            _ => None,
        }
    }
}

/// Something a person has to write before a plan can run.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TransformRequest {
    /// The column it concerns.
    pub column: String,
    /// Why the planner cannot decide it.
    pub reason: TransformReason,
}

/// Why a change needs a human.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TransformReason {
    /// A new column that is not `optional` has no value in any existing row,
    /// and nothing in the schema says what it should be.
    NoValueToFillItWith {
        /// The new column's type.
        ty: String,
    },
    /// The column's type changed and the schema does not say how one becomes
    /// the other.
    NoConversionExists {
        /// The stored type.
        from: String,
        /// The declared type.
        to: String,
    },
    /// A column stopped being `optional`, so every stored `None` needs a value
    /// or the row needs dropping.
    NothingToPutWhereNoneWas,
}

impl TransformReason {
    /// A one-line explanation, for a report.
    pub fn describe(&self) -> String {
        match self {
            Self::NoValueToFillItWith { ty } => {
                format!("new non-optional column of type `{ty}` has no value in existing rows")
            }
            Self::NoConversionExists { from, to } => {
                format!("no conversion from `{from}` to `{to}` is implied by the declaration")
            }
            Self::NothingToPutWhereNoneWas => {
                "stored `None` values need a replacement or the rows need dropping".to_string()
            }
        }
    }
}

/// Everything that differs between two schemas for one table.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Diff {
    /// The table's stored name.
    pub table: String,
    /// The differences, in a fixed order: identity, then columns, then
    /// indexes, then the parts that cannot reach the data.
    pub changes: Vec<Change>,
}

impl Diff {
    /// Compare a stored schema against a declared one.
    ///
    /// `stored` is what is on disk and `declared` is what the binary was
    /// compiled with, and the direction matters: "a column was added" means
    /// added by the binary, and so absent from every row on disk.
    pub fn between(stored: &Schema, declared: &Schema) -> Self {
        let mut changes = Vec::new();

        if stored.name != declared.name {
            changes.push(Change::Renamed {
                from: stored.name.clone(),
                to: declared.name.clone(),
            });
        }
        if stored.version != declared.version {
            changes.push(Change::Version {
                from: stored.version,
                to: declared.version,
            });
        }
        if stored.persist != declared.persist {
            changes.push(Change::PersistenceChanged {
                from: stored.persist,
                to: declared.persist,
            });
        }
        if stored.partition_by != declared.partition_by {
            changes.push(Change::PartitionKeyChanged {
                from: stored.partition_by.clone(),
                to: declared.partition_by.clone(),
            });
        }

        let stored_key: Vec<String> = stored.primary_key().iter().map(|c| c.name.clone()).collect();
        let declared_key: Vec<String> = declared.primary_key().iter().map(|c| c.name.clone()).collect();
        if stored_key != declared_key {
            changes.push(Change::PrimaryKeyChanged {
                from: stored_key,
                to: declared_key,
            });
        }

        diff_columns(stored, declared, &mut changes);
        diff_indexes(stored, declared, &mut changes);

        if stored.primary_index_backend() != declared.primary_index_backend() {
            changes.push(Change::PrimaryIndexBackendChanged {
                from: stored.primary_index_backend(),
                to: declared.primary_index_backend(),
            });
        }
        if stored.queries != declared.queries {
            changes.push(Change::QueriesChanged);
        }
        if stored.config != declared.config {
            changes.push(Change::ConfigChanged);
        }

        Self {
            table: stored.name.clone(),
            changes,
        }
    }

    /// Whether the two schemas are the same.
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    /// Whether the rows on disk can be read by the declared type as they are.
    ///
    /// This is the question the fast path asks. A version match plus this
    /// returning true is an optimistic load with nothing to do; a version match
    /// plus this returning false is a schema changed without a version bump,
    /// which is a mistake rather than a migration and should be said so.
    pub fn rows_are_readable(&self) -> bool {
        self.cost() < Cost::RewriteRows
    }

    /// The cost of the whole diff, which is the cost of its worst change.
    pub fn cost(&self) -> Cost {
        self.changes.iter().map(Change::cost).max().unwrap_or(Cost::Nothing)
    }

    /// Everything a person has to write before this can be applied.
    pub fn transforms_required(&self) -> Vec<TransformRequest> {
        self.changes.iter().filter_map(Change::transform_required).collect()
    }

    /// Everything true about this diff that its cost does not say.
    pub fn warnings(&self) -> Vec<String> {
        self.changes.iter().filter_map(Change::warning).collect()
    }

    /// A report, for an error message or a designer's migration pane.
    pub fn describe(&self) -> String {
        if self.is_empty() {
            return format!("`{}` is unchanged", self.table);
        }

        let mut out = format!("`{}`: {}\n", self.table, self.cost().describe());
        for change in &self.changes {
            let _ = writeln!(out, "  {}", describe_change(change));
        }
        let transforms = self.transforms_required();
        if !transforms.is_empty() {
            out.push_str("  needs a transform written for:\n");
            for transform in transforms {
                let _ = writeln!(out, "    {}: {}", transform.column, transform.reason.describe());
            }
        }
        for warning in self.warnings() {
            let _ = writeln!(out, "  note: {warning}");
        }
        out
    }
}

fn describe_change(change: &Change) -> String {
    match change {
        Change::Version { from, to } => format!("version {from} -> {to}"),
        Change::Renamed { from, to } => format!("table renamed {from} -> {to}"),
        Change::PersistenceChanged { from, to } => format!("persistence {from:?} -> {to:?}"),
        Change::PartitionKeyChanged { from, to } => {
            let name = |key: &Option<PartitionKeySpec>| match key {
                Some(key) => format!("{}: {}", key.name, key.ty),
                None => "none".to_string(),
            };
            format!("partition key {} -> {}", name(from), name(to))
        }
        Change::PrimaryKeyChanged { from, to } => {
            format!("primary key ({}) -> ({})", from.join(", "), to.join(", "))
        }
        Change::ColumnAdded(column) => format!(
            "column added: {}: {}{}",
            column.name,
            column.ty,
            if column.optional { " optional" } else { "" }
        ),
        Change::ColumnDropped(column) => format!("column dropped: {}: {}", column.name, column.ty),
        Change::ColumnTypeChanged { name, from, to } => format!("column {name}: {from} -> {to}"),
        Change::ColumnOptionalityChanged { name, now_optional } => {
            if *now_optional {
                format!("column {name} became optional")
            } else {
                format!("column {name} stopped being optional")
            }
        }
        Change::ColumnMoved { name, from, to } => format!("column {name} moved from position {from} to {to}"),
        Change::IndexAdded(index) => format!("index added: {} over {}", index.name, index.column),
        Change::IndexDropped(index) => format!("index dropped: {} over {}", index.name, index.column),
        Change::IndexColumnChanged { name, from, to } => format!("index {name}: {from} -> {to}"),
        Change::IndexUniquenessChanged { name, now_unique } => {
            if *now_unique {
                format!("index {name} became unique")
            } else {
                format!("index {name} stopped being unique")
            }
        }
        Change::IndexBackendChanged { name, from, to } => {
            format!("index {name}: {} -> {}", from.name(), to.name())
        }
        Change::PrimaryIndexBackendChanged { from, to } => {
            format!("primary index: {} -> {}", from.name(), to.name())
        }
        Change::QueriesChanged => "queries changed".to_string(),
        Change::ConfigChanged => "config changed".to_string(),
    }
}

fn diff_columns(stored: &Schema, declared: &Schema, changes: &mut Vec<Change>) {
    for (position, column) in declared.columns.iter().enumerate() {
        match stored.column(&column.name) {
            None => changes.push(Change::ColumnAdded(column.clone())),
            Some(before) => {
                if before.ty != column.ty {
                    changes.push(Change::ColumnTypeChanged {
                        name: column.name.clone(),
                        from: before.ty.clone(),
                        to: column.ty.clone(),
                    });
                }
                if before.optional != column.optional {
                    changes.push(Change::ColumnOptionalityChanged {
                        name: column.name.clone(),
                        now_optional: column.optional,
                    });
                }
                let was_at = stored
                    .columns
                    .iter()
                    .position(|c| c.name == column.name)
                    .expect("the column was just found by name");
                if was_at != position {
                    changes.push(Change::ColumnMoved {
                        name: column.name.clone(),
                        from: was_at,
                        to: position,
                    });
                }
            }
        }
    }
    for column in &stored.columns {
        if declared.column(&column.name).is_none() {
            changes.push(Change::ColumnDropped(column.clone()));
        }
    }
}

fn diff_indexes(stored: &Schema, declared: &Schema, changes: &mut Vec<Change>) {
    let find = |schema: &Schema, name: &str| schema.indexes.iter().find(|index| index.name == name).cloned();

    let names: BTreeSet<&str> = stored
        .indexes
        .iter()
        .chain(declared.indexes.iter())
        .map(|index| index.name.as_str())
        .collect();

    for name in names {
        match (find(stored, name), find(declared, name)) {
            (None, Some(added)) => changes.push(Change::IndexAdded(added)),
            (Some(dropped), None) => changes.push(Change::IndexDropped(dropped)),
            (Some(before), Some(after)) => {
                if before.column != after.column {
                    changes.push(Change::IndexColumnChanged {
                        name: name.to_string(),
                        from: before.column,
                        to: after.column,
                    });
                }
                if before.unique != after.unique {
                    changes.push(Change::IndexUniquenessChanged {
                        name: name.to_string(),
                        now_unique: after.unique,
                    });
                }
                if before.backend != after.backend {
                    changes.push(Change::IndexBackendChanged {
                        name: name.to_string(),
                        from: before.backend,
                        to: after.backend,
                    });
                }
            }
            (None, None) => unreachable!("the name came from one of the two"),
        }
    }
}

/// What happened to one table between two sets of schemas.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TableChange {
    /// A table the binary declares that is not on disk. There is nothing to
    /// migrate: it is created empty.
    Created(String),
    /// A table on disk that the binary no longer declares. Nothing reads it,
    /// and nothing here deletes it either: that is a decision, not a
    /// consequence.
    Dropped(String),
    /// A table that exists on both sides and differs.
    Changed(Diff),
}

impl TableChange {
    /// What applying this costs.
    pub fn cost(&self) -> Cost {
        match self {
            // A new table has no rows to move.
            Self::Created(_) => Cost::Nothing,
            // Whether to delete a table's data is not something a diff can
            // decide, however obvious the answer looks from the declaration.
            Self::Dropped(_) => Cost::NeedsIntent,
            Self::Changed(diff) => diff.cost(),
        }
    }
}

/// Compare a stored set of schemas against a declared one.
///
/// Tables are matched by name, which is also how they are matched on disk:
/// a space's name is its identity. A renamed table therefore reads as one
/// dropped and one created, and saying it was a rename is a person's job.
pub fn plan(stored: &[Schema], declared: &[Schema]) -> Vec<TableChange> {
    let mut changes = Vec::new();

    for schema in declared {
        match stored.iter().find(|other| other.name == schema.name) {
            None => changes.push(TableChange::Created(schema.name.clone())),
            Some(before) => {
                let diff = Diff::between(before, schema);
                if !diff.is_empty() {
                    changes.push(TableChange::Changed(diff));
                }
            }
        }
    }
    for schema in stored {
        if !declared.iter().any(|other| other.name == schema.name) {
            changes.push(TableChange::Dropped(schema.name.clone()));
        }
    }

    changes
}
