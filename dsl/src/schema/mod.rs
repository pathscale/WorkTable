//! A schema as plain data, and the emitters that render one.
//!
//! # Why a second representation
//!
//! [`crate::model`] is the macro's representation. It is built out of
//! `proc_macro2::Ident` and `TokenStream`, which is exactly right for a thing
//! whose job is to become Rust code: an `Ident` carries a span, and a span is
//! what turns a schema mistake into an error pointing at the offending line.
//!
//! It is the wrong representation for everything else. An `Ident` cannot be
//! serialised, cannot be compared across processes, cannot be sent to a
//! designer over a socket or written into a data file, and cannot be
//! constructed at all outside a proc-macro context without a `Span::call_site`
//! that lies about where it came from. A `TokenStream` is not `PartialEq`, so
//! two schemas cannot even be asked whether they differ, which is the one
//! question a migration planner exists to answer.
//!
//! [`Schema`] is the same declaration with the compiler's concerns removed:
//! `String` where the model has `Ident`, ordered `Vec`s where the model has
//! `HashMap`, and no spans. It derives `PartialEq`, so two of them can be
//! diffed, and (under the `serde` feature) `Serialize`, so one can be stored
//! next to the data it describes and read back by a process that has never
//! seen the Rust type.
//!
//! # What it is not
//!
//! Building a `Schema` runs the *parser*, not the *validator*. The rules that
//! reject, say, a `congee` index over a `String` key live in
//! [`crate::validate`], and `worktable_codegen` reads the same lists from
//! there, so the macro and an editor calling [`crate::check`] cannot disagree
//! about what is legal. A `Schema` can therefore describe a declaration that
//! the macro would refuse to expand. That is deliberate: a designer needs to
//! hold a half-finished schema while the user is still typing it, and a
//! migration planner needs to read an old one whose rules have since changed.
//!
//! # Determinism
//!
//! Every collection here is ordered, and the order is the one written in the
//! declaration. This matters more than it sounds: [`crate::model::Columns`]
//! stores columns in a `HashMap`, whose iteration order Rust randomises per
//! process, so a consumer walking it draws a different diagram on every run.
//! `field_positions` carries the declaration order and is what this sorts by.
//! The query maps have no such field, so those are sorted by name, which is at
//! least stable.

use proc_macro2::TokenStream;
use syn::spanned::Spanned as _;

use crate::model::{Columns, GeneratorType, IndexBackend, Persistence, Queries, RuntimeBackend, Storage};
use crate::parser::Parser;

mod diff;
mod emit_dsl;
#[cfg(feature = "uml")]
mod emit_uml;
mod scan;

pub use diff::{Change, Cost, Diff, TableChange, TransformReason, TransformRequest, plan};
#[cfg(feature = "uml")]
pub use emit_uml::{Relation, infer_relations, schemas_to_mermaid};
pub use scan::{Declarations, declarations_in_source, declarations_in_tokens};

/// One `worktable!` declaration, as data.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Schema {
    /// The table name, as written. This is the Rust type name, so it is
    /// `UpperCamel` by convention but the parser does not enforce that.
    pub name: String,
    /// Schema version. Absent in the declaration means 1, and this stores the
    /// resolved value rather than the absence, because a consumer comparing an
    /// on-disk version against a declared one wants a number either way.
    pub version: u32,
    /// What holds the rows. `serde(default)` is [`Storage::Paged`], so a
    /// schema written before this field existed reads back as the table it
    /// was: every declaration then was paged.
    #[cfg_attr(feature = "serde", serde(default))]
    pub storage: Storage,
    /// Whether persistence was selected, and whether it was selected at all.
    pub persist: Persistence,
    /// The routing key of a partitioned table. Not a column: it is stored once
    /// per partition rather than once per row, and no query can name it.
    pub partition_by: Option<PartitionKeySpec>,
    /// Columns in declaration order.
    pub columns: Vec<ColumnSpec>,
    /// Secondary indexes in declaration order.
    pub indexes: Vec<IndexSpec>,
    /// The runtime the table is built against. Absent in the declaration means
    /// [`RuntimeBackend::default`], and this stores the resolved value for the
    /// same reason `version` does: a consumer asking which runtime a table
    /// uses wants an answer either way.
    #[cfg_attr(feature = "serde", serde(default))]
    pub runtime: RuntimeBackend,
    /// Generated queries, sorted by name within each kind.
    pub queries: QueriesSpec,
    /// The `config` block.
    pub config: ConfigSpec,
    /// Columnar indexes in declaration order.
    ///
    /// **This was parsed and then dropped.** `from_tokens` read the block into
    /// the model and the `Schema` it returned never carried it, so a columnar
    /// table emitted by `to_dsl` came back without its clustering, and every
    /// consumer downstream of this type, including the TypeScript emitter and
    /// the JSON dump, was blind to it.
    ///
    /// The round-trip test could not catch that: the property is
    /// `parse(emit(parse(s))) == parse(s)`, which holds trivially for anything
    /// this type does not model. See `columnar_survives_the_round_trip`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub columnar_indexes: Vec<ColumnarIndexSpec>,
}

/// A column's `columnar(...)` options.
///
/// `Some` means the column declared `columnar`, with or without options.
/// Absent options are absent rather than defaulted, so an emitted declaration
/// says what was written: `columnar` and `columnar(chunk_rows(2))` are
/// different text and the second is not the first plus a default.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ColumnarSpec {
    /// `chunk_rows(n)`, when written.
    pub chunk_rows: Option<usize>,
    /// `compression(name)`, when it differs from the default.
    pub compression: Option<String>,
}

/// A columnar index: `name: { cluster_by: [field, ..] }`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ColumnarIndexSpec {
    /// Index name.
    pub name: String,
    /// The fields it clusters by, in declaration order.
    pub cluster_by: Vec<String>,
}

/// A column declaration: `name: Type [primary_key] [autoincrement|custom] [optional] [using backend]`.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ColumnSpec {
    /// Field name.
    pub name: String,
    /// The type as written, with any `optional` wrapper removed. The grammar
    /// accepts a single identifier here, so this is never a path or a generic.
    pub ty: String,
    /// Whether `optional` was written, making the field `Option<ty>`.
    pub optional: bool,
    /// Whether this column is part of the primary key.
    pub primary_key: bool,
    /// The primary-key generator. Only meaningful when `primary_key` is set,
    /// and shared by every column of a composite key.
    pub generator: GeneratorType,
    /// The `columnar(...)` options, when the column declared them.
    #[cfg_attr(feature = "serde", serde(default))]
    pub columnar: Option<ColumnarSpec>,
    /// The primary index backend. `Some` on primary-key columns, carrying the
    /// declared backend or the default when `using` was omitted; `None`
    /// elsewhere, because `using` on a non-key column is a parse error.
    pub index_backend: Option<IndexBackend>,
}

/// A secondary index declaration: `name: column [unique] [using backend]`.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct IndexSpec {
    /// Index name.
    pub name: String,
    /// The column it is built over.
    pub column: String,
    /// Whether the index rejects duplicate keys.
    pub unique: bool,
    /// The physical implementation.
    pub backend: IndexBackend,
}

/// The `partition_by` key.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PartitionKeySpec {
    /// Key name, used for generated argument names.
    pub name: String,
    /// Unsigned integer type. See [`crate::model::PARTITION_KEY_TYPES`].
    pub ty: String,
    /// The declared `partition_max_size` width, as it is written. See
    /// [`crate::model::PartitionMaxSize`].
    ///
    /// Not optional, because the key it belongs to is not: a stored schema
    /// without it predates the key and would compare unequal to every
    /// declaration, which is the honest answer rather than a defect.
    pub max_size: String,
}

/// The `queries` block.
#[derive(Debug, Clone, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct QueriesSpec {
    /// `update:` operations.
    pub updates: Vec<OperationSpec>,
    /// `delete:` operations.
    pub deletes: Vec<OperationSpec>,
    /// `in_place:` operations.
    pub in_place: Vec<OperationSpec>,
    /// The profile named by `update runtime <profile>:`, if written. Unresolved:
    /// see [`crate::model::Queries::update_runtime`].
    pub update_runtime: Option<String>,
    /// The profile named by `delete runtime <profile>:`, if written.
    pub delete_runtime: Option<String>,
    /// The profile named by `in_place runtime <profile>:`, if written.
    pub in_place_runtime: Option<String>,
}

impl QueriesSpec {
    /// Whether any query was declared. An empty block and an absent one are
    /// the same thing to the macro, so the emitter writes neither.
    pub fn is_empty(&self) -> bool {
        self.updates.is_empty() && self.deletes.is_empty() && self.in_place.is_empty()
    }
}

/// One generated query: `Name(columns) by key`.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct OperationSpec {
    /// Query name, which becomes part of the generated method name.
    pub name: String,
    /// Columns the query touches. Empty for a delete.
    pub columns: Vec<String>,
    /// The column the query selects rows by.
    pub by: String,
}

/// The `config` block.
#[derive(Debug, Clone, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct ConfigSpec {
    /// `page_size`, in bytes.
    pub page_size: Option<u32>,
    /// Extra derives placed on the generated row type.
    pub row_derives: Vec<String>,
    /// `columnar_slot_id`, when it differs from the default.
    ///
    /// Stored as the difference rather than the resolved value, because the
    /// parser applies defaults and a resolved value cannot be told from a
    /// written one. Emitting a default that was never written is noise; not
    /// emitting a written non-default loses it.
    #[cfg_attr(feature = "serde", serde(default))]
    pub columnar_slot_id: Option<String>,
    /// `columnar_chunk_rows`, when it differs from the default.
    #[cfg_attr(feature = "serde", serde(default))]
    pub columnar_chunk_rows: Option<usize>,
}

impl ConfigSpec {
    /// Whether anything was configured.
    pub fn is_empty(&self) -> bool {
        self.page_size.is_none()
            && self.row_derives.is_empty()
            && self.columnar_slot_id.is_none()
            && self.columnar_chunk_rows.is_none()
    }
}

impl Schema {
    /// Read a declaration from the text between a `worktable!`'s braces.
    ///
    /// The input is the body only: `name: Foo, columns: { .. }`, without the
    /// macro name or the surrounding braces.
    pub fn parse(source: &str) -> syn::Result<Self> {
        let tokens: TokenStream = syn::parse_str(source)?;
        Self::from_tokens(tokens)
    }

    /// Read a declaration from tokens.
    ///
    /// This mirrors the macro's own top-level dispatch, including the
    /// diagnostics for keywords written in the wrong position, because those
    /// are properties of the grammar rather than of code generation. It stops
    /// short of the macro's semantic validation: see the module docs.
    pub fn from_tokens(input: TokenStream) -> syn::Result<Self> {
        let mut parser = Parser::new(input);

        let name = parser.parse_name()?;
        let version = parser.parse_version()?.unwrap_or(1);
        let storage = parser.parse_storage()?;
        let persist = parser.parse_persist()?;
        let partition_by = parser.parse_partition_by()?.map(|key| PartitionKeySpec {
            name: key.name.to_string(),
            ty: key.ty.to_string(),
            max_size: key.max_size.type_name().to_string(),
        });

        let mut columns: Option<Columns> = None;
        let mut indexes = None;
        let mut queries: Option<Queries> = None;
        let mut config = None;
        let mut columnar_indexes = None;
        let mut runtime = None;

        while let Some(ident) = parser.peek_next() {
            match ident.to_string().as_str() {
                "columns" => columns = Some(parser.parse_columns()?),
                "indexes" => indexes = Some(parser.parse_indexes()?),
                "queries" => queries = Some(parser.parse_queries()?),
                "config" => config = Some(parser.parse_configs()?),
                "columnar_indexes" => columnar_indexes = Some(parser.parse_columnar_indexes()?),
                // Free-order like the blocks around it, and unlike `persist`
                // and `partition_by`, because nothing downstream of it depends
                // on having been read first.
                "runtime" => {
                    let span = ident.span();
                    if runtime.is_some() {
                        return Err(syn::Error::new(span, crate::parser::DUPLICATE_RUNTIME));
                    }
                    runtime = Some(parser.parse_runtime()?);
                }
                "version" => {
                    return Err(syn::Error::new(
                        ident.span(),
                        "version must be specified before columns/indexes/queries/config",
                    ));
                }
                "vec" | "persist" | "partition_by" | "partition_max_size" => {
                    return Err(syn::Error::new(
                        ident.span(),
                        "`vec`, `persist`, `partition_by` and `partition_max_size` are positional; the required \
                         order is: name, version, vec, persist, partition_by, partition_max_size, then \
                         columns/indexes/queries/config",
                    ));
                }
                other => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!(
                            "Unexpected token `{other}`; expected one of `columns`, `indexes`, `columnar_indexes`, \
                             `queries`, `config`, `runtime`"
                        ),
                    ));
                }
            }
        }

        let mut model =
            columns.ok_or_else(|| syn::Error::new(parser.input.span(), "Expected a `columns` block in declaration"))?;
        if let Some(columnar_indexes) = columnar_indexes {
            model.columnar_indexes = columnar_indexes.indexes;
        }
        if let Some(indexes) = indexes {
            model.indexes = indexes;
        }

        Ok(Self {
            name: name.to_string(),
            version,
            storage,
            persist,
            partition_by,
            runtime: runtime.unwrap_or_default(),
            columns: columns_from_model(&model)?,
            indexes: indexes_from_model(&model),
            queries: queries.map(queries_from_model).unwrap_or_default(),
            config: config
                .map(|config| {
                    let defaults = crate::model::Config::default();
                    ConfigSpec {
                        page_size: config.page_size,
                        row_derives: config.row_derives.iter().map(ToString::to_string).collect(),
                        columnar_slot_id: (config.columnar_slot_id != defaults.columnar_slot_id)
                            .then(|| config.columnar_slot_id.type_name().to_owned()),
                        columnar_chunk_rows: (config.columnar_chunk_rows != defaults.columnar_chunk_rows)
                            .then_some(config.columnar_chunk_rows),
                    }
                })
                .unwrap_or_default(),
            columnar_indexes: model
                .columnar_indexes
                .values()
                .map(|index| ColumnarIndexSpec {
                    name: index.name.to_string(),
                    cluster_by: index.cluster_by.iter().map(ToString::to_string).collect(),
                })
                .collect(),
        })
    }

    /// The columns forming the primary key, in declaration order.
    pub fn primary_key(&self) -> Vec<&ColumnSpec> {
        self.columns.iter().filter(|column| column.primary_key).collect()
    }

    /// Look a column up by name.
    pub fn column(&self, name: &str) -> Option<&ColumnSpec> {
        self.columns.iter().find(|column| column.name == name)
    }
}

fn columns_from_model(model: &Columns) -> syn::Result<Vec<ColumnSpec>> {
    let mut ordered: Vec<_> = model.field_positions.iter().collect();
    ordered.sort_by_key(|(_, position)| **position);

    ordered
        .into_iter()
        .map(|(name, _)| {
            let ty = model.columns_map.get(name).expect("every positioned column has a type");
            let (ty, optional) = split_optional(ty)?;
            let primary_key = model.primary_keys.contains(name);
            Ok(ColumnSpec {
                name: name.to_string(),
                ty,
                optional,
                primary_key,
                generator: if primary_key {
                    model.generator_type
                } else {
                    GeneratorType::None
                },
                columnar: model.columnar_fields.get(name).map(|config| {
                    let defaults = crate::model::ColumnarFieldConfig::default();
                    ColumnarSpec {
                        chunk_rows: config.chunk_rows,
                        compression: (config.compression != defaults.compression)
                            .then(|| config.compression.name().to_owned()),
                    }
                }),
                index_backend: primary_key.then_some(model.primary_index_backend),
            })
        })
        .collect()
}

/// Recover `optional` from the type the model stores.
///
/// `Columns::try_from_rows` folds the `optional` keyword into the type, so by
/// the time a column reaches the model there is no flag left to read: the type
/// is literally `core::option::Option<T>`. Going back out means undoing that,
/// and it has to be done on the parsed type rather than on the token text,
/// because `TokenStream::to_string` spaces punctuation in a way that makes
/// string matching a guess.
fn split_optional(ty: &TokenStream) -> syn::Result<(String, bool)> {
    let parsed: syn::Type = syn::parse2(ty.clone())?;
    let syn::Type::Path(path) = &parsed else {
        return Err(syn::Error::new(ty.span(), "Expected a named column type"));
    };
    let last = path
        .path
        .segments
        .last()
        .ok_or_else(|| syn::Error::new(ty.span(), "Expected a named column type"))?;

    if last.ident == "Option"
        && let syn::PathArguments::AngleBracketed(args) = &last.arguments
        && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
    {
        return Ok((type_name(inner)?, true));
    }

    Ok((last.ident.to_string(), false))
}

fn type_name(ty: &syn::Type) -> syn::Result<String> {
    let syn::Type::Path(path) = ty else {
        return Err(syn::Error::new(ty.span(), "Expected a named column type"));
    };
    path.path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
        .ok_or_else(|| syn::Error::new(ty.span(), "Expected a named column type"))
}

fn indexes_from_model(model: &Columns) -> Vec<IndexSpec> {
    model
        .indexes
        .values()
        .map(|index| IndexSpec {
            name: index.name.to_string(),
            column: index.field.to_string(),
            unique: index.is_unique,
            backend: index.backend,
        })
        .collect()
}

fn queries_from_model(queries: Queries) -> QueriesSpec {
    fn convert(operations: indexmap::IndexMap<proc_macro2::Ident, crate::model::Operation>) -> Vec<OperationSpec> {
        operations
            .into_values()
            .map(|operation| OperationSpec {
                name: operation.name.to_string(),
                columns: operation.columns.iter().map(ToString::to_string).collect(),
                by: operation.by.to_string(),
            })
            .collect()
    }

    QueriesSpec {
        update_runtime: queries.update_runtime.map(|profile| profile.to_string()),
        delete_runtime: queries.delete_runtime.map(|profile| profile.to_string()),
        in_place_runtime: queries.in_place_runtime.map(|profile| profile.to_string()),
        updates: convert(queries.updates),
        deletes: convert(queries.deletes),
        in_place: convert(queries.in_place),
    }
}

/// Whether the declaration selected persistence, for callers that only care
/// about the answer rather than about whether it was written down.
impl Schema {
    /// Whether the table persists to disk.
    pub fn is_persisted(&self) -> bool {
        self.persist.is_persisted()
    }
}

impl Schema {
    /// The implementation backing the primary index.
    ///
    /// Every primary-key column carries the same one: the parser rejects a
    /// composite key whose parts disagree. A table with no primary key cannot
    /// be declared, so the fallback is unreachable through the parser and is
    /// here for a `Schema` built by hand.
    pub fn primary_index_backend(&self) -> IndexBackend {
        self.columns
            .iter()
            .find(|column| column.primary_key)
            .and_then(|column| column.index_backend)
            .unwrap_or_default()
    }
}
