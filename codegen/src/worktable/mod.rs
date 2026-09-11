use proc_macro2::TokenStream;
use quote::quote;

use crate::common::Parser;
use crate::common::model::RuntimeBackend;
use crate::common::name_generator::WorktableNameGenerator;
use crate::generators::runtime_backend::{resolve_runtime, runtime_type};

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    // Keep the tokens. The declaration is read a second time at the end, as
    // data, so the generated table can carry its own schema. It happens at the
    // end rather than here so that this function's diagnostics are the ones a
    // bad declaration produces: both parses reject the same inputs, but only
    // one of them knows to say that a separate `attributes` section is not
    // part of the 1.0 grammar.
    let declaration = input.clone();

    let mut parser = Parser::new(input);
    let mut columns = None;
    let mut queries = None;
    let mut indexes = None;
    let mut columnar_indexes = None;
    let mut config = None;
    let mut runtime = None;

    let name = parser.parse_name()?;
    let version = parser.parse_version()?.unwrap_or(1);
    let storage = parser.parse_storage()?;
    let persistence = parser.parse_persist()?;
    let partition_by = parser.parse_partition_by()?;
    while let Some(ident) = parser.peek_next() {
        match ident.to_string().as_str() {
            "columns" => {
                let res = parser.parse_columns()?;
                columns = Some(res)
            }
            "indexes" => {
                let res = parser.parse_indexes()?;
                indexes = Some(res);
            }
            "columnar_indexes" => {
                let res = parser.parse_columnar_indexes()?;
                columnar_indexes = Some(res);
            }
            "queries" => {
                let res = parser.parse_queries()?;
                queries = Some(res)
            }
            "config" => {
                let res = parser.parse_configs()?;
                config = Some(res)
            }
            "runtime" => {
                // Free-order, but not repeatable: two `runtime:` keys would
                // silently keep one of them, and which one is a detail of this
                // loop rather than anything the author could read off the
                // declaration.
                if runtime.is_some() {
                    return Err(syn::Error::new(ident.span(), "duplicate `runtime` section"));
                }
                let res = parser.parse_runtime()?;
                runtime = Some(res)
            }
            "version" => {
                return Err(syn::Error::new(
                    ident.span(),
                    "version must be specified before columns/indexes/queries/config",
                ));
            }
            // Positional declarations that landed after the blocks began, or in
            // the wrong relative order, would otherwise die as a bare
            // "Unexpected identifier" and cost the next person a bisect.
            "vec" => {
                return Err(syn::Error::new(
                    ident.span(),
                    "`vec` is positional and must come before `persist`; the required order is: name, version, vec, persist, partition_by, partition_max_size, then columns/indexes/queries/config",
                ));
            }
            "persist" => {
                return Err(syn::Error::new(
                    ident.span(),
                    "`persist` is positional and must come after `vec` and before `partition_by` and the blocks; the required order is: name, version, vec, persist, partition_by, partition_max_size, then columns/indexes/queries/config",
                ));
            }
            "partition_by" => {
                return Err(syn::Error::new(
                    ident.span(),
                    "`partition_by` is positional and must come after `persist` and before the blocks; the required order is: name, version, vec, persist, partition_by, partition_max_size, then columns/indexes/queries/config",
                ));
            }
            // Reached only when `partition_by` was absent: with it present this
            // key is consumed there, and a stray second one would have to get
            // past that. So the useful thing to say is that it needs a
            // `partition_by` to belong to, not that it is out of order.
            "partition_max_size" => {
                return Err(syn::Error::new(
                    ident.span(),
                    "`partition_max_size` describes how large one partition gets, so it means nothing without `partition_by:` before it. Add the routing key, or remove this",
                ));
            }
            "attributes" => {
                return Err(syn::Error::new(
                    ident.span(),
                    "a separate `attributes` section is not part of the 1.0 grammar; keep `primary_key`, `autoincrement`, `custom`, `optional`, and `using` inline on their column or index declarations",
                ));
            }
            other => {
                return Err(syn::Error::new(
                    ident.span(),
                    format!(
                        "Unexpected token `{other}`; expected one of `columns`, `indexes`, `columnar_indexes`, `queries`, `config`, `runtime`"
                    ),
                ));
            }
        }
    }

    let mut columns = columns.expect("defined");
    if let Some(i) = indexes {
        columns.indexes = i
    }
    if let Some(i) = columnar_indexes {
        columns.columnar_indexes = i.indexes;
    }

    // `storage: vec` generates a different table, so it leaves here rather
    // than falling through the paging, columnar and runtime machinery below.
    //
    // The keys it refuses are refused with an error naming what to use
    // instead. A silent no-op would be worse: `runtime: nagoya(locality)` on a
    // synchronous table is a reasonable thing to write and a completely
    // meaningless thing to have accepted.
    if storage.is_vec() {
        if !columns.columnar_indexes.is_empty() || !columns.columnar_fields.is_empty() {
            return Err(syn::Error::new(
                name.span(),
                "`vec: true` has no pages, and columnar storage is a paging feature. Remove \
                 the columnar declarations, or drop `vec: true` for a paged table.",
            ));
        }
        if runtime.is_some() {
            return Err(syn::Error::new(
                name.span(),
                "`vec: true` is synchronous and never reaches a runtime. Remove `runtime:`, or \
                 drop `vec: true` for a paged table.",
            ));
        }
        if config.is_some() {
            return Err(syn::Error::new(
                name.span(),
                "`vec: true` has no page size and no columnar chunking to configure. Remove \
                 `config:`, or drop `vec: true` for a paged table.",
            ));
        }
        if persistence != worktable_dsl::Persistence::Omitted {
            return Err(syn::Error::new(
                name.span(),
                "`vec: true` has no persistence engine, so `persist` says nothing here. The rows \
                 go to bytes and back through `unload` and `load`, which you call when you want \
                 them: there is no task, no flush, and nothing paid for durability that is not \
                 asked for. Remove `persist:`, or drop `vec: true` for a paged table.",
            ));
        }
        // The router needs the columns to pick its payload, and `vec_table`
        // consumes them. Cloned only when there is a router to build.
        let vec_columns = partition_by.as_ref().map(|_| columns.clone());
        let narrow_key_lint = if partition_by.is_none() {
            gen_narrow_primary_key_lint(&columns)
        } else {
            quote! {}
        };
        let mut generated = crate::generators::vec_table::expand(name.clone(), columns, queries.as_ref())?;
        generated.extend(narrow_key_lint);
        // The router is storage-agnostic: it needs `Default` and `used_bytes`
        // from its payload and nothing else, and a `vec: true` table has both.
        // Partitioning is what makes the `Vec` shape correct rather than
        // something it has nothing to do with, so this composes instead of
        // being refused.
        if let Some(key) = partition_by {
            let columns = vec_columns.expect("cloned whenever `partition_by` is present");
            generated.extend(crate::generators::partitions::expand(
                &name,
                &key,
                worktable_dsl::Persistence::MemoryOnly,
                &columns,
                // `vec: true` refuses `queries:` above, so there are none.
                &crate::generators::dense_table::DenseQueries::default(),
            )?);
        }
        generated.extend(gen_schema_const(&worktable_dsl::Schema::from_tokens(declaration)?));
        return Ok(generated);
    }

    // Past this point the table is paged, and `fxhash` cannot be.
    //
    // Two reasons, and neither is a matter of taste. A paged table answers
    // ranges — `select_by_<column>_range` is generated for every secondary
    // index, and the persistence worker reads its own queue by range — and a
    // hash map cannot answer one at any price. And a persisted index's on-disk
    // form *is* sorted pages: `from_persisted` rebuilds each index with
    // `attach_nodes` from B-tree nodes read off the file, and a hash map has no
    // node structure to attach.
    //
    // Refused here rather than left to fail somewhere inside the index
    // generators, where the error would land on a type the author never wrote.
    {
        let mut offenders = Vec::new();
        if columns.primary_index_backend == worktable_dsl::IndexBackend::FxHash {
            offenders.push((
                columns
                    .primary_keys
                    .first()
                    .map(|key| key.span())
                    .unwrap_or_else(proc_macro2::Span::call_site),
                "the primary key".to_string(),
            ));
        }
        for index in columns.indexes.values() {
            if index.backend == worktable_dsl::IndexBackend::FxHash {
                offenders.push((index.name.span(), format!("`{}`", index.name)));
            }
        }
        if let Some((span, what)) = offenders.into_iter().next() {
            return Err(syn::Error::new(
                span,
                format!(
                    "`using fxhash` on {what}: a hash index has no ordered scan and no persisted \
                     page form, so it cannot back a paged table. This table generates \
                     `select_by_<column>_range` for its indexes and, if persisted, writes each \
                     index as sorted pages. Use `vec: true`, which is single-writer and asks its \
                     index only for point operations, or pick an ordered backend \
                     (`arctic` is the default)."
                ),
            ));
        }
    }

    let columnar_chunk_rows = config
        .as_ref()
        .map(|config| config.columnar_chunk_rows)
        .unwrap_or(crate::common::model::DEFAULT_COLUMNAR_CHUNK_ROWS);
    columns.column_slot_id = config
        .as_ref()
        .map(|config| config.columnar_slot_id)
        .unwrap_or_default();
    for field in columns.columnar_fields.values_mut() {
        let chunk_rows = field.chunk_rows.unwrap_or(columnar_chunk_rows);
        let (smaller, larger) = if chunk_rows <= columnar_chunk_rows {
            (chunk_rows, columnar_chunk_rows)
        } else {
            (columnar_chunk_rows, chunk_rows)
        };
        let nested = larger % smaller == 0 && (larger / smaller).is_power_of_two();
        if !nested {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!(
                    "columnar chunk_rows({chunk_rows}) must be a power-of-two multiple or divisor of config.columnar_chunk_rows ({columnar_chunk_rows})"
                ),
            ));
        }
        field.chunk_rows = Some(chunk_rows);
    }

    worktable_dsl::validate::validate_index_backends(&columns, persistence)?;
    worktable_dsl::validate::validate_columnar_indexes(&columns)?;
    worktable_dsl::validate::validate_page_size(config.as_ref(), persistence)?;
    worktable_dsl::validate::validate_arctic_page_size(&columns, config.as_ref())?;
    if let Some(q) = &queries {
        worktable_dsl::validate::validate_in_place_queries(&columns, q)?;
    }

    // The router needs the columns to decide its payload: a narrow
    // `partition_max_size` selects a position-addressed table whose shape
    // depends on the primary key. Cloned rather than borrowed because the table
    // generators below consume `columns`, and only a partitioned declaration
    // pays for the clone.
    let partition_columns = partition_by.as_ref().map(|_| columns.clone());
    // Lifted before the table generators consume `queries`. `Queries` is not
    // `Clone`, and the dense payload is emitted after them.
    let partition_queries = crate::generators::dense_table::DenseQueries::from_model(queries.as_ref());

    let narrow_key_lint = if partition_by.is_none() {
        gen_narrow_primary_key_lint(&columns)
    } else {
        quote! {}
    };

    let mut generated = if persistence.is_persisted() {
        crate::generators::persist::expand(name.clone(), columns, queries, config, version)?
    } else {
        crate::generators::in_memory::expand_from_parsed(name.clone(), columns, queries, config)?
    };

    generated.extend(narrow_key_lint);
    generated.extend(gen_runtime_type(&name, runtime));

    if let Some(key) = partition_by {
        let columns = partition_columns.expect("cloned whenever `partition_by` is present");
        generated.extend(crate::generators::partitions::expand(
            &name,
            &key,
            persistence,
            &columns,
            &partition_queries,
        )?);
    }

    generated.extend(gen_schema_const(&worktable_dsl::Schema::from_tokens(declaration)?));

    Ok(generated)
}

/// Warn about a primary key too narrow to be a table's, when it is a table's.
///
/// A `u8` primary key counts to 256 and a `bool` one to two. On a *partitioned*
/// table that is correct and is the whole point: the routing key does the
/// spreading and the inner key only separates the handful of rows inside one
/// partition, which is what `partition_max_size` exists to say. On an
/// unpartitioned table it is a table that can never hold more than 256 rows,
/// which is almost always a key that was meant to be wider.
///
/// A lint and not a ban, deliberately. Narrow keys are what make the dense
/// partition possible, and a 256-row lookup table is a real thing to want.
///
/// # Why a deprecation
///
/// A proc macro cannot emit a warning on stable. A `#[deprecated]` item used
/// once in the expansion produces one, carries a message naming the column, and
/// can be silenced the ordinary way: `#[allow(deprecated)]` on the module
/// holding the declaration. Everything is emitted inside an anonymous `const`
/// so none of it is nameable and nothing leaks into the consumer's namespace.
fn gen_narrow_primary_key_lint(columns: &worktable_dsl::model::Columns) -> TokenStream {
    if columns.primary_keys.len() != 1 {
        return quote! {};
    }
    let pk = columns.primary_keys.first().expect("checked above");
    let Some(ty) = columns.columns_map.get(pk) else {
        return quote! {};
    };
    let ty = ty.to_string().replace(' ', "");
    let rows = match ty.as_str() {
        "u8" => "256",
        "bool" => "2",
        _ => return quote! {},
    };

    let note = format!(
        "`{pk}: {ty}` is the primary key of an unpartitioned table, so this table can never hold \
         more than {rows} rows. That is correct beside `partition_by`, where the routing key does \
         the spreading and this key only separates the rows inside one partition; on its own it is \
         usually a key that was meant to be wider. Partition the table, widen the key, or put \
         `#[allow(deprecated)]` on the module if {rows} rows is what you meant."
    );

    quote! {
        const _: () = {
            #[deprecated(note = #note)]
            const NARROW_PRIMARY_KEY: () = ();
            #[allow(unused)]
            fn narrow_primary_key() {
                let _ = NARROW_PRIMARY_KEY;
            }
        };
    }
}

/// Name the runtime the table resolved to, once, as a type.
///
/// This is the runtime half of what `index_backend` does for indexes: the DSL
/// carries an enum, the enum becomes a concrete type, and the generated code
/// names the type rather than knowing which backend was picked.
///
/// It is an alias rather than a generic argument on the emitted `WorkTable<..>`
/// because `Runtime` is not a parameter of that type yet. When it becomes one,
/// this alias is the argument to pass, and the six emitted `worktable::prelude`
/// call sites become `<#ident as Runtime>::sleep` and friends, so the seam is
/// already in the right place.
///
/// `allow(dead_code)` for the same reason `gen_schema_const` needs it: a
/// `worktable!` inside a function body puts this alias in that body, where a
/// user building with `-D warnings` would otherwise fail over a name they never
/// wrote.
fn gen_runtime_type(name: &proc_macro2::Ident, runtime: Option<RuntimeBackend>) -> TokenStream {
    // Emit nothing into a `no_std` build. Every backend needs threads, so the
    // prelude exports no runtime type there and naming one would not resolve.
    // A table that never spawns is still a table, which is why this is silent
    // rather than an error.
    //
    // This intentionally evaluates the proc-macro crate's own feature, the same
    // way `index_backend` does: `worktable`'s `std` forwards to
    // `worktable_codegen/std` in Cargo.toml, so the runtime types and the
    // emitted types are selected together. Emitting a `cfg` into the expansion
    // would instead test the consuming package's unrelated feature namespace.
    if !cfg!(feature = "std") {
        return TokenStream::new();
    }

    let ident = WorktableNameGenerator::from_table_name(name.to_string()).get_runtime_type_ident();
    // An omitted `runtime:` resolves through the same chain as an unannotated
    // section, so a declaration written before this key existed emits exactly
    // what `runtime: nagoya` emits.
    let ty = runtime_type(resolve_runtime(None, runtime));

    quote::quote! {
        #[allow(dead_code)]
        pub type #ident = #ty;
    }
}

/// Bake the declaration into the generated code, as the text it was written in.
///
/// The point is that a compiled binary should be able to say what schema it was
/// built against, without the source. A migration planner needs it as the
/// "declared" side of a comparison against what is on disk; a designer needs it
/// to draw a diagram of an application it did not build.
///
/// The stored form is the DSL text rather than a serialised structure. It needs
/// no format decision, no serde in the dependency graph of every user's build,
/// and it is legible in a hex dump; `worktable_dsl` reads it back with the same
/// parser that read the original, and `dsl/tests/round_trip.rs` holds that
/// property against all 116 declarations in this repository.
///
/// `allow(dead_code)` because a `worktable!` inside a function body puts this
/// const inside that body, where nothing refers to it and `-D warnings` would
/// otherwise fail a user's build over a const they never asked for.
fn gen_schema_const(schema: &worktable_dsl::Schema) -> TokenStream {
    let ident = WorktableNameGenerator::from_table_name(schema.name.clone()).get_schema_const_ident();
    let text = schema.to_dsl();
    let doc = format!(
        "The `worktable!` declaration `{}` was generated from, as text. Read it with `worktable_dsl::Schema::parse`.",
        schema.name
    );

    quote::quote! {
        #[doc = #doc]
        #[allow(dead_code)]
        pub const #ident: &str = #text;
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::expand;

    #[test]
    fn separate_attributes_section_has_an_actionable_1_0_diagnostic() {
        let error = expand(quote! {
            name: AttributesSection,
            columns: {
                id: u64 primary_key,
            },
            attributes: {
                id: primary_key,
            },
        })
        .unwrap_err();

        assert!(error.to_string().contains("not part of the 1.0 grammar"));
        assert!(error.to_string().contains("keep `primary_key`"));
    }

    #[test]
    fn columnar_index_requires_columnar_fields() {
        let error = expand(quote! {
            name: InvalidColumnarIndex,
            persist: false,
            columns: {
                id: u64 primary_key,
                host_id: u64,
            },
            columnar_indexes: {
                host_lookup: {
                    cluster_by: [host_id],
                },
            },
        })
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("requires at least one field declaring `columnar`")
        );
    }

    #[test]
    fn columnar_field_and_index_generate_scan_projection_and_lookup_apis() {
        let output = expand(quote! {
            name: ColumnarCodegen,
            persist: false,
            columns: {
                id: u64 primary_key,
                host_id: u64 columnar(chunk_rows(1024), compression(none)),
                timestamp: i64 columnar(chunk_rows(2048), compression(none)),
            },
            columnar_indexes: {
                host_time: {
                    cluster_by: [host_id, timestamp],
                },
            },
        })
        .unwrap()
        .to_string();

        assert!(output.contains("columnar_scan_host_id"));
        assert!(output.contains("columnar_project_timestamp"));
        assert!(output.contains("columnar_select_host_time"));
        assert!(output.contains("ColumnarColumn :: new (1024"));
    }

    #[test]
    fn columnar_config_is_table_scoped_and_row_derives_stops_at_new_keys() {
        let output = expand(quote! {
            name: ColumnarConfig,
            persist: false,
            columns: {
                id: u64 primary_key,
                value: u64 columnar,
            },
            config: {
                row_derives: Default,
                columnar_slot_id: ColumnSlotId16,
                columnar_chunk_rows: 1024,
            },
        })
        .unwrap()
        .to_string();

        assert!(output.contains("ColumnSlotId16"));
        assert!(output.contains("ColumnarColumn :: new (1024"));
    }

    #[test]
    fn columnar_chunk_override_must_nest_with_table_default() {
        let error = expand(quote! {
            name: InvalidColumnarChunk,
            persist: false,
            columns: {
                id: u64 primary_key,
                value: u64 columnar(chunk_rows(50_000)),
            },
        })
        .unwrap_err();

        assert!(error.to_string().contains("power-of-two multiple or divisor"));
    }

    #[test]
    fn primary_key_cannot_redeclare_columnar_identity() {
        let error = expand(quote! {
            name: InvalidColumnarPrimaryKey,
            persist: false,
            columns: {
                id: u64 primary_key columnar,
            },
        })
        .unwrap_err();

        assert!(error.to_string().contains("must not declare `columnar`"));
    }

    #[test]
    fn duplicate_columnar_config_is_rejected() {
        let error = expand(quote! {
            name: DuplicateColumnarConfig,
            persist: false,
            columns: {
                id: u64 primary_key,
                value: u64 columnar,
            },
            config: {
                columnar_slot_id: ColumnSlotId16,
                columnar_slot_id: ColumnSlotId32,
            },
        })
        .unwrap_err();

        assert!(error.to_string().contains("Duplicate `columnar_slot_id`"));
    }

    fn assert_composite_primary_key_field_order(output: proc_macro2::TokenStream) {
        let output = output.to_string();
        let get_primary_key = output
            .split("fn get_primary_key")
            .nth(1)
            .expect("generated TableRow implementation");
        let tenant = get_primary_key
            .find("self . tenant_id . clone")
            .expect("first primary-key field");
        let record = get_primary_key
            .find("self . record_id . clone")
            .expect("second primary-key field");

        assert!(tenant < record, "composite primary-key declaration order changed");
    }

    #[test]
    fn composite_primary_key_codegen_preserves_declaration_order() {
        for persist in [true, false] {
            let output = expand(quote! {
                name: CompositePrimaryKeyOrder,
                persist: #persist,
                columns: {
                    tenant_id: u64 primary_key using worktables_index,
                    record_id: u64 primary_key using worktables_index,
                    value: i64,
                },
            })
            .unwrap();

            assert_composite_primary_key_field_order(output);
        }
    }

    #[test]
    fn absent_using_selects_arctic_runtime_with_compatible_persistence() {
        let output = expand(quote! {
            name: DefaultBackend,
            persist: true,
            columns: {
                id: u64 primary_key autoincrement,
                value: u64,
            },
            indexes: {
                value_idx: value unique,
            },
        })
        .unwrap()
        .to_string();

        assert!(output.contains("PersistentArcticIndex"));
        assert!(output.contains("table (pk_arctic)"));
    }

    #[test]
    fn fixed_width_update_on_unsized_table_uses_archived_swap() {
        let output = expand(quote! {
            name: MixedWidthUpdate,
            persist: false,
            columns: {
                id: u64 primary_key,
                payload: String,
                balance: f64,
            },
            queries: {
                update: {
                    Balance(balance) by id,
                }
            }
        })
        .unwrap()
        .to_string();

        let update = output
            .split("pub async fn update_balance")
            .nth(1)
            .expect("generated balance update");
        assert!(
            update.contains("data . with_mut_ref"),
            "fixed-width unindexed field must update archived storage in place"
        );
        assert!(
            !update.contains("self . reinsert"),
            "an unrelated String column must not force a fixed-width update through reinsert"
        );
        assert!(
            !update.contains("Uuid :: now_v7"),
            "non-persistent updates must not generate an unused operation id"
        );
    }

    #[cfg(feature = "logical-index-persistence")]
    #[test]
    fn logical_persistence_wraps_explicit_wti_backends() {
        let output = expand(quote! {
            name: LogicalDefaultBackend,
            persist: true,
            columns: {
                id: u64 primary_key autoincrement,
                wti_value: u64,
                congee_value: u64,
                arctic_value: u64,
            },
            indexes: {
                wti_idx: wti_value unique using worktables_index,
                congee_idx: congee_value unique using congee,
                arctic_idx: arctic_value unique using arctic,
            },
        })
        .unwrap()
        .to_string();

        assert!(output.contains("PersistentWtiIndex"));
        assert!(output.contains("PersistentCongeeIndex"));
        assert!(output.contains("PersistentArcticIndex"));
    }

    #[test]
    fn explicit_indexset_is_persistence_compatible() {
        let output = expand(quote! {
            name: ExplicitIndexset,
            persist: true,
            columns: {
                id: u64 primary_key autoincrement using indexset,
                value: u64,
            },
            indexes: {
                value_idx: value unique using indexset,
            },
        });

        assert!(output.is_ok());
    }

    #[test]
    fn explicit_worktables_index_is_persistence_compatible() {
        let output = expand(quote! {
            name: ExplicitWorktablesIndex,
            persist: true,
            columns: {
                id: u64 primary_key autoincrement using worktables_index,
                value: u64,
            },
            indexes: {
                value_idx: value unique using worktables_index,
            },
        });

        assert!(output.is_ok());
    }

    #[test]
    fn congee_backend_requires_explicit_persistence_choice() {
        let error = expand(quote! {
            name: MissingAcknowledgement,
            columns: {
                id: u64 primary_key using congee,
            },
        })
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("explicit `persist: true` or `persist: false`")
        );
    }

    #[test]
    fn art_backend_accepts_persistence() {
        let output = expand(quote! {
            name: PersistentArctic,
            persist: true,
            columns: {
                id: u64 primary_key using arctic,
                value: u64,
            },
            indexes: {
                value_idx: value unique using congee,
            },
        });

        assert!(output.is_ok());
    }

    #[test]
    fn memory_backend_accepts_explicit_false() {
        let output = expand(quote! {
            name: ExplicitMemory,
            persist: false,
            columns: {
                id: u64 primary_key using congee,
            },
        });

        assert!(output.is_ok());
    }

    #[test]
    fn memory_tables_accept_non_unique_arctic_indexes() {
        for key_type in ["u16", "u32", "u64", "u128"] {
            let key_type: proc_macro2::TokenStream = key_type.parse().unwrap();
            let output = expand(quote! {
                name: NonUniqueArctic,
                persist: false,
                columns: {
                    id: u64 primary_key,
                    value: #key_type,
                },
                indexes: {
                    value_idx: value using arctic,
                },
            });

            assert!(output.is_ok(), "{:?}", output.err());
            assert!(output.unwrap().to_string().contains("ArcticMultiIndex"));
        }
    }

    #[test]
    fn persisted_tables_accept_non_unique_arctic_indexes() {
        let output = expand(quote! {
            name: PersistedNonUniqueArctic,
            persist: true,
            columns: {
                id: u64 primary_key,
                value: u64,
            },
            indexes: {
                value_idx: value using arctic,
            },
        });

        assert!(output.is_ok(), "{:?}", output.err());
        assert!(output.unwrap().to_string().contains("PersistentArcticMultiIndex"));
    }

    #[test]
    fn non_unique_arctic_rejects_unsupported_key_types() {
        let error = expand(quote! {
            name: BoolNonUniqueArctic,
            persist: false,
            columns: {
                id: u64 primary_key,
                enabled: bool,
            },
            indexes: {
                enabled_idx: enabled using arctic,
            },
        })
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("supported types: String, u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128")
        );
    }

    #[test]
    fn non_unique_indexes_reject_other_explicit_backends() {
        for backend in ["congee", "indexset"] {
            let backend: proc_macro2::TokenStream = backend.parse().unwrap();
            let error = expand(quote! {
                name: NonUniqueOther,
                persist: false,
                columns: {
                    id: u64 primary_key,
                    value: u64,
                },
                indexes: {
                    value_idx: value using #backend,
                },
            })
            .unwrap_err();

            assert!(
                error
                    .to_string()
                    .contains("non-unique indexes currently require `worktables_index` or `arctic`")
            );
        }
    }

    #[test]
    fn congee_rejects_non_machine_word_secondary_keys() {
        let error = expand(quote! {
            name: StringCongee,
            persist: false,
            columns: {
                id: u64 primary_key,
                name: String,
            },
            indexes: {
                name_idx: name unique using congee,
            },
        })
        .unwrap_err();

        assert!(error.to_string().contains("does not support key type `String`"));
    }

    #[test]
    fn arctic_rejects_unsupported_secondary_keys() {
        let error = expand(quote! {
            name: BoolArctic,
            persist: false,
            columns: {
                id: u64 primary_key,
                value: bool,
            },
            indexes: {
                value_idx: value unique using arctic,
            },
        })
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("supported types: String, u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128")
        );
    }

    /// This used to assert the opposite. A persisted table was refused any page
    /// size but 16384, because the seeks computed every offset from a hardcoded
    /// constant while the generated table threaded the configured one, so the
    /// two disagreed and the file was silently corrupt. Both take the stride as
    /// a parameter now.
    #[test]
    fn persisted_tables_accept_a_non_default_page_size() {
        expand(quote! {
            name: PersistedSmallPages,
            persist: true,
            columns: {
                id: u64 primary_key,
            },
            config: {
                page_size: 8192,
            }
        })
        .expect("a persisted table may choose its page size");
    }

    /// What is left of the rule: a page on disk carries a 28-byte header, so
    /// one this small is mostly header.
    #[test]
    fn persisted_tables_reject_a_page_smaller_than_the_floor() {
        let error = expand(quote! {
            name: PersistedTinyPages,
            persist: true,
            columns: {
                id: u64 primary_key,
            },
            config: {
                page_size: 64,
            }
        })
        .unwrap_err();

        assert!(
            error.to_string().contains("below the 512-byte"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn persisted_tables_accept_the_default_page_size() {
        expand(quote! {
            name: PersistedDefaultPages,
            persist: true,
            columns: {
                id: u64 primary_key,
            },
            config: {
                page_size: 16384,
            }
        })
        .unwrap();
    }

    #[test]
    fn in_memory_tables_keep_custom_page_sizes() {
        expand(quote! {
            name: InMemorySmallPages,
            persist: false,
            columns: {
                id: u64 primary_key,
            },
            config: {
                page_size: 1024,
            }
        })
        .unwrap();
    }

    #[test]
    fn congee_rejects_unsupported_primary_keys() {
        let error = expand(quote! {
            name: StringPrimaryCongee,
            persist: false,
            columns: {
                id: String primary_key using congee,
            },
        })
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("requires a directly named primitive primary-key type; found `String`")
        );
    }
}

#[cfg(test)]
mod position_tests {
    use quote::quote;

    use super::expand;

    #[test]
    fn persisted_partitioned_table_expands() {
        // Regression: the emitted `partition_or_create` carried a
        // `where Table: Default` bound on a concrete type, which rustc rejects
        // as a trivial bound, so `persist: true` + `partition_by` failed to
        // expand at all. The persisted facade now omits that method.
        let expanded = expand(quote! {
            name: SymbolPosting,
            persist: true,
            partition_by: generation: u32,
            partition_max_size: u64,
            columns: { id: u64 primary_key autoincrement, posting_hash: u64, records_blob: String },
            indexes: { posting_idx: posting_hash unique }
        })
        .expect("persist + partition_by must expand")
        .to_string();
        assert!(
            !expanded.contains("partition_or_create"),
            "a persisted facade must not emit the Default-bound constructor"
        );
        assert!(
            expanded.contains("partition_or_insert_with"),
            "the closure-based constructor is the persisted entry point"
        );
    }

    #[test]
    fn in_memory_partitioned_table_keeps_partition_or_create() {
        let expanded = expand(quote! {
            name: Price,
            partition_by: symbol_id: u16,
            partition_max_size: u64,
            columns: { exchange_id: u8 primary_key, bid: f64 }
        })
        .expect("in-memory partitioned table must expand")
        .to_string();
        assert!(expanded.contains("partition_or_create"));
    }

    /// A wide width keeps the full table, which is what every partitioned
    /// declaration had before the width was declarable.
    #[test]
    fn a_wide_partition_max_size_keeps_the_full_table() {
        let expanded = expand(quote! {
            name: Price,
            partition_by: symbol_id: u16,
            partition_max_size: u64,
            columns: { exchange_id: u8 primary_key, bid: f64 }
        })
        .expect("must expand")
        .to_string();
        assert!(
            expanded.contains("PartitionSet < PriceWorkTable >"),
            "the payload must be the full table: {expanded}"
        );
        assert!(
            !expanded.contains("PriceDenseTable"),
            "no dense payload should be emitted"
        );
    }

    /// A narrow one swaps the payload, and only the payload.
    #[test]
    fn a_narrow_partition_max_size_swaps_the_payload() {
        let expanded = expand(quote! {
            name: Price,
            partition_by: symbol_id: u16,
            partition_max_size: u8,
            columns: { exchange_id: u8 primary_key, bid: f64 }
        })
        .expect("must expand")
        .to_string();
        assert!(
            expanded.contains("PartitionSet < PriceDenseTable >"),
            "the payload must be the dense table: {expanded}"
        );
        // The full table is still generated. It is the type the declaration
        // names, and a caller may want one outside the router.
        assert!(
            expanded.contains("struct PriceWorkTable"),
            "the full table is still declared"
        );
    }

    /// A key that is not a position is refused, naming the column.
    #[test]
    fn a_dense_partition_refuses_a_key_that_cannot_be_a_position() {
        let error = expand(quote! {
            name: Named,
            partition_by: symbol_id: u16,
            partition_max_size: u8,
            columns: { label: String primary_key, bid: f64 }
        })
        .expect_err("a String key has no position to be")
        .to_string();
        assert!(error.contains("label"), "must name the column: {error}");
        assert!(error.contains("String"), "must name the type it refused: {error}");
        assert!(
            error.contains("partition_max_size: u64"),
            "must name the way out: {error}"
        );
    }

    /// A composite key is refused for the same reason, and points at the
    /// width that takes one.
    #[test]
    fn a_dense_partition_refuses_a_composite_key() {
        let error = expand(quote! {
            name: Pair,
            partition_by: symbol_id: u16,
            partition_max_size: u8,
            columns: { left: u32 primary_key, right: u32 primary_key, bid: f64 }
        })
        .expect_err("a composite key has no single position")
        .to_string();
        assert!(error.contains("2 primary key columns"), "must say what it saw: {error}");
        assert!(
            error.contains("partition_max_size: u64"),
            "must name the way out: {error}"
        );
    }

    /// A width wider than the key declares rows the key cannot reach.
    ///
    /// Not a soundness problem, always a mistake: `u16` beside a `u8` key
    /// declares 65,536 rows into a partition that can hold 256.
    #[test]
    fn a_width_the_key_cannot_reach_is_refused() {
        let error = expand(quote! {
            name: Price,
            partition_by: symbol_id: u16,
            partition_max_size: u16,
            columns: { exchange_id: u8 primary_key, bid: f64 }
        })
        .expect_err("a u8 key cannot reach 65,536 rows")
        .to_string();
        assert!(error.contains("exchange_id"), "must name the column: {error}");
        assert!(error.contains("65536"), "must say how many rows were declared: {error}");
        assert!(error.contains("partition_max_size: u8"), "must name the fix: {error}");
    }

    /// A narrow key on an unpartitioned table warns.
    #[test]
    fn a_narrow_primary_key_on_an_unpartitioned_table_is_linted() {
        // `using worktables_index` on the `bool` arm: arctic, the default,
        // refuses a `bool` key outright, so that arm is only reachable through
        // a backend that takes one. It is still worth linting, because WTI does.
        for (ty, rows, backend) in [
            ("u8", "256", quote! {}),
            ("bool", "2", quote! { using worktables_index }),
        ] {
            let ty = syn::Ident::new(ty, proc_macro2::Span::call_site());
            let expanded = expand(quote! {
                name: Flag,
                columns: { id: #ty primary_key #backend, v: u64 }
            })
            .expect("must expand")
            .to_string();
            assert!(
                expanded.contains("NARROW_PRIMARY_KEY"),
                "`{ty}` should be linted: {expanded}"
            );
            assert!(
                expanded.contains(rows),
                "the note should say how many rows `{ty}` reaches: {expanded}"
            );
        }
    }

    /// Beside `partition_by` the same key is correct, so it is silent.
    ///
    /// This is the half that matters: a narrow key is what makes a dense
    /// partition possible, and a lint that fired on it would be telling people
    /// to undo the optimisation.
    #[test]
    fn a_narrow_primary_key_on_a_partitioned_table_is_silent() {
        let expanded = expand(quote! {
            name: Price,
            partition_by: symbol_id: u16,
            partition_max_size: u8,
            columns: { exchange_id: u8 primary_key, bid: f64 }
        })
        .expect("must expand")
        .to_string();
        assert!(
            !expanded.contains("NARROW_PRIMARY_KEY"),
            "a partitioned narrow key is correct and must not warn: {expanded}"
        );
    }

    /// A key wide enough to be a table's is not linted.
    #[test]
    fn a_wide_primary_key_is_not_linted() {
        for ty in ["u16", "u32", "u64", "String"] {
            let ty = syn::Ident::new(ty, proc_macro2::Span::call_site());
            let expanded = expand(quote! {
                name: Wide,
                columns: { id: #ty primary_key, v: u64 }
            })
            .expect("must expand")
            .to_string();
            assert!(!expanded.contains("NARROW_PRIMARY_KEY"), "`{ty}` must not be linted");
        }
    }

    /// A dense partition takes update and delete queries keyed by position.
    #[test]
    fn a_dense_partition_generates_its_queries() {
        let expanded = expand(quote! {
            name: Price,
            partition_by: symbol_id: u16,
            partition_max_size: u8,
            columns: { exchange_id: u8 primary_key, bid: f64, ask: f64 },
            queries: {
                update: { TopPrice(bid, ask) by exchange_id, },
                delete: { Stale() by exchange_id, }
            }
        })
        .expect("must expand")
        .to_string();
        assert!(
            expanded.contains("impl PriceDenseTable"),
            "the dense payload must be emitted: {expanded}"
        );
        assert!(expanded.contains("fn update_top_price"), "missing the update query");
        assert!(expanded.contains("fn delete_stale"), "missing the delete query");
    }

    /// Keyed by anything else, it refuses rather than quietly scanning.
    #[test]
    fn a_dense_query_keyed_by_a_column_it_cannot_index_is_refused() {
        let error = expand(quote! {
            name: Price,
            partition_by: symbol_id: u16,
            partition_max_size: u8,
            columns: { exchange_id: u8 primary_key, venue: u32, bid: f64 },
            indexes: { venue_idx: venue },
            queries: {
                update: { ByVenue(bid) by venue, }
            }
        })
        .expect_err("a dense partition has no secondary index")
        .to_string();
        assert!(error.contains("venue"), "must name the column: {error}");
        assert!(error.contains("exchange_id"), "must name the key it can use: {error}");
        assert!(
            error.contains("partition_max_size: u64"),
            "must name the way out: {error}"
        );
    }

    /// `in_place` is a synonym here, so it says so rather than generating a
    /// second name for one method.
    #[test]
    fn in_place_on_a_dense_partition_is_refused_as_a_synonym() {
        let error = expand(quote! {
            name: Price,
            partition_by: symbol_id: u16,
            partition_max_size: u8,
            columns: { exchange_id: u8 primary_key, bid: f64 },
            queries: {
                in_place: { Bump(bid) by exchange_id, }
            }
        })
        .expect_err("in_place has no meaning on a dense partition")
        .to_string();
        assert!(error.contains("already in place"), "must say why: {error}");
        assert!(error.contains("update Bump"), "must name the replacement: {error}");
    }

    /// A dense partition cannot persist, and says so rather than pretending.
    #[test]
    fn a_dense_partition_refuses_persistence() {
        let error = expand(quote! {
            name: Price,
            persist: true,
            partition_by: symbol_id: u16,
            partition_max_size: u8,
            columns: { exchange_id: u8 primary_key, bid: f64 }
        })
        .expect_err("a dense partition has no persistence engine")
        .to_string();
        assert!(error.contains("persist"), "must name the key it cannot honour: {error}");
        assert!(
            error.contains("partition_max_size: u64"),
            "must name the width that does persist: {error}"
        );
    }

    /// The dense payload is a partition payload and nothing else: an
    /// unpartitioned declaration never sees one.
    #[test]
    fn an_unpartitioned_table_gets_no_dense_payload() {
        let expanded = expand(quote! {
            name: Price,
            columns: { exchange_id: u8 primary_key, bid: f64 }
        })
        .expect("must expand")
        .to_string();
        assert!(
            !expanded.contains("DenseTable"),
            "nothing to be dense about: {expanded}"
        );
    }

    #[test]
    fn partition_by_before_persist_names_the_required_order() {
        let error = expand(quote! {
            name: Wrong,
            partition_by: generation: u32,
            partition_max_size: u64,
            persist: true,
            columns: { id: u64 primary_key, v: u64 }
        })
        .expect_err("wrong order must be an error")
        .to_string();
        assert!(
            error.contains("name, version, vec, persist, partition_by"),
            "the error must name the required order, got: {error}"
        );
    }

    #[test]
    fn partition_by_after_the_blocks_names_the_required_order() {
        let error = expand(quote! {
            name: Wrong,
            columns: { id: u64 primary_key, v: u64 },
            partition_by: generation: u32,
            partition_max_size: u64,
        })
        .expect_err("late partition_by must be an error")
        .to_string();
        assert!(
            error.contains("name, version, vec, persist, partition_by"),
            "the error must name the required order, got: {error}"
        );
    }
}

/// What a designer needs from the schema IR: a declaration that has been read
/// into [`worktable_dsl::Schema`] and written back out is a declaration this
/// macro accepts.
///
/// `worktable_dsl` can test its own round trip, which shows nothing was lost
/// between its parser and its emitter. It cannot show that the text it emits
/// is a declaration *this* macro accepts, because it cannot call this macro:
/// that check has to live on the near side of the proc-macro boundary.
///
/// The stronger claim — that repeated expansion generates the *same code* —
/// is asserted separately by `the_same_declaration_expands_the_same_way_twice`.
#[cfg(test)]
mod emitted_declarations {
    use quote::quote;
    use worktable_dsl::Schema;

    use super::expand;

    fn survives_the_round_trip(declaration: proc_macro2::TokenStream) {
        expand(declaration.clone()).expect("the original expands");

        let schema = Schema::from_tokens(declaration).expect("the IR reads it");
        let emitted = schema.to_dsl();
        let reparsed: proc_macro2::TokenStream = syn::parse_str(&emitted)
            .unwrap_or_else(|error| panic!("emitted text does not tokenise: {error}\n{emitted}"));

        assert_eq!(
            Schema::from_tokens(reparsed.clone()).expect("the emitted text reads back"),
            schema,
            "the emitted declaration describes a different schema\n{emitted}"
        );
        expand(reparsed).unwrap_or_else(|error| panic!("the emitted declaration does not expand: {error}\n{emitted}"));
    }

    #[test]
    fn a_minimal_declaration() {
        survives_the_round_trip(quote! {
            name: Minimal,
            columns: { id: u64 primary_key },
        });
    }

    #[test]
    fn a_persisted_declaration_with_indexes_and_queries() {
        survives_the_round_trip(quote! {
            name: Account,
            version: 3,
            persist: true,
            columns: {
                id: u64 primary_key autoincrement,
                email: String,
                tenant: u64,
                nickname: String optional,
                balance: f64,
            },
            indexes: {
                email_idx: email unique,
                tenant_idx: tenant,
            },
            queries: {
                update: {
                    Nickname(nickname) by id,
                    Email(email) by tenant,
                }
                delete: {
                    ById() by id,
                }
                in_place: {
                    Balance(balance) by id,
                }
            }
        });
    }

    #[test]
    fn a_partitioned_declaration() {
        survives_the_round_trip(quote! {
            name: Price,
            partition_by: symbol_id: u16,
            partition_max_size: u64,
            columns: {
                exchange_id: u8 primary_key,
                bid: f64,
            },
        });
    }

    #[test]
    fn a_composite_key_keeps_its_column_order() {
        // The order of a composite key decides the field order of the
        // generated `get_primary_key`, and so the layout of the key type. An
        // emitter that wrote the columns back in a `HashMap`'s order would
        // change it.
        survives_the_round_trip(quote! {
            name: CompositeKey,
            persist: true,
            columns: {
                tenant_id: u64 primary_key,
                record_id: u64 primary_key,
                value: i64,
            },
        });
    }

    #[test]
    fn an_explicit_backend_and_a_custom_page_size() {
        survives_the_round_trip(quote! {
            name: Tuned,
            persist: false,
            columns: {
                id: u64 primary_key using congee,
                value: u64,
            },
            indexes: {
                value_idx: value unique using arctic,
            },
            config: {
                page_size: 1024,
                row_derives: Clone, Debug,
            }
        });
    }
}

#[cfg(test)]
mod generator_determinism {
    use quote::quote;

    use super::expand;

    /// Expansion order is part of the generated program. This fixture includes
    /// columns, indexes and multiple query blocks so a randomized collection in
    /// any of those paths changes the output and fails the test.
    #[test]
    fn the_same_declaration_expands_the_same_way_twice() {
        let declaration = quote! {
            name: Twice,
            persist: true,
            columns: {
                id: u64 primary_key autoincrement,
                email: String,
                tenant: u64,
                balance: f64,
            },
            indexes: {
                email_idx: email unique,
                tenant_idx: tenant,
            },
            queries: {
                update: {
                    SetBalance(balance) by id,
                    MoveTenant(tenant) by email,
                },
                delete: {
                    ByEmail() by email,
                    ByTenant() by tenant,
                },
            },
        };

        let first = expand(declaration.clone()).expect("expands").to_string();
        let second = expand(declaration).expect("expands").to_string();
        if first != second {
            let at = first
                .bytes()
                .zip(second.bytes())
                .position(|(left, right)| left != right)
                .unwrap_or_else(|| first.len().min(second.len()));
            let start = at.saturating_sub(120);
            let first_end = (at + 240).min(first.len());
            let second_end = (at + 240).min(second.len());
            panic!(
                "expansions first differ at byte {at}\nfirst:  {}\nsecond: {}",
                &first[start..first_end],
                &second[start..second_end],
            );
        }
    }
}

/// The generated table carries its own declaration.
#[cfg(test)]
mod schema_const {
    use proc_macro2::{TokenStream, TokenTree};
    use quote::quote;
    use worktable_dsl::Schema;

    use super::expand;

    /// Pull the string out of `pub const <NAME>: &str = "..";` in generated code.
    fn baked_schema(generated: TokenStream, const_name: &str) -> String {
        let mut trees = generated
            .into_iter()
            .skip_while(|tree| !matches!(tree, TokenTree::Ident(ident) if ident == const_name));
        assert!(trees.next().is_some(), "no `{const_name}` const in the generated code");
        for tree in trees {
            if let TokenTree::Literal(literal) = tree {
                let text = literal.to_string();
                return syn::parse_str::<syn::LitStr>(&text).expect("a string literal").value();
            }
        }
        panic!("`{const_name}` has no value");
    }

    #[test]
    fn a_persisted_table_carries_the_declaration_it_was_built_from() {
        let declaration = quote! {
            name: Account,
            version: 3,
            persist: true,
            columns: {
                id: u64 primary_key autoincrement,
                email: String,
                nickname: String optional,
            },
            indexes: { email_idx: email unique },
            queries: { update: { Nickname(nickname) by id } }
        };

        let baked = baked_schema(expand(declaration.clone()).expect("expands"), "ACCOUNT_SCHEMA");

        assert_eq!(
            Schema::parse(&baked).expect("the baked text parses"),
            Schema::from_tokens(declaration).expect("the declaration parses"),
            "the baked declaration is not the one the table was generated from"
        );
    }

    #[test]
    fn an_in_memory_table_carries_it_too() {
        // A designer reading a crate wants every table, not only the persisted
        // ones, and the const costs a string either way.
        let declaration = quote! {
            name: Price,
            partition_by: symbol_id: u16,
            partition_max_size: u64,
            columns: { exchange_id: u8 primary_key, bid: f64 },
        };

        let baked = baked_schema(expand(declaration.clone()).expect("expands"), "PRICE_SCHEMA");

        assert_eq!(
            Schema::parse(&baked).expect("the baked text parses"),
            Schema::from_tokens(declaration).expect("the declaration parses"),
        );
    }

    #[test]
    fn the_baked_text_is_a_declaration_the_macro_accepts() {
        // Which is what makes it usable as the old table definition a
        // migration would otherwise need kept by hand.
        let declaration = quote! {
            name: Regenerated,
            persist: true,
            columns: { id: u64 primary_key autoincrement, payload: String },
            indexes: { payload_idx: payload unique }
        };

        let baked = baked_schema(expand(declaration).expect("expands"), "REGENERATED_SCHEMA");
        let reparsed: TokenStream = syn::parse_str(&baked).expect("tokenises");
        expand(reparsed).expect("the baked declaration expands");
    }

    /// A paged table cannot take a hash index, and says why.
    ///
    /// Both halves matter. The refusal has to fire, because the alternative is
    /// failing somewhere inside the index generators on a type the author never
    /// wrote; and it has to name `vec: true`, because the backend does work
    /// there and a refusal that does not say where to go sends people to the
    /// issue tracker.
    #[test]
    fn fxhash_is_refused_on_a_paged_table() {
        let on_the_primary_key = expand(quote! {
            name: HashedPaged,
            columns: {
                id: u64 primary_key using fxhash,
                value: u64,
            },
        })
        .unwrap_err()
        .to_string();
        assert!(
            on_the_primary_key.contains("vec: true"),
            "the refusal must say where the backend does work: {on_the_primary_key}"
        );
        assert!(
            on_the_primary_key.contains("ordered scan"),
            "the refusal must say why: {on_the_primary_key}"
        );

        // And on a secondary, which reaches the same check by the other branch.
        let on_a_secondary = expand(quote! {
            name: HashedSecondary,
            columns: {
                id: u64 primary_key,
                value: u64,
            },
            indexes: {
                value_idx: value unique using fxhash,
            },
        })
        .unwrap_err()
        .to_string();
        assert!(
            on_a_secondary.contains("value_idx"),
            "the refusal must name the index the author wrote: {on_a_secondary}"
        );
    }

    /// A `vec: true` table accepts it, which is what makes the refusal above a
    /// redirection rather than a ban.
    #[test]
    fn fxhash_is_accepted_on_a_vec_table() {
        let output = expand(quote! {
            name: HashedVec,
            vec: true,
            columns: {
                id: u64 primary_key using fxhash,
                value: u64,
            },
        })
        .expect("a vec: true table takes a hash index");
        let text = output.to_string();
        assert!(text.contains("FxHashMap"), "the table should hold a hash map");
        assert!(
            !text.contains("pub fn range"),
            "a hash-backed table must not get a range method"
        );
    }
}

/// What the `runtime:` key generates.
///
/// The table's runtime is named once, as `#{Name}Runtime`, and these assert the
/// mapping from `codegen::generators::runtime_backend` reaches that alias
/// unchanged. The mapping itself is unit-tested next to the function; what is
/// checked here is that a declaration selects it.
///
/// Gated on `std` because the alias is: a build with no runtime emits no
/// runtime type. Without the gate these tests pass under `cargo test
/// --workspace`, where feature unification turns `std` on for them, and fail
/// under `cargo test -p worktable_codegen`, where nothing does. A test whose
/// result depends on which crate you ran it from is a false green either way.
#[cfg(all(test, feature = "std"))]
mod runtime_tests {
    use quote::quote;

    use super::expand;

    /// Everything up to the alias, so a comparison is not defeated by the
    /// unrelated tokens either side of it.
    fn runtime_alias(declaration: proc_macro2::TokenStream) -> String {
        let output = expand(declaration).expect("expands").to_string();
        let alias = output
            .split("pub type SelectRuntime = ")
            .nth(1)
            .expect("the generated runtime alias");
        alias.split(';').next().expect("the alias body").trim().to_string()
    }

    fn declaration(runtime: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
        quote! {
            name: Select,
            persist: false,
            columns: {
                id: u64 primary_key,
                value: u64,
            },
            #runtime
        }
    }

    /// A bare `nagoya` is whatever flavor is currently the default, spelled
    /// out of the registry rather than named here, so that moving the default
    /// is one edit rather than a hunt through the tests.
    #[test]
    fn bare_nagoya_selects_the_default_tuning() {
        let expected = format!("NagoyaRt < {} >", worktable_dsl::model::Flavor::default().type_name());
        assert_eq!(runtime_alias(declaration(quote! { runtime: nagoya, })), expected);
    }

    #[test]
    fn each_flavor_selects_its_marker() {
        assert_eq!(
            runtime_alias(declaration(quote! { runtime: nagoya(locality), })),
            "NagoyaRt < Locality >"
        );
        assert_eq!(
            runtime_alias(declaration(quote! { runtime: nagoya(spread), })),
            "NagoyaRt < Spread >"
        );
        assert_eq!(
            runtime_alias(declaration(quote! { runtime: nagoya(throughput), })),
            "NagoyaRt < Throughput >"
        );
    }

    #[test]
    fn tokio_selects_the_tokio_runtime() {
        assert_eq!(runtime_alias(declaration(quote! { runtime: tokio, })), "TokioRt");
    }

    /// The no-regression guarantee, and the reason it is stated on the whole
    /// expansion rather than on the alias: every `worktable!` written before
    /// this key existed omits it, and none of them may generate a different
    /// byte than they would with `runtime: nagoya` written in.
    #[test]
    fn omitting_the_key_emits_exactly_what_bare_nagoya_emits() {
        let omitted = expand(declaration(quote! {})).expect("expands").to_string();
        let declared = expand(declaration(quote! { runtime: nagoya, }))
            .expect("expands")
            .to_string();

        assert_same_tokens(&omitted, &declared);
    }

    /// Borrowed from `generator_determinism`: two expansions that differ by one
    /// token differ by one byte in a string thousands of bytes long, and
    /// `assert_eq!` prints both in full rather than saying where.
    fn assert_same_tokens(first: &str, second: &str) {
        if first == second {
            return;
        }
        let at = first
            .bytes()
            .zip(second.bytes())
            .position(|(left, right)| left != right)
            .unwrap_or_else(|| first.len().min(second.len()));
        let start = at.saturating_sub(120);
        let first_end = (at + 240).min(first.len());
        let second_end = (at + 240).min(second.len());
        panic!(
            "expansions first differ at byte {at}\nfirst:  {}\nsecond: {}",
            &first[start..first_end],
            &second[start..second_end],
        );
    }

    /// The free-order position: `runtime` is an arm beside the blocks, so it
    /// may be written before or after any of them.
    #[test]
    fn the_key_may_be_written_before_or_after_the_blocks() {
        let before = expand(quote! {
            name: Select,
            persist: false,
            runtime: nagoya(spread),
            columns: { id: u64 primary_key, value: u64 },
        })
        .expect("expands")
        .to_string();
        let after = expand(declaration(quote! { runtime: nagoya(spread), }))
            .expect("expands")
            .to_string();

        assert_same_tokens(&before, &after);
    }

    #[test]
    fn a_second_runtime_key_is_a_duplicate_section() {
        let error = expand(quote! {
            name: Select,
            persist: false,
            columns: { id: u64 primary_key },
            runtime: nagoya,
            runtime: tokio,
        })
        .unwrap_err()
        .to_string();

        assert!(error.contains("duplicate `runtime` section"), "{error}");
    }

    #[test]
    fn an_unimplemented_backend_is_refused_by_name() {
        for name in ["forte", "blocking", "bwos"] {
            let name: proc_macro2::TokenStream = name.parse().unwrap();
            let error = expand(declaration(quote! { runtime: #name, })).unwrap_err().to_string();

            assert!(error.contains("recognised but not implemented"), "{error}");
            assert!(error.contains("`nagoya` and `tokio`"), "{error}");
        }
    }

    #[test]
    fn an_unknown_flavor_is_refused_with_every_flavor_that_exists() {
        let error = expand(declaration(quote! { runtime: nagoya(banana), }))
            .unwrap_err()
            .to_string();

        assert!(error.contains("unknown nagoya flavor `banana`"), "{error}");
        // Every flavor in the registry, rather than a sentence. Pinning the
        // wording is how this test came to fail for adding a flavor, which is
        // the one thing it should not object to.
        for flavor in worktable_dsl::model::Flavor::ALL {
            assert!(error.contains(flavor.name()), "{} missing from: {error}", flavor.name());
        }
    }

    #[test]
    fn tokio_is_refused_a_flavor() {
        let error = expand(declaration(quote! { runtime: tokio(spread), }))
            .unwrap_err()
            .to_string();

        assert!(error.contains("`tokio` has no flavors"), "{error}");
    }

    /// The middle step of the fallback chain, at the only site that can show it
    /// today: a table that declares a runtime and annotates no section reaches
    /// the declared backend, not the built-in default.
    #[test]
    fn an_unannotated_table_body_takes_the_tables_runtime() {
        let alias = runtime_alias(quote! {
            name: Select,
            persist: false,
            runtime: tokio,
            columns: { id: u64 primary_key, value: u64 },
            queries: {
                update: { Value(value) by id, },
                delete: { ById() by id, },
            }
        });

        assert_eq!(alias, "TokioRt");
    }
}
