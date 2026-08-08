use proc_macro2::TokenStream;

use crate::common::Parser;
use crate::common::model::{Columns, IndexBackend, Persistence};

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let mut parser = Parser::new(input);
    let mut columns = None;
    let mut queries = None;
    let mut indexes = None;
    let mut columnar_indexes = None;
    let mut config = None;

    let name = parser.parse_name()?;
    let version = parser.parse_version()?.unwrap_or(1);
    let persistence = parser.parse_persist()?;
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
            "version" => {
                return Err(syn::Error::new(
                    ident.span(),
                    "version must be specified before columns/indexes/queries/config",
                ));
            }
            "attributes" => {
                return Err(syn::Error::new(
                    ident.span(),
                    "a separate `attributes` section is not part of the 1.0 grammar; keep `primary_key`, `autoincrement`, `custom`, `optional`, and `using` inline on their column or index declarations",
                ));
            }
            _ => return Err(syn::Error::new(ident.span(), "Unexpected identifier")),
        }
    }

    let mut columns = columns.expect("defined");
    if let Some(i) = indexes {
        columns.indexes = i
    }
    if let Some(i) = columnar_indexes {
        columns.columnar_indexes = i.indexes;
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

    validate_index_backends(&columns, persistence)?;
    validate_columnar_indexes(&columns)?;

    if persistence.is_persisted() {
        crate::generators::persist::expand(name, columns, queries, config, version)
    } else {
        crate::generators::in_memory::expand_from_parsed(name, columns, queries, config)
    }
}

fn validate_columnar_indexes(columns: &Columns) -> syn::Result<()> {
    for primary_key in &columns.primary_keys {
        if columns.columnar_fields.contains_key(primary_key) {
            return Err(syn::Error::new(
                primary_key.span(),
                "the primary key participates in columnar identity implicitly and must not declare `columnar`",
            ));
        }
    }

    if !columns.columnar_indexes.is_empty() && columns.columnar_fields.is_empty() {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "`columnar_indexes` requires at least one field declaring `columnar`",
        ));
    }

    for index in columns.columnar_indexes.values() {
        if columns.columnar_fields.contains_key(&index.name) {
            return Err(syn::Error::new(
                index.name.span(),
                format!(
                    "columnar index `{}` conflicts with a columnar field name and would generate duplicate scan methods",
                    index.name
                ),
            ));
        }
        for field in &index.cluster_by {
            if !columns.columns_map.contains_key(field) {
                return Err(syn::Error::new(
                    field.span(),
                    format!("columnar index `{}` references unknown field `{field}`", index.name),
                ));
            }
            if !columns.columnar_fields.contains_key(field) {
                return Err(syn::Error::new(
                    field.span(),
                    format!(
                        "columnar index `{}` requires field `{field}` to declare `columnar(...)`",
                        index.name
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn validate_index_backends(columns: &Columns, persistence: Persistence) -> syn::Result<()> {
    let explicit_backend = if columns.primary_index_backend.requires_explicit_persistence() {
        Some((
            columns.primary_index_backend,
            columns.primary_keys.first().expect("primary key exists"),
            true,
        ))
    } else {
        columns
            .indexes
            .values()
            .find(|index| index.backend.requires_explicit_persistence())
            .map(|index| (index.backend, &index.name, false))
    };

    if let Some((backend, ident, is_primary)) = explicit_backend {
        let kind = if is_primary { "primary index" } else { "index" };
        match persistence {
            Persistence::MemoryOnly => {}
            Persistence::Omitted => {
                return Err(syn::Error::new(
                    ident.span(),
                    format!(
                        "{kind} `{ident}` uses `{}`, which requires an explicit `persist: true` or `persist: false`",
                        backend.name()
                    ),
                ));
            }
            Persistence::Persisted => {}
        }
    }

    if let Some(index) = columns
        .indexes
        .values()
        .find(|index| !index.is_unique && index.backend != IndexBackend::WorktablesIndex)
    {
        return Err(syn::Error::new(
            index.name.span(),
            format!(
                "non-unique index `{}` cannot use `{}`; non-unique indexes currently require `worktables_index`",
                index.name,
                index.backend.name()
            ),
        ));
    }

    for (column, index) in &columns.indexes {
        let key_type = columns
            .columns_map
            .get(column)
            .expect("an index always references a validated column")
            .to_string();
        let supported = match index.backend {
            IndexBackend::Congee => Some(&["u8", "u16", "u32", "u64", "usize"][..]),
            IndexBackend::Arctic => Some(&["u16", "u32", "u64", "u128"][..]),
            IndexBackend::WorktablesIndex | IndexBackend::Indexset => None,
        };
        if let Some(supported) = supported
            && !supported.contains(&key_type.as_str())
        {
            return Err(syn::Error::new(
                index.name.span(),
                format!(
                    "index `{}` uses `{}`, which does not support key type `{key_type}`; supported types: {}",
                    index.name,
                    index.backend.name(),
                    supported.join(", ")
                ),
            ));
        }
    }

    Ok(())
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
                    tenant_id: u64 primary_key,
                    record_id: u64 primary_key,
                    value: i64,
                },
            })
            .unwrap();

            assert_composite_primary_key_field_order(output);
        }
    }

    #[test]
    fn absent_using_keeps_worktables_index_default() {
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

        if cfg!(feature = "logical-index-persistence") {
            assert!(output.contains("PersistentWtiIndex"));
        } else {
            assert!(output.contains("IndexMap"));
            assert!(!output.contains("PersistentWtiIndex"));
        }
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
    fn logical_persistence_wraps_only_default_wti_backends() {
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
                wti_idx: wti_value unique,
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
    fn art_backend_requires_explicit_persistence_choice() {
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
    fn memory_backend_rejects_non_unique_indexes() {
        let error = expand(quote! {
            name: NonUniqueArctic,
            persist: false,
            columns: {
                id: u64 primary_key,
                value: u64,
            },
            indexes: {
                value_idx: value using arctic,
            },
        })
        .unwrap_err();

        assert!(error.to_string().contains("non-unique indexes currently require"));
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
            name: ByteArctic,
            persist: false,
            columns: {
                id: u64 primary_key,
                value: u8,
            },
            indexes: {
                value_idx: value unique using arctic,
            },
        })
        .unwrap_err();

        assert!(error.to_string().contains("supported types: u16, u32, u64, u128"));
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
