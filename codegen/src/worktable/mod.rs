use proc_macro2::TokenStream;

use crate::common::Parser;
use crate::common::model::{Columns, IndexBackend, Persistence};

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let mut parser = Parser::new(input);
    let mut columns = None;
    let mut queries = None;
    let mut indexes = None;
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

    validate_index_backends(&columns, persistence)?;

    if persistence.is_persisted() {
        crate::generators::persist::expand(name, columns, queries, config, version)
    } else {
        crate::generators::in_memory::expand_from_parsed(name, columns, queries, config)
    }
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
        });

        assert!(output.is_ok());
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
