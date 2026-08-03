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
    let memory_only = if columns.primary_index_backend.is_memory_only() {
        Some((
            columns.primary_index_backend,
            columns.primary_keys.first().expect("primary key exists"),
            true,
        ))
    } else {
        columns
            .indexes
            .values()
            .find(|index| index.backend.is_memory_only())
            .map(|index| (index.backend, &index.name, false))
    };

    if let Some((backend, ident, is_primary)) = memory_only {
        let kind = if is_primary { "primary index" } else { "index" };
        match persistence {
            Persistence::MemoryOnly => {}
            Persistence::Omitted => {
                return Err(syn::Error::new(
                    ident.span(),
                    format!(
                        "{kind} `{ident}` uses `{}`, which requires an explicitly written `persist: false`",
                        backend.name()
                    ),
                ));
            }
            Persistence::Persisted => {
                return Err(syn::Error::new(
                    ident.span(),
                    format!(
                        "{kind} `{ident}` uses `{}`, but persisted and S3-backed tables require `worktables_index` or `indexset`",
                        backend.name()
                    ),
                ));
            }
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
    fn memory_backend_requires_explicit_false() {
        let error = expand(quote! {
            name: MissingAcknowledgement,
            columns: {
                id: u64 primary_key using congee,
            },
        })
        .unwrap_err();

        assert!(error.to_string().contains("explicitly written `persist: false`"));
    }

    #[test]
    fn memory_backend_rejects_persistence() {
        let error = expand(quote! {
            name: PersistentArctic,
            persist: true,
            columns: {
                id: u64 primary_key,
                value: u64,
            },
            indexes: {
                value_idx: value unique using arctic,
            },
        })
        .unwrap_err();

        assert!(error.to_string().contains("persisted and S3-backed tables"));
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

        assert!(error.to_string().contains("does not support primary-key type `String`"));
    }
}
