use proc_macro2::{Ident, TokenStream};
use quote::quote;

use crate::common::model::IndexBackend;

/// Generates the concrete unique-map type selected by the DSL while keeping
/// WorkTablesIndex's custom node type available for persisted/unsized indexes.
pub(crate) fn unique_index_type(
    backend: IndexBackend,
    key: &TokenStream,
    value: &TokenStream,
    worktables_node: Option<TokenStream>,
) -> syn::Result<TokenStream> {
    match backend {
        IndexBackend::WorktablesIndex => Ok(match worktables_node {
            Some(node) => quote! { IndexMap<#key, #value, #node> },
            None => quote! { IndexMap<#key, #value> },
        }),
        IndexBackend::Indexset => {
            if worktables_node.is_some() {
                Err(syn::Error::new_spanned(
                    key,
                    "`using indexset` does not yet support variable-sized keys; use `worktables_index` for this index",
                ))
            } else {
                Ok(quote! { UpstreamIndexMap<#key, #value> })
            }
        }
        IndexBackend::Congee => Ok(quote! { CongeeIndex<#key, #value> }),
        IndexBackend::Arctic => Ok(quote! { ArcticIndex<#key, #value> }),
    }
}

/// Generates the persisted-table variant. ART backends receive a write-side
/// sequencing wrapper; memory-only tables keep the zero-overhead native type.
pub(crate) fn persistent_unique_index_type(
    backend: IndexBackend,
    key: &TokenStream,
    value: &TokenStream,
    worktables_node: Option<TokenStream>,
) -> syn::Result<TokenStream> {
    // This intentionally evaluates the proc-macro crate's feature. WorkTable's
    // public feature forwards to worktable_codegen in Cargo.toml, so the
    // runtime types and emitted types are selected together. Emitting a cfg in
    // the expansion would instead test the consuming package's unrelated
    // feature namespace, which may rename or omit the dependency feature.
    match backend {
        IndexBackend::WorktablesIndex if cfg!(feature = "logical-index-persistence") => Ok(match worktables_node {
            Some(node) => quote! { PersistentWtiIndex<#key, #value, #node> },
            None => quote! { PersistentWtiIndex<#key, #value> },
        }),
        IndexBackend::Congee => Ok(quote! { PersistentCongeeIndex<#key, #value> }),
        IndexBackend::Arctic => Ok(quote! { PersistentArcticIndex<#key, #value> }),
        _ => unique_index_type(backend, key, value, worktables_node),
    }
}

/// Generates the small codec needed when an ART is used for WorkTable's
/// generated primary-key newtype. ART backends intentionally accept only the
/// lossless, native integer shapes supported by their public APIs.
pub(crate) fn primary_key_backend_impl(
    backend: IndexBackend,
    primary_key: &Ident,
    fields: &[&TokenStream],
) -> syn::Result<(TokenStream, TokenStream)> {
    match backend {
        IndexBackend::WorktablesIndex | IndexBackend::Indexset => Ok((quote! {}, quote! {})),
        IndexBackend::Congee => {
            let field = single_supported_field(backend, fields, supported_types(backend))?;
            let width_guard = if primitive_name(field).as_deref() == Some("u64") {
                quote! {
                    #[cfg(not(target_pointer_width = "64"))]
                    compile_error!("`using congee` with a `u64` primary key requires a 64-bit target");
                }
            } else {
                quote! {}
            };

            Ok((
                quote! { Copy, },
                quote! {
                    #width_guard
                    impl CongeeKey for #primary_key {
                        #[inline]
                        fn into_congee(self) -> usize {
                            CongeeKey::into_congee(self.0)
                        }

                        #[inline]
                        fn from_congee(value: usize) -> Self {
                            Self(<#field as CongeeKey>::from_congee(value))
                        }
                    }

                    impl ArtPersistenceKey for #primary_key {
                        const WIDTH: u8 = <#field as ArtPersistenceKey>::WIDTH;

                        fn encode_art_key(&self, output: &mut Vec<u8>) {
                            self.0.encode_art_key(output)
                        }

                        fn decode_art_key(bytes: &[u8]) -> eyre::Result<Self> {
                            Ok(Self(<#field as ArtPersistenceKey>::decode_art_key(bytes)?))
                        }
                    }
                },
            ))
        }
        IndexBackend::Arctic => {
            let field = single_supported_field(backend, fields, supported_types(backend))?;
            Ok((
                quote! {},
                quote! {
                    impl ArcticKey for #primary_key {
                        type Raw = <#field as ArcticKey>::Raw;

                        #[inline]
                        fn to_arctic(&self) -> Self::Raw {
                            ArcticKey::to_arctic(&self.0)
                        }

                        #[inline]
                        fn from_arctic(value: Self::Raw) -> Self {
                            Self(<#field as ArcticKey>::from_arctic(value))
                        }
                    }

                    impl ArtPersistenceKey for #primary_key {
                        const WIDTH: u8 = <#field as ArtPersistenceKey>::WIDTH;

                        fn encode_art_key(&self, output: &mut Vec<u8>) {
                            self.0.encode_art_key(output)
                        }

                        fn decode_art_key(bytes: &[u8]) -> eyre::Result<Self> {
                            Ok(Self(<#field as ArtPersistenceKey>::decode_art_key(bytes)?))
                        }
                    }
                },
            ))
        }
    }
}

/// The key types a backend accepts, from `worktable_dsl` so the macro and the
/// editor-facing check cannot drift. They were two hand-maintained lists, and
/// widening one without the other produced a check that accepted a
/// declaration the macro then refused.
fn supported_types(backend: IndexBackend) -> &'static [&'static str] {
    worktable_dsl::validate::supported_key_types(backend)
        .expect("a backend reaching this point declares a key-type list")
}

fn single_supported_field<'a>(
    backend: IndexBackend,
    fields: &'a [&TokenStream],
    supported: &[&str],
) -> syn::Result<&'a TokenStream> {
    let [field] = fields else {
        return Err(syn::Error::new_spanned(
            fields.first().copied().cloned().unwrap_or_default(),
            format!("`using {}` requires a single-column primary key", backend.name()),
        ));
    };
    let primitive = primitive_name(field);
    if !primitive.as_deref().is_some_and(|name| supported.contains(&name)) {
        return Err(syn::Error::new_spanned(
            *field,
            format!(
                "`using {}` requires a directly named primitive primary-key type; found `{}`; supported types: {} (type aliases cannot be resolved by the macro)",
                backend.name(),
                field,
                supported.join(", ")
            ),
        ));
    }
    Ok(field)
}

fn primitive_name(field: &TokenStream) -> Option<String> {
    let syn::Type::Path(type_path) = syn::parse2::<syn::Type>(field.clone()).ok()? else {
        return None;
    };
    type_path.path.segments.last().map(|segment| segment.ident.to_string())
}
