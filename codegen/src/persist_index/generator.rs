use std::collections::HashMap;

use proc_macro2::{Ident, Literal, TokenStream};
use quote::__private::Span;
use quote::{ToTokens, quote};
use syn::{Field, ItemStruct};

use crate::common::name_generator::{WorktableNameGenerator, is_unsized};
use crate::persist_table::WT_INDEX_EXTENSION;

#[derive(Default)]
pub struct PersistIndexAttributes {
    pub read_only: bool,
}

pub struct Generator {
    pub struct_def: ItemStruct,
    pub field_types: HashMap<Ident, TokenStream>,
    pub attributes: PersistIndexAttributes,
}

pub(super) struct IndexLayout {
    type_ident: Ident,
    is_unique: bool,
    uses_upstream: bool,
    pub(super) art_backend: Option<ArtBackend>,
    pub(super) logical_wti: bool,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum ArtBackend {
    Arctic,
    Congee,
}

pub(super) fn index_layout(field: &Field) -> syn::Result<IndexLayout> {
    let syn::Type::Path(type_path) = &field.ty else {
        return Err(syn::Error::new_spanned(
            &field.ty,
            "index field must be a concrete index type",
        ));
    };
    let type_ident = type_path
        .path
        .segments
        .last()
        .ok_or_else(|| syn::Error::new_spanned(&field.ty, "index type path cannot be empty"))?
        .ident
        .clone();
    let (is_unique, uses_upstream, art_backend, logical_wti) = match type_ident.to_string().as_str() {
        "IndexMap" | "TreeIndex" => (true, false, None, false),
        "PersistentWtiIndex" => (true, false, None, true),
        "UpstreamIndexMap" => (true, true, None, false),
        "IndexMultiMap" | "TreeMultiIndex" => (false, false, None, false),
        "PersistentArcticIndex" => (true, false, Some(ArtBackend::Arctic), false),
        "PersistentCongeeIndex" => (true, false, Some(ArtBackend::Congee), false),
        _ => {
            return Err(syn::Error::new_spanned(
                &field.ty,
                "unsupported persisted index type; use a WorkTable-generated index backend directly",
            ));
        }
    };
    Ok(IndexLayout {
        type_ident,
        is_unique,
        uses_upstream,
        art_backend,
        logical_wti,
    })
}

impl WorktableNameGenerator {
    pub fn from_index_ident(index_ident: &Ident) -> Self {
        Self {
            name: index_ident
                .to_string()
                .strip_suffix("Index")
                .expect("index type nae should end on `Index`")
                .to_string(),
        }
    }

    pub fn get_persisted_index_ident(&self) -> Ident {
        Ident::new(format!("{}IndexPersisted", self.name).as_str(), Span::mixed_site())
    }
}

impl Generator {
    pub fn with_attributes(struct_def: ItemStruct, attributes: PersistIndexAttributes) -> Self {
        let mut fields = vec![];
        let mut types = vec![];

        for field in &struct_def.fields {
            fields.push(field.ident.clone().expect("index fields should always be named fields"));

            let syn::Type::Path(type_path) = &field.ty else {
                unreachable!();
            };

            let last_segment = type_path
                .path
                .segments
                .last()
                .expect("Index type should have at least one segment");

            let syn::PathArguments::AngleBracketed(arguments) = &last_segment.arguments else {
                unreachable!("IndexMap always have angle brackets arguments which are generic")
            };

            let first_arg = arguments
                .args
                .first()
                .expect("Index type should have at least one type argument");

            let syn::GenericArgument::Type(ty) = first_arg else {
                unreachable!("Index type should have at least one type argument")
            };

            types.push(ty.to_token_stream());
        }
        let map = fields.into_iter().zip(types).collect::<HashMap<_, _>>();

        Self {
            struct_def,
            field_types: map,
            attributes,
        }
    }

    /// Generates persisted index type. This type has same name as index, but with `Persisted` postfix. Field names of
    /// this type are same to index type, and values are `Vec<GeneralPage<IndexPage<T>>>`, where `T` is index key
    /// type.
    pub fn gen_persist_type(&mut self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let name_ident = name_generator.get_persisted_index_ident();

        let fields: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|field| {
                let layout = index_layout(field)?;
                let i = field.ident.as_ref().expect("index fields should be named");
                let t = self.field_types.get(i).expect("field type was collected");
                if layout.art_backend.is_some() {
                    let field_type = &field.ty;
                    Ok(quote! { #i: #field_type, })
                } else if is_unsized(&t.to_string()) {
                    let const_size = name_generator.get_page_inner_size_const_ident();
                    Ok(quote! {
                        #i: (Vec<GeneralPage<TableOfContentsPage<(#t, Link)>>>, Vec<GeneralPage<UnsizedIndexPage<#t, {#const_size as u32}>>>),
                    })
                } else {
                    Ok(quote! {
                        #i: (Vec<GeneralPage<TableOfContentsPage<(#t, Link)>>>, Vec<GeneralPage<IndexPage<#t>>>),
                    })
                }
            })
            .collect::<syn::Result<Vec<_>>>()?;

        Ok(quote! {
            #[derive(Debug, Default)]
            pub struct #name_ident {
                #(#fields)*
            }
        })
    }

    pub fn gen_persist_impl(&mut self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let name_ident = name_generator.get_persisted_index_ident();

        let persist_fn = if self.attributes.read_only {
            quote! {}
        } else {
            self.gen_persist_fn()
        };
        let parse_from_file_fn = self.gen_parse_from_file_fn();

        Ok(quote! {
            impl #name_ident {
                #persist_fn
                #parse_from_file_fn
            }
        })
    }

    /// Generates `persist` function for persisted index. It calls `persist_page` function for every page in index.
    fn gen_persist_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let ident = name_generator.get_work_table_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();
        let version_const_name = name_generator.get_version_const_ident();
        let index_extension = Literal::string(WT_INDEX_EXTENSION);

        let persist_logic = self
            .struct_def
            .fields
            .iter()
            .map(|field| {
                let layout = index_layout(field)?;
                let i = field.ident.as_ref().expect("index fields should be named");
                let ty = self.field_types.get(i).expect("field type was collected");
                let index_name_literal = Literal::string(i.to_string().as_str());
                Ok(match layout.art_backend {
                    Some(ArtBackend::Arctic) => quote! {
                        SpaceArcticIndex::<#ty, { #inner_const_name as u32 }>::write_checkpoint(
                            format!("{}/{}{}", path, #index_name_literal, #index_extension),
                            #version_const_name,
                            &mut self.#i,
                        ).await?;
                    },
                    Some(ArtBackend::Congee) => quote! {
                        SpaceCongeeIndex::<#ty, { #inner_const_name as u32 }>::write_checkpoint(
                            format!("{}/{}{}", path, #index_name_literal, #index_extension),
                            #version_const_name,
                            &mut self.#i,
                        ).await?;
                    },
                    None => quote! {
                        {
                            let mut file = tokio::fs::File::create(format!("{}/{}{}", path, #index_name_literal, #index_extension)).await?;
                            let mut info = #ident::space_info_default();
                            info.inner.page_count = self.#i.1.len() as u32 + self.#i.0.len() as u32;
                            persist_page(&mut info, &mut file).await?;
                            for mut page in &mut self.#i.0 {
                                persist_page(&mut page, &mut file).await?;
                            }
                            for mut page in &mut self.#i.1 {
                                persist_page(&mut page, &mut file).await?;
                            }
                        }
                    },
                })
            })
            .collect::<syn::Result<Vec<_>>>()
            .expect("generated index layouts were validated");

        quote! {
            pub async fn persist(&mut self, path: &str) -> eyre::Result<()>
            {
                #(#persist_logic)*
                Ok(())
            }
        }
    }

    /// Generates `parse_from_file` function for persisted index. It calls `parse_page` function for every page in each
    /// index interval and collects them into `Vec`'s. Then this `Vec`'s are used to construct persisted index object.
    fn gen_parse_from_file_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let page_const_name = name_generator.get_page_size_const_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();
        let version_const_name = name_generator.get_version_const_ident();
        let index_extension = Literal::string(WT_INDEX_EXTENSION);

        let field_names_literals: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|field| {
                let layout = index_layout(field)?;
                let i = field.ident.as_ref().expect("index fields should be named");
                let ty = self.field_types.get(i).expect("field type was collected");
                let literal = Literal::string(i.to_string().as_str());
                Ok(match layout.art_backend {
                    Some(ArtBackend::Arctic) => quote! {
                        let #i = SpaceArcticIndex::<#ty, { #inner_const_name as u32 }>::load_index(
                            format!("{}/{}{}", path, #literal, #index_extension),
                            #version_const_name,
                        ).await?;
                    },
                    Some(ArtBackend::Congee) => quote! {
                        let #i = SpaceCongeeIndex::<#ty, { #inner_const_name as u32 }>::load_index(
                            format!("{}/{}{}", path, #literal, #index_extension),
                            #version_const_name,
                        ).await?;
                    },
                    None => quote! {
                        let #i = {
                            let mut #i = vec![];
                            let mut file = tokio::fs::File::open(format!("{}/{}{}", path, #literal, #index_extension)).await?;
                            let info = parse_page::<SpaceInfoPage<()>, { #page_const_name as u32 }>(&mut file, 0).await?;
                            let file_length = file.metadata().await?.len();
                            // Pages sit at a fixed #page_const_name stride
                            // (header inside the slot): the next free page id
                            // is ceil(len / stride). The previous divisor used
                            // stride + header and an unconditional +1.
                            let page_id = file_length.div_ceil(#page_const_name as u64);
                            let next_page_id = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(page_id as u32));
                            let toc = IndexTableOfContents::<_, { #page_const_name as u32 }>::parse_from_file(&mut file, 0.into(), next_page_id.clone()).await?;
                            for page_id in toc.iter().map(|(_, page_id)| page_id) {
                                let index = parse_page::<_, { #page_const_name as u32 }>(&mut file, (*page_id).into()).await?;
                                #i.push(index);
                            }
                            (toc.pages, #i)
                        };
                    }
                })
            })
            .collect::<syn::Result<Vec<_>>>()
            .expect("generated index layouts were validated");

        let idents = self
            .struct_def
            .fields
            .iter()
            .map(|f| f.ident.as_ref().expect("index fields should always be named fields"))
            .collect::<Vec<_>>();

        quote! {
            pub async fn parse_from_file(path: &str) -> eyre::Result<Self> {
                #(#field_names_literals)*

                Ok(Self {
                    #(#idents,)*
                })
            }
        }
    }

    /// Generates `PersistableIndex` trait implementation for persisted index.
    pub fn gen_persistable_impl(&self) -> syn::Result<TokenStream> {
        let ident = &self.struct_def.ident;
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let name_ident = name_generator.get_persisted_index_ident();

        let get_persisted_index_fn = if self.attributes.read_only {
            let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
            let name_ident = name_generator.get_persisted_index_ident();
            quote! {
                fn get_persisted_index(&self) -> Self::PersistedIndex {
                    #name_ident::default()
                }
            }
        } else {
            self.gen_get_persisted_index_fn()?
        };
        let from_persisted_fn = self.gen_from_persisted_fn()?;

        Ok(quote! {
            impl PersistableIndex for #ident {
                type PersistedIndex = #name_ident;

                #get_persisted_index_fn
                #from_persisted_fn
            }
        })
    }

    /// Generates `get_persisted_index` function of `PersistableIndex` trait for persisted index. It maps every
    /// `TreeIndex` into `Vec` of `IndexPage`s using `IndexPage::from_nod` function.
    fn gen_get_persisted_index_fn(&self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let const_name = name_generator.get_page_inner_size_const_ident();

        let idents = self
            .struct_def
            .fields
            .iter()
            .map(|f| f.ident.as_ref().expect("index fields should always be named fields"))
            .collect::<Vec<_>>();
        let field_names_init: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|field| {
                let layout = index_layout(field)?;
                let i = field
                    .ident
                    .as_ref()
                    .expect("index fields should always be named fields");
                let ty = self
                    .field_types
                    .get(i)
                    .expect("should be available as constructed from same values");
                if layout.art_backend.is_some() {
                    let field_type = &field.ty;
                    Ok(quote! {
                        let #i: #field_type = Default::default();
                        for (key, value) in self.#i.iter_values() {
                            #i.insert_value(key, value);
                        }
                    })
                } else if is_unsized(&ty.to_string()) {
                    Ok(quote! {
                        let mut pages = vec![];
                        for node in self.#i.iter_nodes() {
                            let page = UnsizedIndexPage::from_node(node.lock_arc().as_ref());
                            pages.push(page);
                        }
                        let (toc, pages) = map_unsized_index_pages_to_toc_and_general::<_, { #const_name as u32 }>(pages);
                        let #i = (toc.pages, pages);
                    })
                } else if layout.uses_upstream {
                    Ok(quote! {
                        let size = get_index_page_size_from_data_length::<#ty>(#const_name);
                        let mut pages = vec![];
                        for node in self.#i.iter_nodes() {
                            let node: Vec<IndexPair<#ty, OffsetEqLink>> = node
                                .lock_arc()
                                .iter()
                                .map(|pair| IndexPair {
                                    key: pair.key.clone(),
                                    value: pair.value,
                                })
                                .collect();
                            pages.push(IndexPage::from_node(&node, size));
                        }
                        let (toc, pages) = map_index_pages_to_toc_and_general::<_, { #const_name as u32 }>(pages);
                        let #i = (toc.pages, pages);
                    })
                } else {
                    Ok(quote! {
                        let size = get_index_page_size_from_data_length::<#ty>(#const_name);
                        let mut pages = vec![];
                        for node in self.#i.iter_nodes() {
                            let page = IndexPage::from_node(node.lock_arc().as_ref(), size);
                            pages.push(page);
                        }
                        let (toc, pages) = map_index_pages_to_toc_and_general::<_, { #const_name as u32 }>(pages);
                        let #i = (toc.pages, pages);
                    })
                }
            })
            .collect::<syn::Result<Vec<_>>>()?;

        Ok(quote! {
            fn get_persisted_index(&self) -> Self::PersistedIndex {
                #(#field_names_init)*
                Self::PersistedIndex {
                    #(#idents,)*
                }
            }
        })
    }

    /// Generates `from_persisted` function of `PersistableIndex` trait for persisted index. It maps every page in
    /// persisted page back to `TreeIndex`
    fn gen_from_persisted_fn(&self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let const_name = name_generator.get_page_inner_size_const_ident();

        let idents = self
            .struct_def
            .fields
            .iter()
            .map(|f| f.ident.as_ref().expect("index fields should always be named fields"))
            .collect::<Vec<_>>();
        let index_gen = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                let layout = index_layout(f)?;
                let i = f.ident.as_ref().expect("index fields should always be named fields");
                let is_unique = layout.is_unique;
                let uses_upstream = layout.uses_upstream;
                let t = layout.type_ident;
                let ty = self
                    .field_types
                    .get(i)
                    .expect("should be available as constructed from same values");

                // KEY FORMAT FACT driving the reconstruction below: index
                // pages store their cells in event-arrival order, but the
                // slot table preserves the tree-logical order the in-memory
                // node had when persisted, and `get_node()` resolves through
                // it. That logical order is authoritative and must be kept
                // verbatim: CDC events are positional (`InsertAt`/`RemoveAt`
                // carry an index into the node) and the on-disk page applies
                // them through the same slot order, so re-sorting entries at
                // reconstruction desyncs every later positional event and the
                // node-id successor tracking that keeps the table of contents
                // addressable.
                //
                // `unique_reconstruct` handles unique indexes: each page is
                // exactly one node, `get_node()` already yields it in order
                // with the true maximum last, so it attaches directly.
                let unique_reconstruct = |attach: TokenStream| {
                    let pair_type = if uses_upstream {
                        quote! { UpstreamIndexPair }
                    } else {
                        quote! { IndexPair }
                    };
                    quote! {
                        for page in persisted.#i.1 {
                            let inner: Vec<#pair_type<#ty, OffsetEqLink>> = page
                                .inner
                                .get_node()
                                .into_iter()
                                .map(|p| #pair_type {
                                    key: p.key,
                                    value: p.value.into(),
                                })
                                .collect();
                            #attach
                        }
                    }
                };
                // Non-unique indexes additionally need their duplicate
                // ordering re-derived (MultiPair discriminators are not
                // persisted), which has to be globally consistent across
                // nodes. They delegate to the runtime helper
                // `reconstruct_multi_index_nodes` (see its doc comment for
                // the ordering and discriminator invariants — notably why
                // same-max-key nodes must be ordered by their first entry
                // key, not by node-id link). The macro only maps pages into
                // (node_id, entries) form and attaches the returned nodes,
                // so the algorithm stays unit-testable with synthetic pages.
                let index_name_literal = Literal::string(i.to_string().as_str());
                let multi_reconstruct = |attach: TokenStream| {
                    quote! {
                        let mut raw_nodes: Vec<(IndexPair<#ty, OffsetEqLink>, Vec<IndexPair<#ty, OffsetEqLink>>)> =
                            Vec::with_capacity(persisted.#i.1.len());
                        for page in persisted.#i.1 {
                            let node_id = IndexPair {
                                key: page.inner.node_id.key.clone(),
                                value: OffsetEqLink(page.inner.node_id.link),
                            };
                            let entries = page
                                .inner
                                .get_node()
                                .into_iter()
                                .map(|p| IndexPair {
                                    key: p.key,
                                    value: OffsetEqLink(p.value),
                                })
                                .collect();
                            raw_nodes.push((node_id, entries));
                        }
                        for sorted in reconstruct_multi_index_nodes(#index_name_literal, raw_nodes) {
                            #attach
                        }
                    }
                };

                if layout.art_backend.is_some() {
                    Ok(quote! {
                        let #i = persisted.#i;
                    })
                } else if is_unsized(&ty.to_string()) {
                    if is_unique {
                        let body = unique_reconstruct(quote! {
                            let node = UnsizedNode::from_inner(inner, #const_name);
                            #i.attach_node(node);
                        });
                        Ok(quote! {
                            let #i: #t<_, OffsetEqLink, UnsizedNode<_>> = #t::with_maximum_node_size(#const_name);
                            #body
                        })
                    } else {
                        let body = multi_reconstruct(quote! {
                            let node = UnsizedNode::from_inner(sorted, #const_name);
                            #i.attach_multi_node(node);
                        });
                        Ok(quote! {
                            let #i: #t<_, OffsetEqLink, UnsizedNode<_>> = #t::with_maximum_node_size(#const_name);
                            #body
                        })
                    }
                } else if is_unique {
                    let body = unique_reconstruct(quote! {
                        #i.attach_node(inner);
                    });
                    Ok(quote! {
                        let size = get_index_page_size_from_data_length::<#ty>(#const_name);
                        let #i: #t<_, OffsetEqLink> = #t::with_maximum_node_size(size);
                        #body
                    })
                } else {
                    let body = multi_reconstruct(quote! {
                        #i.attach_multi_node(sorted);
                    });
                    Ok(quote! {
                        let size = get_index_page_size_from_data_length::<#ty>(#const_name);
                        let #i: #t<_, OffsetEqLink> = #t::with_maximum_node_size(size);
                        #body
                    })
                }
            })
            .collect::<syn::Result<Vec<_>>>()?;

        Ok(quote! {
            fn from_persisted(persisted: Self::PersistedIndex) -> Self {
                #(#index_gen)*

                Self {
                    #(#idents,)*
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::{Ident, Span};
    use quote::quote;

    use crate::persist_index::generator::{Generator, PersistIndexAttributes};
    use crate::persist_index::parser::Parser;

    #[test]
    fn correctly_collects_fields() {
        let input = quote! {
            #[derive(Debug, Default, Clone)]
            pub struct TestIndex {
                test_idx: TreeIndex<i64, Link>,
                exchnage_idx: TreeMultiIndex<String, Link>
            }
        };
        let struct_ = Parser::parse_struct(input).unwrap();
        let generator = Generator::with_attributes(struct_, PersistIndexAttributes::default());

        assert_eq!(
            generator
                .field_types
                .get(&Ident::new("test_idx", Span::call_site()))
                .unwrap()
                .to_string()
                .as_str(),
            "i64"
        );
        assert_eq!(
            generator
                .field_types
                .get(&Ident::new("exchnage_idx", Span::call_site()))
                .unwrap()
                .to_string()
                .as_str(),
            "String"
        );
    }

    #[test]
    fn parses_read_only_attribute() {
        let input = quote! {
            #[derive(Debug, Default, Clone)]
            #[index(read_only)]
            pub struct TestIndex {
                test_idx: TreeIndex<i64, Link>,
            }
        };
        let struct_ = Parser::parse_struct(input).unwrap();
        let attrs = Parser::parse_attributes(&struct_.attrs);

        assert!(attrs.read_only);
    }

    #[test]
    fn default_attributes_are_false() {
        let attrs = PersistIndexAttributes::default();
        assert!(!attrs.read_only);
    }

    #[test]
    fn without_read_only_attribute() {
        let input = quote! {
            #[derive(Debug, Default, Clone)]
            pub struct TestIndex {
                test_idx: TreeIndex<i64, Link>,
            }
        };
        let struct_ = Parser::parse_struct(input).unwrap();
        let attrs = Parser::parse_attributes(&struct_.attrs);

        assert!(!attrs.read_only);
    }

    #[test]
    fn rejects_aliases_instead_of_guessing_the_persisted_layout() {
        let input = quote! {
            #[derive(Debug, Default, Clone)]
            pub struct TestIndex {
                test_idx: MyIndexAlias<i64, Link>,
            }
        };
        let struct_ = Parser::parse_struct(input).unwrap();
        let generator = Generator::with_attributes(struct_, PersistIndexAttributes::default());

        let error = generator.gen_persistable_impl().unwrap_err();
        assert!(error.to_string().contains("unsupported persisted index type"));
    }
}
