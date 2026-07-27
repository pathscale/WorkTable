use std::collections::HashMap;

use proc_macro2::{Ident, Literal, TokenStream};
use quote::__private::Span;
use quote::{ToTokens, quote};
use syn::ItemStruct;

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
            .field_types
            .iter()
            .map(|(i, t)| {
                if is_unsized(&t.to_string()) {
                    let const_size = name_generator.get_page_inner_size_const_ident();
                    quote! {
                        #i: (Vec<GeneralPage<TableOfContentsPage<(#t, Link)>>>, Vec<GeneralPage<UnsizedIndexPage<#t, {#const_size as u32}>>>),
                    }
                } else {
                    quote! {
                        #i: (Vec<GeneralPage<TableOfContentsPage<(#t, Link)>>>, Vec<GeneralPage<IndexPage<#t>>>),
                    }
                }
            })
            .collect();

        Ok(quote! {
            #[derive(Debug, Default, Clone)]
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
        let index_extension = Literal::string(WT_INDEX_EXTENSION);

        let persist_logic = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                f.ident
                    .as_ref()
                    .expect("index fields should always be named fields")
            })
            .map(|i| {
                let index_name_literal = Literal::string(i.to_string().as_str());
                quote! {
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
                }
            })
            .collect::<Vec<_>>();

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
        let index_extension = Literal::string(WT_INDEX_EXTENSION);

        let field_names_literals: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| (
                Literal::string(
                    f.ident
                        .as_ref()
                        .expect("index fields should always be named fields")
                        .to_string()
                        .as_str()
                ),
                f.ident
                    .as_ref()
                    .expect("index fields should always be named fields")
            ))
            .map(|(l, i)| quote! {
                let #i = {
                    let mut #i = vec![];
                    let mut file = tokio::fs::File::open(format!("{}/{}{}", path, #l, #index_extension)).await?;
                    let info = parse_page::<SpaceInfoPage<()>, { #page_const_name as u32 }>(&mut file, 0).await?;
                    let file_length = file.metadata().await?.len();
                    let page_id = file_length / (#page_const_name as u64 + GENERAL_HEADER_SIZE as u64) + 1;
                    let next_page_id = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(page_id as u32));
                    let toc = IndexTableOfContents::<_, { #page_const_name as u32 }>::parse_from_file(&mut file, 0.into(), next_page_id.clone()).await?;
                    for page_id in toc.iter().map(|(_, page_id)| page_id) {
                        let index = parse_page::<_, { #page_const_name as u32 }>(&mut file, (*page_id).into()).await?;
                        #i.push(index);
                    }
                    (toc.pages, #i)
                };
            })
            .collect();

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
            self.gen_get_persisted_index_fn()
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
    fn gen_get_persisted_index_fn(&self) -> TokenStream {
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
            .map(|f| {
                f.ident
                    .as_ref()
                    .expect("index fields should always be named fields")
            })
            .map(|i| {
                let ty = self
                    .field_types
                    .get(i)
                    .expect("should be available as constructed from same values");
                if is_unsized(&ty.to_string()) {
                    quote! {
                        let mut pages = vec![];
                        for node in self.#i.iter_nodes() {
                            let page = UnsizedIndexPage::from_node(node.lock_arc().as_ref());
                            pages.push(page);
                        }
                        let (toc, pages) = map_unsized_index_pages_to_toc_and_general::<_, { #const_name as u32 }>(pages);
                        let #i = (toc.pages, pages);
                    }
                } else {
                    quote! {
                        let size = get_index_page_size_from_data_length::<#ty>(#const_name);
                        let mut pages = vec![];
                        for node in self.#i.iter_nodes() {
                            let page = IndexPage::from_node(node.lock_arc().as_ref(), size);
                            pages.push(page);
                        }
                        let (toc, pages) = map_index_pages_to_toc_and_general::<_, { #const_name as u32 }>(pages);
                        let #i = (toc.pages, pages);
                    }
                }
            })
            .collect();

        quote! {
            fn get_persisted_index(&self) -> Self::PersistedIndex {
                #(#field_names_init)*
                Self::PersistedIndex {
                    #(#idents,)*
                }
            }
        }
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
                let i = f.ident.as_ref().expect("index fields should always be named fields");
                let index_type = f.ty.to_token_stream().to_string();
                let is_unique = !index_type.contains("IndexMultiMap");
                let mut split = index_type.split("<");
                let t = Ident::new(
                    split.next().expect("index type should always have generics").trim(),
                    Span::mixed_site(),
                );
                let ty = self
                    .field_types
                    .get(i)
                    .expect("should be available as constructed from same values");

                // KEY FORMAT ASSUMPTION driving all of the reconstruction
                // below: index pages store entries in event-arrival order,
                // NOT sorted order. Only bootstrap-written pages happen to be
                // sorted with the node maximum last; pages that were
                // incrementally updated through CDC events are arbitrary.
                // Reconstruction therefore must sort every page itself —
                // assuming "last entry == node id" registers a wrong maximum
                // in the in-memory node index and makes every entry above it
                // unreachable.
                //
                // `unique_reconstruct` handles unique indexes: sorting each
                // page is all they need, since a page is exactly one node and
                // unique keys make the sorted last entry the true maximum.
                let unique_reconstruct = |attach: TokenStream| {
                    quote! {
                        for page in persisted.#i.1 {
                            let mut inner: Vec<IndexPair<#ty, OffsetEqLink>> = page
                                .inner
                                .get_node()
                                .into_iter()
                                .map(|p| IndexPair {
                                    key: p.key,
                                    value: p.value.into(),
                                })
                                .collect();
                            inner.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.value.cmp(&b.value)));
                            #attach
                        }
                    }
                };
                // `multi_reconstruct` handles non-unique indexes, where one
                // key's duplicates can straddle nodes. In-memory order for
                // duplicates comes from per-entry discriminators that were
                // never persisted, so reconstruction re-derives them — and it
                // must do so consistently across the whole index, not per
                // node: the node index (a B-tree) requires every entry in a
                // node to compare <= that node's registered maximum, so a
                // later node's segment of one key has to compare greater than
                // an earlier node's segment of the same key. Processing nodes
                // in ascending node-id order while letting discriminators
                // keep growing across node boundaries within one key gives
                // every duplicate a globally consistent position; numbering
                // per node would make straddling segments overlap and leave
                // entries unreachable.
                //
                // Two more constraints:
                // - Each node must end with exactly the entry the on-disk
                //   table of contents knows the node by (its persisted
                //   node_id): CDC events emitted after the reload address
                //   nodes by their maximum, and a node whose maximum drifted
                //   from the TOC key can no longer be resolved by the space
                //   index. The page sort pins that entry last within its key.
                // - Discriminators 0 and u64::MAX are reserved (range infimum
                //   and supremum used by lookups), which is why numbering
                //   starts at 1 and is capped at u64::MAX - 1. The cap and
                //   the saturating_add can only matter for a key with more
                //   than u64::MAX - 2 duplicates — unreachable in practice
                //   (the table could not hold the rows) — so they are purely
                //   defensive.
                //
                // Pages are sorted and consumed in place, one node at a time,
                // so peak memory stays at the parsed pages plus a single
                // node — no intermediate copy of the whole index.
                let multi_reconstruct = |attach: TokenStream| {
                    quote! {
                        let mut pages = persisted.#i.1;
                        pages.sort_by(|a, b| {
                            let a_link: OffsetEqLink = OffsetEqLink(a.inner.node_id.link);
                            let b_link: OffsetEqLink = OffsetEqLink(b.inner.node_id.link);
                            a.inner.node_id.key.cmp(&b.inner.node_id.key).then_with(|| a_link.cmp(&b_link))
                        });
                        let mut prev_key = None;
                        let mut next_discriminator = 1u64;
                        for page in pages {
                            let node_id = IndexPair {
                                key: page.inner.node_id.key.clone(),
                                value: OffsetEqLink(page.inner.node_id.link),
                            };
                            let mut inner: Vec<IndexPair<#ty, OffsetEqLink>> = page
                                .inner
                                .get_node()
                                .into_iter()
                                .map(|p| IndexPair {
                                    key: p.key,
                                    value: OffsetEqLink(p.value),
                                })
                                .collect();
                            inner.sort_by(|a, b| {
                                a.key.cmp(&b.key).then_with(|| {
                                    let a_is_id = a.key == node_id.key && a.value == node_id.value;
                                    let b_is_id = b.key == node_id.key && b.value == node_id.value;
                                    a_is_id.cmp(&b_is_id).then_with(|| a.value.cmp(&b.value))
                                })
                            });
                            if inner.is_empty() {
                                continue;
                            }
                            let mut sorted = Vec::with_capacity(inner.len());
                            for p in inner {
                                if prev_key.as_ref() != Some(&p.key) {
                                    prev_key = Some(p.key.clone());
                                    next_discriminator = 1;
                                }
                                sorted.push(IndexMultiPair {
                                    key: p.key,
                                    value: p.value,
                                    discriminator: next_discriminator.min(u64::MAX - 1),
                                });
                                next_discriminator = next_discriminator.saturating_add(1);
                            }
                            #attach
                        }
                    }
                };

                if is_unsized(&ty.to_string()) {
                    if is_unique {
                        let body = unique_reconstruct(quote! {
                            let node = UnsizedNode::from_inner(inner, #const_name);
                            #i.attach_node(node);
                        });
                        quote! {
                            let #i: #t<_, OffsetEqLink, UnsizedNode<_>> = #t::with_maximum_node_size(#const_name);
                            #body
                        }
                    } else {
                        let body = multi_reconstruct(quote! {
                            let node = UnsizedNode::from_inner(sorted, #const_name);
                            #i.attach_multi_node(node);
                        });
                        quote! {
                            let #i: #t<_, OffsetEqLink, UnsizedNode<_>> = #t::with_maximum_node_size(#const_name);
                            #body
                        }
                    }
                } else if is_unique {
                    let body = unique_reconstruct(quote! {
                        #i.attach_node(inner);
                    });
                    quote! {
                        let size = get_index_page_size_from_data_length::<#ty>(#const_name);
                        let #i: #t<_, OffsetEqLink> = #t::with_maximum_node_size(size);
                        #body
                    }
                } else {
                    let body = multi_reconstruct(quote! {
                        #i.attach_multi_node(sorted);
                    });
                    quote! {
                        let size = get_index_page_size_from_data_length::<#ty>(#const_name);
                        let #i: #t<_, OffsetEqLink> = #t::with_maximum_node_size(size);
                        #body
                    }
                }
            })
            .collect::<Vec<_>>();

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
}
