mod info;
mod usual;

use crate::common::name_generator::{WorktableNameGenerator, is_float, is_unsized};
use crate::generators::index_backend::unique_index_type;
use crate::generators::read_only::ReadOnlyGenerator;
use convert_case::{Case, Casing};
use proc_macro2::TokenStream;
use quote::quote;

impl ReadOnlyGenerator {
    pub fn gen_index_def(&mut self) -> syn::Result<TokenStream> {
        let columnar_def = crate::generators::columnar::definitions(&self.name, &self.columns);
        let type_def = self.gen_type_def()?;
        let impl_def = self.gen_secondary_index_impl_def();
        let info_def = self.gen_secondary_index_info_impl_def();
        let default_impl = self.gen_index_default_impl()?;
        let available_indexes = self.gen_available_indexes();

        Ok(quote! {
            #columnar_def
            #type_def
            #impl_def
            #info_def
            #default_impl
            #available_indexes
        })
    }

    fn gen_type_def(&mut self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_index_type_ident();
        let index_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let Some(t) = self.columns.columns_map.get(i) else {
                    return Err(syn::Error::new(
                        i.span(),
                        format!("cannot find column `{i}` in this table"),
                    ));
                };
                let t = if is_float(t.to_string().as_str()) {
                    quote! { OrderedFloat<#t> }
                } else {
                    quote! { #t }
                };
                let i = &idx.name;

                #[allow(clippy::collapsible_else_if)]
                let res = if idx.is_unique {
                    let value_type = quote! { OffsetEqLink };
                    let worktables_node = if is_unsized(&t.to_string()) {
                        Some(quote! { UnsizedNode<IndexPair<#t, OffsetEqLink>> })
                    } else {
                        None
                    };
                    let index_type = unique_index_type(idx.backend, &t, &value_type, worktables_node)?;
                    quote! { #i: #index_type }
                } else {
                    if is_unsized(&t.to_string()) {
                        quote! {#i: IndexMultiMap<#t, OffsetEqLink, UnsizedNode<IndexMultiPair<#t, OffsetEqLink>>>}
                    } else {
                        quote! {#i: IndexMultiMap<#t, OffsetEqLink>}
                    }
                };
                Ok::<_, syn::Error>(res)
            })
            .collect::<Result<Vec<_>, syn::Error>>()?;

        let derive = quote! {
            #[derive(Debug, MemStat, PersistIndex)]
            #[index(read_only)]
        };
        let columnar_field = crate::generators::columnar::index_struct_field(&self.name, &self.columns, true);

        Ok(quote! {
            #derive
            pub struct #ident {
                #(#index_rows,)*
                #columnar_field
            }
        })
    }

    fn gen_index_default_impl(&self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let index_type_ident = name_generator.get_index_type_ident();
        let const_name = name_generator.get_page_inner_size_const_ident();

        let index_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let Some(t) = self.columns.columns_map.get(i) else {
                    return Err(syn::Error::new(
                        i.span(),
                        format!("cannot find column `{i}` in this table"),
                    ));
                };
                let t = if is_float(t.to_string().as_str()) {
                    quote! { OrderedFloat<#t> }
                } else {
                    quote! { #t }
                };
                let i = &idx.name;

                #[allow(clippy::collapsible_else_if)]
                let res = if idx.is_unique {
                    match idx.backend {
                        crate::common::model::IndexBackend::WorktablesIndex => {
                            if is_unsized(&t.to_string()) {
                                quote! { #i: IndexMap::with_maximum_node_size(#const_name), }
                            } else {
                                quote! {
                                    #i: IndexMap::with_maximum_node_size(
                                        get_index_page_size_from_data_length::<#t>(#const_name)
                                    ),
                                }
                            }
                        }
                        crate::common::model::IndexBackend::Indexset => quote! {
                            #i: UpstreamIndexMap::with_maximum_node_size(
                                get_index_page_size_from_data_length::<#t>(#const_name)
                            ),
                        },
                        crate::common::model::IndexBackend::Congee
                        | crate::common::model::IndexBackend::Arctic => {
                            quote! { #i: Default::default(), }
                        }
                    }
                } else {
                    if is_unsized(&t.to_string()) {
                        quote! {#i: IndexMultiMap::with_maximum_node_size(#const_name), }
                    } else {
                        quote! {#i: IndexMultiMap::with_maximum_node_size(get_index_page_size_from_data_length::<#t>(#const_name)),}
                    }
                };

                Ok::<_, syn::Error>(res)
            })
            .collect::<Result<Vec<_>, syn::Error>>()?;
        let columnar_field = crate::generators::columnar::index_default_field(&self.columns);

        Ok(quote! {
            impl Default for #index_type_ident {
                fn default() -> Self {
                    Self {
                        #(#index_rows)*
                        #columnar_field
                    }
                }
            }
        })
    }

    fn gen_available_indexes(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_type_ident = name_generator.get_available_indexes_ident();

        let indexes = self.columns.indexes.values().map(|i| {
            let camel_case_name = i.name.to_string().from_case(Case::Snake).to_case(Case::Pascal);
            let i: TokenStream = camel_case_name.parse().unwrap();
            quote! {
                #i,
            }
        });

        if self.columns.indexes.is_empty() {
            quote! {
                pub type #avt_type_ident = ();
            }
        } else {
            quote! {
                #[derive(Debug, Clone, Copy, MoreDisplay, PartialEq, PartialOrd, Ord, Hash, Eq)]
                pub enum #avt_type_ident {
                    #(#indexes)*
                }

                impl AvailableIndex for #avt_type_ident {
                    fn to_string_value(&self) -> String {
                        ToString::to_string(&self)
                    }
                }
            }
        }
    }
}
