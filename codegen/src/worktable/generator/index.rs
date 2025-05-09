use crate::name_generator::{is_float, is_unsized, WorktableNameGenerator};
use crate::worktable::generator::Generator;

use crate::worktable::generator::queries::r#type::map_to_uppercase;
use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;

impl Generator {
    /// Generates index type and it's impls.
    pub fn gen_index_def(&mut self) -> TokenStream {
        let type_def = self.gen_type_def();
        let impl_def = self.gen_impl_def();
        let cdc_impl_def = if self.is_persist {
            self.gen_cdc_impl_def()
        } else {
            quote! {}
        };
        let default_impl = self.gen_index_default_impl();

        quote! {
            #type_def
            #impl_def
            #cdc_impl_def
            #default_impl
        }
    }

    /// Generates table's secondary index struct definition. It has fields with index names and types varying on index
    /// uniqueness. For unique index it's `TreeIndex<T, Link`, for non-unique `TreeIndex<T, Arc<LockFreeSet<Link>>>`.
    /// Index also derives `PersistIndex` and `MemStat` macro.
    fn gen_type_def(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_index_type_ident();
        let index_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let t = self.columns.columns_map.get(i).unwrap();
                let t = if is_float(t.to_string().as_str()) {
                    quote! { OrderedFloat<#t> }
                } else {
                    quote! { #t }
                };
                let i = &idx.name;

                #[allow(clippy::collapsible_else_if)]
                if idx.is_unique {
                    if is_unsized(&t.to_string()) {
                        quote! {
                            #i: IndexMap<#t, Link, UnsizedNode<IndexPair<#t, Link>>>
                        }
                    } else {
                        quote! {#i: IndexMap<#t, Link>}
                    }
                } else {
                    if is_unsized(&t.to_string()) {
                        quote! {#i: IndexMultiMap<#t, Link, UnsizedNode<IndexMultiPair<#t, Link>>>}
                    } else {
                        quote! {#i: IndexMultiMap<#t, Link>}
                    }
                }
            })
            .collect::<Vec<_>>();

        let derive = if self.is_persist {
            quote! {
                #[derive(Debug, MemStat, PersistIndex)]
            }
        } else {
            quote! {
                #[derive(Debug, MemStat)]
            }
        };

        quote! {
            #derive
            pub struct #ident {
                #(#index_rows),*
            }
        }
    }

    fn gen_index_default_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let index_type_ident = name_generator.get_index_type_ident();
        let const_name = name_generator.get_page_inner_size_const_ident();

        let index_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let t = self.columns.columns_map.get(i).unwrap();
                let t = if is_float(t.to_string().as_str()) {
                    quote! { OrderedFloat<#t> }
                } else {
                    quote! { #t }
                };
                let i = &idx.name;

                #[allow(clippy::collapsible_else_if)]
                if idx.is_unique {
                    if is_unsized(&t.to_string()) {
                        quote! {
                            #i: IndexMap::with_maximum_node_size(#const_name),
                        }
                    } else {
                        quote! {#i: IndexMap::with_maximum_node_size(get_index_page_size_from_data_length::<#t>(#const_name)),}
                    }
                } else {
                    if is_unsized(&t.to_string()) {
                        quote! {#i: IndexMultiMap::with_maximum_node_size(#const_name), }
                    } else {
                        quote! {#i: IndexMultiMap::with_maximum_node_size(get_index_page_size_from_data_length::<#t>(#const_name)),}
                    }
                }
            })
            .collect::<Vec<_>>();

        quote! {
            impl Default for #index_type_ident {
                fn default() -> Self {
                    Self {
                        #(#index_rows)*
                    }
                }
            }
        }
    }

    /// Generates implementation of `TableSecondaryIndex` trait for index.
    fn gen_impl_def(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();
        let index_type_ident = name_generator.get_index_type_ident();
        let avt_type_ident = name_generator.get_available_type_ident();

        let save_row_fn = self.gen_save_row_index_fn();
        let delete_row_fn = self.gen_delete_row_index_fn();
        let process_difference_fn = self.gen_process_difference_index_fn();
        let info_fn = self.gen_index_info_fn();

        quote! {
            impl TableSecondaryIndex<#row_type_ident, #avt_type_ident> for #index_type_ident {
                #save_row_fn
                #delete_row_fn
                #process_difference_fn
                #info_fn
            }
        }
    }

    fn gen_cdc_impl_def(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let index_type_ident = name_generator.get_index_type_ident();
        let row_type_ident = name_generator.get_row_type_ident();
        let events_ident = name_generator.get_space_secondary_index_events_ident();
        let available_types_ident = name_generator.get_available_type_ident();

        let save_row_cdc = self.gen_save_row_cdc_index_fn();
        let delete_row_cdc = self.gen_delete_row_cdc_index_fn();
        let process_diff_cdc = self.gen_process_diff_cdc_index_fn();

        quote! {
            impl TableSecondaryIndexCdc<#row_type_ident, #available_types_ident, #events_ident> for #index_type_ident {
                #save_row_cdc
                #delete_row_cdc
                #process_diff_cdc
            }
        }
    }

    fn gen_save_row_cdc_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();
        let events_ident = name_generator.get_space_secondary_index_events_ident();

        let save_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let index_field_name = &idx.name;
                quote! {
                    let (exists, events) = self.#index_field_name.insert_cdc(row.#i, link);
                    if exists.is_some() {
                        return Err(WorkTableError::AlreadyExists);
                    }
                    let #index_field_name = events.into_iter().map(|ev| ev.into()).collect();
                }
            })
            .collect::<Vec<_>>();
        let idents = self
            .columns
            .indexes
            .values()
            .map(|idx| &idx.name)
            .collect::<Vec<_>>();

        quote! {
            fn save_row_cdc(&self, row: #row_type_ident, link: Link) -> Result<#events_ident, WorkTableError> {
                #(#save_rows)*
                core::result::Result::Ok(
                    #events_ident {
                        #(#idents,)*
                    }
                )
            }
        }
    }

    fn gen_delete_row_cdc_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();
        let events_ident = name_generator.get_space_secondary_index_events_ident();

        let delete_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let index_field_name = &idx.name;
                quote! {
                    let (_, events) = TableIndexCdc::remove_cdc(&self.#index_field_name, row.#i, link);
                    let #index_field_name = events.into_iter().map(|ev| ev.into()).collect();
                }
            })
            .collect::<Vec<_>>();
        let idents = self
            .columns
            .indexes
            .values()
            .map(|idx| &idx.name)
            .collect::<Vec<_>>();

        quote! {
            fn delete_row_cdc(&self, row: #row_type_ident, link: Link) -> Result<#events_ident, WorkTableError> {
                #(#delete_rows)*
                core::result::Result::Ok(
                    #events_ident {
                        #(#idents,)*
                    }
                )
            }
        }
    }

    fn gen_process_diff_cdc_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_type_ident = name_generator.get_available_type_ident();
        let events_ident = name_generator.get_space_secondary_index_events_ident();

        let process_difference_rows = self.columns.indexes.iter().map(|(i, idx)| {
            let index_field_name = &idx.name;
            let diff_key = Literal::string(i.to_string().as_str());

            let match_arm = if let Some(t) = self.columns.columns_map.get(&idx.field) {
                let type_str = t.to_string();
                let variant_ident = Ident::new(&map_to_uppercase(&type_str), Span::mixed_site());

                let (new_value_expr, old_value_expr) = if type_str == "String" {
                    (quote! { new.to_string() }, quote! { old.to_string() })
                } else {
                    (quote! { *new }, quote! { *old })
                };

                quote! {
                    let #index_field_name = if let Some(diff) = difference.get(#diff_key) {
                        let mut events = vec![];
                        if let #avt_type_ident::#variant_ident(old) = &diff.old {
                            let key_old = #old_value_expr;
                            let (_, evs) = TableIndexCdc::remove_cdc(&self.#index_field_name, key_old, link);
                            events.extend_from_slice(evs.as_ref());
                        }

                        if let #avt_type_ident::#variant_ident(new) = &diff.new {
                            let key_new = #new_value_expr;
                            let (_, evs) = TableIndexCdc::insert_cdc(&self.#index_field_name, key_new, link);
                            events.extend_from_slice(evs.as_ref());
                        }
                        events
                    } else {
                        vec![]
                    };
                }
            } else {
                quote! {}
            };

            match_arm
        });
        let idents = self
            .columns
            .indexes
            .values()
            .map(|idx| &idx.name)
            .collect::<Vec<_>>();

        quote! {
            fn process_difference_cdc(
                &self,
                link: Link,
                difference: std::collections::HashMap<&str, Difference<#avt_type_ident>>
            ) -> core::result::Result<#events_ident, WorkTableError> {
                #(#process_difference_rows)*
                core::result::Result::Ok(
                    #events_ident {
                        #(#idents,)*
                    }
                )
            }
        }
    }

    /// Generates `save_row` function of `TableSecondaryIndex` trait for index. It saves `Link` to all secondary
    /// indexes. Logic varies on index uniqueness. For unique index we can just insert `Link` in index, but for
    /// non-unique we need to get set from index first and then insert `Link` in set.
    fn gen_save_row_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();

        let save_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let index_field_name = &idx.name;
                let row = if is_float(
                    self.columns
                        .columns_map
                        .get(i)
                        .unwrap()
                        .to_string()
                        .as_str(),
                ) {
                    quote! {
                        OrderedFloat(row.#i)
                    }
                } else {
                    quote! {
                        row.#i
                    }
                };
                quote! {
                    self.#index_field_name.insert(#row, link)
                        .map_or(Ok(()), |_| Err(WorkTableError::AlreadyExists))?;
                }
            })
            .collect::<Vec<_>>();

        quote! {
            fn save_row(&self, row: #row_type_ident, link: Link) -> core::result::Result<(), WorkTableError> {
                #(#save_rows)*
                core::result::Result::Ok(())
            }
        }
    }

    /// Generates `delete_row` function of `TableIndex` trait for index. It removes `Link` from all secondary indexes.
    /// Logic varies on index uniqueness. For unique index we can just delete `Link` from index, but for non-unique we
    /// need to get set from index first and then delete `Link` from set.
    fn gen_delete_row_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();

        let delete_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let index_field_name = &idx.name;
                let row = if is_float(
                    self.columns
                        .columns_map
                        .get(i)
                        .unwrap()
                        .to_string()
                        .as_str(),
                ) {
                    quote! {
                        OrderedFloat(row.#i)
                    }
                } else {
                    quote! {
                        row.#i
                    }
                };
                if idx.is_unique {
                    quote! {
                        self.#index_field_name.remove(&#row);
                    }
                } else {
                    quote! {
                        self.#index_field_name.remove(&#row, &link);
                    }
                }
            })
            .collect::<Vec<_>>();

        quote! {
            fn delete_row(&self, row: #row_type_ident, link: Link) -> core::result::Result<(), WorkTableError> {
                #(#delete_rows)*
                core::result::Result::Ok(())
            }
        }
    }

    /// Generates `process_difference` function of `TableIndex` trait for index. It updates `Link` for all secondary indexes.
    /// Uses HashMap<&str, Difference<AvaialableTypes>> for storing all changes
    fn gen_process_difference_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_type_ident = name_generator.get_available_type_ident();

        let process_difference_rows = self.columns.indexes.iter().map(|(i, idx)| {
            let index_field_name = &idx.name;
            let diff_key = Literal::string(i.to_string().as_str());

            let match_arm = if let Some(t) = self.columns.columns_map.get(&idx.field) {
                let type_str = t.to_string();
                let variant_ident = Ident::new(&map_to_uppercase(&type_str), Span::mixed_site());

                let (new_value_expr, old_value_expr) = if type_str == "String" {
                    (quote! { new.to_string() }, quote! { old.to_string() })
                } else if is_float(type_str.as_str()) {
                    (quote! { OrderedFloat(*new) }, quote! { OrderedFloat(*old) })
                } else {
                    (quote! { *new }, quote! { *old })
                };

                quote! {
                    if let Some(diff) = difference.get(#diff_key) {
                        if let #avt_type_ident::#variant_ident(old) = &diff.old {
                            let key_old = #old_value_expr;
                            TableIndex::remove(&self.#index_field_name, key_old, link);
                        }

                        if let #avt_type_ident::#variant_ident(new) = &diff.new {
                            let key_new = #new_value_expr;
                            TableIndex::insert(&self.#index_field_name, key_new, link);
                        }
                    }
                }
            } else {
                quote! {}
            };

            match_arm
        });

        quote! {
            fn process_difference(
                &self,
                link: Link,
                difference: std::collections::HashMap<&str, Difference<#avt_type_ident>>
            ) -> core::result::Result<(), WorkTableError> {
                #(#process_difference_rows)*
                core::result::Result::Ok(())
            }
        }
    }

    fn gen_index_info_fn(&self) -> TokenStream {
        let rows = self.columns.indexes.values().map(|idx| {
            let index_field_name = &idx.name;
            let index_name_str = index_field_name.to_string();

            if idx.is_unique {
                quote! {

                    info.push(IndexInfo {
                        name: #index_name_str.to_string(),
                        index_type: IndexKind::Unique,
                        key_count: self.#index_field_name.len(),
                        capacity: self.#index_field_name.capacity(),
                        heap_size: self.#index_field_name.heap_size(),
                        used_size: self.#index_field_name.used_size(),
                        node_count: self.#index_field_name.node_count(),


                    });
                }
            } else {
                quote! {
                    info.push(IndexInfo {
                        name: #index_name_str.to_string(),
                        index_type: IndexKind::NonUnique,
                        key_count: self.#index_field_name.len(),
                        capacity: self.#index_field_name.capacity(),
                        heap_size: self.#index_field_name.heap_size(),
                        used_size: self.#index_field_name.used_size(),
                        node_count: self.#index_field_name.node_count(),
                    });
                }
            }
        });

        quote! {
            fn index_info(&self) -> Vec<IndexInfo> {
                let mut info = Vec::new();
                #(#rows)*
                info
            }
        }
    }
}

// TODO: tests...
