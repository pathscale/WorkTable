mod impls;
mod index_fns;
mod select_executor;

use proc_macro2::TokenStream;
use quote::quote;

use crate::common::name_generator::{WorktableNameGenerator, is_unsized_vec};
use crate::generators::read_only::ReadOnlyGenerator;

impl ReadOnlyGenerator {
    pub fn gen_table_def(&mut self) -> syn::Result<TokenStream> {
        let page_size_consts = self.gen_page_size_consts();
        let type_ = self.gen_table_type();
        let impl_ = self.gen_table_impl();
        let index_fns = self.gen_table_index_fns()?;
        let select_query_executor_impl = self.gen_table_select_query_executor_impl();
        let column_range_type = self.gen_table_column_range_type();

        Ok(quote! {
            #page_size_consts
            #type_
            #impl_
            #index_fns
            #select_query_executor_impl
            #column_range_type
        })
    }

    fn gen_page_size_consts(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let page_const_name = name_generator.get_page_size_const_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();

        quote! {
            const #page_const_name: usize = PAGE_SIZE;
            const #inner_const_name: usize = #page_const_name - GENERAL_HEADER_SIZE;
        }
    }

    fn gen_table_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();
        let index_type = name_generator.get_index_type_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();
        let avt_type_ident = name_generator.get_available_type_ident();
        let avt_index_ident = name_generator.get_available_indexes_ident();
        let lock_ident = name_generator.get_lock_type_ident();

        let pk_types = &self
            .columns
            .primary_keys
            .iter()
            .map(|i| {
                self.columns
                    .columns_map
                    .get(i)
                    .expect("should exist as got from definition")
                    .to_string()
            })
            .collect::<Vec<_>>();
        let pk_types_unsized = is_unsized_vec(pk_types);

        let derive = if pk_types_unsized {
            quote! {
                #[derive(Debug, PersistTable)]
                #[table(pk_unsized)]
            }
        } else {
            quote! {
                #[derive(Debug, PersistTable)]
                #[table(read_only)]
            }
        };

        let node_type = if pk_types_unsized {
            quote! {
                UnsizedNode<IndexPair<#primary_key_type, OffsetEqLink<#inner_const_name>>>
            }
        } else {
            quote! {
                Vec<IndexPair<#primary_key_type, OffsetEqLink<#inner_const_name>>>
            }
        };

        quote! {
            #derive
            pub struct #ident(
                WorkTable<
                    #row_type,
                    #primary_key_type,
                    #avt_type_ident,
                    #avt_index_ident,
                    #index_type,
                    #lock_ident,
                    <#primary_key_type as TablePrimaryKey>::Generator,
                    #inner_const_name,
                    #node_type
                >
            );
        }
    }
}