use proc_macro2::{Literal, TokenStream};
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;

mod impls;

impl Generator {
    pub fn gen_table_def(&mut self) -> TokenStream {
        let page_size_consts = self.gen_page_size_consts();
        let type_ = self.gen_table_type();

        quote! {
            #page_size_consts
            #type_
        }
    }

    fn gen_table_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();
        let primary_index_type = &self.columns.primary_keys.1;
        let index_type = name_generator.get_index_type_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();

        let derive = if self.is_persist {
            quote! {
                 #[derive(Debug, PersistTable)]
            }
        } else {
            quote! {
                 #[derive(Debug)]
            }
        };
        let persist_type_part = if self.is_persist {
            quote! {
                , std::sync::Arc<DatabaseManager>
            }
        } else {
            quote! {}
        };

        if let Some(_) = &self.config.as_ref().map(|c| c.page_size).flatten() {
            quote! {
                #derive
                pub struct #ident(
                    WorkTable<
                        #row_type,
                        #primary_key_type,
                        #primary_index_type<#primary_key_type, Link>,
                        #index_type,
                        <#primary_key_type as TablePrimaryKey>::Generator,
                        #inner_const_name
                    >
                    #persist_type_part
                );
            }
        } else {
            quote! {
                #derive
                pub struct #ident(
                    WorkTable<
                        #row_type,
                        #primary_key_type,
                        #primary_index_type<#primary_key_type, Link>,
                        #index_type
                    >
                    #persist_type_part
                );
            }
        }
    }

    fn gen_page_size_consts(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let page_const_name = name_generator.get_page_size_const_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();

        if let Some(page_size) = &self.config.as_ref().map(|c| c.page_size).flatten() {
            let page_size = Literal::usize_unsuffixed(*page_size as usize);
            quote! {
                const #page_const_name: usize = #page_size;
                const #inner_const_name: usize = #page_size - GENERAL_HEADER_SIZE;
            }
        } else {
            quote! {
                const #page_const_name: usize = PAGE_SIZE;
                const #inner_const_name: usize = #page_const_name - GENERAL_HEADER_SIZE;
            }
        }
    }
}
