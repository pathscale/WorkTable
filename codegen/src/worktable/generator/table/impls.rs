use proc_macro2::TokenStream;
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;

impl Generator {
    fn gen_table_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();

        let table_name_fn = self.gen_table_name_fn();

        quote! {
            impl #ident {
                #table_name_fn
            }
        }
    }

    fn gen_table_name_fn(&self) -> TokenStream {
        quote! {
            pub fn name(&self) -> &'static str {
                &self.0.table_name
            }
        }
    }

    fn gen_table_select_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();

        quote! {
            pub fn select(&self, pk: #primary_key_type) -> Option<#row_type> {
                self.0.select(pk)
            }
        }
    }

    fn gen_table_insert_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();

        quote! {
            pub fn insert(&self, row: #row_type) -> core::result::Result<#primary_key_type, WorkTableError> {
                self.0.insert::<{ #row_type::ROW_SIZE }>(row)
            }
        }
    }
}
