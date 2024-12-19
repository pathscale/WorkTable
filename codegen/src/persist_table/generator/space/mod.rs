mod space_data_impl;

use proc_macro2::TokenStream;
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::persist_table::generator::Generator;

impl Generator {
    pub fn gen_space_def(&self) -> TokenStream {
        let type_ = self.gen_space_type();
        let space_data_impl = self.gen_space_data_impl();

        quote! {
            #type_
            #space_data_impl
        }
    }

    fn gen_space_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let space_ident = name_generator.get_space_ident();
        let page_size_const = name_generator.get_page_size_const_ident();

        quote! {
            #[derive(Debug)]
            pub struct #space_ident<const DATA_LENGTH: usize = #page_size_const> {
                pub file: std::fs::File,
            }
        }
    }
}
