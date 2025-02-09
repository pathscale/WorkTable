use proc_macro2::TokenStream;
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::persist_table::generator::Generator;

impl Generator {
    pub fn gen_space_def(&self) -> TokenStream {
        quote! {}
    }

    fn gen_file_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let space_ident = name_generator.get_space_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();
        let pk_type = name_generator.get_primary_key_type_ident();

        quote! {
            #[derive(Debug)]
            pub struct #space_ident {
                pub data: SpaceData<#inner_const_name>,
                pub primary: SpaceIndex<#pk_type, #inner_const_name>,
            }
        }
    }
}
