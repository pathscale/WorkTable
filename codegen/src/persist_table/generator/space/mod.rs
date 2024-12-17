mod space_data_impl;

use proc_macro2::TokenStream;
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::persist_table::generator::Generator;

impl Generator {
    fn gen_space_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let space_ident = name_generator.get_space_ident();

        quote! {
            #[derive(Debug)]
            pub struct #space_ident {
                pub file: std::fs::File,
            }
        }
    }

    fn gen_space_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let space_ident = name_generator.get_space_ident();

        quote! {
            impl #space_ident {

            }
        }
    }

    fn gen_space_seek_to_page_start_fn(&self) -> TokenStream {
        quote! {
            fn seek_to_page_start(&self) -> eyre::Result<()> {

            }
        }
    }
}
