use crate::name_generator::WorktableNameGenerator;
use crate::persist_table::generator::Generator;
use proc_macro2::TokenStream;
use quote::quote;

impl Generator {
    pub fn gen_space_def(&self) -> TokenStream {
        quote! {}
    }

    fn gen_file_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let space_ident = name_generator.get_space_ident();

        quote! {
            #[derive(Debug)]
            pub struct #space_ident {
                pub data_info: GeneralPage<SpaceInfoPage>,
            }
        }
    }
}
