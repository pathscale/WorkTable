use crate::name_generator::WorktableNameGenerator;
use crate::persist_table::generator::Generator;
use crate::persist_table::WT_DATA_EXTENSION;

use proc_macro2::{Literal, TokenStream};
use quote::quote;

impl Generator {
    pub fn gen_space_worktable_impl(&self) -> TokenStream {
        let ident = &self.struct_def.ident;

        let get_space_fn = self.gen_space_worktable_get_space();

        quote! {
            impl #ident {
                #get_space_fn
            }
        }
    }

    fn gen_space_worktable_get_space(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let space_ident = name_generator.get_space_ident();
        let dir_name = name_generator.get_dir_name();
        let data_extension = Literal::string(WT_DATA_EXTENSION);

        quote! {
            pub fn get_space(&self) -> eyre::Result<#space_ident> {
                Ok(#space_ident::new(
                    std::fs::OpenOptions::new()
                            .write(true)
                            .open(format!("{}/{}/{}", self.1.config_path, #dir_name, #data_extension))?
                    )?
                )
            }
        }
    }
}
