mod space_index_impl;
mod worktable_impls;

use proc_macro2::TokenStream;
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::persist_table::generator::Generator;

impl Generator {
    pub fn gen_space_def(&self) -> TokenStream {
        let type_ = self.gen_space_type();
        let impl_ = self.gen_space_impl();
        let worktable_impl = self.gen_space_worktable_impl();

        quote! {
            #type_
            #impl_
            #worktable_impl
        }
    }

    fn gen_space_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let space_ident = name_generator.get_space_ident();
        let page_size_const = name_generator.get_page_size_const_ident();

        quote! {
            #[derive(Debug)]
            pub struct #space_ident<const DATA_LENGTH: usize = #page_size_const> {
                pub data: SpaceData<DATA_LENGTH>,
            }
        }
    }

    fn gen_space_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let space_ident = name_generator.get_space_ident();

        quote! {
            impl<const DATA_LENGTH: usize> #space_ident<DATA_LENGTH> {
                pub fn new(data_file: std::fs::File) -> eyre::Result<Self> {
                    Ok(#space_ident {
                        data: SpaceData {
                            data_file
                        },
                    })
                }
            }
        }
    }
}
