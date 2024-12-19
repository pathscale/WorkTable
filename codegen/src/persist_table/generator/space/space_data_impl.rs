use crate::name_generator::WorktableNameGenerator;
use crate::persist_table::generator::Generator;
use proc_macro2::TokenStream;
use quote::quote;

impl Generator {
    pub(crate) fn gen_space_data_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let space_ident = name_generator.get_space_ident();

        quote! {
            impl<const DATA_LENGTH: usize> SpaceData for #space_ident<DATA_LENGTH> {
                fn save_data(&mut self, link: Link, bytes: &[u8]) -> eyre::Result<()> {
                    update_at::<{ DATA_LENGTH} >(&mut self.file, link, bytes)
                }
            }
        }
    }
}
