use crate::name_generator::WorktableNameGenerator;
use crate::persist_table::generator::Generator;
use proc_macro2::TokenStream;
use quote::quote;

impl Generator {
    pub fn get_persistence_manager_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let ident = name_generator.get_persistence_engine_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();
        let space_secondary_indexes = name_generator.get_space_secondary_index_ident();
        let space_secondary_indexes_events =
            name_generator.get_space_secondary_index_events_ident();

        quote! {
            pub type #ident = PersistenceEngine<
                SpaceData<{ #inner_const_name as u32 }>,
                SpaceIndex<#primary_key_type, { #inner_const_name as u32 }>,
                #space_secondary_indexes,
                #primary_key_type,
                #space_secondary_indexes_events,
            >;
        }
    }
}
