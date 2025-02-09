use proc_macro2::TokenStream;
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::persist_index::generator::Generator;

impl Generator {
    pub fn gen_space_index(&self) -> TokenStream {
        let secondary_index = self.gen_space_secondary_index_type();
        let secondary_index_events = self.gen_space_secondary_index_events_type();

        quote! {
            #secondary_index_events
            #secondary_index
        }
    }

    fn gen_space_secondary_index_events_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let ident = name_generator.get_space_secondary_index_events_ident();

        let fields: Vec<_> = self
            .field_types
            .iter()
            .map(|(i, t)| {
                quote! {
                    #i: Vec<indexset::cdc::change::ChangeEvent<
                        indexset::core::pair::Pair<#t, Link>
                    >>,
                }
            })
            .collect();

        quote! {
            #[derive(Debug)]
            pub struct #ident {
                #(#fields)*
            }
        }
    }

    fn gen_space_secondary_index_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let ident = name_generator.get_space_secondary_index_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();

        let fields: Vec<_> = self
            .field_types
            .iter()
            .map(|(i, t)| {
                quote! {
                    #i: SpaceIndex<#t, { #inner_const_name as u32}>,
                }
            })
            .collect();

        quote! {
            #[derive(Debug)]
            pub struct #ident {
                #(#fields)*
            }
        }
    }
}
