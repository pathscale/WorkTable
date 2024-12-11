use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

impl Generator {
    pub fn gen_row_def(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_row_type_ident();

        let def = self.gen_row_type();

        let pk = self.pk.clone().unwrap();
        let pk_ident = &pk.ident;

        let def = if pk.vals.len() == 1 {
            let pk_field = pk.vals.keys().next().unwrap();
            quote! {
                self.#pk_field.clone().into()
            }
        } else {
            let vals = pk
                .vals
                .keys()
                .map(|i| {
                    quote! {
                        self.#i.clone()
                    }
                })
                .collect::<Vec<_>>();
            quote! {
                (#(#vals),*).into()
            }
        };

        let row_impl = quote! {
            #def

            impl TableRow<#pk_ident> for #ident {
                const ROW_SIZE: usize = ::core::mem::size_of::<#ident>();

                fn get_primary_key(&self) -> #pk_ident {
                    #def
                }
            }
        };

        quote! {
            #row_impl
        }
    }

    fn gen_row_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_row_type_ident();

        let rows: Vec<_> = self
            .columns
            .columns_map
            .iter()
            .map(|(name, type_)| {
                quote! {pub #name: #type_,}
            })
            .collect();

        quote! {
            #[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize, PartialEq)]
            #[rkyv(derive(Debug))]
            #[repr(C)]
            pub struct #ident {
                #(#rows)*
            }
        }
    }
}

// TODO: tests...
