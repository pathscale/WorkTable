use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::Index;

pub(crate) fn gen_borrowed_primary_key_impl(ident: &Ident, types: &[&TokenStream]) -> TokenStream {
    let from_key = quote! {
        impl From<&#ident> for #ident {
            fn from(value: &#ident) -> Self {
                value.clone()
            }
        }
    };

    if types.len() == 1 {
        let type_ = types[0];
        let from_str = (type_.to_string() == "String").then(|| {
            quote! {
                impl From<&str> for #ident {
                    fn from(value: &str) -> Self {
                        Self(value.to_owned())
                    }
                }
            }
        });
        quote! {
            #from_key
            impl From<&#type_> for #ident {
                fn from(value: &#type_) -> Self {
                    Self(value.clone())
                }
            }
            #from_str
        }
    } else {
        let positions = (0..types.len()).map(Index::from).collect::<Vec<_>>();
        quote! {
            #from_key
            impl From<&(#(#types),*)> for #ident {
                fn from(value: &(#(#types),*)) -> Self {
                    Self(#(value.#positions.clone()),*)
                }
            }
        }
    }
}
