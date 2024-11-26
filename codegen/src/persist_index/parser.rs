use proc_macro2::TokenStream;
use syn::spanned::Spanned;
use syn::ItemStruct;

pub struct Parser;

impl Parser {
    pub fn parse_struct(input: TokenStream) -> syn::Result<ItemStruct> {
        match syn::parse2::<ItemStruct>(input.clone()) {
            Ok(data) => Ok(data),
            Err(err) => Err(syn::Error::new(input.span(), err.to_string())),
        }
    }
}
