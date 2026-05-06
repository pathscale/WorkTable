use proc_macro2::TokenStream;
use quote::ToTokens;
use syn::{Attribute, ItemStruct};
use syn::spanned::Spanned;

use crate::persist_index::generator::PersistIndexAttributes;

pub struct Parser;

impl Parser {
    pub fn parse_struct(input: TokenStream) -> syn::Result<ItemStruct> {
        match syn::parse2::<ItemStruct>(input.clone()) {
            Ok(data) => Ok(data),
            Err(err) => Err(syn::Error::new(input.span(), err.to_string())),
        }
    }

    pub fn parse_attributes(attrs: &Vec<Attribute>) -> PersistIndexAttributes {
        let mut res = PersistIndexAttributes::default();

        for attr in attrs {
            if attr.path().to_token_stream().to_string().as_str() == "index" {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("read_only") {
                        res.read_only = true;
                        return Ok(());
                    }
                    Ok(())
                })
                .expect("always ok even on unrecognized attrs");
            }
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use crate::persist_index::parser::Parser;
    use quote::quote;

    #[test]
    fn parses_index_struct() {
        let input = quote! {
            #[derive(Debug, Default, Clone)]
            pub struct TestIndex {
                test_idx: TreeIndex<i64, Link>,
                exchnage_idx: TreeIndex<String, std::sync::Arc<LockFreeSet<Link>>>
            }
        };
        assert!(Parser::parse_struct(input).is_ok())
    }

    #[test]
    fn errors_on_type() {
        let input = quote! {
            pub type TestIndex = Srting;
        };
        assert!(Parser::parse_struct(input).is_err())
    }

    #[test]
    fn errors_on_enum() {
        let input = quote! {
            #[derive(Debug, Default, Clone)]
            pub enum TestIndex {
                First,
                Second
            }
        };
        assert!(Parser::parse_struct(input).is_err())
    }
}
