use proc_macro2::TokenStream;
use quote::quote;

use crate::persist_index::generator::Generator;
use crate::persist_index::parser::Parser;

mod generator;
mod parser;
mod space;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input_struct = Parser::parse_struct(input)?;
    let attributes = Parser::parse_attributes(&input_struct.attrs);
    let mut generator = Generator::with_attributes(input_struct, attributes);

    let type_def = generator.gen_persist_type()?;
    let persistable_def = generator.gen_persistable_impl()?;
    let impl_def = generator.gen_persist_impl()?;
    let space_index = generator.gen_space_index();

    Ok(quote! {
        #type_def
        #impl_def
        #persistable_def
        #space_index
    })
}

#[cfg(test)]
mod tests {
    use quote::quote;
    use rkyv::{Archive, Deserialize, Serialize};

    use crate::persist_index::expand;

    #[derive(
        Archive, Copy, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    pub struct Link {
        pub page_id: u32,
        pub offset: u32,
        pub length: u32,
    }

    #[test]
    fn test() {
        let input = quote! {
            #[derive(Debug, Default, Clone)]
            pub struct TestIndex {
                test_idx: TreeIndex<i64, Link>,
                exchnage_idx: TreeIndex<String, std::sync::Arc<LockFreeSet<Link>>>
            }
        };

        let _res = expand(input).unwrap();
    }

    #[test]
    fn test_read_only() {
        let input = quote! {
            #[derive(Debug, Default, Clone)]
            #[index(read_only)]
            pub struct ReadOnlyIndex {
                test_idx: TreeIndex<i64, Link>,
            }
        };

        let res = expand(input).unwrap();
        let output = res.to_string();

        assert!(
            !output.contains("pub async fn persist"),
            "read_only index should not have persist method"
        );
        assert!(
            !output.contains("fn get_persisted_index"),
            "read_only index should not have get_persisted_index method"
        );
        assert!(
            output.contains("pub async fn parse_from_file"),
            "read_only index should have parse_from_file method"
        );
        assert!(
            output.contains("fn from_persisted"),
            "read_only index should have from_persisted method"
        );
    }
}
