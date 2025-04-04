use proc_macro2::TokenStream;
use quote::quote;

use crate::persist_index::generator::Generator;
use crate::persist_index::parser::Parser;

mod generator;
mod parser;
mod space;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input_struct = Parser::parse_struct(input)?;
    let mut gen = Generator::new(input_struct);

    let type_def = gen.gen_persist_type()?;
    let persistable_def = gen.gen_persistable_impl()?;
    let impl_def = gen.gen_persist_impl()?;
    let space_index = gen.gen_space_index();

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

        let res = expand(input).unwrap();
        println!("{:?}", res.to_string())
    }
}
