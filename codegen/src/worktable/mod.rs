use proc_macro2::TokenStream;
use quote::quote;

mod generator;
mod model;
mod parser;

use crate::worktable::generator::Generator;
pub use parser::Parser;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let mut parser = Parser::new(input);
    let mut columns = None;
    let mut queries = None;
    let mut indexes = None;
    let mut config = None;

    let name = parser.parse_name()?;
    while let Some(ident) = parser.peek_next() {
        match ident.to_string().as_str() {
            "columns" => {
                let res = parser.parse_columns()?;
                columns = Some(res)
            }
            "indexes" => {
                let res = parser.parse_indexes()?;
                indexes = Some(res);
            }
            "queries" => {
                let res = parser.parse_queries()?;
                queries = Some(res)
            }
            "config" => {
                let res = parser.parse_configs()?;
                config = Some(res)
            }
            _ => return Err(syn::Error::new(ident.span(), "Unexpected identifier")),
        }
    }

    let mut columns = columns.expect("defined");
    if let Some(i) = indexes {
        columns.indexes = i
    }
    let mut generator = Generator::new(name, columns);
    generator.queries = queries;
    generator.config = config;

    let pk_def = generator.gen_pk_def()?;
    let row_def = generator.gen_row_def();
    let wrapper_def = generator.gen_wrapper_def();
    let wrapper_impl = generator.gen_wrapper_impl();
    let index_def = generator.gen_index_def();
    let table_def = generator.gen_table_def();
    let table_index_impl = generator.gen_table_index_impl()?;
    let query_types_def = generator.gen_result_types_def()?;
    let query_locks_impls = generator.gen_query_locks_impl()?;
    let select_impls = generator.gen_query_select_impl()?;
    let update_impls = generator.gen_query_update_impl()?;
    let delete_impls = generator.gen_query_delete_impl()?;

    Ok(TokenStream::from(quote! {
        #pk_def
        #row_def
        #wrapper_def
        #wrapper_impl
        #index_def
        #table_def
        #table_index_impl
        #query_types_def
        #query_locks_impls
        #select_impls
        #update_impls
        #delete_impls
    }))
}

#[cfg(test)]
mod test {
    use crate::worktable::expand;
    use quote::quote;

    #[test]
    fn test() {
        let tokens = quote! {
            name: Test,
        columns: {
            id: u64 primary_key,
            test: i64,
            exchnage: String
        },
        indexes: {
            test_idx: test,
            exchnage_idx: exchange
        }
        };

        let _ = expand(tokens).unwrap();
    }
}
