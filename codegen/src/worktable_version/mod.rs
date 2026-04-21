use proc_macro2::TokenStream;
use quote::quote;
use syn::Error;

use crate::common::{Generator, Parser};

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let mut parser = Parser::new(input);
    let mut columns = None;
    let mut indexes = None;

    let name = parser.parse_name()?;

    while let Some(ident) = parser.peek_next() {
        match ident.to_string().as_str() {
            "columns" => columns = Some(parser.parse_columns()?),
            "indexes" => indexes = Some(parser.parse_indexes()?),
            "queries" => {
                return Err(Error::new(
                    ident.span(),
                    "worktable_version! does not support queries",
                ))
            }
            "config" => {
                return Err(Error::new(
                    ident.span(),
                    "worktable_version! does not support config",
                ))
            }
            _ => return Err(Error::new(ident.span(), "Unexpected identifier")),
        }
    }

    let mut columns = columns.expect("columns must be defined");
    if let Some(i) = indexes {
        columns.indexes = i
    }

    let mut generator = Generator::new(name, true, columns);
    generator.set_read_only(true);

    let pk_def = generator.gen_primary_key_def()?;
    let row_def = generator.gen_row_def();
    let query_available_def = generator.gen_available_types_def()?;
    let wrapper_def = generator.gen_wrapper_def();
    let locks_def = generator.gen_locks_def();
    let index_def = generator.gen_index_def()?;
    let table_def = generator.gen_table_def()?;  // includes column_range_type and select_query_executor_impl
    let select_impls = generator.gen_query_select_impl()?;  // select_all method

    Ok(quote! {
        #pk_def
        #row_def
        #query_available_def
        #wrapper_def
        #locks_def
        #index_def
        #table_def
        #select_impls
    })
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::worktable_version::expand;

    #[test]
    fn test_basic_version_macro() {
        let input = quote! {
            name: UserV1,
            columns: {
                id: u64 primary_key,
                name: String,
            },
        };

        let res = expand(input).unwrap();
        let output = res.to_string();

        assert!(
            output.contains("index (read_only)"),
            "should generate read_only index attribute"
        );
        assert!(
            output.contains("table (read_only)"),
            "should generate read_only table attribute"
        );
    }

    #[test]
    fn test_version_with_indexes() {
        let input = quote! {
            name: UserV2,
            columns: {
                id: u64 primary_key,
                email: String,
            },
            indexes: {
                email_idx: email unique,
            },
        };

        let res = expand(input).unwrap();
        let output = res.to_string();

        assert!(output.contains("email_idx"), "should include index field");
    }

    #[test]
    fn test_rejects_queries() {
        let input = quote! {
            name: UserV1,
            columns: {
                id: u64 primary_key,
            },
            queries: {
                select: { ById() by id },
            },
        };

        let res = expand(input);
        assert!(res.is_err(), "should reject queries section");
    }

    #[test]
    fn test_rejects_config() {
        let input = quote! {
            name: UserV1,
            columns: {
                id: u64 primary_key,
            },
            config: {
                page_size: 8192,
            },
        };

        let res = expand(input);
        assert!(res.is_err(), "should reject config section");
    }
}