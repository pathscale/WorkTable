use proc_macro2::TokenStream;
use syn::Error;

use crate::common::Parser;
use crate::generators::read_only;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let mut parser = Parser::new(input);
    let mut columns = None;
    let mut indexes = None;

    let name = parser.parse_name()?;
    let version = parser.parse_version()?.unwrap_or(1);

    while let Some(ident) = parser.peek_next() {
        match ident.to_string().as_str() {
            "columns" => columns = Some(parser.parse_columns()?),
            "indexes" => indexes = Some(parser.parse_indexes()?),
            "queries" => {
                return Err(Error::new(ident.span(), "worktable_version! does not support queries"));
            }
            "config" => {
                return Err(Error::new(ident.span(), "worktable_version! does not support config"));
            }
            "version" => {
                return Err(Error::new(
                    ident.span(),
                    "version must be specified before columns/indexes",
                ));
            }
            _ => return Err(Error::new(ident.span(), "Unexpected identifier")),
        }
    }

    let mut columns = columns.expect("columns must be defined");
    if let Some(i) = indexes {
        columns.indexes = i
    }

    read_only::expand(name, columns, version)
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

    #[test]
    fn test_explicit_version() {
        let input = quote! {
            name: UserV1,
            version: 2,
            columns: {
                id: u64 primary_key,
            },
        };

        let res = expand(input).unwrap();
        let output = res.to_string();

        assert!(
            output.contains("index (read_only)"),
            "should generate read_only index attribute"
        );
    }

    #[test]
    fn test_rejects_version_after_columns() {
        let input = quote! {
            name: UserV1,
            columns: {
                id: u64 primary_key,
            },
            version: 2,
        };

        let res = expand(input);
        assert!(res.is_err(), "should reject version after columns");
    }
}
