//! `worktable_vec!`: the same declaration, a `Vec` behind it.
//!
//! A separate macro rather than a mode of `worktable!`, for the reason
//! `worktable_version!` is: the choice changes the generated type, and
//! inferring it from the absence of other keys would mean two identical
//! declarations with different concurrency guarantees.
//!
//! The blocks it refuses are refused because they describe machinery this
//! table does not have, and a silent no-op would be worse than an error.

use proc_macro2::TokenStream;
use syn::Error;

use crate::common::Parser;
use crate::generators::vec_table;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let mut parser = Parser::new(input);
    let mut columns = None;
    let mut indexes = None;

    let name = parser.parse_name()?;

    while let Some(ident) = parser.peek_next() {
        match ident.to_string().as_str() {
            "columns" => columns = Some(parser.parse_columns()?),
            "indexes" => indexes = Some(parser.parse_indexes()?),
            "persist" => {
                return Err(Error::new(
                    ident.span(),
                    "worktable_vec! has no persistence. Use worktable! with `persist: true`.",
                ));
            }
            "queries" => {
                return Err(Error::new(
                    ident.span(),
                    "worktable_vec! does not generate queries yet; use the select and update \
                     methods directly",
                ));
            }
            "columnar_indexes" => {
                return Err(Error::new(
                    ident.span(),
                    "worktable_vec! does not support columnar fields: columnar storage is a \
                     paging feature and this table has no pages",
                ));
            }
            "config" => {
                return Err(Error::new(
                    ident.span(),
                    "worktable_vec! has no page size and no row derives to configure",
                ));
            }
            "runtime" => {
                return Err(Error::new(
                    ident.span(),
                    "worktable_vec! is synchronous and never reaches a runtime",
                ));
            }
            other => {
                return Err(Error::new(
                    ident.span(),
                    format!("Unexpected token `{other}`; expected one of `columns`, `indexes`"),
                ));
            }
        }
    }

    let mut columns = columns
        .ok_or_else(|| Error::new(name.span(), "Expected a `columns` block in declaration"))?;
    if let Some(i) = indexes {
        columns.indexes = i;
    }

    vec_table::expand(name, columns)
}
