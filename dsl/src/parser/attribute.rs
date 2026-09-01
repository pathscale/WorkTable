use proc_macro2::TokenTree;
use syn::spanned::Spanned as _;

use crate::model::{PARTITION_KEY_TYPES, PartitionKey, Persistence};
use crate::parser::Parser;

// TODO: Move this to separate attributes section because now it only parses persist.
impl Parser {
    pub fn parse_persist(&mut self) -> syn::Result<Persistence> {
        let Some(ident) = self.input_iter.peek().cloned() else {
            return Ok(Persistence::Omitted);
        };
        let TokenTree::Ident(ident) = ident else {
            return Err(syn::Error::new(ident.span(), "Expected field name identifier."));
        };

        if ident.to_string().as_str() == "persist" {
            let _ = self.input_iter.next();
            self.parse_colon()?;
            let bool = self
                .input_iter
                .next()
                .ok_or(syn::Error::new(self.input.span(), "Expected token."))?;
            let res = if let TokenTree::Ident(bool) = bool {
                if bool.to_string().as_str() == "true" {
                    Ok(Persistence::Persisted)
                } else if bool.to_string().as_str() == "false" {
                    Ok(Persistence::MemoryOnly)
                } else {
                    Err(syn::Error::new(bool.span(), "expected `true` or `false`"))
                }
            } else {
                Err(syn::Error::new(bool.span(), "Expected identifier."))
            };
            self.try_parse_comma()?;

            res
        } else {
            Ok(Persistence::Omitted)
        }
    }
}

impl Parser {
    /// Parse an optional `partition_by: <name>: <uint type>,` declaration.
    ///
    /// Positional, like `version` and `persist`, and for the same reason: it
    /// changes the shape of what is generated rather than describing a part
    /// of the table, so it belongs before the blocks.
    pub fn parse_partition_by(&mut self) -> syn::Result<Option<PartitionKey>> {
        let Some(ident) = self.input_iter.peek().cloned() else {
            return Ok(None);
        };
        let TokenTree::Ident(ident) = ident else {
            return Err(syn::Error::new(ident.span(), "Expected field name identifier."));
        };
        if ident.to_string().as_str() != "partition_by" {
            return Ok(None);
        }
        let _ = self.input_iter.next();
        self.parse_colon()?;

        let name = self
            .input_iter
            .next()
            .ok_or_else(|| syn::Error::new(self.input.span(), "Expected a partition key name."))?;
        let TokenTree::Ident(name) = name else {
            return Err(syn::Error::new(name.span(), "Expected a partition key name."));
        };

        self.parse_colon()?;

        let ty = self
            .input_iter
            .next()
            .ok_or_else(|| syn::Error::new(self.input.span(), "Expected a partition key type."))?;
        let TokenTree::Ident(ty) = ty else {
            return Err(syn::Error::new(ty.span(), "Expected a partition key type."));
        };
        if !PARTITION_KEY_TYPES.contains(&ty.to_string().as_str()) {
            return Err(syn::Error::new(
                ty.span(),
                format!(
                    "`{ty}` is not a partition key type; routing is an array index, so the key must be one of {}. \
                     Names belong in a separate registry table looked up once, not in the routing key",
                    PARTITION_KEY_TYPES.join(", ")
                ),
            ));
        }

        self.try_parse_comma()?;
        Ok(Some(PartitionKey { name, ty }))
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::Parser;
    use crate::model::{PARTITION_KEY_TYPES, PartitionKey, Persistence};

    #[test]
    fn test_empty() {
        let tokens = quote! {};
        let mut parser = Parser::new(tokens);
        let empty = parser.parse_persist();
        assert!(empty.is_ok());
        assert_eq!(empty.unwrap(), Persistence::Omitted)
    }

    #[test]
    fn test_literal_field() {
        let tokens = quote! {"nme": TestName,};
        let mut parser = Parser::new(tokens);
        let name = parser.parse_persist();
        assert!(name.is_err());
    }

    #[test]
    fn test_persistence() {
        let tokens = quote! {persist: true,};
        let mut parser = Parser::new(tokens);
        let name = parser.parse_persist();
        assert!(name.is_ok());
        assert_eq!(name.unwrap(), Persistence::Persisted);
    }

    #[test]
    fn test_explicit_memory_only() {
        let tokens = quote! {persist: false,};
        let mut parser = Parser::new(tokens);
        let persistence = parser.parse_persist().unwrap();
        assert_eq!(persistence, Persistence::MemoryOnly);
    }

    #[test]
    fn test_invalid_boolean() {
        let tokens = quote! {persist: maybe,};
        let mut parser = Parser::new(tokens);
        assert!(parser.parse_persist().is_err());
    }

    #[test]
    fn test_wrong_field() {
        let tokens = quote! {nme: TestName,};
        let mut parser = Parser::new(tokens);
        let name = parser.parse_persist();
        assert!(name.is_ok());
        assert_eq!(name.unwrap(), Persistence::Omitted);
    }

    #[test]
    fn test_no_comma() {
        let tokens = quote! {name: TestName};
        let mut parser = Parser::new(tokens);
        let name = parser.parse_persist();
        assert!(name.is_ok());
        assert_eq!(name.unwrap(), Persistence::Omitted);
    }

    // `partition_by`. These were missing entirely: the grammar shipped with
    // its imports in this module and nothing using them, which is what the
    // unused-import lint was telling us.

    #[test]
    fn partition_by_is_optional() {
        let mut parser = Parser::new(quote! {});
        assert!(parser.parse_partition_by().unwrap().is_none());

        // A different attribute must be left alone for its own parser, not
        // consumed or rejected.
        let mut parser = Parser::new(quote! { persist: false, });
        assert!(parser.parse_partition_by().unwrap().is_none());
        assert_eq!(parser.parse_persist().unwrap(), Persistence::MemoryOnly);
    }

    #[test]
    fn partition_by_accepts_every_unsigned_key_type() {
        for ty in PARTITION_KEY_TYPES {
            let ty_ident = syn::Ident::new(ty, proc_macro2::Span::call_site());
            let mut parser = Parser::new(quote! { partition_by: symbol_id: #ty_ident, });
            let key: PartitionKey = parser
                .parse_partition_by()
                .unwrap_or_else(|e| panic!("`{ty}` must be accepted: {e}"))
                .unwrap_or_else(|| panic!("`{ty}` parsed as absent"));
            assert_eq!(key.name.to_string(), "symbol_id");
            assert_eq!(key.ty.to_string(), ty);
        }
    }

    #[test]
    fn partition_by_rejects_a_key_that_is_not_an_array_index() {
        // The routing key indexes into a spine, so a `String` key would have
        // to be hashed on every lookup. The refusal has to name the types and
        // point at the registry table, because that is the whole workaround.
        for ty in ["String", "i32", "f64", "bool", "Uuid"] {
            let ty_ident = syn::Ident::new(ty, proc_macro2::Span::call_site());
            let mut parser = Parser::new(quote! { partition_by: symbol: #ty_ident, });
            let error = parser
                .parse_partition_by()
                .expect_err(&format!("`{ty}` must be refused"))
                .to_string();
            assert!(error.contains(ty), "the offending type must be named: {error}");
            assert!(
                error.contains("u16") && error.contains("usize"),
                "the accepted types must be listed: {error}"
            );
            assert!(
                error.contains("registry table"),
                "the refusal must point at the workaround: {error}"
            );
        }
    }

    #[test]
    fn partition_by_reports_what_is_missing() {
        // Truncated declarations must name the missing piece rather than
        // panicking or silently parsing as absent.
        let mut parser = Parser::new(quote! { partition_by: });
        assert!(parser.parse_partition_by().is_err(), "a bare key must be an error");

        let mut parser = Parser::new(quote! { partition_by: symbol_id });
        assert!(
            parser.parse_partition_by().is_err(),
            "a name with no type must be an error"
        );

        let mut parser = Parser::new(quote! { partition_by: 7: u16, });
        let error = parser
            .parse_partition_by()
            .expect_err("a literal is not a name")
            .to_string();
        assert!(error.contains("name"), "unexpected reason: {error}");
    }

    #[test]
    fn partition_by_leaves_the_following_attribute_parseable() {
        // It is positional, so what follows has to still parse.
        let mut parser = Parser::new(quote! { partition_by: venue: u32, persist: false, });
        let key = parser.parse_partition_by().unwrap().expect("declared");
        assert_eq!(key.ty.to_string(), "u32");
        assert_eq!(parser.parse_persist().unwrap(), Persistence::MemoryOnly);
    }
}
