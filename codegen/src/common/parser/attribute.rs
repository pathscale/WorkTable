use proc_macro2::TokenTree;
use syn::spanned::Spanned as _;

use crate::common::model::{PARTITION_KEY_TYPES, PartitionKey, Persistence};
use crate::common::parser::Parser;

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

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::common::Parser;
    use crate::common::model::{PARTITION_KEY_TYPES, PartitionKey, Persistence};

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
