use proc_macro2::TokenTree;
use syn::spanned::Spanned as _;

use crate::common::model::Persistence;
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
    use crate::common::model::Persistence;

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
