use proc_macro2::Ident;
use proc_macro2::TokenTree;
use syn::spanned::Spanned as _;

use crate::common::parser::Parser;

impl Parser {
    pub fn parse_name(&mut self) -> syn::Result<Ident> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected `name` field in declaration",
        ))?;
        if let TokenTree::Ident(ident) = ident {
            if ident.to_string().as_str() != "name" {
                return Err(syn::Error::new(
                    ident.span(),
                    "Expected `name` field. `WorkTable` name must be specified",
                ));
            }
        } else {
            return Err(syn::Error::new(
                ident.span(),
                "Expected field name identifier.",
            ));
        };

        self.parse_colon()?;

        let name = self
            .input_iter
            .next()
            .ok_or(syn::Error::new(self.input.span(), "Expected token."))?;
        let name = if let TokenTree::Ident(name) = name {
            name
        } else {
            return Err(syn::Error::new(name.span(), "Expected identifier."));
        };

        self.try_parse_comma()?;

        Ok(name)
    }

    pub fn parse_version(&mut self) -> syn::Result<Option<u32>> {
        if let Some(ident) = self.peek_next()
            && ident.to_string().as_str() == "version" {
                self.input_iter.next();

                self.parse_colon()?;

                let value = self.input_iter.next().ok_or(syn::Error::new(
                    self.input.span(),
                    "Expected version value",
                ))?;
                let value = if let TokenTree::Literal(value) = value {
                    value
                } else {
                    return Err(syn::Error::new(value.span(), "Expected literal for version."));
                };

                self.try_parse_comma()?;

                let value_str = value.to_string().replace("_", "");
                let version = value_str.parse::<u32>().map_err(|_| {
                    syn::Error::new(value.span(), "Expected valid u32 number for version.")
                })?;

                return Ok(Some(version));
            }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::common::Parser;

    #[test]
    fn test_name_parse() {
        let tokens = quote! {name: TestName,};

        let mut parser = Parser::new(tokens);
        let name = parser.parse_name();

        assert!(name.is_ok());
        let name = name.unwrap();

        assert_eq!(name, "TestName");
    }

    #[test]
    fn test_empty() {
        let tokens = quote! {};

        let mut parser = Parser::new(tokens);
        let name = parser.parse_name();

        assert!(name.is_err());
    }

    #[test]
    fn test_literal_field() {
        let tokens = quote! {"nme": TestName,};

        let mut parser = Parser::new(tokens);
        let name = parser.parse_name();

        assert!(name.is_err());
    }

    #[test]
    fn test_wrong_field() {
        let tokens = quote! {nme: TestName,};

        let mut parser = Parser::new(tokens);
        let name = parser.parse_name();

        assert!(name.is_err());
    }

    #[test]
    fn test_version_parse() {
        let tokens = quote! {name: TestName, version: 2,};

        let mut parser = Parser::new(tokens);
        let name = parser.parse_name().unwrap();
        assert_eq!(name, "TestName");

        let version = parser.parse_version().unwrap();
        assert_eq!(version, Some(2));
    }

    #[test]
    fn test_version_default() {
        let tokens = quote! {name: TestName,};

        let mut parser = Parser::new(tokens);
        let name = parser.parse_name().unwrap();
        assert_eq!(name, "TestName");

        let version = parser.parse_version().unwrap();
        assert_eq!(version, None);
    }

    #[test]
    fn test_version_before_other_fields() {
        let tokens = quote! {name: TestName, version: 5, columns: { id: u64 primary_key },};

        let mut parser = Parser::new(tokens);
        let name = parser.parse_name().unwrap();
        let version = parser.parse_version().unwrap();
        assert_eq!(version, Some(5));

        let next = parser.peek_next().unwrap();
        assert_eq!(next.to_string(), "columns");
    }
}
