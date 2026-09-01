use crate::Parser;
use crate::model::{Index, IndexBackend};
use indexmap::IndexMap;
use proc_macro2::{Delimiter, Ident, TokenTree};
use syn::spanned::Spanned;

impl Parser {
    pub fn try_parse_index_backend(&mut self) -> syn::Result<Option<IndexBackend>> {
        let Some(TokenTree::Ident(using)) = self.input_iter.peek() else {
            return Ok(None);
        };
        if using != "using" {
            return Ok(None);
        }

        let using_span = using.span();
        self.input_iter.next();
        let backend = self.input_iter.next().ok_or_else(|| {
            syn::Error::new(
                using_span,
                "expected an index backend after `using`: `worktables_index`, `indexset`, `congee`, or `arctic`",
            )
        })?;
        let TokenTree::Ident(backend) = backend else {
            return Err(syn::Error::new(
                backend.span(),
                "expected an index backend identifier after `using`",
            ));
        };

        match backend.to_string().as_str() {
            "worktables_index" => Ok(Some(IndexBackend::WorktablesIndex)),
            "indexset" => Ok(Some(IndexBackend::Indexset)),
            "congee" => Ok(Some(IndexBackend::Congee)),
            "arctic" => Ok(Some(IndexBackend::Arctic)),
            _ => Err(syn::Error::new(
                backend.span(),
                "unknown index backend; expected `worktables_index`, `indexset`, `congee`, or `arctic`",
            )),
        }
    }

    pub fn parse_indexes(&mut self) -> syn::Result<IndexMap<Ident, Index>> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected `indexes` field in declaration",
        ))?;

        if let TokenTree::Ident(ident) = ident {
            if ident.to_string().as_str() != "indexes" {
                return Err(syn::Error::new(ident.span(), "Expected `indexes` field"));
            }
        } else {
            return Err(syn::Error::new(ident.span(), "Expected field name identifier."));
        };

        self.parse_colon()?;

        let tt = {
            let group = self
                .input_iter
                .next()
                .ok_or(syn::Error::new(self.input.span(), "Expected `indexes` declarations"))?;
            if let TokenTree::Group(group) = group {
                if group.delimiter() != Delimiter::Brace {
                    return Err(syn::Error::new(group.span(), "Expected brace"));
                }
                group.stream()
            } else {
                return Err(syn::Error::new(group.span(), "Expected `indexes` declarations"));
            }
        };

        let mut parser = Parser::new(tt);

        let mut rows = IndexMap::new();
        let mut ind = true;

        while ind {
            let (name, row) = parser.parse_index()?;
            rows.insert(name, row);
            ind = parser.has_next()
        }

        self.try_parse_comma()?;

        Ok(rows)
    }

    pub fn parse_index(&mut self) -> syn::Result<(Ident, Index)> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected index name field in declaration",
        ))?;
        let ident = if let TokenTree::Ident(ident) = ident {
            ident
        } else {
            return Err(syn::Error::new(ident.span(), "Expected index name"));
        };

        self.parse_colon()?;

        let row_name = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected row name field in declaration",
        ))?;
        let row_name = if let TokenTree::Ident(row_name) = row_name {
            row_name
        } else {
            return Err(syn::Error::new(row_name.span(), "Expected row name"));
        };

        let is_unique = if let Some(TokenTree::Ident(unique)) = self.input_iter.peek() {
            if unique.to_string().as_str() == "unique" {
                self.input_iter.next();
                true
            } else {
                false
            }
        } else {
            false
        };

        let backend = self.try_parse_index_backend()?.unwrap_or_default();

        self.try_parse_comma()?;

        Ok((
            row_name.clone(),
            Index {
                name: ident,
                field: row_name,
                is_unique,
                backend,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::Parser;
    use crate::model::IndexBackend;

    #[test]
    fn absent_using_defaults_to_worktables_index() {
        let mut parser = Parser::new(quote! { value_idx: value unique, });
        let (_, index) = parser.parse_index().unwrap();
        assert_eq!(index.backend, IndexBackend::WorktablesIndex);
    }

    #[test]
    fn parses_all_backends() {
        for (tokens, expected) in [
            (
                quote! { value_idx: value unique using worktables_index, },
                IndexBackend::WorktablesIndex,
            ),
            (
                quote! { value_idx: value unique using indexset, },
                IndexBackend::Indexset,
            ),
            (quote! { value_idx: value unique using congee, }, IndexBackend::Congee),
            (quote! { value_idx: value unique using arctic, }, IndexBackend::Arctic),
        ] {
            let mut parser = Parser::new(tokens);
            let (_, index) = parser.parse_index().unwrap();
            assert_eq!(index.backend, expected);
        }
    }

    #[test]
    fn rejects_unknown_backend() {
        let mut parser = Parser::new(quote! { value_idx: value unique using unknown, });
        let error = parser.parse_index().unwrap_err();
        assert!(error.to_string().contains("unknown index backend"));
    }
}
