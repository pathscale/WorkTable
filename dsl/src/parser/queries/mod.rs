mod delete;
mod in_place;
mod operation;
mod select;
mod update;

use proc_macro2::TokenTree;
use syn::spanned::Spanned;

use crate::Parser;
use crate::model::Queries;

impl Parser {
    pub fn parse_queries(&mut self) -> syn::Result<Queries> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected `queries` field in declaration",
        ))?;
        if let TokenTree::Ident(ident) = ident {
            if ident.to_string().as_str() != "queries" {
                return Err(syn::Error::new(
                    ident.span(),
                    "Expected `queries` field. `WorkTable` name must be specified",
                ));
            }
        } else {
            return Err(syn::Error::new(ident.span(), "Expected field name identifier."));
        };

        self.parse_colon()?;

        let mut queries = Queries::default();
        let ops = self
            .input_iter
            .next()
            .ok_or(syn::Error::new(self.input.span(), "Expected operation declarations"))?;
        if let TokenTree::Group(ops) = ops {
            let mut parser = Parser::new(ops.stream());
            while let Some(ident) = parser.peek_next() {
                match ident.to_string().as_str() {
                    "update" => {
                        let (runtime, updates) = parser.parse_updates()?;
                        queries.updates = updates;
                        queries.update_runtime = runtime;
                    }
                    "delete" => {
                        let (runtime, deletes) = parser.parse_deletes()?;
                        queries.deletes = deletes;
                        queries.delete_runtime = runtime;
                    }
                    "update_in_place" => {
                        let (runtime, updates) = parser.parse_updates_in_place()?;
                        queries.updates_in_place = updates;
                        queries.update_in_place_runtime = runtime;
                    }
                    // The 1.10 rename. Without this arm the old spelling gets
                    // the generic "unexpected token" below, which lists the new
                    // keyword but does not say it is the same thing renamed, so
                    // the reader has to guess that their table still works.
                    "in_place" => {
                        return Err(syn::Error::new(
                            ident.span(),
                            "`in_place:` was renamed to `update_in_place:` in WorkTable 1.10; rename the section keyword, the queries inside it are unchanged",
                        ));
                    }
                    other => {
                        return Err(syn::Error::new(
                            ident.span(),
                            format!(
                                "Unexpected token `{other}`; expected one of `update`, `delete`, `update_in_place`"
                            ),
                        ));
                    }
                }
            }
        } else {
            return Err(syn::Error::new(ops.span(), "Expected operation declarations"));
        };

        self.try_parse_comma()?;

        Ok(queries)
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::Parser;

    #[test]
    fn sections_are_unannotated_by_default() {
        let tokens = quote! {
            queries: {
                update: { Fill(qty) by id },
                delete: { BySymbol() by symbol },
                update_in_place: { Bump(qty) by id },
            }
        };
        let queries = Parser::new(tokens).parse_queries().unwrap();

        assert!(queries.update_runtime.is_none());
        assert!(queries.delete_runtime.is_none());
        assert!(queries.update_in_place_runtime.is_none());
    }

    #[test]
    fn each_section_takes_a_runtime_annotation() {
        let tokens = quote! {
            queries: {
                update runtime fast_local: { Fill(qty) by id },
                delete runtime wide: { BySymbol() by symbol },
                update_in_place runtime bulk: { Bump(qty) by id },
            }
        };
        let queries = Parser::new(tokens).parse_queries().unwrap();

        assert_eq!(queries.update_runtime.unwrap(), "fast_local");
        assert_eq!(queries.delete_runtime.unwrap(), "wide");
        assert_eq!(queries.update_in_place_runtime.unwrap(), "bulk");
        assert_eq!(queries.updates.len(), 1);
        assert_eq!(queries.deletes.len(), 1);
        assert_eq!(queries.updates_in_place.len(), 1);
    }

    #[test]
    fn an_annotated_section_sits_beside_an_unannotated_one() {
        let tokens = quote! {
            queries: {
                update runtime fast_local: { Fill(qty) by id },
                update_in_place: { Bump(qty) by id },
            }
        };
        let queries = Parser::new(tokens).parse_queries().unwrap();

        assert_eq!(queries.update_runtime.unwrap(), "fast_local");
        assert!(queries.update_in_place_runtime.is_none());
    }

    #[test]
    fn a_section_rejects_a_backend_in_place_of_a_profile() {
        let tokens = quote! {
            queries: {
                update runtime nagoya: { Fill(qty) by id },
            }
        };
        let error = Parser::new(tokens).parse_queries().unwrap_err().to_string();

        assert!(
            error.contains("`nagoya` is a runtime backend, not a profile name"),
            "{error}"
        );
    }

    #[test]
    fn legacy_in_place_section_is_rejected() {
        let tokens = quote! {
            queries: { in_place: { Bump(qty) by id } }
        };
        let error = Parser::new(tokens).parse_queries().unwrap_err().to_string();

        assert!(error.contains("Unexpected token `in_place`"), "{error}");
    }
}
