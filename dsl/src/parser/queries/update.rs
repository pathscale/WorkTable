use indexmap::IndexMap;
use proc_macro2::{Ident, TokenTree};
use syn::spanned::Spanned;

use crate::Parser;
use crate::model::Operation;

impl Parser {
    /// The `update_partial` block, and the profile it was annotated with.
    ///
    /// The annotation is returned beside the operations rather than folded
    /// into them because it applies to the block: every query in it runs on
    /// the same runtime, and saying so once is the point of writing it at the
    /// section rather than on each query.
    pub fn parse_update_partials(&mut self) -> syn::Result<(Option<Ident>, IndexMap<Ident, Operation>)> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected `update_partial` field in declaration",
        ))?;
        if let TokenTree::Ident(ident) = ident {
            if ident.to_string().as_str() != "update_partial" {
                return Err(syn::Error::new(ident.span(), "Expected `update_partial` field"));
            }
        } else {
            return Err(syn::Error::new(ident.span(), "Expected field name identifier."));
        };

        let runtime = self.try_parse_section_runtime()?;

        self.parse_colon()?;

        let ops = self
            .input_iter
            .next()
            .ok_or(syn::Error::new(self.input.span(), "Expected operation declarations"))?;
        if let TokenTree::Group(ops) = ops {
            let mut parser = Parser::new(ops.stream());
            let ops = parser.parse_operations()?;
            self.try_parse_comma()?;
            Ok((runtime, ops))
        } else {
            Err(syn::Error::new(ops.span(), "Expected operation declarations"))
        }
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::{Ident, Span};
    use quote::quote;

    use crate::Parser;

    #[test]
    fn test_update() {
        let tokens = quote! {
            update_partial: {
                TestQuery(id, test) by name,
                Test1Query(id, name) by test,
            }
        };
        let mut parser = Parser::new(tokens);
        let (_, ops) = parser.parse_update_partials().unwrap();

        assert_eq!(ops.len(), 2);
        let op = ops.get(&Ident::new("TestQuery", Span::mixed_site())).unwrap();

        assert_eq!(op.name, "TestQuery");
        assert_eq!(op.columns.len(), 2);
        assert_eq!(op.columns[0], "id");
        assert_eq!(op.columns[1], "test");
        assert_eq!(op.by, "name");
    }
}
