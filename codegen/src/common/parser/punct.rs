use proc_macro2::TokenTree;
use syn::spanned::Spanned;

use crate::common::parser::Parser;

impl Parser {
    /// Parses ':' from [`proc_macro2::TokenStream`].
    pub fn parse_colon(&mut self) -> syn::Result<()> {
        let iter = &mut self.input_iter;

        let colon = iter
            .next()
            .ok_or(syn::Error::new(self.input.span(), "Expected token."))?;
        if let TokenTree::Punct(colon) = colon {
            if colon.as_char() != ':' {
                return Err(syn::Error::new(
                    colon.span(),
                    format!("Expected `:` found: `{}`", colon.as_char()),
                ));
            }

            Ok(())
        } else {
            Err(syn::Error::new(colon.span(), "Expected `:`."))
        }
    }

    /// Tries to parse ',' from [`TokenStream`] without calling `next` on wrong token.
    pub fn try_parse_comma(&mut self) -> syn::Result<()> {
        let iter = &mut self.input_iter;

        if let Some(colon) = iter.peek()
            && comma(colon).is_ok()
        {
            iter.next();
        }

        Ok(())
    }

    /// Parses '=>' from token stream.
    pub fn parse_fat_arrow(&mut self) -> syn::Result<()> {
        let iter = &mut self.input_iter;

        let first = iter
            .next()
            .ok_or(syn::Error::new(self.input.span(), "Expected token."))?;
        if let TokenTree::Punct(p) = first {
            if p.as_char() == '=' {
                // next should be '>'
                let second = iter
                    .next()
                    .ok_or(syn::Error::new(self.input.span(), "Expected '>' after '='"))?;
                if let TokenTree::Punct(p2) = second {
                    if p2.as_char() == '>' {
                        return Ok(());
                    }
                    return Err(syn::Error::new(
                        p2.span(),
                        format!("Expected '>' found: '{}'", p2.as_char()),
                    ));
                }
                return Err(syn::Error::new(second.span(), "Expected '>'"));
            }
            return Err(syn::Error::new(
                p.span(),
                format!("Expected '=' found: '{}'", p.as_char()),
            ));
        }
        Err(syn::Error::new(first.span(), "Expected '=>'"))
    }
}

fn comma(tt: &TokenTree) -> syn::Result<()> {
    if let TokenTree::Punct(colon) = tt {
        if colon.as_char() != ',' {
            return Err(syn::Error::new(
                colon.span(),
                format!("Expected `,` found: `{}`", colon.as_char()),
            ));
        }

        Ok(())
    } else {
        Err(syn::Error::new(tt.span(), "Expected `,`."))
    }
}
