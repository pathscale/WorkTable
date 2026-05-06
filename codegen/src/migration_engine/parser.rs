use proc_macro2::{Ident, TokenStream, TokenTree};
use std::collections::BTreeMap;
use syn::spanned::Spanned as _;

use crate::common::{Parser, name_generator::WorktableNameGenerator};

pub struct MigrationEngineInput {
    pub migration: Ident,
    pub current: Ident,
    pub ctx: Ident,
    pub version_tables: BTreeMap<u32, syn::Path>,
    pub name_generator: WorktableNameGenerator,
}

impl MigrationEngineInput {
    pub fn parse(input: TokenStream) -> syn::Result<Self> {
        let span = input.span();
        let mut parser = Parser::new(input);
        let mut migration = None;
        let mut current = None;
        let mut ctx = None;
        let mut version_tables = None;

        while let Some(ident) = parser.peek_next() {
            if let TokenTree::Punct(p) = ident
                && p.as_char() == ','
            {
                parser.input_iter.next();
                continue;
            }
            match ident.to_string().as_str() {
                "migration" => migration = Some(parse_ident_field(&mut parser)?),
                "current" => current = Some(parse_ident_field(&mut parser)?),
                "ctx" => ctx = Some(parse_ident_field(&mut parser)?),
                "version_tables" => version_tables = Some(parse_version_tables(&mut parser)?),
                _ => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("Unexpected identifier: {}", ident),
                    ));
                }
            }
        }

        let current = current.ok_or_else(|| syn::Error::new(span, "missing `current`"))?;
        let name_generator = WorktableNameGenerator::from_struct_ident(&current);

        Ok(Self {
            migration: migration.ok_or_else(|| syn::Error::new(span, "missing `migration`"))?,
            current,
            ctx: ctx.ok_or_else(|| syn::Error::new(span, "missing `ctx`"))?,
            version_tables: version_tables.ok_or_else(|| syn::Error::new(span, "missing `version_tables`"))?,
            name_generator,
        })
    }

    /// Derive the row type path from a table path.
    /// e.g. `v1::UserV1WorkTable` -> `v1::UserV1Row`
    /// e.g. `UserWorkTable` -> `UserRow`
    pub fn row_type_for(table: &syn::Path) -> syn::Path {
        let last_segment = table.segments.last().expect("path should have at least one segment");
        let ident_str = last_segment.ident.to_string();
        let base = ident_str
            .strip_suffix("WorkTable")
            .expect("table type should end with `WorkTable`");
        let row_ident = Ident::new(&format!("{}Row", base), last_segment.ident.span());

        let leading_segments: Vec<_> = table.segments.iter().take(table.segments.len() - 1).cloned().collect();

        let mut new_path = syn::Path {
            leading_colon: table.leading_colon,
            segments: Default::default(),
        };

        for seg in leading_segments {
            new_path.segments.push(seg);
        }
        new_path.segments.push(syn::PathSegment {
            ident: row_ident,
            arguments: syn::PathArguments::None,
        });

        new_path
    }
}

fn parse_ident_field(parser: &mut Parser) -> syn::Result<Ident> {
    let _key = parser.input_iter.next().unwrap(); // consume the key ident
    parser.parse_colon()?;
    let value = parser
        .input_iter
        .next()
        .ok_or_else(|| syn::Error::new(parser.input.span(), "Expected identifier"))?;
    let ident = if let TokenTree::Ident(ident) = value {
        ident
    } else {
        return Err(syn::Error::new(value.span(), "Expected identifier"));
    };
    parser.try_parse_comma()?;
    Ok(ident)
}

fn parse_version_tables(parser: &mut Parser) -> syn::Result<BTreeMap<u32, syn::Path>> {
    let _key = parser.input_iter.next().unwrap();
    parser.parse_colon()?;

    let brace = parser
        .input_iter
        .next()
        .ok_or_else(|| syn::Error::new(parser.input.span(), "Expected `{`"))?;
    let span = brace.span();

    let inner_tokens = match brace {
        TokenTree::Group(group) => group.stream(),
        _ => return Err(syn::Error::new(span, "Expected `{` for version_tables")),
    };

    let mut result = BTreeMap::new();
    let mut inner_parser = Parser::new(inner_tokens);

    while inner_parser.peek_next().is_some() {
        let key_token = inner_parser
            .input_iter
            .next()
            .ok_or_else(|| syn::Error::new(span, "Expected version number"))?;
        let version = if let TokenTree::Literal(lit) = key_token {
            let s = lit.to_string().replace('_', "");
            s.parse::<u32>()
                .map_err(|_| syn::Error::new(lit.span(), "Expected u32 version number"))?
        } else {
            return Err(syn::Error::new(key_token.span(), "Expected version number"));
        };

        inner_parser.parse_fat_arrow()?;

        let path_tokens: TokenStream = inner_parser
            .input_iter
            .by_ref()
            .take_while(|t| {
                if let TokenTree::Punct(p) = t {
                    p.as_char() != ','
                } else {
                    true
                }
            })
            .collect();
        let path: syn::Path = syn::parse2(path_tokens.clone())
            .map_err(|e| syn::Error::new(path_tokens.span(), format!("Invalid path: {}", e)))?;

        inner_parser.try_parse_comma()?;
        result.insert(version, path);
    }

    Ok(result)
}
