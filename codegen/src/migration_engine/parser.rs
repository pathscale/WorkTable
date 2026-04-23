use proc_macro2::{Ident, TokenStream, TokenTree};
use std::collections::BTreeMap;
use syn::spanned::Spanned as _;
use convert_case::{Case, Casing};
use quote::ToTokens;

use crate::common::Parser;

/// Represents a simple path like `v1::UserV1WorkTable`.
pub struct TablePath {
    pub module: Option<Ident>,
    pub ident: Ident,
}

impl ToTokens for TablePath {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ident = &self.ident;
        if let Some(mod_) = &self.module {
            quote::quote! { #mod_::#ident }.to_tokens(tokens);
        } else {
            quote::quote! { #ident }.to_tokens(tokens);
        }
    }
}

impl TablePath {
    /// Returns the full path as tokens.
    pub fn to_tokens(&self) -> TokenStream {
        let ident = &self.ident;
        if let Some(mod_) = &self.module {
            quote::quote! { #mod_::#ident }
        } else {
            quote::quote! { #ident }
        }
    }

    /// Returns just the identifier (e.g., `UserV1WorkTable`).
    #[allow(dead_code)]
    pub fn ident(&self) -> &Ident {
        &self.ident
    }
}

pub struct MigrationEngineInput {
    pub migration: Ident,
    pub current: Ident,
    pub ctx: Ident,
    pub version_tables: BTreeMap<u32, TablePath>,
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
            // Skip stray commas (after closing braces etc.)
            if let TokenTree::Punct(p) = ident {
                if p.as_char() == ',' {
                    parser.input_iter.next();
                    continue;
                }
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
                    ))
                }
            }
        }

        Ok(Self {
            migration: migration.ok_or_else(|| syn::Error::new(span, "missing `migration`"))?,
            current: current.ok_or_else(|| syn::Error::new(span, "missing `current`"))?,
            ctx: ctx.ok_or_else(|| syn::Error::new(span, "missing `ctx`"))?,
            version_tables: version_tables
                .ok_or_else(|| syn::Error::new(span, "missing `version_tables`"))?,
        })
    }

    /// Derive the table base name from the current table type.
    /// e.g. `UserWorkTable` -> `"user"`
    pub fn table_name_snake(&self) -> String {
        let name = self.current.to_string();
        let base = name
            .strip_suffix("WorkTable")
            .expect("current table type should end with `WorkTable`");
        base.to_case(Case::Snake)
    }

    /// Derive the row type from a table path.
    /// e.g. `v1::UserV1WorkTable` -> `v1::UserV1Row`
    /// e.g. `UserWorkTable` -> `UserRow`
    pub fn row_type_for(table: &TablePath) -> TablePath {
        let s = table.ident.to_string();
        if let Some(base) = s.strip_suffix("WorkTable") {
            let row_ident = Ident::new(&format!("{}Row", base), table.ident.span());
            TablePath { module: table.module.clone(), ident: row_ident }
        } else {
            panic!("table type `{}` should end with `WorkTable`", s);
        }
    }

    /// Derive the primary key generator state type from the current table.
    /// Uses the fully-qualified type: `<<NamePrimaryKey as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State`
    pub fn pk_gen_state_type_tokens(&self) -> TokenStream {
        let pk_type = self.pk_type();
        quote::quote! {
            <<#pk_type as worktable::prelude::TablePrimaryKey>::Generator as worktable::prelude::PrimaryKeyGeneratorState>::State
        }
    }

    /// Derive the primary key type from the current table.
    /// e.g. `UserWorkTable` -> `UserPrimaryKey`
    #[allow(dead_code)]
    pub fn pk_type(&self) -> Ident {
        let name = self.current.to_string();
        let base = name
            .strip_suffix("WorkTable")
            .expect("current table type should end with `WorkTable`");
        Ident::new(&format!("{}PrimaryKey", base), self.current.span())
    }

    /// Derive the persistence engine type from the current table.
    /// e.g. `UserWorkTable` -> `UserPersistenceEngine`
    #[allow(dead_code)]
    pub fn persistence_engine_type(&self) -> Ident {
        let name = self.current.to_string();
        let base = name
            .strip_suffix("WorkTable")
            .expect("current table type should end with `WorkTable`");
        Ident::new(&format!("{}PersistenceEngine", base), self.current.span())
    }

    /// Derive the space type from the current table.
    /// e.g. `UserWorkTable` -> `UserSpace`
    #[allow(dead_code)]
    pub fn space_type(&self) -> Ident {
        let name = self.current.to_string();
        let base = name
            .strip_suffix("WorkTable")
            .expect("current table type should end with `WorkTable`");
        Ident::new(&format!("{}Space", base), self.current.span())
    }

    /// Derive the space primary index type.
    #[allow(dead_code)]
    pub fn space_primary_index_type(&self) -> Ident {
        let name = self.current.to_string();
        let base = name
            .strip_suffix("WorkTable")
            .expect("current table type should end with `WorkTable`");
        Ident::new(&format!("{}SpacePrimaryIndex", base), self.current.span())
    }

    /// Derive the space secondary indexes type.
    #[allow(dead_code)]
    pub fn space_secondary_indexes_type(&self) -> Ident {
        let name = self.current.to_string();
        let base = name
            .strip_suffix("WorkTable")
            .expect("current table type should end with `WorkTable`");
        Ident::new(&format!("{}SpaceSecondaryIndexes", base), self.current.span())
    }

    /// Derive the available indexes type.
    #[allow(dead_code)]
    pub fn available_indexes_type(&self) -> Ident {
        let name = self.current.to_string();
        let base = name
            .strip_suffix("WorkTable")
            .expect("current table type should end with `WorkTable`");
        Ident::new(&format!("{}AvailableIndexes", base), self.current.span())
    }

    /// Derive the secondary index events type.
    #[allow(dead_code)]
    pub fn secondary_index_events_type(&self) -> Ident {
        let name = self.current.to_string();
        let base = name
            .strip_suffix("WorkTable")
            .expect("current table type should end with `WorkTable`");
        Ident::new(&format!("{}SpaceSecondaryIndexEvents", base), self.current.span())
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

fn parse_version_tables(parser: &mut Parser) -> syn::Result<BTreeMap<u32, TablePath>> {
    let _key = parser.input_iter.next().unwrap();
    parser.parse_colon()?;

    let brace = parser
        .input_iter
        .next()
        .ok_or_else(|| syn::Error::new(parser.input.span(), "Expected `{`"))?;
    let span = brace.span();

    // Extract tokens inside braces
    let inner_tokens = match brace {
        TokenTree::Group(group) => group.stream(),
        _ => return Err(syn::Error::new(span, "Expected `{` for version_tables")),
    };

    let mut result = BTreeMap::new();
    let mut inner_parser = Parser::new(inner_tokens);

    while inner_parser.peek_next().is_some() {
        // Parse version number
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

        // Parse table type (may include path like v1::UserV1WorkTable)
        let table_ident = parse_path_ident(&mut inner_parser)?;
        inner_parser.try_parse_comma()?;

        result.insert(version, table_ident);
    }

    Ok(result)
}

/// Parse an identifier or path (e.g., `v1::UserV1WorkTable`) and return a TablePath.
fn parse_path_ident(parser: &mut Parser) -> syn::Result<TablePath> {
    let first = parser
        .input_iter
        .next()
        .ok_or_else(|| syn::Error::new(parser.input.span(), "Expected table type identifier"))?;

    if let TokenTree::Ident(ident) = first {
        // Check if followed by `::`
        if let Some(TokenTree::Punct(p)) = parser.peek_next() {
            if p.as_char() == ':' {
                // consume `::`
                parser.input_iter.next();
                parser.input_iter.next();
                // next should be the actual table ident
                let next = parser
                    .input_iter
                    .next()
                    .ok_or_else(|| syn::Error::new(parser.input.span(), "Expected table type after `::`"))?;
                if let TokenTree::Ident(table_ident) = next {
                    return Ok(TablePath { module: Some(ident), ident: table_ident });
                }
            }
        }
        Ok(TablePath { module: None, ident })
    } else {
        Err(syn::Error::new(first.span(), "Expected identifier for table type"))
    }
}
