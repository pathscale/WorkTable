use std::collections::HashSet;
use std::str::FromStr;

use proc_macro2::{Delimiter, TokenTree};
use syn::spanned::Spanned;

use crate::common::Parser;
use crate::common::model::{ColumnSlotIdType, Config};

const CONFIG_FIELD_NAME: &str = "config";

impl Parser {
    pub fn parse_configs(&mut self) -> syn::Result<Config> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            format!("Expected `{CONFIG_FIELD_NAME}` field in declaration"),
        ))?;

        if let TokenTree::Ident(ident) = ident {
            if ident.to_string().as_str() != CONFIG_FIELD_NAME {
                return Err(syn::Error::new(
                    ident.span(),
                    format!("Expected `{CONFIG_FIELD_NAME}` field in declaration"),
                ));
            }
        } else {
            return Err(syn::Error::new(ident.span(), "Expected field name identifier."));
        };

        self.parse_colon()?;

        let tt = {
            let group = self.input_iter.next().ok_or(syn::Error::new(
                self.input.span(),
                format!("Expected `{CONFIG_FIELD_NAME}` declarations"),
            ))?;
            if let TokenTree::Group(group) = group {
                if group.delimiter() != Delimiter::Brace {
                    return Err(syn::Error::new(group.span(), "Expected brace"));
                }
                group.stream()
            } else {
                return Err(syn::Error::new(
                    group.span(),
                    format!("Expected `{CONFIG_FIELD_NAME}` declarations"),
                ));
            }
        };

        let mut parser = Parser::new(tt);
        let mut config = Config::default();
        parser.parse_config(&mut config)?;
        self.try_parse_comma()?;

        Ok(config)
    }

    pub fn parse_config(&mut self, config: &mut Config) -> syn::Result<Option<()>> {
        let mut seen = HashSet::new();
        while self.peek_next().is_some() {
            let Some(_) = self.input_iter.peek() else {
                return Ok(None);
            };
            let ident = self.input_iter.next().unwrap();
            let name = if let TokenTree::Ident(ident) = ident {
                ident
            } else {
                return Err(syn::Error::new(ident.span(), "Expected identifier."));
            };

            let name_string = name.to_string();
            if !seen.insert(name_string.clone()) {
                return Err(syn::Error::new(
                    name.span(),
                    format!("Duplicate `{name_string}` config"),
                ));
            }

            self.parse_colon()?;

            match name_string.as_str() {
                "page_size" => {
                    let value = self.input_iter.next().ok_or(syn::Error::new(
                        self.input.span(),
                        "Expected page size value in declaration",
                    ))?;
                    let value = if let TokenTree::Literal(value) = value {
                        value
                    } else {
                        return Err(syn::Error::new(value.span(), "Expected identifier."));
                    };

                    self.try_parse_comma()?;

                    let value = value.to_string();
                    let value = value.replace("_", "");

                    config.page_size = Some(u32::from_str(value.as_str()).unwrap())
                }
                "columnar_slot_id" => {
                    let value = self.input_iter.next().ok_or(syn::Error::new(
                        self.input.span(),
                        "Expected ColumnSlotId8, ColumnSlotId16, ColumnSlotId32, or ColumnSlotId64",
                    ))?;
                    let TokenTree::Ident(value) = value else {
                        return Err(syn::Error::new(value.span(), "Expected a column slot ID type."));
                    };
                    config.columnar_slot_id = match value.to_string().as_str() {
                        "ColumnSlotId8" => ColumnSlotIdType::U8,
                        "ColumnSlotId16" => ColumnSlotIdType::U16,
                        "ColumnSlotId32" => ColumnSlotIdType::U32,
                        "ColumnSlotId64" => ColumnSlotIdType::U64,
                        _ => {
                            return Err(syn::Error::new(
                                value.span(),
                                "Expected ColumnSlotId8, ColumnSlotId16, ColumnSlotId32, or ColumnSlotId64",
                            ));
                        }
                    };
                    self.try_parse_comma()?;
                }
                "columnar_chunk_rows" => {
                    let value = self.input_iter.next().ok_or(syn::Error::new(
                        self.input.span(),
                        "Expected a positive columnar chunk row count",
                    ))?;
                    let TokenTree::Literal(value) = value else {
                        return Err(syn::Error::new(value.span(), "Expected an integer."));
                    };
                    let parsed = value
                        .to_string()
                        .replace('_', "")
                        .parse::<usize>()
                        .map_err(|_| syn::Error::new(value.span(), "Invalid columnar chunk row count"))?;
                    if parsed == 0 {
                        return Err(syn::Error::new(
                            value.span(),
                            "columnar_chunk_rows must be greater than zero",
                        ));
                    }
                    config.columnar_chunk_rows = parsed;
                    self.try_parse_comma()?;
                }
                "row_derives" => {
                    const CONFIG_VARIANTS: [&str; 4] =
                        ["page_size", "row_derives", "columnar_slot_id", "columnar_chunk_rows"];

                    let mut derives = vec![];

                    while let Some(ident) = self.peek_next() {
                        if CONFIG_VARIANTS.contains(&ident.to_string().as_str()) {
                            if derives.is_empty() {
                                return Err(syn::Error::new(
                                    ident.span(),
                                    "Expected at least one derive in declaration.",
                                ));
                            }
                            break;
                        }

                        let derive = self.input_iter.next().ok_or(syn::Error::new(
                            self.input.span(),
                            "Expected at least one derive in declaration",
                        ))?;
                        let derive = if let TokenTree::Ident(derive) = derive {
                            derive
                        } else {
                            return Err(syn::Error::new(derive.span(), "Expected identifier."));
                        };

                        self.try_parse_comma()?;

                        derives.push(derive)
                    }

                    config.row_derives = derives;
                }
                _ => return Err(syn::Error::new(name.span(), "Unexpected identifier")),
            }
        }

        Ok(Some(()))
    }
}
