use std::collections::HashSet;

use indexmap::IndexMap;
use proc_macro2::{Delimiter, Ident, TokenTree};
use syn::spanned::Spanned as _;

use crate::common::Parser;
use crate::common::model::{ColumnCompression, ColumnarFieldConfig, ColumnarIndex};

impl Parser {
    pub(super) fn try_parse_columnar_field(&mut self) -> syn::Result<Option<ColumnarFieldConfig>> {
        let Some(TokenTree::Ident(attribute)) = self.input_iter.peek() else {
            return Ok(None);
        };
        if attribute != "columnar" {
            return Ok(None);
        }

        let attribute_span = attribute.span();
        self.input_iter.next();
        let Some(TokenTree::Group(group)) = self.input_iter.next() else {
            return Err(syn::Error::new(
                attribute_span,
                "expected `columnar(...)` after the field type",
            ));
        };
        if group.delimiter() != Delimiter::Parenthesis {
            return Err(syn::Error::new(group.span(), "expected `columnar(...)`"));
        }

        let mut config = ColumnarFieldConfig::default();
        let mut saw_chunk_rows = false;
        let mut saw_compression = false;
        let mut parser = Parser::new(group.stream());

        while parser.has_next() {
            let option = parser
                .input_iter
                .next()
                .ok_or_else(|| syn::Error::new(attribute_span, "expected a columnar field option"))?;
            let TokenTree::Ident(option) = option else {
                return Err(syn::Error::new(option.span(), "expected a columnar option name"));
            };
            let value = parser
                .input_iter
                .next()
                .ok_or_else(|| syn::Error::new(option.span(), format!("expected `{option}(...)`")))?;
            let TokenTree::Group(value) = value else {
                return Err(syn::Error::new(value.span(), format!("expected `{option}(...)`")));
            };
            if value.delimiter() != Delimiter::Parenthesis {
                return Err(syn::Error::new(value.span(), format!("expected `{option}(...)`")));
            }

            match option.to_string().as_str() {
                "chunk_rows" => {
                    if saw_chunk_rows {
                        return Err(syn::Error::new(option.span(), "duplicate `chunk_rows` option"));
                    }
                    saw_chunk_rows = true;
                    let mut values = value.stream().into_iter();
                    let Some(TokenTree::Literal(rows)) = values.next() else {
                        return Err(syn::Error::new(value.span(), "`chunk_rows` expects an integer"));
                    };
                    if values.next().is_some() {
                        return Err(syn::Error::new(value.span(), "`chunk_rows` expects one integer"));
                    }
                    let parsed = rows
                        .to_string()
                        .replace('_', "")
                        .parse::<usize>()
                        .map_err(|_| syn::Error::new(rows.span(), "invalid `chunk_rows` integer"))?;
                    if parsed == 0 {
                        return Err(syn::Error::new(rows.span(), "`chunk_rows` must be greater than zero"));
                    }
                    config.chunk_rows = parsed;
                }
                "compression" => {
                    if saw_compression {
                        return Err(syn::Error::new(option.span(), "duplicate `compression` option"));
                    }
                    saw_compression = true;
                    let mut values = value.stream().into_iter();
                    let Some(TokenTree::Ident(compression)) = values.next() else {
                        return Err(syn::Error::new(value.span(), "`compression` expects a policy name"));
                    };
                    if values.next().is_some() {
                        return Err(syn::Error::new(value.span(), "`compression` expects one policy"));
                    }
                    config.compression = match compression.to_string().as_str() {
                        "none" => ColumnCompression::None,
                        "auto" => ColumnCompression::Auto,
                        "delta" => ColumnCompression::Delta,
                        "rle" => ColumnCompression::Rle,
                        "dictionary" => ColumnCompression::Dictionary,
                        _ => {
                            return Err(syn::Error::new(
                                compression.span(),
                                "unknown compression; expected `none`, `auto`, `delta`, `rle`, or `dictionary`",
                            ));
                        }
                    };
                }
                _ => {
                    return Err(syn::Error::new(
                        option.span(),
                        "unknown columnar option; expected `chunk_rows` or `compression`",
                    ));
                }
            }
            parser.try_parse_comma()?;
        }

        Ok(Some(config))
    }

    pub fn parse_columnar_indexes(&mut self) -> syn::Result<IndexMap<Ident, ColumnarIndex>> {
        let section = self
            .input_iter
            .next()
            .ok_or_else(|| syn::Error::new(self.input.span(), "expected `columnar_indexes` section"))?;
        let TokenTree::Ident(section) = section else {
            return Err(syn::Error::new(section.span(), "expected `columnar_indexes`"));
        };
        if section != "columnar_indexes" {
            return Err(syn::Error::new(section.span(), "expected `columnar_indexes`"));
        }
        self.parse_colon()?;

        let body = self
            .input_iter
            .next()
            .ok_or_else(|| syn::Error::new(section.span(), "expected `columnar_indexes: { ... }`"))?;
        let TokenTree::Group(body) = body else {
            return Err(syn::Error::new(body.span(), "expected `columnar_indexes: { ... }`"));
        };
        if body.delimiter() != Delimiter::Brace {
            return Err(syn::Error::new(body.span(), "expected braces around columnar indexes"));
        }

        let mut parser = Parser::new(body.stream());
        let mut indexes = IndexMap::new();
        while parser.has_next() {
            let name = parser
                .input_iter
                .next()
                .ok_or_else(|| syn::Error::new(body.span(), "expected a columnar index name"))?;
            let TokenTree::Ident(name) = name else {
                return Err(syn::Error::new(name.span(), "expected a columnar index name"));
            };
            parser.parse_colon()?;
            let definition = parser
                .input_iter
                .next()
                .ok_or_else(|| syn::Error::new(name.span(), "expected a columnar index definition"))?;
            let TokenTree::Group(definition) = definition else {
                return Err(syn::Error::new(definition.span(), "expected `{ ... }`"));
            };
            if definition.delimiter() != Delimiter::Brace {
                return Err(syn::Error::new(definition.span(), "expected `{ ... }`"));
            }

            let mut definition_parser = Parser::new(definition.stream());
            let mut columns = None;
            let mut cluster_by = None;
            while definition_parser.has_next() {
                let property = definition_parser
                    .input_iter
                    .next()
                    .ok_or_else(|| syn::Error::new(definition.span(), "expected a columnar index property"))?;
                let TokenTree::Ident(property) = property else {
                    return Err(syn::Error::new(property.span(), "expected `columns` or `cluster_by`"));
                };
                definition_parser.parse_colon()?;
                let values = parse_ident_list(&mut definition_parser, property.span())?;
                match property.to_string().as_str() {
                    "columns" if columns.is_none() => columns = Some(values),
                    "cluster_by" if cluster_by.is_none() => cluster_by = Some(values),
                    "columns" | "cluster_by" => {
                        return Err(syn::Error::new(property.span(), "duplicate columnar index property"));
                    }
                    _ => {
                        return Err(syn::Error::new(
                            property.span(),
                            "unknown columnar index property; expected `columns` or `cluster_by`",
                        ));
                    }
                }
                definition_parser.try_parse_comma()?;
            }

            let columns =
                columns.ok_or_else(|| syn::Error::new(name.span(), "columnar index requires `columns: [...]`"))?;
            if columns.is_empty() {
                return Err(syn::Error::new(name.span(), "columnar index `columns` cannot be empty"));
            }
            let cluster_by = cluster_by.unwrap_or_else(|| columns.clone());
            if cluster_by.is_empty() {
                return Err(syn::Error::new(
                    name.span(),
                    "columnar index `cluster_by` cannot be empty",
                ));
            }
            ensure_unique(&columns, "columnar index `columns` contains a duplicate")?;
            ensure_unique(&cluster_by, "columnar index `cluster_by` contains a duplicate")?;

            if indexes.contains_key(&name) {
                return Err(syn::Error::new(name.span(), "duplicate columnar index name"));
            }
            indexes.insert(
                name.clone(),
                ColumnarIndex {
                    name,
                    columns,
                    cluster_by,
                },
            );
            parser.try_parse_comma()?;
        }
        self.try_parse_comma()?;
        Ok(indexes)
    }
}

fn parse_ident_list(parser: &mut Parser, span: proc_macro2::Span) -> syn::Result<Vec<Ident>> {
    let list = parser
        .input_iter
        .next()
        .ok_or_else(|| syn::Error::new(span, "expected `[field, ...]`"))?;
    let TokenTree::Group(list) = list else {
        return Err(syn::Error::new(list.span(), "expected `[field, ...]`"));
    };
    if list.delimiter() != Delimiter::Bracket {
        return Err(syn::Error::new(list.span(), "expected `[field, ...]`"));
    }
    let mut values = Parser::new(list.stream());
    let mut result = Vec::new();
    while values.has_next() {
        let field = values
            .input_iter
            .next()
            .ok_or_else(|| syn::Error::new(list.span(), "expected a field identifier"))?;
        let TokenTree::Ident(field) = field else {
            return Err(syn::Error::new(field.span(), "expected a field identifier"));
        };
        result.push(field);
        values.try_parse_comma()?;
    }
    Ok(result)
}

fn ensure_unique(values: &[Ident], message: &str) -> syn::Result<()> {
    let mut seen = HashSet::new();
    for value in values {
        if !seen.insert(value.to_string()) {
            return Err(syn::Error::new(value.span(), message));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::common::Parser;
    use crate::common::model::{ColumnCompression, ColumnarFieldConfig};

    #[test]
    fn parses_columnar_field_options() {
        let mut parser = Parser::new(quote! {
            columnar(chunk_rows(65_536), compression(delta))
        });
        let config = parser.try_parse_columnar_field().unwrap().unwrap();
        assert_eq!(config.chunk_rows, 65_536);
        assert_eq!(config.compression, ColumnCompression::Delta);
    }

    #[test]
    fn empty_columnar_field_uses_defaults() {
        let mut parser = Parser::new(quote! { columnar() });
        let config = parser.try_parse_columnar_field().unwrap().unwrap();
        assert_eq!(config.chunk_rows, ColumnarFieldConfig::default().chunk_rows);
        assert_eq!(config.compression, ColumnCompression::Auto);
    }

    #[test]
    fn parses_columnar_indexes() {
        let mut parser = Parser::new(quote! {
            columnar_indexes: {
                host_time: {
                    columns: [host_id, timestamp],
                    cluster_by: [host_id, timestamp],
                },
            },
        });
        let indexes = parser.parse_columnar_indexes().unwrap();
        let index = indexes.values().next().unwrap();
        assert_eq!(
            index.columns.iter().map(ToString::to_string).collect::<Vec<_>>(),
            ["host_id", "timestamp"]
        );
        assert_eq!(
            index.cluster_by.iter().map(ToString::to_string).collect::<Vec<_>>(),
            ["host_id", "timestamp"]
        );
    }
}
