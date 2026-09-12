use proc_macro2::TokenTree;
use syn::spanned::Spanned as _;

use crate::model::{
    PARTITION_KEY_TYPES, PARTITION_MAX_SIZE_TYPES, PartitionKey, PartitionMaxSize, Persistence, Storage,
};
use crate::parser::Parser;

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

impl Parser {
    /// Parse an optional `partition_by: <name>: <uint type>,` declaration.
    ///
    /// Positional, like `version` and `persist`, and for the same reason: it
    /// changes the shape of what is generated rather than describing a part
    /// of the table, so it belongs before the blocks.
    pub fn parse_partition_by(&mut self) -> syn::Result<Option<PartitionKey>> {
        let Some(ident) = self.input_iter.peek().cloned() else {
            return Ok(None);
        };
        let TokenTree::Ident(ident) = ident else {
            return Err(syn::Error::new(ident.span(), "Expected field name identifier."));
        };
        if ident.to_string().as_str() != "partition_by" {
            return Ok(None);
        }
        let _ = self.input_iter.next();
        self.parse_colon()?;

        let name = self
            .input_iter
            .next()
            .ok_or_else(|| syn::Error::new(self.input.span(), "Expected a partition key name."))?;
        let TokenTree::Ident(name) = name else {
            return Err(syn::Error::new(name.span(), "Expected a partition key name."));
        };

        self.parse_colon()?;

        let ty = self
            .input_iter
            .next()
            .ok_or_else(|| syn::Error::new(self.input.span(), "Expected a partition key type."))?;
        let TokenTree::Ident(ty) = ty else {
            return Err(syn::Error::new(ty.span(), "Expected a partition key type."));
        };
        if !PARTITION_KEY_TYPES.contains(&ty.to_string().as_str()) {
            return Err(syn::Error::new(
                ty.span(),
                format!(
                    "`{ty}` is not a partition key type; routing is an array index, so the key must be one of {}. \
                     Names belong in a separate registry table looked up once, not in the routing key",
                    PARTITION_KEY_TYPES.join(", ")
                ),
            ));
        }

        self.try_parse_comma()?;

        let max_size = self.parse_partition_max_size(&ident)?;

        self.try_parse_comma()?;
        Ok(Some(PartitionKey { name, ty, max_size }))
    }

    /// Parse the `partition_max_size: <width>,` that must follow `partition_by`.
    ///
    /// Required rather than defaulted. A default would pick one of the two
    /// shapes for the author and generate the other one silently, which is the
    /// implicitness this key exists to remove. `partition_by` is the span the
    /// error points at, because that is the key whose presence made this one
    /// mandatory.
    fn parse_partition_max_size(&mut self, partition_by: &proc_macro2::Ident) -> syn::Result<PartitionMaxSize> {
        let missing = || {
            syn::Error::new(
                partition_by.span(),
                format!(
                    "`partition_by` requires `partition_max_size: <width>,` after it, where <width> is one of {}. \
                     It is how many rows one partition holds, written as an index width: `u8` is 256 rows and \
                     generates a dense partition, `u64` is the escape and generates a full table per partition",
                    PARTITION_MAX_SIZE_TYPES.join(", ")
                ),
            )
        };

        let Some(TokenTree::Ident(ident)) = self.input_iter.peek().cloned() else {
            return Err(missing());
        };
        if ident.to_string().as_str() != "partition_max_size" {
            return Err(missing());
        }
        let _ = self.input_iter.next();
        self.parse_colon()?;

        let width = self
            .input_iter
            .next()
            .ok_or_else(|| syn::Error::new(self.input.span(), "Expected a `partition_max_size` width."))?;
        let TokenTree::Ident(width) = width else {
            return Err(syn::Error::new(width.span(), "Expected a `partition_max_size` width."));
        };
        PartitionMaxSize::from_type_name(width.to_string().as_str()).ok_or_else(|| {
            syn::Error::new(
                width.span(),
                format!(
                    "`{width}` is not a `partition_max_size` width; it is an index width, so it must be one of {}. \
                     A row count is not accepted: a count is not a power of two and duplicates a constant that \
                     lives in the caller's code and will drift",
                    PARTITION_MAX_SIZE_TYPES.join(", ")
                ),
            )
        })
    }
}

impl Parser {
    /// Parse an optional `vec: true,` declaration.
    ///
    /// Positional, like `version` and `persist`, and for the strongest form of
    /// their reason: this does not describe part of the table, it decides
    /// which table is generated. A paged table is concurrent, durable and
    /// async; a `Vec` table is single-writer and synchronous. Reading it after
    /// the blocks would mean reading three screens of columns before learning
    /// what they are columns of.
    ///
    /// # A boolean in the grammar, an enum in the model
    ///
    /// The author writes a flag, which is the shape `persist:` already has and
    /// needs no new noun explained. What comes out is a [`Storage`], because
    /// everything downstream of here crosses a boundary where two flags could
    /// disagree: the canonical schema is serialized, round-tripped through
    /// `to_dsl`, and handed to a TypeScript emitter, and serde will not
    /// enforce a cross-field invariant for anybody. One enum cannot say two
    /// things, so the illegal combination stops existing after this function
    /// rather than being re-checked by each consumer.
    ///
    /// This briefly read `storage: vec`. The key was invented while drafting an
    /// options menu rather than chosen, and a flag turned out to be the better
    /// surface once the invariant could be kept without it.
    pub fn parse_storage(&mut self) -> syn::Result<Storage> {
        let Some(ident) = self.input_iter.peek().cloned() else {
            return Ok(Storage::Paged);
        };
        let TokenTree::Ident(ident) = ident else {
            return Err(syn::Error::new(ident.span(), "Expected field name identifier."));
        };
        if ident.to_string().as_str() != "vec" {
            return Ok(Storage::Paged);
        }
        let _ = self.input_iter.next();
        self.parse_colon()?;
        let value = self
            .input_iter
            .next()
            .ok_or_else(|| syn::Error::new(self.input.span(), "Expected `true` or `false`."))?;
        let TokenTree::Ident(value) = value else {
            return Err(syn::Error::new(value.span(), "Expected `true` or `false`."));
        };
        let storage = match value.to_string().as_str() {
            "true" => Storage::Vec,
            "false" => Storage::Paged,
            other => {
                return Err(syn::Error::new(
                    value.span(),
                    format!("expected `true` or `false`, found `{other}`"),
                ));
            }
        };
        self.try_parse_comma()?;
        Ok(storage)
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::Parser;
    use crate::model::{PARTITION_KEY_TYPES, PARTITION_MAX_SIZE_TYPES, PartitionKey, PartitionMaxSize, Persistence};

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

    // `partition_by`. These were missing entirely: the grammar shipped with
    // its imports in this module and nothing using them, which is what the
    // unused-import lint was telling us.

    #[test]
    fn partition_by_is_optional() {
        let mut parser = Parser::new(quote! {});
        assert!(parser.parse_partition_by().unwrap().is_none());

        // A different attribute must be left alone for its own parser, not
        // consumed or rejected.
        let mut parser = Parser::new(quote! { persist: false, });
        assert!(parser.parse_partition_by().unwrap().is_none());
        assert_eq!(parser.parse_persist().unwrap(), Persistence::MemoryOnly);
    }

    #[test]
    fn partition_by_accepts_every_unsigned_key_type() {
        for ty in PARTITION_KEY_TYPES {
            let ty_ident = syn::Ident::new(ty, proc_macro2::Span::call_site());
            let mut parser = Parser::new(quote! { partition_by: symbol_id: #ty_ident, partition_max_size: u16, });
            let key: PartitionKey = parser
                .parse_partition_by()
                .unwrap_or_else(|e| panic!("`{ty}` must be accepted: {e}"))
                .unwrap_or_else(|| panic!("`{ty}` parsed as absent"));
            assert_eq!(key.name.to_string(), "symbol_id");
            assert_eq!(key.ty.to_string(), ty);
            assert_eq!(key.max_size, PartitionMaxSize::U16);
        }
    }

    #[test]
    fn partition_by_rejects_a_key_that_is_not_an_array_index() {
        // The routing key indexes into a spine, so a `String` key would have
        // to be hashed on every lookup. The refusal has to name the types and
        // point at the registry table, because that is the whole workaround.
        for ty in ["String", "i32", "f64", "bool", "Uuid"] {
            let ty_ident = syn::Ident::new(ty, proc_macro2::Span::call_site());
            let mut parser = Parser::new(quote! { partition_by: symbol: #ty_ident, });
            let error = parser
                .parse_partition_by()
                .expect_err(&format!("`{ty}` must be refused"))
                .to_string();
            assert!(error.contains(ty), "the offending type must be named: {error}");
            assert!(
                error.contains("u16") && error.contains("usize"),
                "the accepted types must be listed: {error}"
            );
            assert!(
                error.contains("registry table"),
                "the refusal must point at the workaround: {error}"
            );
        }
    }

    #[test]
    fn partition_by_reports_what_is_missing() {
        // Truncated declarations must name the missing piece rather than
        // panicking or silently parsing as absent.
        let mut parser = Parser::new(quote! { partition_by: });
        assert!(parser.parse_partition_by().is_err(), "a bare key must be an error");

        let mut parser = Parser::new(quote! { partition_by: symbol_id });
        assert!(
            parser.parse_partition_by().is_err(),
            "a name with no type must be an error"
        );

        let mut parser = Parser::new(quote! { partition_by: 7: u16, });
        let error = parser
            .parse_partition_by()
            .expect_err("a literal is not a name")
            .to_string();
        assert!(error.contains("name"), "unexpected reason: {error}");
    }

    #[test]
    fn partition_max_size_is_required_beside_partition_by() {
        // The whole point of the key: a partitioned declaration that does not
        // say how big a partition gets is refused rather than defaulted, so
        // the two shapes can never look identical.
        let mut parser = Parser::new(quote! { partition_by: symbol_id: u32, });
        let error = parser
            .parse_partition_by()
            .expect_err("a partitioned table must declare its partition size")
            .to_string();
        assert!(
            error.contains("partition_max_size"),
            "the refusal must name the missing key: {error}"
        );
        for width in PARTITION_MAX_SIZE_TYPES {
            assert!(error.contains(width), "`{width}` must be offered: {error}");
        }
    }

    #[test]
    fn partition_max_size_accepts_every_width_and_maps_it_to_a_row_count() {
        // The counts are the contract, not an implementation detail: they are
        // what a reader is being told by writing the width.
        for (width, rows) in [
            ("bool", Some(2u64)),
            ("u8", Some(256)),
            ("u16", Some(65_536)),
            ("u32", None),
            ("u64", None),
        ] {
            let width_ident = syn::Ident::new(width, proc_macro2::Span::call_site());
            let mut parser = Parser::new(quote! { partition_by: symbol_id: u32, partition_max_size: #width_ident, });
            let key = parser
                .parse_partition_by()
                .unwrap_or_else(|e| panic!("`{width}` must be accepted: {e}"))
                .expect("declared");
            assert_eq!(key.max_size.type_name(), width);
            assert_eq!(key.max_size.rows(), rows, "`{width}` row count");
            assert_eq!(key.max_size.is_dense(), rows.is_some(), "`{width}` density");
        }
    }

    #[test]
    fn partition_max_size_rejects_a_row_count() {
        // A literal is the tempting spelling and it is refused, because a count
        // is not an index width, is not a power of two, and duplicates a
        // constant that lives in the caller's code and will drift.
        let mut parser = Parser::new(quote! { partition_by: symbol_id: u32, partition_max_size: 256, });
        let error = parser
            .parse_partition_by()
            .expect_err("a literal is not a width")
            .to_string();
        assert!(
            error.contains("partition_max_size"),
            "the refusal must name the key: {error}"
        );
    }

    #[test]
    fn partition_max_size_rejects_a_width_it_does_not_have() {
        let mut parser = Parser::new(quote! { partition_by: symbol_id: u32, partition_max_size: u128, });
        let error = parser
            .parse_partition_by()
            .expect_err("u128 is not a width we generate")
            .to_string();
        assert!(error.contains("u128"), "the offending width must be named: {error}");
        assert!(error.contains("u16"), "the accepted widths must be listed: {error}");
    }

    #[test]
    fn partition_by_leaves_the_following_attribute_parseable() {
        // It is positional, so what follows has to still parse.
        let mut parser = Parser::new(quote! { partition_by: venue: u32, partition_max_size: u8, persist: false, });
        let key = parser.parse_partition_by().unwrap().expect("declared");
        assert_eq!(key.ty.to_string(), "u32");
        assert_eq!(parser.parse_persist().unwrap(), Persistence::MemoryOnly);
    }
}
