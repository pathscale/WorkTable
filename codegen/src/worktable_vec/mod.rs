//! `worktable_vec!`: the same declaration, a `Vec` behind it.
//!
//! A separate macro rather than a mode of `worktable!`, for the reason
//! `worktable_version!` is: the choice changes the generated type, and
//! inferring it from the absence of other keys would mean two identical
//! declarations with different concurrency guarantees.
//!
//! The blocks it refuses are refused because they describe machinery this
//! table does not have, and a silent no-op would be worse than an error.

use proc_macro2::TokenStream;
use syn::Error;

use crate::common::Parser;
use crate::generators::vec_table;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let mut parser = Parser::new(input);
    let mut columns = None;
    let mut indexes = None;

    let name = parser.parse_name()?;

    while let Some(ident) = parser.peek_next() {
        match ident.to_string().as_str() {
            "columns" => columns = Some(parser.parse_columns()?),
            "indexes" => indexes = Some(parser.parse_indexes()?),
            "persist" => {
                return Err(Error::new(
                    ident.span(),
                    "worktable_vec! has no persistence. Use worktable! with `persist: true`.",
                ));
            }
            "queries" => {
                return Err(Error::new(
                    ident.span(),
                    "worktable_vec! does not generate queries yet; use the select and update \
                     methods directly",
                ));
            }
            "columnar_indexes" => {
                return Err(Error::new(
                    ident.span(),
                    "worktable_vec! does not support columnar fields: columnar storage is a \
                     paging feature and this table has no pages",
                ));
            }
            "config" => {
                return Err(Error::new(
                    ident.span(),
                    "worktable_vec! has no page size and no row derives to configure",
                ));
            }
            "runtime" => {
                return Err(Error::new(
                    ident.span(),
                    "worktable_vec! is synchronous and never reaches a runtime",
                ));
            }
            other => {
                return Err(Error::new(
                    ident.span(),
                    format!("Unexpected token `{other}`; expected one of `columns`, `indexes`"),
                ));
            }
        }
    }

    let mut columns = columns
        .ok_or_else(|| Error::new(name.span(), "Expected a `columns` block in declaration"))?;
    if let Some(i) = indexes {
        columns.indexes = i;
    }

    vec_table::expand(name, columns)
}

#[cfg(test)]
mod tests {
    use quote::quote;

    fn expand_text(input: proc_macro2::TokenStream) -> String {
        super::expand(input).expect("valid declaration").to_string()
    }

    /// The default is Arctic, the same default `worktable!` has.
    ///
    /// This is the regression. The first version of this generator hardcoded
    /// `BTreeMap`, accepted `using arctic` without honouring it, and so
    /// expanded two declarations that differ in a `using` clause into the same
    /// code. `worktable-vec` measures the two representations against each
    /// other and reports Arctic roughly six times faster on point lookups, so
    /// what was silently dropped was most of the reason to use the macro.
    #[test]
    fn the_default_backend_is_arctic() {
        let text = expand_text(quote! {
            name: Defaulted,
            columns: {
                id: u64 primary_key,
                value: u64,
            },
        });
        assert!(text.contains("by_pk : worktable :: prelude :: ArcticIndex < u64 , u64 >"), "got: {text}");
        // The field, not the whole expansion: `delete`'s doc comment names
        // `BTreeMap` to explain why `using indexset` exists, and a bare
        // `contains("BTreeMap")` matches that and fails for the wrong reason.
        assert!(
            !text.contains("by_pk : worktable :: prelude :: BTreeMap"),
            "the primary index should not be a BTreeMap: {text}"
        );
    }

    /// Each `using` clause reaches the emitted type.
    ///
    /// One assertion per backend rather than one for the set, so a failure
    /// names which one stopped being honoured.
    #[test]
    fn each_stated_backend_reaches_the_emitted_type() {
        for (clause, expected) in [
            (quote! { arctic }, "ArcticIndex < u64 , u64 >"),
            (quote! { worktables_index }, "IndexMap < u64 , u64 >"),
            (quote! { congee }, "CongeeIndex < u64 , u64 >"),
            (quote! { indexset }, "BTreeMap < u64 , usize >"),
        ] {
            let text = expand_text(quote! {
                name: Stated,
                columns: {
                    id: u64 primary_key using #clause,
                    value: u64,
                },
            });
            assert!(
                text.contains(expected),
                "`using {clause}` did not emit `{expected}`; got: {text}"
            );
        }
    }

    /// A non-unique index needs a multimap, and picks the one its backend has.
    #[test]
    fn a_non_unique_index_uses_the_matching_multimap() {
        let arctic = expand_text(quote! {
            name: Tagged,
            columns: {
                id: u64 primary_key,
                tag: u64,
            },
            indexes: {
                tag_idx: tag,
            },
        });
        assert!(arctic.contains("ArcticMultiIndex < u64 , u64 >"), "got: {arctic}");

        let ordered = expand_text(quote! {
            name: TaggedOrdered,
            columns: {
                id: u64 primary_key using indexset,
                tag: u64,
            },
            indexes: {
                tag_idx: tag using indexset,
            },
        });
        assert!(
            ordered.contains("tag_map : worktable :: prelude :: BTreeMap < u64 , worktable :: prelude :: Vec < usize >>"),
            "got: {ordered}"
        );
    }

    /// A key Arctic cannot hold is refused, and the refusal says what to do.
    ///
    /// Silently falling back to `BTreeMap` here would be the same defect in a
    /// politer form: the caller asked for the fast index and got the slow one
    /// without being told.
    ///
    /// `bool` and not `String`: Arctic's key list includes `String`, so a
    /// string-keyed table is fine here and picking it would have tested
    /// nothing.
    #[test]
    fn a_key_arctic_cannot_hold_is_refused_by_name() {
        let error = super::expand(quote! {
            name: Flagged,
            columns: {
                id: bool primary_key,
                value: u64,
            },
        })
        .expect_err("bool is not an Arctic key");
        let message = error.to_string();
        assert!(message.contains("worktables_index"), "must name the alternative: {message}");
        assert!(message.contains("bool"), "must name the type it refused: {message}");
    }

    /// A `String` key is not refused: Arctic takes one.
    #[test]
    fn a_string_key_stays_on_arctic() {
        let text = expand_text(quote! {
            name: Named,
            columns: {
                id: String primary_key,
                value: u64,
            },
        });
        assert!(
            text.contains("by_pk : worktable :: prelude :: ArcticIndex < String , u64 >"),
            "got: {text}"
        );
    }

    /// Congee is accepted, and carries the 64-bit guard its key packing needs.
    ///
    /// It was refused here for a while, on the grounds that `worktable!`
    /// demands an explicit `persist` before accepting it. That rule exists
    /// because congee behaves differently persisted and the author has to say
    /// which they meant; this macro has no persistence at all, so the question
    /// is already answered. Refusing on the rule's name rather than its reason
    /// cost the caller a backend for nothing.
    #[test]
    fn congee_is_accepted_with_its_width_guard() {
        let text = expand_text(quote! {
            name: Congeed,
            columns: {
                id: u64 primary_key using congee,
                value: u64,
            },
        });
        assert!(text.contains("by_pk : worktable :: prelude :: CongeeIndex < u64 , u64 >"), "got: {text}");
        assert!(
            text.contains("target_pointer_width") && text.contains("compile_error"),
            "a u64 congee key needs the 64-bit guard: {text}"
        );
    }

    /// A key congee cannot pack into a `usize` is refused by name.
    #[test]
    fn a_key_congee_cannot_pack_is_refused() {
        let error = super::expand(quote! {
            name: Signed,
            columns: {
                id: i64 primary_key using congee,
                value: u64,
            },
        })
        .expect_err("congee takes unsigned keys only");
        let message = error.to_string();
        assert!(message.contains("congee"), "must name the backend: {message}");
        assert!(message.contains("i64"), "must name the type it refused: {message}");
    }

    /// A non-unique congee index is refused: congee has no multimap.
    #[test]
    fn a_non_unique_congee_index_is_refused() {
        let error = super::expand(quote! {
            name: CongeeMulti,
            columns: {
                id: u64 primary_key,
                tag: u64,
            },
            indexes: {
                tag_idx: tag using congee,
            },
        })
        .expect_err("congee has no multimap");
        let message = error.to_string();
        assert!(message.contains("tag_idx"), "must name the declared index: {message}");
        assert!(message.contains("congee"), "must name the backend: {message}");
    }

    /// A non-unique WTI index is refused rather than quietly becoming something else.
    #[test]
    fn a_non_unique_wti_index_is_refused() {
        let error = super::expand(quote! {
            name: WtiMulti,
            columns: {
                id: u64 primary_key,
                tag: u64,
            },
            indexes: {
                tag_idx: tag using worktables_index,
            },
        })
        .expect_err("no WTI multimap path yet");
        let message = error.to_string();
        assert!(message.contains("tag_idx"), "must name the declared index: {message}");
        assert!(message.contains("unique"), "must say how to proceed: {message}");
    }
}
