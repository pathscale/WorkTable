//! Reading a declaration the way an editor has to read one.
//!
//! [`crate::Schema::parse`] is the right entry point for a tool that has a
//! finished declaration and wants it as data. It is the wrong one for a live
//! editor, for two reasons this module exists to fix.
//!
//! **It reports one problem.** `syn::Result` carries a single error, because
//! the macro stops at the first one: it cannot generate code either way, so
//! finding the rest costs a compile it is not going to do. An editor has the
//! opposite economics. Fixing one error, recompiling, and finding the next is
//! the loop a live checker exists to remove, so [`check`] runs every rule and
//! returns all of them.
//!
//! **It reports no location.** A [`crate::Schema`] deliberately has no spans:
//! an `Ident` cannot be serialised, compared across processes, or sent to a
//! designer over a socket, which is the whole reason the IR is plain data. But
//! an editor that cannot underline the offending token is showing a message
//! about a file rather than about a place in it.
//!
//! [`Diagnostic`] resolves that by keeping the location *outside* the IR, as a
//! byte range into the source text that was parsed. The `Schema` stays plain
//! data; the ranges live next to it, in the result of the call that produced
//! it. A consumer that wants neither pays for neither.
//!
//! # The `spans` feature
//!
//! Byte ranges need `proc-macro2/span-locations`, and `worktable_codegen`
//! depends on this crate. A proc macro is compiled for the host before
//! anything else in a dependent's build, so anything added here is added to
//! every WorkTable user's first compile. Span tracking is therefore behind an
//! off-by-default `spans` feature, for the same reason `serde` is: a designer
//! turns both on, and the compiler pays for neither.
//!
//! Without the feature, [`Diagnostic::span`] is `None`. The messages are
//! identical either way, so a consumer degrades to file-level diagnostics
//! rather than losing them.

use crate::schema::Schema;

/// Where a diagnostic points, as a half-open byte range into the source that
/// was handed to [`check`].
///
/// Byte offsets rather than line and column on purpose: an editor converts to
/// whichever it needs, and a byte range survives being sent to one that
/// disagrees about what a column is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SourceSpan {
    /// First byte of the offending text.
    pub start: usize,
    /// One past the last byte.
    pub end: usize,
}

/// Why a declaration was rejected, and where.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Diagnostic {
    /// The message, identical to the one the macro would print.
    pub message: String,
    /// The offending text, when this crate was built with the `spans` feature
    /// and the error carried a span. `None` otherwise: absence of a location
    /// is never absence of a problem.
    pub span: Option<SourceSpan>,
    /// Whether the declaration was still readable despite this.
    pub stage: Stage,
}

/// Which half of reading a declaration produced a diagnostic.
///
/// The distinction is the one an editor acts on. A [`Stage::Grammar`] failure
/// means there is no schema to draw; a [`Stage::Rules`] failure means there is
/// one, it can be rendered, and the macro would refuse to expand it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Stage {
    /// The text is not a declaration. Nothing was produced.
    Grammar,
    /// The text is a declaration, and it breaks a rule the macro enforces.
    Rules,
}

/// A declaration, everything wrong with it, or both.
#[derive(Debug, Clone, PartialEq)]
pub struct Checked {
    /// The declaration, when the grammar accepted it. Present even when
    /// `diagnostics` is not empty: a schema that breaks a rule is still a
    /// schema, and an editor has to draw it in order to let anyone fix it.
    pub schema: Option<Schema>,
    /// Everything wrong, in the order a reader would work through it.
    pub diagnostics: Vec<Diagnostic>,
}

impl Checked {
    /// Whether the macro would accept this declaration.
    ///
    /// The question `worktable_dsl` could not answer before this module, and
    /// the one a designer has to answer on every keystroke to say whether the
    /// thing on screen would compile.
    pub fn is_acceptable(&self) -> bool {
        self.schema.is_some() && self.diagnostics.is_empty()
    }
}

#[cfg(feature = "spans")]
fn span_of(error: &syn::Error) -> Option<SourceSpan> {
    let range = error.span().byte_range();
    // A synthesised span (`Span::call_site` in a non-macro context) reports an
    // empty range at zero, which would point an editor at the first character
    // for an error that is not there. Reporting no location beats a wrong one.
    if range.is_empty() {
        None
    } else {
        Some(SourceSpan {
            start: range.start,
            end: range.end,
        })
    }
}

#[cfg(not(feature = "spans"))]
fn span_of(_error: &syn::Error) -> Option<SourceSpan> {
    None
}

/// Read a declaration, and report everything wrong with it.
///
/// The input is the macro body, `name: Foo, columns: { .. }`, without the
/// macro name or the surrounding braces, exactly as [`Schema::parse`] takes it.
///
/// ```
/// use worktable_dsl::check;
///
/// // A declaration the macro would refuse: `congee` cannot index a `String`.
/// let checked = check(
///     "name: Bad,
///      persist: false,
///      columns: { id: u64 primary_key, label: String },
///      indexes: { label_idx: label unique using congee }",
/// );
///
/// // Readable, and drawable, even though it would not compile.
/// assert!(checked.schema.is_some());
/// assert!(!checked.is_acceptable());
/// assert!(checked.diagnostics[0].message.contains("does not support key type"));
/// ```
pub fn check(source: &str) -> Checked {
    let tokens: proc_macro2::TokenStream = match syn::parse_str(source) {
        Ok(tokens) => tokens,
        Err(error) => {
            // Tokenisation failed, which in practice means an unbalanced
            // delimiter: the state a declaration is in for most of the time
            // somebody is typing one. There is nothing to parse and nothing to
            // draw, and saying so is more use than a partial tree that claims
            // the missing half does not exist.
            return Checked {
                schema: None,
                diagnostics: vec![Diagnostic {
                    message: error.to_string(),
                    span: span_of(&error),
                    stage: Stage::Grammar,
                }],
            };
        }
    };

    let schema = match Schema::from_tokens(tokens.clone()) {
        Ok(schema) => schema,
        Err(error) => {
            return Checked {
                schema: None,
                diagnostics: vec![Diagnostic {
                    message: error.to_string(),
                    span: span_of(&error),
                    stage: Stage::Grammar,
                }],
            };
        }
    };

    // The rules run against the model rather than the IR, because that is
    // where the spans are and because it is the same code the macro runs. A
    // second parse is cheap next to a compile, and it is what keeps this
    // answering "would the macro accept this?" rather than "would a
    // reimplementation of the macro accept this?".
    let diagnostics = match model_of(tokens) {
        Ok((columns, queries, config, persistence)) => {
            crate::validate::all(&columns, queries.as_ref(), config.as_ref(), persistence)
                .iter()
                .map(|error| Diagnostic {
                    message: error.to_string(),
                    span: span_of(error),
                    stage: Stage::Rules,
                })
                .collect()
        }
        // Unreachable in practice: the same tokens parsed a moment ago. If the
        // two dispatches ever disagree, report it rather than panicking in an
        // editor's keystroke handler.
        Err(error) => vec![Diagnostic {
            message: error.to_string(),
            span: span_of(&error),
            stage: Stage::Grammar,
        }],
    };

    Checked {
        schema: Some(schema),
        diagnostics,
    }
}

type Model = (
    crate::model::Columns,
    Option<crate::model::Queries>,
    Option<crate::model::Config>,
    crate::model::Persistence,
);

/// The macro's own top-level dispatch, kept to the parts the rules read.
fn model_of(tokens: proc_macro2::TokenStream) -> syn::Result<Model> {
    let mut parser = crate::Parser::new(tokens);
    parser.parse_name()?;
    parser.parse_version()?;
    let persistence = parser.parse_persist()?;
    parser.parse_partition_by()?;

    let mut columns = None;
    let mut indexes = None;
    let mut queries = None;
    let mut config = None;
    while let Some(ident) = parser.peek_next() {
        match ident.to_string().as_str() {
            "columns" => columns = Some(parser.parse_columns()?),
            "indexes" => indexes = Some(parser.parse_indexes()?),
            "queries" => queries = Some(parser.parse_queries()?),
            "config" => config = Some(parser.parse_configs()?),
            other => {
                return Err(syn::Error::new(ident.span(), format!("Unexpected token `{other}`")));
            }
        }
    }

    let mut columns = columns.ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "Expected a `columns` block in declaration",
        )
    })?;
    if let Some(indexes) = indexes {
        columns.indexes = indexes;
    }
    Ok((columns, queries, config, persistence))
}
