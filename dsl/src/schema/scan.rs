//! Finding the declarations in Rust source.
//!
//! A designer opening a project, a documentation generator, and a migration
//! tool comparing a checkout against a running database all start from the same
//! problem: a schema is written inside a `worktable!` invocation somewhere in a
//! crate, and there is no index of where.
//!
//! This walks tokens rather than `syn`'s item tree. An invocation inside a
//! function body is not an item, and real code puts them there, so an item walk
//! would quietly miss a table and the caller would never know a table existed to
//! be missed.
//!
//! # What gets set aside
//!
//! Some invocations are not declarations. A `macro_rules!` body writing
//! `name: $name, ... using $backend` is a template: the metavariables stand for
//! text that only exists once the outer macro expands, and no parser for this
//! grammar can accept them. Those are counted rather than reported as errors,
//! because they are not mistakes.
//!
//! Everything else that fails to parse *is* reported, with the text that failed,
//! because a designer that silently drops a table the compiler accepts is worse
//! than one that says it could not read it.

use proc_macro2::{Delimiter, TokenStream, TokenTree};

use super::Schema;

/// What was found in one piece of source.
#[derive(Debug, Default)]
pub struct Declarations {
    /// The schemas, in the order they appear.
    pub schemas: Vec<Schema>,
    /// Invocations inside a `macro_rules!` body, which are templates rather
    /// than declarations. The text of each, for a caller that wants to say so.
    pub templates: Vec<String>,
    /// Invocations that did not parse: the text, and why.
    pub rejected: Vec<(String, syn::Error)>,
}

impl Declarations {
    /// Whether every invocation found was read as a schema.
    pub fn is_complete(&self) -> bool {
        self.rejected.is_empty()
    }

    /// How many invocations were found, read or not.
    pub fn found(&self) -> usize {
        self.schemas.len() + self.templates.len() + self.rejected.len()
    }
}

/// Read every `worktable!` declaration in a Rust source file.
///
/// The error case is the file not tokenising at all, which is a broken file
/// rather than a broken declaration. A declaration that does not parse lands in
/// [`Declarations::rejected`] and does not stop the rest.
pub fn declarations_in_source(source: &str) -> syn::Result<Declarations> {
    let tokens: TokenStream = syn::parse_str(source)?;
    Ok(declarations_in_tokens(tokens))
}

/// Read every `worktable!` declaration in a token stream.
pub fn declarations_in_tokens(tokens: TokenStream) -> Declarations {
    let mut bodies = Vec::new();
    collect(tokens, &mut bodies);

    let mut found = Declarations::default();
    for body in bodies {
        let text = body.to_string();
        if is_macro_template(&body) {
            found.templates.push(text);
            continue;
        }
        match Schema::from_tokens(body) {
            Ok(schema) => found.schemas.push(schema),
            Err(error) => found.rejected.push((text, error)),
        }
    }
    found
}

/// Both delimiter forms appear in real code: `worktable! { .. }` and
/// `worktable!( .. )`. Either is accepted, and so is `[ .. ]`, because rustc
/// accepts it and a reader that did not would be wrong about the language.
fn collect(tokens: TokenStream, found: &mut Vec<TokenStream>) {
    let trees: Vec<TokenTree> = tokens.into_iter().collect();
    let mut index = 0;
    while index < trees.len() {
        if let TokenTree::Ident(ident) = &trees[index]
            && ident == "worktable"
            && let Some(TokenTree::Punct(bang)) = trees.get(index + 1)
            && bang.as_char() == '!'
            && let Some(TokenTree::Group(body)) = trees.get(index + 2)
            && body.delimiter() != Delimiter::None
        {
            found.push(body.stream());
            index += 3;
            continue;
        }
        if let TokenTree::Group(group) = &trees[index] {
            collect(group.stream(), found);
        }
        index += 1;
    }
}

fn is_macro_template(tokens: &TokenStream) -> bool {
    tokens.clone().into_iter().any(|tree| match tree {
        TokenTree::Punct(punct) => punct.as_char() == '$',
        TokenTree::Group(group) => is_macro_template(&group.stream()),
        _ => false,
    })
}
