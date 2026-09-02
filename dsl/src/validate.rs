//! The rules a declaration must satisfy beyond being grammatical.
//!
//! These lived in `worktable_codegen`, next to the code they would have
//! generated. That is the right home for the *explanation* and the wrong home
//! for the *check*: a proc-macro crate exports nothing but macros, so the only
//! way to ask "would the macro accept this?" was to expand it. A designer
//! cannot expand a proc macro, and an editor that finds out by compiling is not
//! an editor.
//!
//! They are moved here unchanged, still operating on [`crate::model`] types,
//! which carry `proc_macro2` spans. `worktable_codegen` calls these functions
//! and its diagnostics are identical, down to the span each error points at.
//! Same rule the parser follows: one implementation, two callers, no second
//! copy to drift.
//!
//! [`all`] is the addition. The macro stops at the first error because it will
//! not generate code either way; an editor has the opposite economics, where
//! fix-recompile-find-the-next is the loop a live checker exists to remove.

use crate::model::{Columns, IndexBackend, Persistence};

/// data_bucket's on-disk layer seeks with its own hardcoded `PAGE_SIZE` of
/// 16384 bytes (`seek_to_page_start`, `seek_by_link`, `persist_page`), while
/// the generated table threads the user's `page_size` through its page-id and
/// length arithmetic. Any other value therefore reads and writes the wrong
/// file offsets as soon as the table persists, silently corrupting it.
/// In-memory tables never seek a file: for them `page_size` only sizes index
/// nodes and stays configurable.
const DATA_BUCKET_PAGE_SIZE: u32 = 16384;

pub fn validate_page_size(config: Option<&crate::model::Config>, persistence: Persistence) -> syn::Result<()> {
    let Some(config) = config else { return Ok(()) };
    let Some(page_size) = config.page_size else {
        return Ok(());
    };
    if persistence.is_persisted() && page_size != DATA_BUCKET_PAGE_SIZE {
        let span = config.page_size_span.unwrap_or_else(proc_macro2::Span::call_site);
        return Err(syn::Error::new(
            span,
            format!(
                "`page_size: {page_size}` cannot be combined with `persist: true`: the on-disk \
                 layer (data_bucket) hardcodes {DATA_BUCKET_PAGE_SIZE}-byte pages in every file \
                 seek, so a persisted table with any other page size reads and writes the wrong \
                 pages and corrupts its files. Remove `page_size` (or set it to \
                 {DATA_BUCKET_PAGE_SIZE}); custom page sizes remain available for in-memory \
                 tables, where they only size index nodes"
            ),
        ));
    }
    Ok(())
}

/// `in_place` queries hand the caller a mutable reference to the archived
/// column bytes and bypass all index maintenance, so a column that any index
/// is built over cannot be mutated in place: the index would keep resolving
/// the old value.
pub fn validate_in_place_queries(columns: &Columns, queries: &crate::model::Queries) -> syn::Result<()> {
    for (name, op) in &queries.in_place {
        for column in &op.columns {
            if columns.indexes.values().any(|index| &index.field == column) {
                return Err(syn::Error::new(
                    column.span(),
                    format!(
                        "in_place query `{name}` mutates column `{column}`, which is covered by an index; \
                         indexed columns cannot be updated in place because secondary indexes are not \
                         maintained on this path. Use an `update` query instead"
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Every backend rule. The `syn::Result` form below is what the macro calls.
///
/// The three checks here are independent, so a declaration with an unsupported
/// key type *and* a non-unique congee index has two things wrong with it, not
/// one thing and a surprise after the fix.
fn index_backends_into(columns: &Columns, persistence: Persistence, errors: &mut Vec<syn::Error>) {
    let explicit_backend = if columns.primary_index_backend.requires_explicit_persistence() {
        Some((
            columns.primary_index_backend,
            columns.primary_keys.first().expect("primary key exists"),
            true,
        ))
    } else {
        columns
            .indexes
            .values()
            .find(|index| index.backend.requires_explicit_persistence())
            .map(|index| (index.backend, &index.name, false))
    };

    if let Some((backend, ident, is_primary)) = explicit_backend {
        let kind = if is_primary { "primary index" } else { "index" };
        match persistence {
            Persistence::MemoryOnly => {}
            Persistence::Omitted => {
                errors.push(syn::Error::new(
                    ident.span(),
                    format!(
                        "{kind} `{ident}` uses `{}`, which requires an explicit `persist: true` or `persist: false`",
                        backend.name()
                    ),
                ));
            }
            Persistence::Persisted => {}
        }
    }

    for index in columns.indexes.values().filter(|index| !index.is_unique) {
        match index.backend {
            IndexBackend::WorktablesIndex | IndexBackend::Arctic => {}
            IndexBackend::Indexset | IndexBackend::Congee => {
                errors.push(syn::Error::new(
                    index.name.span(),
                    format!(
                        "non-unique index `{}` cannot use `{}`; non-unique indexes currently require \
                         `worktables_index` or `arctic`",
                        index.name,
                        index.backend.name()
                    ),
                ));
            }
        }
    }

    for (column, index) in &columns.indexes {
        let key_type = columns
            .columns_map
            .get(column)
            .expect("an index always references a validated column")
            .to_string();
        let supported = match index.backend {
            IndexBackend::Congee => Some(&["u8", "u16", "u32", "u64", "usize"][..]),
            IndexBackend::Arctic => Some(&["u16", "u32", "u64", "u128"][..]),
            IndexBackend::WorktablesIndex | IndexBackend::Indexset => None,
        };
        if let Some(supported) = supported
            && !supported.contains(&key_type.as_str())
        {
            errors.push(syn::Error::new(
                index.name.span(),
                format!(
                    "index `{}` uses `{}`, which does not support key type `{key_type}`; supported types: {}",
                    index.name,
                    index.backend.name(),
                    supported.join(", ")
                ),
            ));
        }
    }
}

/// The first backend rule that fails, which is all the macro can act on.
pub fn validate_index_backends(columns: &Columns, persistence: Persistence) -> syn::Result<()> {
    let mut errors = Vec::new();
    index_backends_into(columns, persistence, &mut errors);
    match errors.into_iter().next() {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Every rule, collecting all failures rather than stopping at the first.
///
/// Ordered backends, page size, then in-place queries: the order a reader
/// would work through them.
pub fn all(
    columns: &Columns,
    queries: Option<&crate::model::Queries>,
    config: Option<&crate::model::Config>,
    persistence: Persistence,
) -> Vec<syn::Error> {
    let mut errors = Vec::new();
    index_backends_into(columns, persistence, &mut errors);
    if let Err(error) = validate_page_size(config, persistence) {
        errors.push(error);
    }
    if let Some(queries) = queries
        && let Err(error) = validate_in_place_queries(columns, queries)
    {
        errors.push(error);
    }
    errors
}
