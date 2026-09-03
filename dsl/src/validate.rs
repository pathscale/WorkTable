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
    // Every offending index, in declaration order, not the first one found.
    //
    // This used to pick one: the primary if it qualified, and otherwise the
    // first secondary that did. A declaration with three such indexes reported
    // one, and if the primary qualified no secondary was examined at all. That
    // contradicts `all`'s contract of collecting every failure, and recreates
    // the fix-one-recompile-find-the-next loop the collecting form exists to
    // remove.
    let mut explicit_backends = Vec::new();
    if columns.primary_index_backend.requires_explicit_persistence() {
        // A schema with no primary key is malformed, but `check` runs these
        // rules over half-typed input where that is an ordinary state, so it
        // is skipped rather than unwrapped.
        if let Some(ident) = columns.primary_keys.first() {
            explicit_backends.push((columns.primary_index_backend, ident, true));
        }
    }
    for index in columns.indexes.values() {
        if index.backend.requires_explicit_persistence() {
            explicit_backends.push((index.backend, &index.name, false));
        }
    }

    if persistence == Persistence::Omitted {
        for (backend, ident, is_primary) in explicit_backends {
            let kind = if is_primary { "primary index" } else { "index" };
            errors.push(syn::Error::new(
                ident.span(),
                format!(
                    "{kind} `{ident}` uses `{}`, which requires an explicit `persist: true` or `persist: false`",
                    backend.name()
                ),
            ));
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

    // The primary key's own backend and generator.
    //
    // Both used to be skipped entirely: the loop below walks `columns.indexes`,
    // which holds the *secondary* indexes, and the primary backend was pushed
    // into `explicit_backends` above for the persistence rule only. So
    // `id: String primary_key using congee` and `id: usize primary_key
    // autoincrement` both passed `check` and then failed to build, which is the
    // exact failure this module exists to prevent: an editor shows nothing and
    // the compiler refuses.
    if let Some(primary) = columns.primary_keys.first() {
        let key_type = columns.columns_map.get(primary).map(ToString::to_string);
        if let Some(key_type) = key_type {
            if let Some(supported) = supported_key_types(columns.primary_index_backend)
                && !supported.contains(&key_type.as_str())
            {
                errors.push(syn::Error::new(
                    primary.span(),
                    // Word for word what the macro says when it reaches the
                    // same conclusion at expansion. `check` exists to answer
                    // "would the macro accept this", so a caller reading the
                    // two side by side should not have to work out that they
                    // are the same refusal. The alias note matters: a key
                    // declared through a type alias reads as unsupported here
                    // because neither this nor the macro can resolve it.
                    format!(
                        "`using {}` requires a directly named primitive primary-key type; found `{key_type}`; \
                         supported types: {} (type aliases cannot be resolved by the macro)",
                        columns.primary_index_backend.name(),
                        supported.join(", ")
                    ),
                ));
            }

            if columns.generator_type == crate::model::GeneratorType::Autoincrement
                && !AUTOINCREMENT_TYPES.contains(&key_type.as_str())
            {
                errors.push(syn::Error::new(
                    primary.span(),
                    format!(
                        "primary key `{primary}` is `autoincrement` over key type `{key_type}`, which cannot \
                         be generated; supported types: {}",
                        AUTOINCREMENT_TYPES.join(", ")
                    ),
                ));
            }
        }
    }

    for (column, index) in &columns.indexes {
        // An index over a column that does not exist. The macro never reaches
        // this: its own parse fails first, which is why this used to be an
        // `expect`. `check` does reach it, because it runs the same rules over
        // whatever a person has typed so far, and an index naming a column
        // that is not there yet is what half-finished input looks like. It
        // panicked, which for an editor calling `check` on every keystroke is
        // a crash rather than a squiggle.
        //
        // Reported rather than skipped: nothing else validates this, so
        // without it a schema with a dangling index was accepted in silence.
        let Some(key_type) = columns.columns_map.get(column) else {
            errors.push(syn::Error::new(
                index.name.span(),
                format!(
                    "index `{}` is declared over `{column}`, which is not a column",
                    index.name
                ),
            ));
            continue;
        };
        let key_type = key_type.to_string();
        let supported = supported_key_types(index.backend);
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

/// The key types `autoincrement` can generate.
///
/// One list, exported, because `worktable_codegen` maps exactly these to
/// atomics and errors on anything else. Two copies of this would drift, and
/// the way it would show is `check` accepting a declaration that then fails to
/// build, which is the whole failure this module exists to prevent.
///
/// `usize` is the case worth knowing: it reads like one of the accepted set
/// and there is no `AtomicUsize` in the mapping.
pub const AUTOINCREMENT_TYPES: &[&str] = &["u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64"];

/// The key types an index backend can hold.
///
/// `None` means the backend takes any key type.
pub fn supported_key_types(backend: IndexBackend) -> Option<&'static [&'static str]> {
    match backend {
        IndexBackend::Congee => Some(&["u8", "u16", "u32", "u64", "usize"]),
        IndexBackend::Arctic => Some(&["u16", "u32", "u64", "u128", "i16", "i32", "i64", "i128"]),
        IndexBackend::WorktablesIndex | IndexBackend::Indexset => None,
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
