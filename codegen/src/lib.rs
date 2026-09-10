// `common` is now a thin front for `worktable_dsl`, which holds the schema
// model and parser so that anything other than this macro can read a
// declaration. Kept as a module rather than an alias because the name
// generator stays here: generators define inherent `impl`s on it, which the
// orphan rule allows only in the crate that owns the type.
//
// The 127 `crate::common::` paths across this crate are unchanged, so the diff
// is a move rather than a sweep.
mod common;
mod generators;
mod mem_stat;
mod migration_engine;
mod persist_index;
mod persist_table;
mod runtimes;
#[cfg(feature = "s3-support")]
mod s3_persistence;
mod worktable;
mod worktable_vec;
mod worktable_version;

use proc_macro::TokenStream;
// TODO: Refactor this codegen stuff because it's now too strange.

#[proc_macro]
pub fn worktable(input: TokenStream) -> TokenStream {
    worktable::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[cfg(feature = "s3-support")]
#[proc_macro]
pub fn s3_sync_persistence(input: TokenStream) -> TokenStream {
    s3_persistence::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Declares the process's named runtime profiles.
///
/// ```ignore
/// runtimes! {
///     tokio_max:  tokio,
///     fast_local: nagoya(locality),
///     wide:       nagoya(spread),
/// }
/// ```
///
/// One unit struct per entry, implementing `worktable::prelude::Profile`. Every
/// pool the process will ever create can be enumerated by reading one of these
/// blocks, which is the reason profiles are named rather than spelled out at
/// call sites.
#[proc_macro]
pub fn runtimes(input: TokenStream) -> TokenStream {
    runtimes::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[proc_macro_derive(PersistIndex, attributes(index))]
pub fn persist_index(input: TokenStream) -> TokenStream {
    persist_index::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[proc_macro_derive(PersistTable, attributes(table))]
pub fn persist_table(input: TokenStream) -> TokenStream {
    persist_table::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[proc_macro_derive(MemStat)]
pub fn mem_stat(input: TokenStream) -> TokenStream {
    mem_stat::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// The same declaration, backed by a `Vec` instead of pages.
///
/// See `generators::vec_table` for what it drops and why.
#[proc_macro]
pub fn worktable_vec(input: TokenStream) -> TokenStream {
    worktable_vec::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[proc_macro]
pub fn worktable_version(input: TokenStream) -> TokenStream {
    worktable_version::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[proc_macro]
pub fn migration_engine(input: TokenStream) -> TokenStream {
    migration_engine::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
