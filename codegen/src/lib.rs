mod common;
mod generators;
mod mem_stat;
mod migration_engine;
mod persist_index;
mod persist_table;
#[cfg(feature = "s3-support")]
mod s3_persistence;
mod worktable;
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
