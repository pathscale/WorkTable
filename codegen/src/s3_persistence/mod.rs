use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, Result};

use crate::common::name_generator::WorktableNameGenerator;

struct S3PersistenceInput {
    table_name: Ident,
}

impl Parse for S3PersistenceInput {
    fn parse(input: ParseStream) -> Result<Self> {
        let table_name: Ident = input.parse()?;
        Ok(S3PersistenceInput { table_name })
    }
}

pub fn expand(input: TokenStream) -> Result<TokenStream> {
    let input: S3PersistenceInput = syn::parse2(input)?;

    let name_str = input.table_name.to_string();
    let base_name = name_str.strip_suffix("WorkTable").unwrap_or(&name_str).to_string();

    let generator = WorktableNameGenerator::from_table_name(base_name);

    let output_ident = generator.get_s3_sync_persistence_engine_ident();
    let primary_key = generator.get_primary_key_type_ident();
    let space_secondary_index = generator.get_space_secondary_index_ident();
    let space_secondary_index_events = generator.get_space_secondary_index_events_ident();
    let available_indexes = generator.get_available_indexes_ident();
    let inner_size_const = generator.get_page_inner_size_const_ident();
    let page_size_const = generator.get_page_size_const_ident();

    Ok(quote! {
        pub type #output_ident = S3SyncDiskPersistenceEngine<
            SpaceData<
                <<#primary_key as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
                { #inner_size_const },
                { #page_size_const as u32 },
            >,
            SpaceIndex<#primary_key, { #inner_size_const as u32 }>,
            #space_secondary_index,
            #primary_key,
            #space_secondary_index_events,
            #available_indexes,
        >;
    })
}
