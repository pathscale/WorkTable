use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, Result};

use crate::common::name_generator::WorktableNameGenerator;

struct Input {
    table_name: Ident,
}

impl Parse for Input {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok(Self {
            table_name: input.parse()?,
        })
    }
}

pub fn expand(input: TokenStream) -> Result<TokenStream> {
    let input: Input = syn::parse2(input)?;
    let name = input.table_name.to_string();
    let base = name.strip_suffix("WorkTable").unwrap_or(&name).to_string();
    let output_name = format!("{base}DatabaseS3PersistenceEngine");
    let names = WorktableNameGenerator::from_table_name(base);
    let output = Ident::new(&output_name, input.table_name.span());
    let primary_key = names.get_primary_key_type_ident();
    let space_primary_index = names.get_space_primary_index_ident();
    let space_secondary_index = names.get_space_secondary_index_ident();
    let secondary_events = names.get_space_secondary_index_events_ident();
    let available_indexes = names.get_available_indexes_ident();
    let inner_size = names.get_page_inner_size_const_ident();
    let page_size = names.get_page_size_const_ident();

    Ok(quote! {
        pub type #output = worktable::prelude::DatabaseS3PersistenceEngine<
            worktable::prelude::SpaceData<
                <<#primary_key as worktable::prelude::TablePrimaryKey>::Generator as worktable::prelude::PrimaryKeyGeneratorState>::State,
                { #inner_size },
                { #page_size as u32 },
            >,
            #space_primary_index,
            #space_secondary_index,
            #primary_key,
            #secondary_events,
            #available_indexes,
        >;
    })
}
