mod locks;
mod primary_key;
pub mod queries;
mod row;
mod table;
mod index;
mod wrapper;

use proc_macro2::{Ident, TokenStream};
use quote::quote;

use crate::common::model::{Columns, Config, PrimaryKey, Queries};

pub struct InMemoryGenerator {
    pub name: Ident,
    pub pk: Option<PrimaryKey>,
    pub queries: Option<Queries>,
    pub config: Option<Config>,
    pub columns: Columns,
}

impl InMemoryGenerator {
    pub fn new(name: Ident, columns: Columns) -> Self {
        Self {
            name,
            pk: None,
            queries: None,
            config: None,
            columns,
        }
    }

    pub fn set_queries(&mut self, queries: Queries) {
        self.queries = Some(queries);
    }

    pub fn set_config(&mut self, config: Config) {
        self.config = Some(config);
    }
}

#[allow(dead_code)]
pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let mut parser = crate::common::parser::Parser::new(input);
    let mut columns = None;
    let mut queries = None;
    let mut indexes = None;
    let mut config = None;

    let name = parser.parse_name()?;
    while let Some(ident) = parser.peek_next() {
        match ident.to_string().as_str() {
            "columns" => {
                let res = parser.parse_columns()?;
                columns = Some(res)
            }
            "indexes" => {
                let res = parser.parse_indexes()?;
                indexes = Some(res);
            }
            "queries" => {
                let res = parser.parse_queries()?;
                queries = Some(res)
            }
            "config" => {
                let res = parser.parse_configs()?;
                config = Some(res)
            }
            "persist" => {
                // Skip persist flag for in_memory - it's always false
                parser.parse_persist()?;
            }
            _ => return Err(syn::Error::new(ident.span(), "Unexpected identifier")),
        }
    }

    let mut columns = columns.expect("defined");
    if let Some(i) = indexes {
        columns.indexes = i
    }

    expand_from_parsed(name, columns, queries, config)
}

pub fn expand_from_parsed(
    name: proc_macro2::Ident,
    columns: crate::common::model::Columns,
    queries: Option<crate::common::model::Queries>,
    config: Option<crate::common::model::Config>,
) -> syn::Result<TokenStream> {
    let mut generator = InMemoryGenerator::new(name, columns);
    if let Some(q) = queries {
        generator.set_queries(q);
    }
    if let Some(c) = config {
        generator.set_config(c);
    }

    let pk_def = generator.gen_primary_key_def()?;
    let row_def = generator.gen_row_def();
    let wrapper_def = generator.gen_wrapper_def();
    let locks_def = generator.gen_locks_def();
    let index_def = generator.gen_index_def()?;
    let table_def = generator.gen_table_def()?;
    let query_types_def = generator.gen_result_types_def()?;
    let query_available_def = generator.gen_available_types_def()?;
    let query_locks_impls = generator.gen_query_locks_impl()?;
    let select_impls = generator.gen_query_select_impl()?;
    let update_impls = generator.gen_query_update_impl()?;
    let update_in_place_impls = generator.gen_query_in_place_impl()?;
    let delete_impls = generator.gen_query_delete_impl()?;
    let unsized_impl = generator.gen_unsized_impls();

    Ok(quote! {
        #pk_def
        #row_def
        #query_available_def
        #wrapper_def
        #locks_def
        #index_def
        #table_def
        #query_types_def
        #query_locks_impls
        #select_impls
        #update_impls
        #update_in_place_impls
        #delete_impls
        #unsized_impl
    })
}