mod index;
mod locks;
mod primary_key;
mod queries;
mod row;
mod table;
mod wrapper;

use proc_macro2::Ident;
use quote::quote;

use crate::common::model::{Columns, Config, Queries};

pub struct PersistGenerator {
    pub name: Ident,
    pub columns: Columns,
    pub pk: Option<crate::common::model::PrimaryKey>,
    pub queries: Option<Queries>,
    pub config: Option<Config>,
    pub version: u32,
}

impl PersistGenerator {
    pub fn new(name: Ident, columns: Columns, version: u32) -> Self {
        Self {
            name,
            columns,
            pk: None,
            queries: None,
            config: None,
            version,
        }
    }

    pub fn set_queries(&mut self, queries: Queries) {
        self.queries = Some(queries);
    }

    pub fn set_config(&mut self, config: Config) {
        self.config = Some(config);
    }
}

pub fn expand(
    name: proc_macro2::Ident,
    columns: crate::common::model::Columns,
    queries: Option<Queries>,
    config: Option<Config>,
    version: u32,
) -> syn::Result<proc_macro2::TokenStream> {
    let mut generator = PersistGenerator::new(name, columns, version);
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
