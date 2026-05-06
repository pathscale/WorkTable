mod index;
mod locks;
mod primary_key;
mod queries;
mod row;
mod table;
mod wrapper;

use proc_macro2::Ident;
use quote::quote;

use crate::common::model::Columns;

pub struct ReadOnlyGenerator {
    pub name: Ident,
    pub columns: Columns,
    pub pk: Option<crate::common::model::PrimaryKey>,
    pub version: u32,
}

impl ReadOnlyGenerator {
    pub fn new(name: Ident, columns: Columns, version: u32) -> Self {
        Self {
            name,
            columns,
            pk: None,
            version,
        }
    }
}

pub fn expand(
    name: proc_macro2::Ident,
    columns: crate::common::model::Columns,
    version: u32,
) -> syn::Result<proc_macro2::TokenStream> {
    let mut generator = ReadOnlyGenerator::new(name, columns, version);

    let pk_def = generator.gen_primary_key_def()?;
    let row_def = generator.gen_row_def();
    let query_available_def = generator.gen_available_types_def()?;
    let wrapper_def = generator.gen_wrapper_def();
    let locks_def = generator.gen_locks_def();
    let index_def = generator.gen_index_def()?;
    let table_def = generator.gen_table_def()?;
    let select_impls = generator.gen_query_select_impl()?;

    Ok(quote! {
        #pk_def
        #row_def
        #query_available_def
        #wrapper_def
        #locks_def
        #index_def
        #table_def
        #select_impls
    })
}
