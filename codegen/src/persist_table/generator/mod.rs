use proc_macro2::{Ident, Span};
use syn::ItemStruct;

use crate::common::name_generator::WorktableNameGenerator;

pub use space_file::WT_INDEX_EXTENSION;

mod space;
mod space_file;

pub struct PersistTableAttributes {
    pub pk_unsized: bool,
    pub read_only: bool,
    pub pk_upstream: bool,
    pub pk_arctic: bool,
    pub pk_arctic_string: bool,
    pub pk_congee: bool,
    pub pk_wti_logical: bool,
    pub row_schema: Vec<(String, String)>,
    pub primary_key_fields: Vec<String>,
    pub secondary_index_types: Vec<(String, String)>,
}

pub struct Generator {
    pub struct_def: ItemStruct,
    pub pk_ident: Ident,
    pub pk_upstream: bool,
    pub attributes: PersistTableAttributes,
}

impl WorktableNameGenerator {
    pub fn get_space_file_ident(&self) -> Ident {
        Ident::new(format!("{}SpaceFile", self.name).as_str(), Span::mixed_site())
    }

    pub fn get_persistence_engine_ident(&self) -> Ident {
        Ident::new(format!("{}PersistenceEngine", self.name).as_str(), Span::mixed_site())
    }

    pub fn get_persistence_task_ident(&self) -> Ident {
        Ident::new(format!("{}PersistenceTask", self.name).as_str(), Span::mixed_site())
    }
}
