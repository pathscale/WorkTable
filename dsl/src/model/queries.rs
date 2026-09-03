use indexmap::IndexMap;
use proc_macro2::Ident;

use crate::model::Operation;

#[derive(Debug, Default)]
pub struct Queries {
    pub updates: IndexMap<Ident, Operation>,
    pub deletes: IndexMap<Ident, Operation>,
    pub in_place: IndexMap<Ident, Operation>,
}
