use indexmap::IndexMap;
use proc_macro2::{Ident, TokenStream};

#[derive(Debug, Clone)]
pub struct PrimaryKey {
    pub ident: Ident,
    pub values: IndexMap<Ident, TokenStream>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum GeneratorType {
    None,
    Autoincrement,
    Custom,
}
