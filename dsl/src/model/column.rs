use std::collections::HashMap;

use indexmap::IndexMap;

use crate::model::index::Index;
use crate::model::{GeneratorType, IndexBackend};
use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::spanned::Spanned;

fn is_sized(ident: &Ident) -> bool {
    !matches!(ident.to_string().as_str(), "String")
}

#[derive(Debug, Clone)]
pub struct Columns {
    pub is_sized: bool,
    /// Column types in declaration order.
    ///
    /// Code generation iterates this map to emit ordered Rust constructs,
    /// including archived enums. A randomized `HashMap` made two expansions
    /// of the same declaration produce different variant order and therefore
    /// potentially different discriminants.
    pub columns_map: IndexMap<Ident, TokenStream>,
    pub field_positions: HashMap<Ident, usize>,
    pub indexes: IndexMap<Ident, Index>,
    pub primary_keys: Vec<Ident>,
    pub primary_index_backend: IndexBackend,
    pub generator_type: GeneratorType,
}

#[derive(Debug)]
pub struct Row {
    pub name: Ident,
    pub type_: Ident,
    pub is_primary_key: bool,
    pub gen_type: GeneratorType,
    pub optional: bool,
    pub index_backend: Option<IndexBackend>,
}

impl Columns {
    pub fn try_from_rows(rows: Vec<Row>, input: &TokenStream) -> syn::Result<Self> {
        let mut columns_map = IndexMap::new();
        let mut field_positions = HashMap::new();
        let mut sized = true;
        let mut pk = vec![];
        let mut gen_type = None;
        let mut primary_index_backend = None;

        for (pos, row) in rows.into_iter().enumerate() {
            let type_ = &row.type_;
            if sized {
                sized = is_sized(type_)
            }
            let type_ = if row.optional {
                quote! { core::option::Option<#type_> }
            } else {
                quote! { #type_ }
            };
            columns_map.insert(row.name.clone(), type_);
            field_positions.insert(row.name.clone(), pos);

            if row.is_primary_key {
                if let Some(t) = gen_type {
                    if t != row.gen_type {
                        return Err(syn::Error::new(input.span(), "Generator type must be same"));
                    }
                } else {
                    gen_type = Some(row.gen_type)
                }
                let backend = row.index_backend.unwrap_or_default();
                if let Some(existing) = primary_index_backend {
                    if existing != backend {
                        return Err(syn::Error::new(
                            row.name.span(),
                            "all columns in a composite primary key must use the same index backend",
                        ));
                    }
                } else {
                    primary_index_backend = Some(backend);
                }
                pk.push(row.name);
            } else if row.index_backend.is_some() {
                return Err(syn::Error::new(
                    row.name.span(),
                    "`using` on a column is only valid after `primary_key`; select secondary index backends in `indexes`",
                ));
            }
        }

        if pk.is_empty() {
            return Err(syn::Error::new(input.span(), "Primary key must be set"));
        }

        Ok(Self {
            is_sized: sized,
            columns_map,
            indexes: Default::default(),
            primary_keys: pk,
            primary_index_backend: primary_index_backend.unwrap_or_default(),
            generator_type: gen_type.expect("set"),
            field_positions,
        })
    }
}
