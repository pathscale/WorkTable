//! A `Vec`-backed table with the same shape as a `worktable!` and none of its machinery.
//!
//! # What this is for
//!
//! `worktable!` buys concurrency and durability with an archived row, paged
//! storage behind links, a row-level lock map and change-data-capture. A
//! single-threaded table that never persists pays all of that for nothing, and
//! the pattern applications otherwise grow by hand is a `Vec` plus a
//! `BTreeMap`.
//!
//! So this generates that, from the same declaration, so the two can sit side
//! by side and be compared on identical rows.
//!
//! # What it drops, deliberately
//!
//! Each of these is the reason a `worktable!` costs what it does, and dropping
//! them is the point rather than an omission:
//!
//! - **The archived row.** Rows are stored as themselves. No `rkyv`, no
//!   serialize on write, no `Archived` type on read.
//! - **Paging and links.** One contiguous `Vec<Row>` and an index into it, so
//!   no page ids, no offsets, no empty-link registry and nothing for vacuum to
//!   do.
//! - **The lock map.** Mutation takes `&mut self`. That is what makes it
//!   single-writer, and what makes it as fast as the `Vec` it is.
//! - **Change-data-capture.** CDC exists to feed persistence and vacuum. With
//!   neither, it is pure cost.
//! - **The async surface.** `insert` and friends are synchronous, because
//!   nothing here can queue. That also takes the executor off the hot path.
//!
//! # What it keeps
//!
//! The declaration and the method names. `insert`, `upsert`, `select`,
//! `select_all` and `delete` mean what they mean on a `worktable!`, so a table
//! can be moved between the two by changing which macro is called.
//!
//! Secondary indexes become `BTreeMap<Key, Vec<usize>>` over row positions.
//! Unique ones still reject a duplicate, which is the behaviour a caller
//! depends on rather than an implementation detail.

use proc_macro2::TokenStream;
use quote::quote;
use syn::Ident;
use worktable_dsl::Columns;

use crate::common::name_generator::WorktableNameGenerator;

// Paths are written through `worktable::prelude`, never as bare `alloc::` or
// `std::`. The macro expands in the consumer's crate, so anything it names has
// to resolve there: emitting `alloc::` requires the consumer to have declared
// `extern crate alloc`, and emitting a crate name makes that crate part of this
// macro's contract. The same mistake has been made here with `tokio::`,
// `futures::` and `rkyv::`.

pub fn expand(name: Ident, columns: Columns) -> syn::Result<TokenStream> {
    if columns.primary_keys.len() != 1 {
        return Err(syn::Error::new(
            name.span(),
            "worktable_vec! takes a single-column primary key. A composite key needs a tuple key \
             type, which is the machinery this macro exists to avoid.",
        ));
    }
    if !columns.columnar_fields.is_empty() || !columns.columnar_indexes.is_empty() {
        return Err(syn::Error::new(
            name.span(),
            "worktable_vec! does not support columnar fields. Columnar storage is a paging \
             feature and this table has no pages.",
        ));
    }

    let generator = WorktableNameGenerator::from_table_name(name.to_string());
    let row_ident = generator.get_row_type_ident();
    let table_ident = Ident::new(&format!("{name}VecTable"), name.span());

    let pk = columns.primary_keys.first().expect("checked above").clone();
    let pk_type = columns
        .columns_map
        .get(&pk)
        .expect("the primary key is a column")
        .clone();

    let field_names: Vec<_> = columns.columns_map.keys().cloned().collect();
    let field_types: Vec<_> = columns.columns_map.values().cloned().collect();

    // Secondary indexes, as position lists. Unique ones keep the reject.
    let mut index_fields = Vec::new();
    let mut index_types = Vec::new();
    let mut index_columns = Vec::new();
    let mut index_unique = Vec::new();
    for (index_name, index) in &columns.indexes {
        let column = &index.field;
        let ty = columns
            .columns_map
            .get(column)
            .ok_or_else(|| syn::Error::new(index_name.span(), format!("no column `{column}`")))?;
        index_fields.push(Ident::new(&format!("{index_name}_map"), index_name.span()));
        index_types.push(ty.clone());
        index_columns.push(column.clone());
        index_unique.push(index.is_unique);
    }

    let select_by: Vec<_> = columns
        .indexes
        .iter()
        .map(|(index_name, index)| {
            let column = &index.field;
            let fn_name = Ident::new(&format!("select_by_{column}"), index_name.span());
            let map = Ident::new(&format!("{index_name}_map"), index_name.span());
            let ty = columns.columns_map.get(column).expect("checked above");
            if index.is_unique {
                quote! {
                    /// The row this key indexes, if any.
                    pub fn #fn_name(&self, key: &#ty) -> Option<&#row_ident> {
                        self.#map.get(key).and_then(|positions| positions.first()).map(|at| &self.rows[*at])
                    }
                }
            } else {
                quote! {
                    /// Every row this key indexes, in insertion order.
                    pub fn #fn_name(&self, key: &#ty) -> Vec<&#row_ident> {
                        self.#map
                            .get(key)
                            .map(|positions| positions.iter().map(|at| &self.rows[*at]).collect())
                            .unwrap_or_default()
                    }
                }
            }
        })
        .collect();

    Ok(quote! {
        #[derive(Clone, Debug, PartialEq)]
        pub struct #row_ident {
            #(pub #field_names: #field_types,)*
        }

        /// A `Vec`-backed table with the same surface as the generated `WorkTable`.
        ///
        /// Single-writer by construction: every mutation takes `&mut self`.
        #[derive(Debug, Default)]
        pub struct #table_ident {
            rows: worktable::prelude::Vec<#row_ident>,
            /// Primary key to position. The lookup a bare `Vec` does linearly.
            by_pk: worktable::prelude::BTreeMap<#pk_type, usize>,
            #(#index_fields: worktable::prelude::BTreeMap<#index_types, worktable::prelude::Vec<usize>>,)*
        }

        impl #table_ident {
            #[must_use]
            pub fn new() -> Self {
                Self::default()
            }

            #[must_use]
            pub fn len(&self) -> usize {
                self.rows.len()
            }

            #[must_use]
            pub fn is_empty(&self) -> bool {
                self.rows.is_empty()
            }

            /// Insert, refusing a key that is already present.
            ///
            /// `Err` carries the row back rather than dropping it, so a caller
            /// that wants `upsert` semantics on failure still has the value.
            pub fn insert(&mut self, row: #row_ident) -> Result<(), #row_ident> {
                if self.by_pk.contains_key(&row.#pk) {
                    return Err(row);
                }
                #(
                    if #index_unique && self.#index_fields.contains_key(&row.#index_columns) {
                        return Err(row);
                    }
                )*
                let at = self.rows.len();
                self.by_pk.insert(row.#pk.clone(), at);
                #(
                    self.#index_fields
                        .entry(row.#index_columns.clone())
                        .or_default()
                        .push(at);
                )*
                self.rows.push(row);
                Ok(())
            }

            /// Insert, or replace the row this key already names.
            pub fn upsert(&mut self, row: #row_ident) {
                if let Some(at) = self.by_pk.get(&row.#pk).copied() {
                    #(
                        if let Some(positions) = self.#index_fields.get_mut(&self.rows[at].#index_columns) {
                            positions.retain(|p| *p != at);
                        }
                        self.#index_fields
                            .entry(row.#index_columns.clone())
                            .or_default()
                            .push(at);
                    )*
                    self.rows[at] = row;
                    return;
                }
                let _ = self.insert(row);
            }

            /// The row this key names, if any.
            #[must_use]
            pub fn select(&self, key: &#pk_type) -> Option<&#row_ident> {
                self.by_pk.get(key).map(|at| &self.rows[*at])
            }

            /// Every row, in insertion order.
            #[must_use]
            pub fn select_all(&self) -> &[#row_ident] {
                &self.rows
            }

            #(#select_by)*

            /// Remove the row this key names, returning it.
            ///
            /// A swap-remove would be cheaper and is not used: it reorders the
            /// table, and `select_all` promising insertion order is the point
            /// of comparing against a `Vec` at all.
            pub fn delete(&mut self, key: &#pk_type) -> Option<#row_ident> {
                let at = self.by_pk.remove(key)?;
                let row = self.rows.remove(at);
                for position in self.by_pk.values_mut() {
                    if *position > at {
                        *position -= 1;
                    }
                }
                #(
                    self.#index_fields.retain(|_, positions| {
                        positions.retain(|p| *p != at);
                        for position in positions.iter_mut() {
                            if *position > at {
                                *position -= 1;
                            }
                        }
                        !positions.is_empty()
                    });
                )*
                Some(row)
            }
        }
    })
}
