use convert_case::{Case, Casing};
use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::{format_ident, quote};

use crate::common::model::{ColumnCompression, Columns};
use crate::common::name_generator::{WorktableNameGenerator, is_float};

fn data_ident(table: &Ident) -> Ident {
    format_ident!("{}ColumnarData", table)
}

fn column_field(field: &Ident) -> Ident {
    format_ident!("column_{}", field)
}

fn index_field(index: &Ident) -> Ident {
    format_ident!("columnar_index_{}", index)
}

fn compression_variant(compression: ColumnCompression) -> Ident {
    Ident::new(
        &compression.name().from_case(Case::Snake).to_case(Case::Pascal),
        Span::mixed_site(),
    )
}

fn key_type(columns: &Columns, fields: &[Ident]) -> TokenStream {
    let fields = fields.iter().map(|field| {
        let ty = columns.columns_map.get(field).expect("validated columnar index field");
        if is_float(&ty.to_string()) {
            quote! { OrderedFloat<#ty> }
        } else {
            quote! { #ty }
        }
    });
    quote! { (#(#fields,)*) }
}

fn row_key(columns: &Columns, fields: &[Ident], row: TokenStream) -> TokenStream {
    let fields = fields.iter().map(|field| {
        let ty = columns.columns_map.get(field).expect("validated columnar index field");
        if is_float(&ty.to_string()) {
            quote! { OrderedFloat(#row.#field) }
        } else {
            quote! { #row.#field.clone() }
        }
    });
    quote! { (#(#fields,)*) }
}

pub(crate) fn index_struct_field(table: &Ident, columns: &Columns, persisted: bool) -> TokenStream {
    if columns.columnar_fields.is_empty() {
        return quote! {};
    }
    let data = data_ident(table);
    let skip = persisted.then(|| quote! { #[index(skip)] });
    quote! {
        #skip
        columnar: ParkingRwLock<#data>
    }
}

pub(crate) fn index_default_field(columns: &Columns) -> TokenStream {
    if columns.columnar_fields.is_empty() {
        quote! {}
    } else {
        quote! { columnar: ParkingRwLock::new(Default::default()), }
    }
}

pub(crate) fn save_row(columns: &Columns) -> TokenStream {
    if columns.columnar_fields.is_empty() {
        quote! {}
    } else {
        quote! { self.columnar.write().save_row(&row); }
    }
}

pub(crate) fn reinsert_row(columns: &Columns) -> TokenStream {
    if columns.columnar_fields.is_empty() {
        quote! {}
    } else {
        quote! { self.columnar.write().replace_row(&row_old, &row_new); }
    }
}

pub(crate) fn delete_row(columns: &Columns) -> TokenStream {
    if columns.columnar_fields.is_empty() {
        quote! {}
    } else {
        quote! { self.columnar.write().delete_row(&row); }
    }
}

pub(crate) fn mark_dirty(columns: &Columns) -> TokenStream {
    if columns.columnar_fields.is_empty() {
        quote! {}
    } else {
        quote! { self.columnar.write().mark_dirty(); }
    }
}

pub(crate) fn table_mark_dirty(columns: &Columns) -> TokenStream {
    if columns.columnar_fields.is_empty() {
        quote! {}
    } else {
        quote! { self.0.indexes.columnar.write().mark_dirty(); }
    }
}

pub(crate) fn definitions(table: &Ident, columns: &Columns) -> TokenStream {
    if columns.columnar_fields.is_empty() {
        return quote! {};
    }

    let names = WorktableNameGenerator::from_table_name(table.to_string());
    let row = names.get_row_type_ident();
    let pk = names.get_primary_key_type_ident();
    let data = data_ident(table);

    let column_fields = columns.columnar_fields.iter().map(|(field, _)| {
        let storage = column_field(field);
        let ty = columns.columns_map.get(field).expect("columnar field exists");
        quote! { #storage: ColumnarColumn<#ty>, }
    });
    let column_defaults = columns.columnar_fields.iter().map(|(field, config)| {
        let storage = column_field(field);
        let chunk_rows = Literal::usize_unsuffixed(config.chunk_rows);
        let compression = compression_variant(config.compression);
        quote! { #storage: ColumnarColumn::new(#chunk_rows, ColumnCompression::#compression), }
    });
    let index_fields = columns.columnar_indexes.values().map(|index| {
        let field = index_field(&index.name);
        let ty = key_type(columns, &index.cluster_by);
        quote! { #field: ClusteredColumnarIndex<#ty>, }
    });
    let index_defaults = columns.columnar_indexes.values().map(|index| {
        let field = index_field(&index.name);
        quote! { #field: Default::default(), }
    });

    let set_columns = columns.columnar_fields.keys().map(|field| {
        let storage = column_field(field);
        quote! { self.#storage.set(row_id, row.#field.clone()); }
    });
    let remove_columns = columns.columnar_fields.keys().map(|field| {
        let storage = column_field(field);
        quote! { self.#storage.remove(row_id); }
    });
    let insert_indexes = columns.columnar_indexes.values().map(|index| {
        let field = index_field(&index.name);
        let key = row_key(columns, &index.cluster_by, quote! { row });
        quote! { self.#field.insert(#key, row_id); }
    });
    let delete_indexes = columns.columnar_indexes.values().map(|index| {
        let field = index_field(&index.name);
        let key = row_key(columns, &index.cluster_by, quote! { row });
        quote! { self.#field.remove(&#key, row_id); }
    });
    let replace_remove_indexes = columns.columnar_indexes.values().map(|index| {
        let field = index_field(&index.name);
        let key = row_key(columns, &index.cluster_by, quote! { row_old });
        quote! { self.#field.remove(&#key, row_id); }
    });
    let replace_insert_indexes = columns.columnar_indexes.values().map(|index| {
        let field = index_field(&index.name);
        let key = row_key(columns, &index.cluster_by, quote! { row_new });
        quote! { self.#field.insert(#key, row_id); }
    });
    let replace_columns = columns.columnar_fields.keys().map(|field| {
        let storage = column_field(field);
        quote! { self.#storage.set(row_id, row_new.#field.clone()); }
    });

    quote! {
        #[derive(Debug, MemStat)]
        struct #data {
            next_row_id: u64,
            dirty: bool,
            row_ids: std::collections::BTreeMap<#pk, ColumnRowId>,
            primary_keys: ColumnarColumn<#pk>,
            #(#column_fields)*
            #(#index_fields)*
        }

        impl Default for #data {
            fn default() -> Self {
                Self {
                    next_row_id: 0,
                    // Persisted and read-only tables reconstruct this derived
                    // replica from authoritative rows on first access.
                    dirty: true,
                    row_ids: Default::default(),
                    primary_keys: ColumnarColumn::new(65_536, ColumnCompression::None),
                    #(#column_defaults)*
                    #(#index_defaults)*
                }
            }
        }

        impl #data {
            fn save_row(&mut self, row: &#row) {
                let primary_key = row.get_primary_key();
                let row_id = if let Some(row_id) = self.row_ids.get(&primary_key).copied() {
                    row_id
                } else {
                    let row_id = ColumnRowId::new(self.next_row_id);
                    self.next_row_id = self.next_row_id.saturating_add(1);
                    self.row_ids.insert(primary_key.clone(), row_id);
                    self.primary_keys.set(row_id, primary_key);
                    row_id
                };
                #(#set_columns)*
                #(#insert_indexes)*
            }

            fn delete_row(&mut self, row: &#row) {
                let primary_key = row.get_primary_key();
                let Some(row_id) = self.row_ids.remove(&primary_key) else {
                    return;
                };
                #(#delete_indexes)*
                #(#remove_columns)*
                self.primary_keys.remove(row_id);
            }

            fn replace_row(&mut self, row_old: &#row, row_new: &#row) {
                let old_primary_key = row_old.get_primary_key();
                let new_primary_key = row_new.get_primary_key();
                if old_primary_key != new_primary_key {
                    self.delete_row(row_old);
                    self.save_row(row_new);
                    return;
                }
                let Some(row_id) = self.row_ids.get(&old_primary_key).copied() else {
                    self.save_row(row_new);
                    return;
                };
                #(#replace_remove_indexes)*
                #(#replace_columns)*
                #(#replace_insert_indexes)*
            }

            fn mark_dirty(&mut self) {
                self.dirty = true;
            }
        }
    }
}

pub(crate) fn table_methods(table: &Ident, columns: &Columns) -> TokenStream {
    if columns.columnar_fields.is_empty() {
        return quote! {};
    }

    let names = WorktableNameGenerator::from_table_name(table.to_string());
    let row = names.get_row_type_ident();
    let pk = names.get_primary_key_type_ident();
    let data = data_ident(table);

    let field_methods = columns.columnar_fields.iter().map(|(field, _)| {
        let storage = column_field(field);
        let scan = format_ident!("columnar_scan_{}", field);
        let project = format_ident!("columnar_project_{}", field);
        let ty = columns.columns_map.get(field).expect("columnar field exists");
        quote! {
            pub fn #scan(&self) -> Vec<(ColumnRowId, #ty)> {
                loop {
                    self.ensure_columnar_current();
                    let columnar = self.0.indexes.columnar.read();
                    if !columnar.dirty {
                        return columnar.#storage.iter()
                            .map(|(row_id, value)| (row_id, value.clone()))
                            .collect();
                    }
                }
            }

            pub fn #project(&self, row_ids: &[ColumnRowId]) -> Vec<(ColumnRowId, #ty)> {
                loop {
                    self.ensure_columnar_current();
                    let columnar = self.0.indexes.columnar.read();
                    if !columnar.dirty {
                        return row_ids.iter().filter_map(|row_id| {
                            columnar.#storage.get(*row_id).cloned().map(|value| (*row_id, value))
                        }).collect();
                    }
                }
            }
        }
    });

    let index_methods = columns.columnar_indexes.values().map(|index| {
        let storage = index_field(&index.name);
        let select = format_ident!("columnar_select_{}", index.name);
        let scan = format_ident!("columnar_scan_{}", index.name);
        let args = index.cluster_by.iter().map(|field| {
            let ty = columns.columns_map.get(field).expect("columnar index field exists");
            quote! { #field: #ty }
        });
        let key_fields = index.cluster_by.iter().map(|field| {
            let ty = columns.columns_map.get(field).expect("columnar index field exists");
            if is_float(&ty.to_string()) {
                quote! { OrderedFloat(#field) }
            } else {
                quote! { #field }
            }
        });
        quote! {
            pub fn #select(&self, #(#args),*) -> Vec<ColumnRowId> {
                let key = (#(#key_fields,)*);
                loop {
                    self.ensure_columnar_current();
                    let columnar = self.0.indexes.columnar.read();
                    if !columnar.dirty {
                        return columnar.#storage.exact(&key);
                    }
                }
            }

            pub fn #scan(&self) -> Vec<ColumnRowId> {
                loop {
                    self.ensure_columnar_current();
                    let columnar = self.0.indexes.columnar.read();
                    if !columnar.dirty {
                        return columnar.#storage.ordered_row_ids();
                    }
                }
            }
        }
    });

    quote! {
        fn ensure_columnar_current(&self) {
            // Take the writer lock before reading authoritative rows. A row
            // mutation publishes to this same lock after changing row storage,
            // so it either lands in this rebuild or dirties/updates the replica
            // after the rebuild. Scanning first and locking later would allow a
            // stale rebuild to overwrite a concurrent mutation.
            let mut columnar = self.0.indexes.columnar.write();
            if !columnar.dirty {
                return;
            }
            let rows: Vec<#row> = {
                let read_guard = self.0.data.read_guard();
                self.0.primary_index.pk_map.iter_values().filter_map(|(_, link)| {
                    let _read_guard = &read_guard;
                    self.0.data.select_non_ghosted(link.0).ok()
                }).collect()
            };
            // Rebuild derived vectors and clustered metadata while retaining
            // every assigned row id. A concurrent reinsert may temporarily
            // publish a ghost link in the primary index while waiting for this
            // columnar lock; dropping a key that is absent from this scan would
            // then change its stable row id. Delete paths remove their mapping
            // explicitly, so a dirty refresh never infers deletion from an
            // absent/transient row.
            let mut rebuilt: #data = Default::default();
            rebuilt.next_row_id = columnar.next_row_id;
            rebuilt.row_ids = std::mem::take(&mut columnar.row_ids);
            rebuilt.primary_keys = std::mem::replace(
                &mut columnar.primary_keys,
                ColumnarColumn::new(65_536, ColumnCompression::None),
            );
            for row in &rows {
                rebuilt.save_row(row);
            }
            rebuilt.dirty = false;
            *columnar = rebuilt;
        }

        /// Resolves logical columnar row ids back to authoritative WorkTable
        /// primary keys without exposing physical data-page links.
        pub fn columnar_resolve_primary_keys(
            &self,
            row_ids: &[ColumnRowId],
        ) -> Vec<(ColumnRowId, #pk)> {
            loop {
                self.ensure_columnar_current();
                let columnar = self.0.indexes.columnar.read();
                if !columnar.dirty {
                    return row_ids.iter().filter_map(|row_id| {
                        columnar.primary_keys.get(*row_id).cloned().map(|key| (*row_id, key))
                    }).collect();
                }
            }
        }

        #(#field_methods)*
        #(#index_methods)*
    }
}
