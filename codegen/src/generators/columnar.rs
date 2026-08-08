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

fn slot_id_type(columns: &Columns) -> Ident {
    Ident::new(columns.column_slot_id.type_name(), Span::mixed_site())
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
        quote! {
            if let Err(bits) = self.columnar.write().save_row(&row) {
                return Err(IndexError::ColumnSlotIdExhausted {
                    bits,
                    inserted_already: inserted_indexes.clone(),
                });
            }
        }
    }
}

pub(crate) fn save_row_cdc(columns: &Columns) -> TokenStream {
    if columns.columnar_fields.is_empty() {
        quote! {}
    } else {
        quote! {
            if let Err(bits) = self.columnar.write().save_row(&row) {
                return (partial_events, Err(IndexError::ColumnSlotIdExhausted {
                    bits,
                    inserted_already: inserted_indexes.clone(),
                }));
            }
        }
    }
}

pub(crate) fn reinsert_row(columns: &Columns) -> TokenStream {
    if columns.columnar_fields.is_empty() {
        quote! {}
    } else {
        quote! {
            if let Err(bits) = self.columnar.write().replace_row(&row_old, &row_new) {
                return Err(IndexError::ColumnSlotIdExhausted {
                    bits,
                    inserted_already: inserted_indexes.clone(),
                });
            }
        }
    }
}

pub(crate) fn reinsert_row_cdc(columns: &Columns) -> TokenStream {
    if columns.columnar_fields.is_empty() {
        quote! {}
    } else {
        quote! {
            if let Err(bits) = self.columnar.write().replace_row(&row_old, &row_new) {
                return (partial_events, Err(IndexError::ColumnSlotIdExhausted {
                    bits,
                    inserted_already: inserted_indexes.clone(),
                }));
            }
        }
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
    let slot_id = slot_id_type(columns);

    let column_fields = columns.columnar_fields.iter().map(|(field, _)| {
        let storage = column_field(field);
        let ty = columns.columns_map.get(field).expect("columnar field exists");
        quote! { #storage: ColumnarColumn<#ty>, }
    });
    let column_defaults = columns.columnar_fields.iter().map(|(field, config)| {
        let storage = column_field(field);
        let chunk_rows = Literal::usize_unsuffixed(config.chunk_rows.expect("columnar defaults applied"));
        let compression = compression_variant(config.compression);
        quote! { #storage: ColumnarColumn::new(#chunk_rows, ColumnCompression::#compression), }
    });
    let index_fields = columns.columnar_indexes.values().map(|index| {
        let field = index_field(&index.name);
        let ty = key_type(columns, &index.cluster_by);
        quote! { #field: ClusteredColumnarIndex<#ty, #slot_id>, }
    });
    let index_defaults = columns.columnar_indexes.values().map(|index| {
        let field = index_field(&index.name);
        quote! { #field: Default::default(), }
    });

    let set_columns = columns.columnar_fields.keys().map(|field| {
        let storage = column_field(field);
        quote! { self.#storage.set(slot_id, row.#field.clone()); }
    });
    let remove_columns = columns.columnar_fields.keys().map(|field| {
        let storage = column_field(field);
        quote! { self.#storage.remove(slot_id); }
    });
    let insert_indexes = columns.columnar_indexes.values().map(|index| {
        let field = index_field(&index.name);
        let key = row_key(columns, &index.cluster_by, quote! { row });
        quote! { self.#field.insert(#key, slot_id); }
    });
    let delete_indexes = columns.columnar_indexes.values().map(|index| {
        let field = index_field(&index.name);
        let key = row_key(columns, &index.cluster_by, quote! { row });
        quote! { self.#field.remove(&#key, slot_id); }
    });
    let replace_remove_indexes = columns.columnar_indexes.values().map(|index| {
        let field = index_field(&index.name);
        let key = row_key(columns, &index.cluster_by, quote! { row_old });
        quote! { self.#field.remove(&#key, slot_id); }
    });
    let replace_insert_indexes = columns.columnar_indexes.values().map(|index| {
        let field = index_field(&index.name);
        let key = row_key(columns, &index.cluster_by, quote! { row_new });
        quote! { self.#field.insert(#key, slot_id); }
    });
    let replace_columns = columns.columnar_fields.keys().map(|field| {
        let storage = column_field(field);
        quote! { self.#storage.set(slot_id, row_new.#field.clone()); }
    });

    quote! {
        #[derive(Debug, MemStat)]
        struct #data {
            next_slot_position: Option<u64>,
            free_slot_ids: std::collections::BTreeSet<#slot_id>,
            slot_generations: Vec<u64>,
            incarnation: u64,
            slots_high_water: usize,
            dirty: bool,
            slots: std::collections::BTreeMap<#pk, (#slot_id, u64)>,
            primary_keys: ColumnarColumn<#pk>,
            #(#column_fields)*
            #(#index_fields)*
        }

        impl Default for #data {
            fn default() -> Self {
                Self {
                    next_slot_position: Some(0),
                    free_slot_ids: Default::default(),
                    slot_generations: Default::default(),
                    incarnation: next_columnar_incarnation(),
                    slots_high_water: 0,
                    dirty: true,
                    slots: Default::default(),
                    primary_keys: ColumnarColumn::new(65_536, ColumnCompression::None),
                    #(#column_defaults)*
                    #(#index_defaults)*
                }
            }
        }

        impl #data {
            fn allocate_slot(&mut self) -> Result<(#slot_id, u64), u8> {
                if let Some(slot_id) = self.free_slot_ids.pop_first() {
                    let generation = self.slot_generations[slot_id.slot()];
                    return Ok((slot_id, generation));
                }
                let position = self
                    .next_slot_position
                    .ok_or(<#slot_id as ColumnSlotId>::BITS)?;
                let slot_id = <#slot_id as ColumnSlotId>::try_from_position(position)
                    .ok_or(<#slot_id as ColumnSlotId>::BITS)?;
                self.next_slot_position = position.checked_add(1);
                let slot = slot_id.slot();
                if self.slot_generations.len() <= slot {
                    self.slot_generations.resize(slot + 1, 0);
                }
                Ok((slot_id, self.slot_generations[slot]))
            }

            fn save_row(&mut self, row: &#row) -> Result<(), u8> {
                let primary_key = row.get_primary_key();
                let (slot_id, _) = if let Some(slot) = self.slots.get(&primary_key).copied() {
                    slot
                } else {
                    let slot = self.allocate_slot()?;
                    self.slots.insert(primary_key.clone(), slot);
                    self.primary_keys.set(slot.0, primary_key);
                    self.slots_high_water = self.slots_high_water.max(self.slots.len());
                    slot
                };
                #(#set_columns)*
                #(#insert_indexes)*
                Ok(())
            }

            fn delete_row(&mut self, row: &#row) {
                let primary_key = row.get_primary_key();
                let Some((slot_id, generation)) = self.slots.remove(&primary_key) else {
                    return;
                };
                #(#delete_indexes)*
                #(#remove_columns)*
                self.primary_keys.remove(slot_id);
                if let Some(next_generation) = generation.checked_add(1) {
                    self.slot_generations[slot_id.slot()] = next_generation;
                    self.free_slot_ids.insert(slot_id);
                }
            }

            fn replace_row(&mut self, row_old: &#row, row_new: &#row) -> Result<(), u8> {
                let old_primary_key = row_old.get_primary_key();
                let new_primary_key = row_new.get_primary_key();
                if old_primary_key != new_primary_key {
                    self.delete_row(row_old);
                    return self.save_row(row_new);
                }
                let Some((slot_id, _)) = self.slots.get(&old_primary_key).copied() else {
                    return self.save_row(row_new);
                };
                #(#replace_remove_indexes)*
                #(#replace_columns)*
                #(#replace_insert_indexes)*
                Ok(())
            }

            fn row_ref(&self, slot_id: #slot_id) -> Option<ColumnarRowRef<#pk, #slot_id>> {
                let primary_key = self.primary_keys.get(slot_id)?.clone();
                let (current_slot, generation) = self.slots.get(&primary_key).copied()?;
                (current_slot == slot_id).then(|| {
                    ColumnarRowRef::__new(primary_key, slot_id, generation, self.incarnation)
                })
            }

            fn validates(&self, row_ref: &ColumnarRowRef<#pk, #slot_id>) -> bool {
                row_ref.__incarnation() == self.incarnation
                    && self.slots.get(row_ref.primary_key()).copied()
                        == Some((row_ref.__slot_id(), row_ref.__generation()))
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
    let slot_id = slot_id_type(columns);
    let row_ref = quote! { ColumnarRowRef<#pk, #slot_id> };

    let field_methods = columns.columnar_fields.iter().map(|(field, _)| {
        let storage = column_field(field);
        let scan = format_ident!("columnar_scan_{}", field);
        let project = format_ident!("columnar_project_{}", field);
        let ty = columns.columns_map.get(field).expect("columnar field exists");
        quote! {
            pub fn #scan(&self) -> Result<Vec<(#row_ref, #ty)>, WorkTableError> {
                loop {
                    self.ensure_columnar_current()?;
                    let columnar = self.0.indexes.columnar.read();
                    if !columnar.dirty {
                        return Ok(columnar.#storage.iter::<#slot_id>()
                            .filter_map(|(slot_id, value)| {
                                columnar.row_ref(slot_id).map(|row_ref| (row_ref, value.clone()))
                            })
                            .collect());
                    }
                }
            }

            pub fn #project(&self, rows: &[#row_ref]) -> Result<Vec<(#row_ref, #ty)>, WorkTableError> {
                loop {
                    self.ensure_columnar_current()?;
                    let columnar = self.0.indexes.columnar.read();
                    if !columnar.dirty {
                        return Ok(rows.iter().filter_map(|row_ref| {
                            columnar.validates(row_ref).then(|| {
                                columnar.#storage.get(row_ref.__slot_id())
                                    .cloned()
                                    .map(|value| (row_ref.clone(), value))
                            }).flatten()
                        }).collect());
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
            pub fn #select(&self, #(#args),*) -> Result<Vec<#row_ref>, WorkTableError> {
                let key = (#(#key_fields,)*);
                loop {
                    self.ensure_columnar_current()?;
                    let columnar = self.0.indexes.columnar.read();
                    if !columnar.dirty {
                        return Ok(columnar.#storage.exact(&key).into_iter()
                            .filter_map(|slot_id| columnar.row_ref(slot_id))
                            .collect());
                    }
                }
            }

            pub fn #scan(&self) -> Result<Vec<#row_ref>, WorkTableError> {
                loop {
                    self.ensure_columnar_current()?;
                    let columnar = self.0.indexes.columnar.read();
                    if !columnar.dirty {
                        return Ok(columnar.#storage.ordered_slot_ids().into_iter()
                            .filter_map(|slot_id| columnar.row_ref(slot_id))
                            .collect());
                    }
                }
            }
        }
    });

    quote! {
        fn ensure_columnar_current(&self) -> Result<(), WorkTableError> {
            // Take the writer lock before reading authoritative rows. A row
            // mutation publishes to this same lock after changing row storage,
            // so it either lands in this rebuild or dirties/updates the replica
            // after the rebuild.
            let mut columnar = self.0.indexes.columnar.write();
            if !columnar.dirty {
                return Ok(());
            }
            let rows: Vec<#row> = {
                let read_guard = self.0.data.read_guard();
                self.0.primary_index.pk_map.iter_values().filter_map(|(_, link)| {
                    let _read_guard = &read_guard;
                    self.0.data.select_non_ghosted(link.0).ok()
                }).collect()
            };
            // Retain every assigned primary-key/slot pair. A concurrent
            // reinsert may temporarily publish a ghost link while waiting for
            // this lock; absence from this scan is not proof of deletion.
            let mut rebuilt: #data = Default::default();
            rebuilt.next_slot_position = columnar.next_slot_position;
            rebuilt.free_slot_ids = std::mem::take(&mut columnar.free_slot_ids);
            rebuilt.slot_generations = std::mem::take(&mut columnar.slot_generations);
            rebuilt.incarnation = columnar.incarnation;
            rebuilt.slots_high_water = columnar.slots_high_water;
            rebuilt.slots = std::mem::take(&mut columnar.slots);
            rebuilt.primary_keys = std::mem::replace(
                &mut columnar.primary_keys,
                ColumnarColumn::new(65_536, ColumnCompression::None),
            );
            for row in &rows {
                rebuilt.save_row(row).map_err(WorkTableError::ColumnSlotIdExhausted)?;
            }
            rebuilt.dirty = false;
            *columnar = rebuilt;
            Ok(())
        }

        pub fn columnar_slots_in_use(&self) -> usize {
            self.0.indexes.columnar.read().slots.len()
        }

        pub fn columnar_slots_high_water(&self) -> usize {
            self.0.indexes.columnar.read().slots_high_water
        }

        /// Returns whether a fallback mutation has invalidated the derived
        /// columnar replica. The next columnar read rebuilds it automatically.
        pub fn columnar_is_dirty(&self) -> bool {
            self.0.indexes.columnar.read().dirty
        }

        /// Rebuilds a dirty derived columnar replica at an application-chosen
        /// point instead of charging the first later columnar reader.
        pub fn rebuild_columnar(&self) -> Result<(), WorkTableError> {
            self.ensure_columnar_current()
        }

        #(#field_methods)*
        #(#index_methods)*
    }
}
