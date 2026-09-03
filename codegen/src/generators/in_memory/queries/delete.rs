use convert_case::{Case, Casing};
use indexmap::IndexMap;
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::common::model::Index;
use crate::common::model::Operation;
use crate::common::name_generator::{WorktableNameGenerator, is_float};
use crate::generators::in_memory::InMemoryGenerator;

impl InMemoryGenerator {
    pub fn gen_query_delete_impl(&mut self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let table_ident = name_generator.get_work_table_ident();

        let custom_deletes = if let Some(q) = &self.queries {
            let custom_deletes = self.gen_custom_deletes(q.deletes.clone());
            quote! {
                #custom_deletes
            }
        } else {
            quote! {}
        };
        let full_row_delete = self.gen_full_row_delete();
        let full_row_delete_without_lock = self.gen_full_row_delete_without_lock();

        Ok(quote! {
            impl #table_ident {
                #full_row_delete
                #full_row_delete_without_lock
                #custom_deletes
            }
        })
    }

    fn gen_full_row_delete(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let pk_ident = name_generator.get_primary_key_type_ident();
        let delete_logic = self.gen_delete_logic(true);
        let full_row_lock = self.gen_full_lock_for_update();

        quote! {
            pub async fn delete<Pk>(&self, pk: Pk) -> core::result::Result<(), WorkTableError>
            where #pk_ident: From<Pk>
            {
                let pk: #pk_ident = pk.into();
                let pending_lock = { #full_row_lock };
                let _guard = pending_lock.into_guard_with_mutation();

                #delete_logic

                core::result::Result::Ok(())
            }
        }
    }

    fn gen_full_row_delete_without_lock(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let pk_ident = name_generator.get_primary_key_type_ident();
        let delete_logic = self.gen_delete_logic(false);

        quote! {
            pub async fn delete_without_lock<Pk>(&self, pk: Pk) -> core::result::Result<(), WorkTableError>
            where #pk_ident: From<Pk>
            {
                let pk: #pk_ident = pk.into();
                let _mutation_guard = self.0.lock_manager.mutation_guard(&pk);
                #delete_logic
                core::result::Result::Ok(())
            }
        }
    }

    fn gen_delete_logic(&self, is_locked: bool) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let pk_ident = name_generator.get_primary_key_type_ident();
        let secondary_events_ident = name_generator.get_space_secondary_index_events_ident();

        let process = if false {
            quote! {
                let (secondary_keys_events, res) = self.0.indexes.delete_row_cdc(row, link);
                res?;
                let (_, primary_key_events) = self.0.primary_index.remove_cdc(pk.clone(), link);
                self.0.data.delete(link).map_err(WorkTableError::PagesError)?;
                let mut op: Operation<
                    <<#pk_ident as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
                    #pk_ident,
                    #secondary_events_ident
                > = Operation::Delete(DeleteOperation {
                    id: uuid::Uuid::now_v7().into(),
                    secondary_keys_events,
                    primary_key_events,
                    link,
                });
                self.1.apply_operation(op);
            }
        } else {
            quote! {
                // Index removals deliberately run BEFORE the data delete:
                // insert publishes data first and indexes second, so tearing
                // down in the reverse order guarantees no index entry ever
                // resolves to freed (or reused) row storage. A failed data
                // delete is compensated by restoring the index entries.
                self.0.indexes.delete_row(row, link)?;
                self.0.primary_index.remove(&pk, link);
                if let core::result::Result::Err(e) = self.0.data.delete(link) {
                    // The delete failed before the row was ghosted, so its
                    // bytes are normally still readable: republish the index
                    // entries instead of leaving a live row unreachable
                    // through every index.
                    if let core::result::Result::Ok(row) = self.0.data.select_non_ghosted(link) {
                        self.0.primary_index.insert(pk.clone(), link);
                        // A partially failed republish (a unique key claimed
                        // concurrently in the removal window) cannot be
                        // repaired here; the row stays reachable by primary
                        // key either way.
                        let _ = self.0.indexes.save_row(row, link);
                    }
                    return Err(WorkTableError::PagesError(e));
                }
            }
        };
        if is_locked {
            quote! {
                let link = match self.0
                        .primary_index
                        .pk_map
                        .get_value(&pk)
                        .map(Into::into)
                        .ok_or(WorkTableError::NotFound) {
                    Ok(l) => l,
                    Err(e) => {
                        return Err(e);
                    }
                };
                // Low-level staged or hydrated state can publish index
                // reachability before clearing the row's ghost bit. Ordinary
                // insert shares this delete's per-key mutation gate, but keep
                // this boundary defensive instead of panicking on a hidden
                // version.
                let row = self.0.select(pk.clone()).ok_or(WorkTableError::NotFound)?;
                #process
            }
        } else {
            quote! {
                let link = self.0
                        .primary_index
                        .pk_map
                        .get_value(&pk)
                        .map(Into::into)
                        .ok_or(WorkTableError::NotFound)?;
                let row = self.0.select(pk.clone()).ok_or(WorkTableError::NotFound)?;
                #process
            }
        }
    }

    fn gen_custom_deletes(&mut self, deleted: IndexMap<Ident, Operation>) -> TokenStream {
        let defs = deleted
            .iter()
            .map(|(name, op)| {
                let snake_case_name = name.to_string().from_case(Case::Pascal).to_case(Case::Snake);
                let method_ident = Ident::new(format!("delete_{snake_case_name}").as_str(), Span::mixed_site());
                let index = self.columns.indexes.values().find(|idx| idx.field == op.by);
                let type_ = self.columns.columns_map.get(&op.by).unwrap();
                if let Some(index) = index {
                    let index_name = &index.name;

                    if index.is_unique {
                        Self::gen_unique_delete(type_, &method_ident, index_name)
                    } else {
                        Self::gen_non_unique_delete(type_, &method_ident, index)
                    }
                } else {
                    Self::gen_brute_force_delete_field(&op.by, type_, &method_ident)
                }
            })
            .collect::<Vec<_>>();

        quote! {
            #(#defs)*
        }
    }

    fn gen_brute_force_delete_field(field: &Ident, type_: &TokenStream, name: &Ident) -> TokenStream {
        quote! {
            pub async fn #name(&self, by: #type_) -> core::result::Result<(), WorkTableError> {
                let _bulk_mutation = self.0.lock_manager.bulk_mutation_guard();
                let pks = std::cell::RefCell::new(Vec::new());
                self.iter_with(|row| {
                    if row.#field == by {
                        pks.borrow_mut().push(row.get_primary_key());
                    }
                    Ok(())
                })?;
                let pks = pks.into_inner();
                for pk in pks {
                    match self.delete(pk).await {
                        core::result::Result::Ok(()) => {}
                        core::result::Result::Err(WorkTableError::NotFound) => {}
                        core::result::Result::Err(e) => return core::result::Result::Err(e),
                    }
                }
                core::result::Result::Ok(())
            }
        }
    }

    fn gen_non_unique_delete(type_: &TokenStream, name: &Ident, index: &Index) -> TokenStream {
        let by_field = &index.field;
        let index = &index.name;
        let by = if is_float(type_.to_string().as_str()) {
            quote! {
                &OrderedFloat(by)
            }
        } else {
            quote! {
                &by
            }
        };
        quote! {
            pub async fn #name(&self, by: #type_) -> core::result::Result<(), WorkTableError> {
                // A non-unique predicate can delete many rows. Signal the
                // whole operation without holding its row locks all at once.
                let _bulk_mutation = self.0.lock_manager.bulk_mutation_guard();
                // Snapshot the matching rows as validated primary keys before
                // deleting anything. Storage links are not stable identities:
                // a concurrent delete can free a slot and an insert can reuse
                // it for an unrelated row before this loop runs, so resolving
                // a stale link later could delete the wrong row. Every
                // candidate is resolved and checked against the predicate at
                // snapshot time; a reused slot only stays in the set if the
                // row now living there genuinely matches. Keys are sorted for
                // a deterministic delete order (non-unique indexes iterate
                // equal keys in random-discriminator order) and the per-row
                // delete takes its own row lock and resolves by primary key,
                // never through the snapshotted link.
                let mut pks: Vec<_> = Vec::new();
                for link in self.0.indexes.#index.get(#by).map(|kv| kv.1.0) {
                    match self.0.data.select_non_ghosted(link) {
                        core::result::Result::Ok(r) => {
                            if r.#by_field == by {
                                pks.push(r.get_primary_key());
                            }
                        }
                        // The row vanished between the index read and the
                        // resolve; it is simply not part of the snapshot.
                        core::result::Result::Err(e) if e.is_row_absent() => {}
                        // Anything else (corrupt page, invalid link, ...) is a
                        // real storage error, not an empty snapshot slot.
                        core::result::Result::Err(e) => {
                            return core::result::Result::Err(WorkTableError::PagesError(e));
                        }
                    }
                }
                pks.sort_unstable();
                pks.dedup();
                for pk in pks {
                    match self.delete(pk).await {
                        core::result::Result::Ok(()) => {}
                        // Deleted concurrently after the snapshot: the goal
                        // state for this row is already reached.
                        core::result::Result::Err(WorkTableError::NotFound) => {}
                        core::result::Result::Err(e) => return core::result::Result::Err(e),
                    }
                }
                core::result::Result::Ok(())
            }
        }
    }

    fn gen_unique_delete(type_: &TokenStream, name: &Ident, index: &Ident) -> TokenStream {
        let by = if is_float(type_.to_string().as_str()) {
            quote! {
                &OrderedFloat(by)
            }
        } else {
            quote! {
                &by
            }
        };
        quote! {
            pub async fn #name(&self, by: #type_) -> core::result::Result<(), WorkTableError> {
                let row_to_update = self.0.indexes.#index.get_value(#by).map(Into::into);
                if let Some(link) = row_to_update {
                    let pk = {
                        let row = self.0.data.select_non_ghosted(link).map_err(WorkTableError::PagesError)?;
                        row.get_primary_key()
                    };
                    self.delete(pk).await?;
                }
                core::result::Result::Ok(())
            }
        }
    }
}
