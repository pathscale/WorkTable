use std::collections::HashMap;

use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::common::model::Index;
use crate::common::model::Operation;
use crate::common::name_generator::{WorktableNameGenerator, is_float};
use crate::generators::persist::PersistGenerator;

impl PersistGenerator {
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
        let row_ident = name_generator.get_row_type_ident();
        let avt_type_ident = name_generator.get_available_type_ident();
        let available_index_ident = name_generator.get_available_indexes_ident();

        let process = quote! {
            // Index removals deliberately run BEFORE the data delete: insert
            // publishes data first and indexes second, so tearing down in the
            // reverse order guarantees no index entry ever resolves to freed
            // (or reused) row storage. A failed data delete is therefore
            // compensated by restoring the index entries, not avoided by
            // reordering, which would expose that stale-entry window to every
            // concurrent reader on every delete.
            // Fully qualified: the CDC trait's AvailableTypes parameter is
            // absent from delete_row_cdc's signature, and the error path below
            // calls a method on `secondary_keys_events`, which needs the
            // receiver type resolved immediately rather than through the later
            // operation annotation.
            let (secondary_keys_events, res) =
                TableSecondaryIndexCdc::<#row_ident, #avt_type_ident, #secondary_events_ident, #available_index_ident>::delete_row_cdc(
                    self.0.indexes.as_ref(),
                    row,
                    link,
                );
            res?;
            let (_, primary_key_events) = self.0.primary_index.remove_cdc(pk.clone(), link);
            if let core::result::Result::Err(e) = self.0.data.delete(link) {
                let mut secondary_keys_events = secondary_keys_events;
                let mut primary_key_events = primary_key_events;
                // The delete failed before the row was ghosted, so its bytes
                // are normally still readable: republish every index entry
                // instead of leaving a live row unreachable through every
                // index. If even the read-back fails the entries stay
                // removed, which is exactly what the acknowledge below
                // reports.
                if let core::result::Result::Ok(row) = self.0.data.select_non_ghosted(link) {
                    let (_, restore_pk_events) = self.0.primary_index.insert_cdc(pk.clone(), link);
                    primary_key_events.extend(restore_pk_events);
                    // Fully qualified call: save_row_cdc's signature never
                    // mentions AvailableTypes, so next to the blanket
                    // `()`-events impl plain method syntax cannot infer the
                    // trait's type parameters.
                    let (restore_secondary_events, _restore_res) =
                        TableSecondaryIndexCdc::<#row_ident, #avt_type_ident, #secondary_events_ident, #available_index_ident>::save_row_cdc(
                            self.0.indexes.as_ref(),
                            row,
                            link,
                        );
                    secondary_keys_events.extend(restore_secondary_events);
                }
                // Acknowledge every CDC event that DID happen (removals plus
                // restores) so the persisted index stream keeps replaying the
                // in-memory churn instead of silently diverging until
                // restart.
                let ack_op: Operation<
                    <<#pk_ident as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
                    #pk_ident,
                    #secondary_events_ident
                > = Operation::Acknowledge(AcknowledgeOperation {
                    id: OperationId::Single(uuid::Uuid::now_v7()),
                    primary_key_events,
                    secondary_keys_events,
                });
                self.1.apply_operation(ack_op)?;
                return Err(WorkTableError::PagesError(e));
            }
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
            self.1.apply_operation(op)?;
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

    fn gen_custom_deletes(&mut self, deleted: HashMap<Ident, Operation>) -> TokenStream {
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
                self.iter_with_async(|row| {
                    if row.#field == by {
                        futures::future::Either::Left(async move {
                            self.delete::<_>(row.get_primary_key()).await
                        })
                    } else {
                        futures::future::Either::Right(async {
                            Ok(())
                        })
                    }
                }).await?;
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
                    let row = self.0.data.select_non_ghosted(link).map_err(WorkTableError::PagesError)?;
                    self.delete(row.get_primary_key()).await?;
                }
                core::result::Result::Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::{Ident, Span};
    use quote::quote;

    use crate::common::Parser;
    use crate::generators::persist::PersistGenerator;

    /// A data-delete failure is not forcible through the public API (no fail
    /// point exists in DataPages and the link was just resolved under the
    /// held row lock), so the compensation wiring is pinned on the generated
    /// tokens: index teardown stays BEFORE the data delete (the reverse of
    /// insert's publication order), and the failure path must republish the
    /// index entries and acknowledge the CDC churn.
    #[test]
    fn delete_data_failure_restores_indexes_and_acknowledges() {
        let mut parser = Parser::new(quote! {
            columns: {
                id: u64 primary_key,
                code: u64,
            }
        });
        let mut columns = parser.parse_columns().unwrap();
        let mut parser = Parser::new(quote! {
            indexes: {
                code_idx: code,
            }
        });
        columns.indexes = parser.parse_indexes().unwrap();

        let mut generator = PersistGenerator::new(Ident::new("DeleteProbe", Span::call_site()), columns, 1);
        let emitted = generator.gen_query_delete_impl().unwrap().to_string();

        // Teardown order is unchanged: secondary removal, primary removal,
        // then the data delete.
        let secondary = emitted.find("delete_row_cdc").expect("secondary removal emitted");
        let primary = emitted.find("remove_cdc (pk . clone () , link)").expect("primary removal emitted");
        let data = emitted.find(". data . delete (link)").expect("data delete emitted");
        assert!(secondary < primary && primary < data, "teardown order broken:\n{emitted}");

        // The failure path republishes both index layers and acknowledges
        // everything that happened.
        assert!(
            emitted.contains("insert_cdc (pk . clone () , link)"),
            "primary-key restore missing:\n{emitted}"
        );
        assert!(emitted.contains("save_row_cdc"), "secondary restore missing:\n{emitted}");
        assert!(
            emitted.contains("Operation :: Acknowledge"),
            "acknowledge op missing:\n{emitted}"
        );
    }
}
