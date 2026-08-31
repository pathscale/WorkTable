use proc_macro2::Literal;
use std::collections::HashMap;

use crate::common::model::{Index, Operation};
use crate::common::name_generator::{WorktableNameGenerator, is_float};
use crate::generators::persist::PersistGenerator;
use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

impl PersistGenerator {
    pub fn gen_query_update_impl(&mut self) -> syn::Result<TokenStream> {
        let custom_updates = if let Some(q) = &self.queries {
            let custom_updates = self.gen_custom_updates(q.updates.clone());

            quote! {
                #custom_updates
            }
        } else {
            quote! {}
        };
        let full_row_update = self.gen_full_row_update();

        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let table_ident = name_generator.get_work_table_ident();
        Ok(quote! {
            impl #table_ident {
                #full_row_update
                #custom_updates
            }
        })
    }

    fn gen_full_row_update(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_ident = name_generator.get_row_type_ident();
        let lock_ident = name_generator.get_lock_type_ident();
        let pk_ident = name_generator.get_primary_key_type_ident();

        let row_updates = self
            .columns
            .columns_map
            .keys()
            .map(|i| {
                quote! {
                    std::mem::swap(&mut archived.inner.#i, &mut archived_row.#i);
                }
            })
            .collect::<Vec<_>>();

        let idents: Vec<_> = self.columns.indexes.values().map(|idx| idx.field.clone()).collect();

        // No secondary indexes means no diffs to compute: pass None (as
        // gen_custom_updates does) so the generated update skips the second
        // deserialize + clone + diff HashMap machinery entirely.
        let idx_idents = if idents.is_empty() { None } else { Some(&idents) };
        let diff_process_insert = self.gen_process_diffs_insert_on_index(idents.as_slice(), idx_idents);
        let diff_process_remove = self.gen_process_diffs_remove_on_index(idx_idents);
        let persist_call = self.gen_persist_call();
        let persist_op = self.gen_persist_op();
        let full_row_lock = self.gen_full_lock_for_update();
        let const_name = name_generator.get_page_inner_size_const_ident();
        let secondary_events_ident = name_generator.get_space_secondary_index_events_ident();
        // A full-row update rewrites every column, hence every secondary
        // index; only a table with NO secondary indexes may take the
        // same-size in-place path (mirrors the in-memory generator).
        let full_row_in_place_eligible = !self.columns.is_sized && self.columns.indexes.is_empty();
        let size_check = if self.columns.is_sized {
            quote! {}
        } else {
            let in_place_attempt = if full_row_in_place_eligible {
                quote! {
                    // Same-size unsized full-row update overwrites its current
                    // slot; `update_in_place` re-validates the serialized
                    // length and we fall back to reinsert otherwise. The write
                    // is persisted as an event-less data operation.
                    let in_place_ok = unsafe {
                        self.0.data.update_in_place::<{ #const_name }>(row.clone(), link).is_ok()
                    };
                    if in_place_ok {
                        let secondary_keys_events: #secondary_events_ident = core::default::Default::default();
                        let op: Operation<
                            <<#pk_ident as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
                            #pk_ident,
                            #secondary_events_ident
                        > = Operation::Update(UpdateOperation {
                            id: OperationId::Single(uuid::Uuid::now_v7()),
                            primary_key_events: vec![],
                            secondary_keys_events,
                            bytes: self.0.data.select_raw(link)?,
                            link,
                        });
                        self.1.apply_operation(op)?;
                        return core::result::Result::Ok(());
                    }
                }
            } else {
                quote! {}
            };
            quote! {
                #in_place_attempt
                {
                    drop(_guard);
                    let pending_lock = { #full_row_lock };
                    let _guard = pending_lock.into_guard_with_mutation();
                    // Re-resolve under the re-acquired lock: the link captured
                    // before the unlock window can be stale (a concurrent
                    // reinsert moves the row; the slot may be reused).
                    let link: Link = self.0
                        .primary_index
                        .pk_map
                        .get_value(&pk)
                        .map(Into::into)
                        .ok_or(WorkTableError::NotFound)?;
                    let row_old = self.0.data.select_non_ghosted(link)?;
                    if let Err(e) = self.reinsert(row_old, row).await {

                        return Err(e);
                    }

                    return core::result::Result::Ok(());
                }
            }
        };

        // For an unsized table the size_check body handles (and returns
        // from) every path, so the archived-swap tail is emitted only for
        // sized rows; emitting both would leave unreachable code behind the
        // diverging size_check block.
        let update_body = if self.columns.is_sized {
            quote! {
                let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row).map_err(|_| WorkTableError::SerializeError)?;
                let mut archived_row = unsafe { rkyv::access_unchecked_mut::<<#row_ident as rkyv::Archive>::Archived>(&mut bytes[..]).unseal_unchecked() };

                let op_id = OperationId::Single(uuid::Uuid::now_v7());
                #diff_process_insert
                #persist_op

                unsafe { self.0.data.with_mut_ref(link, move |archived| {
                    #(#row_updates)*
                }).map_err(WorkTableError::PagesError)? };

                #diff_process_remove

                #persist_call

                core::result::Result::Ok(())
            }
        } else {
            quote! {
                #size_check
            }
        };

        quote! {
            pub async fn update(&self, row: #row_ident) -> core::result::Result<(), WorkTableError> {
                let pk = row.get_primary_key();
                let pending_lock = { #full_row_lock };
                let guard = pending_lock.into_guard_with_mutation();

                self.update_with_guard(row, guard).await
            }

            #[inline]
            async fn update_with_guard(
                &self,
                row: #row_ident,
                _guard: LockGuard<#lock_ident, #pk_ident>,
            ) -> core::result::Result<(), WorkTableError> {
                let pk = row.get_primary_key();

                let mut link: Link = self.0
                    .primary_index
                    .pk_map
                    .get_value(&pk)
                    .map(Into::into)
                    .ok_or(WorkTableError::NotFound)?;

                // Validate that the link still resolves to a live row before
                // touching anything.
                self.0.data.select_non_ghosted(link)?;

                #update_body
            }
        }
    }

    fn gen_custom_updates(&mut self, updates: HashMap<Ident, Operation>) -> TokenStream {
        let defs = updates
            .iter()
            .map(|(name, op)| {
                let snake_case_name = name.to_string().from_case(Case::Pascal).to_case(Case::Snake);
                let index = self.columns.indexes.values().find(|idx| idx.field == op.by);

                let indexes_columns: Option<Vec<_>> = {
                    let columns: Vec<_> = self
                        .columns
                        .indexes
                        .values()
                        .filter(|idx| op.columns.contains(&idx.field))
                        .map(|idx| idx.field.clone())
                        .collect();

                    if columns.is_empty() { None } else { Some(columns) }
                };
                let unsized_columns = if self.columns.is_sized {
                    None
                } else {
                    let fields = op
                        .columns
                        .iter()
                        .filter(|c| self.columns.columns_map.get(c).unwrap().to_string() == "String")
                        .collect::<Vec<_>>();
                    if fields.is_empty() { None } else { Some(fields) }
                };

                let idents = &op.columns;
                if let Some(index) = index {
                    if index.is_unique {
                        self.gen_unique_update(
                            snake_case_name,
                            name,
                            index,
                            idents,
                            indexes_columns.as_ref(),
                            unsized_columns,
                        )
                    } else {
                        self.gen_non_unique_update(
                            snake_case_name,
                            name,
                            index,
                            idents,
                            indexes_columns.as_ref(),
                            unsized_columns,
                        )
                    }
                } else if self.columns.primary_keys.len() == 1 {
                    if *self.columns.primary_keys.first().unwrap() == op.by {
                        self.gen_pk_update(snake_case_name, name, idents, indexes_columns.as_ref(), unsized_columns)
                    } else {
                        todo!()
                    }
                } else {
                    todo!()
                }
            })
            .collect::<Vec<_>>();

        quote! {
            #(#defs)*
        }
    }

    fn gen_persist_call(&self) -> TokenStream {
        quote! {
            if let Operation::Update(op) = &mut op {
                 op.bytes = self.0.data.select_raw(link)?;
            } else {
                unreachable!("")
            };
            self.1.apply_operation(op)?;
        }
    }

    fn gen_size_check(
        &self,
        unsized_fields: Option<Vec<&Ident>>,
        idents: &[Ident],
        idx_idents: Option<&Vec<Ident>>,
    ) -> TokenStream {
        // Port of the in-memory generator's gen_size_check: the in-place fast
        // path bypasses the index diff machinery, so it only applies when no
        // updated column is indexed; and it must persist the same-slot write
        // as an event-less data operation.
        let touches_index = idx_idents.map(|v| !v.is_empty()).unwrap_or(false);
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let secondary_events_ident = name_generator.get_space_secondary_index_events_ident();
        let primary_key_ident = name_generator.get_primary_key_type_ident();
        if let (Some(f), false) = (unsized_fields, touches_index) {
            let fields_check: Vec<_> = f
                .iter()
                .map(|f| {
                    let fn_ident = Ident::new(format!("get_{f}_size").as_str(), Span::call_site());
                    quote! {
                        need_to_reinsert |= archived_row.#fn_ident() != self.#fn_ident(link)?;
                    }
                })
                .collect();
            let row_updates = idents
                .iter()
                .map(|i| {
                    quote! {
                        row_new.#i = row.#i.clone();
                    }
                })
                .collect::<Vec<_>>();
            let full_row_lock = self.gen_full_lock_for_update();
            let const_name = name_generator.get_page_inner_size_const_ident();

            quote! {
                // Reinsert ONLY when an unsized field's serialized size
                // changed. `need_to_reinsert` starts false; the old `true`
                // initializer made these per-field size checks dead and forced
                // every unsized update through a full delete-and-reinsert.
                let mut need_to_reinsert = false;
                #(#fields_check)*

                {
                    // Serialize the whole read-modify-write against other
                    // updates of this key by holding the full-row lock, as the
                    // in-memory generator does.
                    drop(_guard);
                    let pending_lock = { #full_row_lock };
                    let _guard = pending_lock.into_guard_with_mutation();

                    let row_old = self.0.select(pk.clone()).ok_or(WorkTableError::NotFound)?;
                    let mut row_new = row_old.clone();
                    #(#row_updates)*

                    if need_to_reinsert {
                        if let Err(e) = self.reinsert(row_old, row_new).await {
                            return Err(e);
                        }
                        return core::result::Result::Ok(());
                    }

                    // Same-size write at the CURRENT slot, re-resolved under
                    // the held full-row lock. `update_in_place` re-validates
                    // the exact slot length and errors on mismatch; the
                    // reinsert fallback below keeps correctness.
                    let current_link: Link = self.0
                        .primary_index
                        .pk_map
                        .get_value(&pk)
                        .map(Into::into)
                        .ok_or(WorkTableError::NotFound)?;
                    let in_place_ok = unsafe {
                        self.0.data.update_in_place::<{ #const_name }>(row_new.clone(), current_link).is_ok()
                    };
                    if in_place_ok {
                        // Persist the same-slot write as an event-less data
                        // operation (no indexed column was touched).
                        let secondary_keys_events: #secondary_events_ident = core::default::Default::default();
                        let op: Operation<
                            <<#primary_key_ident as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
                            #primary_key_ident,
                            #secondary_events_ident
                        > = Operation::Update(UpdateOperation {
                            id: OperationId::Single(uuid::Uuid::now_v7()),
                            primary_key_events: vec![],
                            secondary_keys_events,
                            bytes: self.0.data.select_raw(current_link)?,
                            link: current_link,
                        });
                        self.1.apply_operation(op)?;
                        return core::result::Result::Ok(());
                    }

                    if let Err(e) = self.reinsert(row_old, row_new).await {
                        return Err(e);
                    }
                    return core::result::Result::Ok(());
                }
            }
        } else if self.columns.is_sized {
            // A fixed-size row can always update archived fields in place.
            quote! {}
        } else if touches_index {
            // Updating an indexed column must keep the index-maintaining
            // reinsert path, regardless of the row's storage shape.
            let row_updates = idents
                .iter()
                .map(|i| quote! { row_new.#i = row.#i.clone(); })
                .collect::<Vec<_>>();
            let full_row_lock = self.gen_full_lock_for_update();
            quote! {
                {
                    drop(_guard);
                    let pending_lock = { #full_row_lock };
                    let _guard = pending_lock.into_guard_with_mutation();

                    let row_old = self.0.select(pk.clone()).ok_or(WorkTableError::NotFound)?;
                    let mut row_new = row_old.clone();
                    #(#row_updates)*
                    if let Err(e) = self.reinsert(row_old, row_new).await {
                        return Err(e);
                    }
                    return core::result::Result::Ok(());
                }
            }
        } else {
            // Other columns make the row unsized, but this query updates only
            // fixed-width, unindexed fields: the archived swap is safe.
            quote! {}
        }
    }

    fn gen_persist_op(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let secondary_events_ident = name_generator.get_space_secondary_index_events_ident();
        let primary_key_ident = name_generator.get_primary_key_type_ident();

        quote! {
            let mut op: Operation<
                <<#primary_key_ident as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
                #primary_key_ident,
                #secondary_events_ident
            > = Operation::Update(UpdateOperation {
                id: op_id,
                primary_key_events: vec![],
                secondary_keys_events,
                bytes: updated_bytes,
                link,
            });
        }
    }

    fn gen_process_diffs_insert_on_index(&self, idents: &[Ident], idx_idents: Option<&Vec<Ident>>) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_type_ident = name_generator.get_available_type_ident();
        let diff_container = if idx_idents.is_some() {
            quote! {
                let row_old = self.0.data.select_non_ghosted(link)?;
                let row_new = row.clone();
                let updated_bytes: Vec<u8> = vec![];
                let mut diffs: std::collections::HashMap<&str, Difference<#avt_type_ident>> = std::collections::HashMap::new();
            }
        } else {
            quote! {
                let updated_bytes: Vec<u8> = vec![];
            }
        };

        let diff = if let Some(idx_idents) = idx_idents {
            idents
                .iter()
                .filter(|i| idx_idents.contains(i))
                .map(|i| {
                    let diff_key = Literal::string(i.to_string().as_str());
                    quote! {
                        let old = &row_old.#i;
                        let new = &row_new.#i;

                        if old != new {
                            let diff = Difference::<#avt_type_ident> {
                                old: old.clone().into(),
                                new: new.clone().into(),
                            };

                            diffs.insert(#diff_key, diff);
                        }
                    }
                })
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        let process_difference = {
            let secondary_events_ident = name_generator.get_space_secondary_index_events_ident();
            if idx_idents.is_some() {
                quote! {
                    let (secondary_events, indexes_res): (#secondary_events_ident, _) = self.0.indexes.process_difference_insert_cdc(link, diffs.clone());
                    if let Err(e) = indexes_res {
                        return match e {
                            IndexError::AlreadyExists {
                                at,
                                inserted_already,
                            } => {
                                let (rollback_secondary_events, _): (#secondary_events_ident, _) = self.0.indexes.delete_from_indexes_cdc(
                                    row_new.merge(row_old.clone()),
                                    link,
                                    inserted_already
                                );

                                let mut merged_events = secondary_events.clone();
                                merged_events.extend(rollback_secondary_events);

                                let ack_op = Operation::Acknowledge(AcknowledgeOperation {
                                    id: OperationId::Single(uuid::Uuid::now_v7()),
                                    primary_key_events: vec![],
                                    secondary_keys_events: merged_events,
                                });
                                self.1.apply_operation(ack_op)?;

                                Err(WorkTableError::AlreadyExists(at.to_string_value()))
                            }
                            IndexError::NotFound => Err(WorkTableError::NotFound),
                        };
                    }
                    let mut secondary_keys_events = secondary_events;
                }
            } else {
                quote! {
                    let secondary_keys_events: #secondary_events_ident = core::default::Default::default();
                }
            }
        };

        quote! {
            #diff_container
            #(#diff)*
            #process_difference
        }
    }

    fn gen_process_diffs_remove_on_index(&self, idx_idents: Option<&Vec<Ident>>) -> TokenStream {
        if idx_idents.is_some() {
            quote! {
                let (secondary_keys_events_remove, res) = self.0.indexes.process_difference_remove_cdc(link, diffs);
                res?;
                op.extend_secondary_key_events(secondary_keys_events_remove);
            }
        } else {
            quote! {}
        }
    }

    fn gen_pk_update(
        &self,
        snake_case_name: String,
        name: &Ident,
        idents: &[Ident],
        idx_idents: Option<&Vec<Ident>>,
        unsized_fields: Option<Vec<&Ident>>,
    ) -> TokenStream {
        let pk_ident = &self.pk.as_ref().unwrap().ident;
        let method_ident = Ident::new(format!("update_{snake_case_name}").as_str(), Span::mixed_site());
        let query_ident = Ident::new(format!("{name}Query").as_str(), Span::mixed_site());
        let lock_ident = WorktableNameGenerator::get_update_query_lock_ident(&snake_case_name);

        let row_updates = idents
            .iter()
            .map(|i| {
                quote! {
                    std::mem::swap(&mut archived.inner.#i, &mut archived_row.#i);
                }
            })
            .collect::<Vec<_>>();

        // Same gate as the in-memory generator: when the size_check body
        // handles (and returns from) every path, emitting the archived-swap
        // tail too would leave unreachable code.
        let archived_swap_is_safe = self.columns.is_sized || (unsized_fields.is_none() && idx_idents.is_none());
        let size_check = self.gen_size_check(unsized_fields, idents, idx_idents);
        let diff_process_insert = self.gen_process_diffs_insert_on_index(idents, idx_idents);
        let diff_process_remove = self.gen_process_diffs_remove_on_index(idx_idents);
        let persist_call = self.gen_persist_call();
        let persist_op = self.gen_persist_op();
        let custom_lock = self.gen_custom_lock_for_update(lock_ident);

        let finish_update = if archived_swap_is_safe {
            quote! {
                let op_id = OperationId::Single(uuid::Uuid::now_v7());
                #diff_process_insert
                #persist_op

                unsafe { self.0.data.with_mut_ref(link, |archived| {
                    #(#row_updates)*
                }).map_err(WorkTableError::PagesError)? };

                #diff_process_remove

                #persist_call

                core::result::Result::Ok(())
            }
        } else {
            quote! {}
        };

        quote! {
            pub async fn #method_ident<Pk>(&self, row: #query_ident, pk: Pk) -> core::result::Result<(), WorkTableError>
            where #pk_ident: From<Pk>
            {
                let pk: #pk_ident = pk.into();
                let pending_lock = { #custom_lock };
                let _guard = pending_lock.into_guard_with_mutation();

                let mut link: Link = self.0
                        .primary_index
                        .pk_map
                        .get_value(&pk)
                        .map(Into::into)
                        .ok_or(WorkTableError::NotFound)?;

                let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row).map_err(|_| WorkTableError::SerializeError)?;
                let mut archived_row = unsafe { rkyv::access_unchecked_mut::<<#query_ident as rkyv::Archive>::Archived>(&mut bytes[..]).unseal_unchecked() };

                #size_check
                #finish_update
            }
        }
    }

    fn gen_non_unique_update(
        &self,
        snake_case_name: String,
        name: &Ident,
        index: &Index,
        idents: &[Ident],
        idx_idents: Option<&Vec<Ident>>,
        unsized_fields: Option<Vec<&Ident>>,
    ) -> TokenStream {
        let by_field = &index.field;
        let index = &index.name;
        let method_ident = Ident::new(format!("update_{snake_case_name}").as_str(), Span::mixed_site());

        let query_ident = Ident::new(format!("{name}Query").as_str(), Span::mixed_site());
        let by_ident = Ident::new(format!("{name}By").as_str(), Span::mixed_site());

        let row_updates = idents
            .iter()
            .map(|i| {
                quote! {
                    std::mem::swap(&mut archived.inner.#i, &mut archived_row.#i);
                }
            })
            .collect::<Vec<_>>();

        // When the query touches an unsized field the size_check body ends
        // every loop iteration itself (in-place or reinsert, then continue),
        // so the archived-swap tail is only emitted otherwise; emitting both
        // would leave unreachable code after the size_check block.
        let has_unsized = unsized_fields.is_some();
        let size_check = if let Some(f) = unsized_fields {
            let fields_check: Vec<_> = f
                .iter()
                .map(|f| {
                    let fn_ident = Ident::new(format!("get_{f}_size").as_str(), Span::call_site());
                    quote! {
                        need_to_reinsert |= archived_row.#fn_ident() != self.#fn_ident(link)?;
                    }
                })
                .collect();
            let row_updates = idents
                .iter()
                .map(|i| {
                    quote! {
                        row_new.#i = row.#i.clone();
                    }
                })
                .collect::<Vec<_>>();
            let touches_index = idx_idents.map(|v| !v.is_empty()).unwrap_or(false);
            let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
            let const_name = name_generator.get_page_inner_size_const_ident();
            let secondary_events_ident = name_generator.get_space_secondary_index_events_ident();
            let primary_key_ident = name_generator.get_primary_key_type_ident();
            // The full-row lock for this key is already held: every matched
            // key was locked up front, in sorted order (see the guards loop).
            if touches_index {
                // Updating an indexed column must keep the index-maintaining
                // reinsert path unconditionally.
                quote! {
                    {
                        let row_old = self.0.select(pk.clone()).ok_or(WorkTableError::NotFound)?;
                        let mut row_new = row_old.clone();
                        #(#row_updates)*
                        if let Err(e) = self.reinsert(row_old, row_new).await {
                            return Err(e);
                        }

                        guards.remove(&pk);
                        continue;
                    }
                }
            } else {
                quote! {
                    // Reinsert ONLY when an unsized field's serialized size
                    // changed. `need_to_reinsert` starts false; the old `true`
                    // initializer made these per-field size checks dead and
                    // forced every update through a full delete-and-reinsert.
                    let mut need_to_reinsert = false;
                    #(#fields_check)*
                    {
                        let row_old = self.0.select(pk.clone()).ok_or(WorkTableError::NotFound)?;
                        let mut row_new = row_old.clone();
                        #(#row_updates)*
                        if !need_to_reinsert {
                            // Same-size write at the CURRENT slot: re-serialize
                            // the full rebuilt row and overwrite the slot bytes
                            // (NOT a mem::swap of archived fields, which would
                            // dangle out-of-line String pointers).
                            // `update_in_place` re-validates the exact slot
                            // length and errors on mismatch, so the reinsert
                            // fallback below keeps correctness.
                            let in_place_ok = unsafe {
                                self.0.data.update_in_place::<{ #const_name }>(row_new.clone(), link).is_ok()
                            };
                            if in_place_ok {
                                // Persist the same-slot write as an event-less
                                // data operation, grouped under this multi-row
                                // operation id.
                                let secondary_keys_events: #secondary_events_ident = core::default::Default::default();
                                let op: Operation<
                                    <<#primary_key_ident as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
                                    #primary_key_ident,
                                    #secondary_events_ident
                                > = Operation::Update(UpdateOperation {
                                    id: op_id,
                                    primary_key_events: vec![],
                                    secondary_keys_events,
                                    bytes: self.0.data.select_raw(link)?,
                                    link,
                                });
                                self.1.apply_operation(op)?;
                                guards.remove(&pk);
                                continue;
                            }
                        }
                        if let Err(e) = self.reinsert(row_old, row_new).await {
                            return Err(e);
                        }

                        guards.remove(&pk);
                        continue;
                    }
                }
            }
        } else {
            quote! {}
        };
        let diff_process_insert = self.gen_process_diffs_insert_on_index(idents, idx_idents);
        let diff_process_remove = self.gen_process_diffs_remove_on_index(idx_idents);
        let persist_call = self.gen_persist_call();
        let persist_op = self.gen_persist_op();
        let by = if is_float(by_ident.to_string().as_str()) {
            quote! {
                &OrderedFloat(by)
            }
        } else {
            quote! {
                &by
            }
        };
        let full_row_lock = self.gen_full_lock_for_update();

        let loop_tail = if has_unsized {
            quote! {}
        } else {
            quote! {
                    #diff_process_insert
                    #persist_op

                    unsafe {
                        self.0.data.with_mut_ref(link, |archived| {
                            #(#row_updates)*
                        }).map_err(WorkTableError::PagesError)?;
                    }

                    #diff_process_remove

                    #persist_call

                    guards.remove(&pk);
            }
        };

        quote! {
            pub async fn #method_ident(&self, row: #query_ident, by: #by_ident) -> core::result::Result<(), WorkTableError> {
                // Snapshot the matching rows' primary keys once; the same set
                // is locked and then processed. Locking one index scan and
                // processing a fresh second scan would let rows that joined the
                // range in between be processed without a held lock. The keys
                // are sorted so concurrent multi-row operations acquire row
                // locks in one global order (a non-unique index iterates equal
                // keys in random-discriminator order, which would otherwise
                // also make the partially-updated subset on a mid-way failure
                // nondeterministic).
                let mut pks: Vec<_> = Vec::new();
                for link in self.0.indexes.#index.get(#by).map(|(_, l)| l.0) {
                    match self.0.data.select_non_ghosted(link) {
                        core::result::Result::Ok(r) => pks.push(r.get_primary_key()),
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

                let mut guards: std::collections::HashMap<_, _> = std::collections::HashMap::new();
                // Full-row locks, not per-column custom locks: each row's
                // unsized reinsert path mutates the whole row under these
                // guards, and one uniform lock kind keeps every concurrent
                // multi-row update acquiring in the same sorted-key order.
                for pk in pks.iter() {
                    let pk = pk.clone();
                    let pending_lock = { #full_row_lock };
                    guards.insert(pk.clone(), pending_lock.into_guard());
                }

                let op_id = OperationId::Multi(uuid::Uuid::now_v7());
                for pk in pks.into_iter() {
                    // Re-resolve and re-validate under the held lock. The
                    // query's lock set includes the predicate column, so the
                    // value read here cannot be rewritten concurrently while
                    // the lock is held. Rows deleted or updated out of the
                    // matched range before their lock was acquired are
                    // skipped; rows that joined the range after the snapshot
                    // are not touched.
                    let link: Link = match self.0.primary_index.pk_map.get_value(&pk) {
                        Some(v) => v.into(),
                        None => continue,
                    };
                    if self.0.data.select_non_ghosted(link)?.#by_field != by {
                        continue;
                    }
                    let _mutation_guard = self.0.lock_manager.mutation_guard(&pk);
                    let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row)
                        .map_err(|_| WorkTableError::SerializeError)?;

                    let mut archived_row = unsafe {
                        rkyv::access_unchecked_mut::<<#query_ident as rkyv::Archive>::Archived>(&mut bytes[..])
                            .unseal_unchecked()
                    };

                    #size_check
                    #loop_tail
                }
                core::result::Result::Ok(())
            }
        }
    }

    fn gen_unique_update(
        &self,
        snake_case_name: String,
        name: &Ident,
        index: &Index,
        idents: &[Ident],
        idx_idents: Option<&Vec<Ident>>,
        unsized_fields: Option<Vec<&Ident>>,
    ) -> TokenStream {
        let by_field = &index.field;
        let by_is_float = is_float(
            self.columns
                .columns_map
                .get(&index.field)
                .expect("indexed column exists")
                .to_string()
                .as_str(),
        );
        let index = &index.name;
        let method_ident = Ident::new(format!("update_{snake_case_name}").as_str(), Span::mixed_site());

        let query_ident = Ident::new(format!("{name}Query").as_str(), Span::mixed_site());
        let by_ident = Ident::new(format!("{name}By").as_str(), Span::mixed_site());
        let lock_ident = WorktableNameGenerator::get_update_query_lock_ident(&snake_case_name);

        let row_updates = idents
            .iter()
            .map(|i| {
                quote! {
                    std::mem::swap(&mut archived.inner.#i, &mut archived_row.#i);
                }
            })
            .collect::<Vec<_>>();
        // Same gate as the in-memory generator: when the size_check body
        // handles (and returns from) every path, emitting the archived-swap
        // tail too would leave unreachable code.
        let archived_swap_is_safe = self.columns.is_sized || (unsized_fields.is_none() && idx_idents.is_none());
        let size_check = self.gen_size_check(unsized_fields, idents, idx_idents);
        let diff_process_insert = self.gen_process_diffs_insert_on_index(idents, idx_idents);
        let diff_process_remove = self.gen_process_diffs_remove_on_index(idx_idents);
        let persist_call = self.gen_persist_call();
        let persist_op = self.gen_persist_op();
        let by = if by_is_float {
            quote! {
                &OrderedFloat(by)
            }
        } else {
            quote! {
                &by
            }
        };
        // Verify, under the lock, that the row resolved BY PRIMARY KEY still
        // carries the queried unique value, with the index key's equality
        // semantics (OrderedFloat for floats, so NaN behaves like the index).
        let by_match_check = if by_is_float {
            quote! {
                if OrderedFloat(self.0.data.select_non_ghosted(link)?.#by_field) != OrderedFloat(by) {
                    return Err(WorkTableError::NotFound);
                }
            }
        } else {
            quote! {
                if self.0.data.select_non_ghosted(link)?.#by_field != by {
                    return Err(WorkTableError::NotFound);
                }
            }
        };
        let custom_lock = self.gen_custom_lock_for_update(lock_ident);

        let finish_update = if archived_swap_is_safe {
            quote! {
                let op_id = OperationId::Single(uuid::Uuid::now_v7());
                #diff_process_insert
                #persist_op

                unsafe {
                    self.0.data.with_mut_ref(link, |archived| {
                        #(#row_updates)*
                    }).map_err(WorkTableError::PagesError)?;
                }

                #diff_process_remove

                #persist_call

                core::result::Result::Ok(())
            }
        } else {
            quote! {}
        };

        quote! {
            pub async fn #method_ident(&self, row: #query_ident, by: #by_ident) -> core::result::Result<(), WorkTableError> {
                 let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row)
                    .map_err(|_| WorkTableError::SerializeError)?;

                let mut archived_row = unsafe {
                    rkyv::access_unchecked_mut::<<#query_ident as rkyv::Archive>::Archived>(&mut bytes[..])
                        .unseal_unchecked()
                };

                let mut link: Link = self.0.indexes
                    .#index
                    .get_value(#by)
                    .map(Into::into)
                    .ok_or(WorkTableError::NotFound)?;

                let pk = self.0.data.select_non_ghosted(link)?.get_primary_key().clone();

                let pending_lock = { #custom_lock };
                let _guard = pending_lock.into_guard_with_mutation();

                // Re-resolve through pk_map BY THE LOCKED PK, not by value: the
                // queried unique value can move to a different row between the
                // unlocked read above and the lock acquisition, and a by-value
                // lookup here would mutate whatever row now carries it.
                let link = {
                    let mut vacuum_retries = 0u32;
                    loop {
                        let link: Link = self.0
                            .primary_index
                            .pk_map
                            .get_value(&pk)
                            .map(Into::into)
                            .ok_or(WorkTableError::NotFound)?;
                        match self.0.data.select_non_vacuumed(link) {
                            core::result::Result::Ok(_) => break link,
                            core::result::Result::Err(e) if e.is_vacuumed() => {
                                // Bounded cooperative retry, mirroring the
                                // 64-attempt cap used by select.
                                if vacuum_retries >= 64 {
                                    return Err(WorkTableError::NotFound);
                                }
                                vacuum_retries += 1;
                                tokio::task::yield_now().await;
                            }
                            core::result::Result::Err(e) => return Err(e.into()),
                        }
                    }
                };
                #by_match_check

                #size_check
                #finish_update
            }
        }
    }
}
