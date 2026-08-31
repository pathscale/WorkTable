use proc_macro2::Literal;
use std::collections::HashMap;

use crate::common::model::{Index, Operation};
use crate::common::name_generator::{WorktableNameGenerator, is_float};
use crate::generators::in_memory::InMemoryGenerator;
use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

impl InMemoryGenerator {
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

        let diff_process_insert = self.gen_process_diffs_insert_on_index(idents.as_slice(), Some(&idents));
        let diff_process_remove = self.gen_process_diffs_remove_on_index(Some(&idents));
        let persist_call = self.gen_persist_call();
        let persist_op = self.gen_persist_op();
        let full_row_lock = self.gen_full_lock_for_update();
        // A full-row `update(row)` replaces EVERY column, so it inherently
        // rewrites every secondary index. The in-place fast path only applies
        // when no updated field is indexed (it emits no index diff), so a
        // full-row update on a table with any secondary index must reinsert.
        // Only a table with NO secondary indexes and an unsized column can use
        // the in-place same-size path here. (The custom single-column updates —
        // gen_unique_update / gen_pk_update — get the fast path via
        // gen_size_check; gen_non_unique_update updates a non-unique-indexed
        // column and therefore always reinserts, correctly.)
        let const_name = name_generator.get_page_inner_size_const_ident();
        let full_row_in_place_eligible = !self.columns.is_sized && self.columns.indexes.is_empty();
        let update_body = if self.columns.is_sized {
            quote! {
                let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row)
                    .map_err(|_| WorkTableError::SerializeError)?;
                let mut archived_row = unsafe {
                    rkyv::access_unchecked_mut::<<#row_ident as rkyv::Archive>::Archived>(&mut bytes[..])
                        .unseal_unchecked()
                };

                #diff_process_insert
                #persist_op

                unsafe {
                    self.0
                        .data
                        .with_mut_ref(link, move |archived| {
                            #(#row_updates)*
                        })
                        .map_err(WorkTableError::PagesError)?
                };

                #diff_process_remove

                self.0.update_state.remove(&pk);

                #persist_call

                core::result::Result::Ok(())
            }
        } else if full_row_in_place_eligible {
            quote! {
                // No secondary indexes: same-size unsized full-row update may go
                // in place at the current slot. `update_in_place` re-validates
                // the serialized length and we fall back to reinsert otherwise.
                let in_place_ok = unsafe {
                    self.0.data.update_in_place::<{ #const_name }>(row.clone(), link).is_ok()
                };
                if in_place_ok {
                    self.0.update_state.remove(&pk);
                    return core::result::Result::Ok(());
                }
                drop(_guard);
                let pending_lock = { #full_row_lock };
                let _guard = pending_lock.into_guard_with_mutation();
                let row_old = self.0.data.select_non_ghosted(link)?;
                if let Err(e) = self.reinsert(row_old, row).await {
                    self.0.update_state.remove(&pk);
                    return Err(e);
                }
                self.0.update_state.remove(&pk);
                core::result::Result::Ok(())
            }
        } else {
            quote! {
                drop(_guard);
                let pending_lock = { #full_row_lock };
                let _guard = pending_lock.into_guard_with_mutation();
                let row_old = self.0.data.select_non_ghosted(link)?;
                if let Err(e) = self.reinsert(row_old, row).await {
                    self.0.update_state.remove(&pk);

                    return Err(e);
                }

                self.0.update_state.remove(&pk);

                core::result::Result::Ok(())
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

                let row_old = self.0.data.select_non_ghosted(link)?;
                self.0.update_state.insert(pk.clone(), row_old);

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
                    let index_name = &index.name;

                    if index.is_unique {
                        self.gen_unique_update(
                            snake_case_name,
                            name,
                            index_name,
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
        if false {
            quote! {
                if let Operation::Update(op) = &mut op {
                     op.bytes = self.0.data.select_raw(link)?;
                } else {
                    unreachable!("")
                };
                self.1.apply_operation(op);
            }
        } else {
            quote! {}
        }
    }

    fn gen_size_check(
        &self,
        unsized_fields: Option<Vec<&Ident>>,
        idents: &[Ident],
        idx_idents: Option<&Vec<Ident>>,
    ) -> TokenStream {
        // The in-place fast path re-serializes the row directly into its slot and
        // republishes it, bypassing the generated secondary-index diff. That is
        // only safe when NONE of the updated columns are indexed; an update that
        // touches an indexed column must keep the index-maintaining reinsert
        // path (and its unique-constraint check). Fall back to always-reinsert in
        // that case.
        let touches_index = idx_idents.map(|v| !v.is_empty()).unwrap_or(false);
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
            let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
            let const_name = name_generator.get_page_inner_size_const_ident();

            quote! {
                // Reinsert ONLY when an unsized field's serialized size CHANGED,
                // so the row no longer fits its slot region. A same-size update
                // (the common case) is written in place at the SAME slot below.
                // `need_to_reinsert` starts false; the old `true` initializer
                // forced every unsized update through a full delete-and-reinsert.
                let mut need_to_reinsert = false;
                #(#fields_check)*

                {
                    // Serialize the whole read-modify-write against other
                    // updates of this key by holding the full-row lock (the
                    // reinsert path does the same). The original query lock only
                    // covers this query's columns, so two different-column
                    // updates could otherwise each rebuild the row from a stale
                    // snapshot and lose each other's write.
                    drop(_guard);
                    let pending_lock = { #full_row_lock };
                    let _guard = pending_lock.into_guard_with_mutation();

                    // Re-read the current row UNDER the full-row lock so the
                    // rebuilt row reflects any committed concurrent update.
                    let row_old = self.0.select(pk.clone()).ok_or(WorkTableError::NotFound)?;
                    let mut row_new = row_old.clone();
                    #(#row_updates)*

                    if need_to_reinsert {
                        if let Err(e) = self.reinsert(row_old, row_new).await {
                            self.0.update_state.remove(&pk);

                            return Err(e);
                        }

                        self.0.update_state.remove(&pk);
                        return core::result::Result::Ok(());
                    }

                    // Same-size in-place write at the CURRENT slot. Re-serialize
                    // the full rebuilt row (only the changed fields differ) and
                    // overwrite the slot's bytes so any out-of-line `String`
                    // field's archived pointer resolves within the slot (NOT a
                    // `mem::swap`, which would dangle the pointer), then republish
                    // the row as live.
                    // Re-resolve the link under the held full-row lock. The
                    // earlier size check (#fields_check) read field sizes from the
                    // link captured before the lock, which a concurrent reinsert
                    // could have moved. CORRECTNESS DOES NOT rely on that size
                    // decision being current: `update_in_place` re-validates that
                    // the serialized row is EXACTLY the slot length and returns
                    // Err on mismatch, and we fall back to a full reinsert below.
                    // Do not remove that length re-check or this fallback.
                    let current_link: Link = self.0
                        .primary_index
                        .pk_map
                        .get_value(&pk)
                        .map(Into::into)
                        .ok_or(WorkTableError::NotFound)?;
                    // Equal field sizes do not guarantee an equal TOTAL serialized
                    // length (alignment), so a same-slot write may not fit — fall
                    // back to a full reinsert rather than fail. Correctness-first.
                    // The clone is paid only so `row_new` survives for that rare
                    // reinsert fallback; the common in-place path drops it.
                    let in_place_ok = unsafe {
                        self.0.data.update_in_place::<{ #const_name }>(row_new.clone(), current_link).is_ok()
                    };
                    if in_place_ok {
                        self.0.update_state.remove(&pk);
                        return core::result::Result::Ok(());
                    }

                    // `update_in_place` checks serialization and exact slot
                    // length before touching page bytes, so its error path
                    // leaves this locked snapshot authoritative for fallback.
                    if let Err(e) = self.reinsert(row_old, row_new).await {
                        self.0.update_state.remove(&pk);
                        return Err(e);
                    }
                    self.0.update_state.remove(&pk);
                    return core::result::Result::Ok(());
                }
            }
        } else if self.columns.is_sized {
            // A fixed-size row can always update archived fields in place. If
            // one is indexed, finish_update applies the corresponding index
            // diffs around that mutation.
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
                        self.0.update_state.remove(&pk);
                        return Err(e);
                    }

                    self.0.update_state.remove(&pk);
                    return core::result::Result::Ok(());
                }
            }
        } else {
            // Other columns make the row unsized, but this query updates only
            // fixed-width, unindexed fields. The caller can safely swap those
            // archived fields in place without rebuilding the full row.
            quote! {}
        }
    }

    fn gen_persist_op(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let secondary_events_ident = name_generator.get_space_secondary_index_events_ident();
        let primary_key_ident = name_generator.get_primary_key_type_ident();

        if false {
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
        } else {
            quote! {}
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

        let process_difference = if false {
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
                                // Generate rollback CDC events for secondary indexes
                                let (rollback_secondary_events, _): (#secondary_events_ident, _) = self.0.indexes.delete_from_indexes_cdc(
                                    row_new.merge(row_old.clone()),
                                    link,
                                    inserted_already
                                );

                                // Merge original partial insert events with rollback events
                                let mut merged_events = secondary_events.clone();
                                merged_events.extend(rollback_secondary_events);

                                // Create AcknowledgeOperation with all events
                                let ack_op = Operation::Acknowledge(AcknowledgeOperation {
                                    id: OperationId::Single(uuid::Uuid::now_v7()),
                                    primary_key_events: vec![],  // Updates don't modify primary key
                                    secondary_keys_events: merged_events,
                                });
                                self.1.apply_operation(ack_op);

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
        } else if idx_idents.is_some() {
            quote! {
                let indexes_res = self.0.indexes.process_difference_insert(link, diffs.clone());
                if let Err(e) = indexes_res {
                    return match e {
                        IndexError::AlreadyExists {
                            at,
                            inserted_already,
                        } => {
                            self.0.indexes
                                .delete_from_indexes(row_new.merge(row_old.clone()), link, inserted_already)?;

                            Err(WorkTableError::AlreadyExists(at.to_string_value()))
                        }
                        IndexError::NotFound => Err(WorkTableError::NotFound),
                    };
                }
            }
        } else {
            quote! {}
        };

        quote! {
            #diff_container
            #(#diff)*
            #process_difference
        }
    }

    fn gen_process_diffs_remove_on_index(&self, idx_idents: Option<&Vec<Ident>>) -> TokenStream {
        let process_difference = if false {
            if idx_idents.is_some() {
                quote! {
                    let (secondary_keys_events_remove, res) = self.0.indexes.process_difference_remove_cdc(link, diffs);
                    res?;
                    op.extend_secondary_key_events(secondary_keys_events_remove);
                }
            } else {
                quote! {}
            }
        } else if idx_idents.is_some() {
            quote! {
                self.0.indexes.process_difference_remove(link, diffs)?;
            }
        } else {
            quote! {}
        };

        quote! {
            #process_difference
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

        let archived_swap_is_safe = self.columns.is_sized || (unsized_fields.is_none() && idx_idents.is_none());
        let size_check = self.gen_size_check(unsized_fields, idents, idx_idents);
        let diff_process_insert = self.gen_process_diffs_insert_on_index(idents, idx_idents);
        let diff_process_remove = self.gen_process_diffs_remove_on_index(idx_idents);
        let persist_call = self.gen_persist_call();
        let persist_op = self.gen_persist_op();
        let custom_lock = self.gen_custom_lock_for_update(lock_ident);

        let finish_update = if archived_swap_is_safe {
            quote! {
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
            quote! {
                let mut need_to_reinsert = true;
                #(#fields_check)*
                if need_to_reinsert {
                    // The full-row lock for this key is already held: every
                    // matched key was locked up front, in sorted order. The
                    // old drop/re-acquire dance re-locked this key while later
                    // keys' guards were still held, inverting the lock order
                    // against a concurrent overlapping multi-row update.
                    let row_old = self.0.select(pk.clone()).ok_or(WorkTableError::NotFound)?;
                    let mut row_new = row_old.clone();
                    #(#row_updates)*
                    if let Err(e) = self.reinsert(row_old, row_new).await {
                        self.0.update_state.remove(&pk);
                        return Err(e);
                    }

                    guards.remove(&pk);
                    continue;
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
                core::result::Result::Ok(())
            }
        }
    }

    fn gen_unique_update(
        &self,
        snake_case_name: String,
        name: &Ident,
        index: &Ident,
        idents: &[Ident],
        idx_idents: Option<&Vec<Ident>>,
        unsized_fields: Option<Vec<&Ident>>,
    ) -> TokenStream {
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
        let size_check = self.gen_size_check(unsized_fields, idents, idx_idents);
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
        let custom_lock = self.gen_custom_lock_for_update(lock_ident);

        let finish_update = if self.columns.is_sized {
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

                let link = loop {
                    let link = self.0.indexes.#index
                        .get_value(#by)
                        .map(Into::into)
                        .ok_or(WorkTableError::NotFound)?;

                    if let Err(e) = self.0.data.select_non_vacuumed(link) {
                        if e.is_vacuumed() {
                            continue;
                        }
                        return Err(e.into());
                    } else  {
                        break link;
                    }
                };

                #size_check
                #finish_update
            }
        }
    }
}
