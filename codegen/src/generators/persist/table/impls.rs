use convert_case::{Case, Casing};
use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;

use crate::common::model::GeneratorType;
use crate::common::name_generator::{WorktableNameGenerator, is_float, is_unsized_vec};
use crate::generators::persist::PersistGenerator;

impl PersistGenerator {
    pub fn gen_table_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();

        let persisted_impl = self.gen_table_new_fn();
        let name_fn = self.gen_table_name_fn();
        let version_fn = self.gen_table_version_fn();
        let select_fn = self.gen_table_select_fn();
        let select_range_fn = self.gen_table_select_range_fn();
        let insert_fn = self.gen_table_insert_fn();
        let insert_many_fn = self.gen_table_insert_many_fn();
        let delete_many_fn = self.gen_table_delete_many_fn();
        let reinsert_fn = self.gen_table_reinsert_fn();
        let upsert_fn = self.gen_table_upsert_fn();
        let get_next_fn = self.gen_table_get_next_fn();
        let reserve_pks_fn = self.gen_table_reserve_pks_fn();
        let pk_gen_state_fn = self.gen_table_pk_gen_state_fn();
        let iter_with_fn = self.gen_table_iter_with_fn();
        let iter_with_async_fn = self.gen_table_iter_with_async_fn();
        let count_fn = self.gen_table_count_fn();
        let system_info_fn = self.gen_system_info_fn();
        let vacuum_fn = self.gen_table_vacuum_fn();
        let validate_loaded_secondary_state_fn = self.gen_validate_loaded_secondary_state_fn();

        quote! {
            #persisted_impl
            impl #ident {
                #name_fn
                #version_fn
                #select_fn
                #select_range_fn
                #insert_fn
                #insert_many_fn
                #delete_many_fn
                #reinsert_fn
                #upsert_fn
                #count_fn
                #get_next_fn
                #reserve_pks_fn
                #pk_gen_state_fn
                #iter_with_fn
                #iter_with_async_fn
                #system_info_fn
                #vacuum_fn
                #validate_loaded_secondary_state_fn
            }
        }
    }

    fn gen_validate_loaded_secondary_state_fn(&self) -> TokenStream {
        if self.columns.indexes.is_empty() {
            return quote! {
                fn validate_loaded_secondary_state(
                    &self,
                    _path: &str,
                    _mode: LoadMode,
                ) -> Result<(), PersistenceLoadError> {
                    Ok(())
                }
            };
        }

        let expected_entries = self
            .columns
            .indexes
            .iter()
            .map(|(column, index)| {
                let index_field = &index.name;
                let row_field = &index.field;
                let index_name = Literal::string(&index_field.to_string());
                let field_type = self
                    .columns
                    .columns_map
                    .get(column)
                    .expect("indexed column should exist")
                    .to_string();
                let key = if is_float(&field_type) {
                    quote! { OrderedFloat(row.#row_field) }
                } else {
                    quote! { row.#row_field.clone() }
                };

                if index.is_unique {
                    quote! {
                        if self.0.indexes.#index_field.lookup_for_select(&#key).map(|link| link.0) != Some(offset_link.0) {
                            return Err(PersistenceLoadError::corrupt(
                                path,
                                format!("secondary index {} does not reference primary key {primary_key:?}", #index_name),
                            ));
                        }
                    }
                } else {
                    quote! {
                        if !self.0.indexes.#index_field
                            .get(&#key)
                            .any(|(_, candidate_link)| candidate_link.0 == offset_link.0)
                        {
                            return Err(PersistenceLoadError::corrupt(
                                path,
                                format!("secondary index {} does not reference primary key {primary_key:?}", #index_name),
                            ));
                        }
                    }
                }
            })
            .collect::<Vec<_>>();
        let entry_counts = self
            .columns
            .indexes
            .values()
            .map(|index| {
                let index_field = &index.name;
                let index_name = Literal::string(&index_field.to_string());
                quote! {
                    if self.0.indexes.#index_field.len() != primary_count {
                        return Err(PersistenceLoadError::corrupt(
                            path,
                            format!("secondary index {} contains a different number of rows than the primary index", #index_name),
                        ));
                    }
                }
            })
            .collect::<Vec<_>>();
        let recovery_entries = self
            .columns
            .indexes
            .iter()
            .map(|(column, index)| {
                let index_field = &index.name;
                let row_field = &index.field;
                let index_name = Literal::string(&index_field.to_string());
                let field_type = self
                    .columns
                    .columns_map
                    .get(column)
                    .expect("indexed column should exist")
                    .to_string();
                let expected_key = if is_float(&field_type) {
                    quote! { OrderedFloat(row.#row_field) }
                } else {
                    quote! { row.#row_field.clone() }
                };

                if index.is_unique {
                    quote! {
                        for (indexed_key, offset_link) in self.0.indexes.#index_field.iter_values() {
                            let row = self.0.data.select_non_ghosted_checked(offset_link.0).map_err(|error| {
                                PersistenceLoadError::corrupt(
                                    path,
                                    format!("secondary index {} references an invalid row: {error}", #index_name),
                                )
                            })?;
                            let expected_key = #expected_key;
                            if indexed_key != expected_key {
                                return Err(PersistenceLoadError::corrupt(
                                    path,
                                    format!("secondary index {} key does not match its referenced row", #index_name),
                                ));
                            }
                        }
                    }
                } else {
                    // Every multimap backend yields owned pairs: the arctic
                    // multimap snapshots, and WorkTablesIndex iterators yield
                    // owned clones as of 0.0.8.
                    let key_mismatch = quote! { indexed_key != expected_key };
                    quote! {
                        for (indexed_key, offset_link) in self.0.indexes.#index_field.iter() {
                            let row = self.0.data.select_non_ghosted_checked(offset_link.0).map_err(|error| {
                                PersistenceLoadError::corrupt(
                                    path,
                                    format!("secondary index {} references an invalid row: {error}", #index_name),
                                )
                            })?;
                            let expected_key = #expected_key;
                            if #key_mismatch {
                                return Err(PersistenceLoadError::corrupt(
                                    path,
                                    format!("secondary index {} key does not match its referenced row", #index_name),
                                ));
                            }
                        }
                    }
                }
            })
            .collect::<Vec<_>>();

        quote! {
            fn validate_loaded_secondary_state(
                &self,
                path: &str,
                mode: LoadMode,
            ) -> Result<(), PersistenceLoadError> {
                match mode {
                    LoadMode::Strict => {
                        let primary_count = self.0.primary_index.pk_map.len();
                        #(#entry_counts)*
                        for (primary_key, offset_link) in self.0.primary_index.pk_map.iter_values() {
                            let row = self.0.data.select_non_ghosted_checked(offset_link.0).map_err(|error| {
                                PersistenceLoadError::corrupt(
                                    path,
                                    format!("primary key {primary_key:?} references an invalid row: {error}"),
                                )
                            })?;
                            #(#expected_entries)*
                        }
                    }
                    LoadMode::Recovery => {
                        #(#recovery_entries)*
                    }
                }
                Ok(())
            }
        }
    }

    fn gen_table_new_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();
        let table_name = name_generator.get_work_table_literal_name();
        let task = name_generator.get_persistence_task_ident();
        let space_ident = name_generator.get_space_file_ident();
        let pk_type = name_generator.get_primary_key_type_ident();
        let const_name = name_generator.get_page_inner_size_const_ident();
        let secondary_index_events = name_generator.get_space_secondary_index_events_ident();
        let avt_index_ident = name_generator.get_available_indexes_ident();

        let pk_types = &self
            .columns
            .primary_keys
            .iter()
            .map(|i| {
                self.columns
                    .columns_map
                    .get(i)
                    .expect("should exist as got from definition")
                    .to_string()
            })
            .collect::<Vec<_>>();
        let pk_types_unsized = is_unsized_vec(pk_types);
        let wti_map = if cfg!(feature = "logical-index-persistence") {
            quote! { PersistentWtiIndex }
        } else {
            quote! { IndexMap }
        };
        let index_setup = if self.columns.primary_index_backend == crate::common::model::IndexBackend::Arctic {
            quote! {
                inner.primary_index = std::sync::Arc::new(PrimaryIndex::from_map(
                    PersistentArcticIndex::<#pk_type, OffsetEqLink<#const_name>>::default()
                ));
            }
        } else if pk_types_unsized {
            quote! {
                inner.primary_index = std::sync::Arc::new(PrimaryIndex::from_map(
                    #wti_map::<#pk_type, OffsetEqLink<#const_name>, UnsizedNode<_>>::with_maximum_node_size(#const_name)
                ));
            }
        } else {
            match self.columns.primary_index_backend {
                crate::common::model::IndexBackend::WorktablesIndex => quote! {
                    let size = get_index_page_size_from_data_length::<#pk_type>(#const_name);
                    inner.primary_index = std::sync::Arc::new(PrimaryIndex::from_map(
                        #wti_map::<_, OffsetEqLink<#const_name>>::with_maximum_node_size(size)
                    ));
                },
                crate::common::model::IndexBackend::Indexset => quote! {
                    let size = get_index_page_size_from_data_length::<#pk_type>(#const_name);
                    inner.primary_index = std::sync::Arc::new(PrimaryIndex::from_map(
                        UpstreamIndexMap::<_, OffsetEqLink<#const_name>>::with_maximum_node_size(size)
                    ));
                },
                crate::common::model::IndexBackend::Arctic => unreachable!("handled before variable-size dispatch"),
                crate::common::model::IndexBackend::Congee => quote! {
                    inner.primary_index = std::sync::Arc::new(PrimaryIndex::from_map(
                        PersistentCongeeIndex::<#pk_type, OffsetEqLink<#const_name>>::default()
                    ));
                },
            }
        };

        quote! {
            impl<E, C> PersistedWorkTable<E> for #ident
            where
                E: PersistenceEngine<
                    <<#pk_type as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
                    #pk_type,
                    #secondary_index_events,
                    #avt_index_ident,
                    Config=C
                > + Send
                    + 'static,
                C: Clone + PersistenceConfig,
            {
                async fn new(mut engine: E) -> eyre::Result<Self> {
                    let schema = Self::space_info_default().inner;
                    engine
                        .ensure_schema(
                            schema.row_schema,
                            schema.primary_key_fields,
                            schema.secondary_index_types,
                        )
                        .await?;
                    let mut inner = WorkTable::default();
                    inner.table_name = #table_name;
                    #index_setup
                    core::result::Result::Ok(Self(
                        inner,
                        #task::run_engine(engine)
                    ))
                }

                async fn load(engine: E) -> eyre::Result<Self> {
                    Self::load_with(engine, LoadMode::Strict).await
                }

                async fn load_with(mut engine: E, mode: LoadMode) -> eyre::Result<Self> {
                    let schema = Self::space_info_default().inner;
                    engine
                        .validate_schema(
                            schema.row_schema,
                            schema.primary_key_fields,
                            schema.secondary_index_types,
                        )
                        .await?;
                    let table_path = engine.config().table_path().to_owned();
                    if !std::path::Path::new(&table_path).exists() {
                        return Self::new(engine).await;
                    };
                    let table = load_persisted_state(&table_path, async {
                        let space = #space_ident::parse_file(&table_path).await?;
                        Ok::<_, eyre::Report>(space.into_worktable_with_mode(engine, &table_path, mode).await?)
                    }).await?;
                    Ok(table)
                }
            }
        }
    }

    fn gen_table_name_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let dir_name = name_generator.get_dir_name();

        quote! {
            pub fn name(&self) -> &'static str {
                &self.0.table_name
            }

            pub fn name_snake_case() -> &'static str {
                #dir_name
            }
        }
    }

    fn gen_table_version_fn(&self) -> TokenStream {
        let version = self.version;

        quote! {
            pub fn version() -> u32 {
                #version
            }
        }
    }

    fn gen_table_select_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();

        quote! {
            pub fn select<Pk>(&self, pk: Pk) -> Option<#row_type>
            where #primary_key_type: From<Pk> {
                self.0.select(pk.into())
            }
        }
    }

    fn gen_table_select_range_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();
        let column_range_type = name_generator.get_column_range_type_ident();
        let row_fields_ident = name_generator.get_row_fields_enum_ident();

        let pk_sorted_by = if self.columns.primary_keys.len() == 1 {
            let pk_field = &self.columns.primary_keys[0];
            let pk_pascal = Ident::new(&pk_field.to_string().to_case(Case::Pascal), Span::mixed_site());
            quote! {
                SelectQueryBuilder::new_sorted(rows, #row_fields_ident::#pk_pascal)
            }
        } else {
            quote! {
                SelectQueryBuilder::new(rows)
            }
        };

        quote! {
            pub fn select_by_pk_range<'a, R, Pk>(&'a self, range: R) -> SelectQueryBuilder<#row_type,
                                                                     impl DoubleEndedIterator<Item = #row_type> + 'a,
                                                                     #column_range_type,
                                                                     #row_fields_ident>
            where
                #primary_key_type: From<Pk>,
                R: std::ops::RangeBounds<Pk> + 'a,
                Pk: Clone + 'a,
            {
                let converted_range = (
                    range.start_bound().map(|v| #primary_key_type::from(v.clone())),
                    range.end_bound().map(|v| #primary_key_type::from(v.clone())),
                );
                // Delay the grace-period guard until the returned iterator is
                // consumed so an idle query builder cannot pin reclamation.
                let rows = std::iter::once_with(move || {
                    let read_guard = self.0.data.read_guard();
                    self.0.primary_index.pk_map
                        .range_links(converted_range)
                        .filter_map(move |link| {
                            let _read_guard = &read_guard;
                            self.0.data.select_non_ghosted(link.0).ok()
                        })
                }).flatten();

                #pk_sorted_by
            }
        }
    }

    fn gen_table_insert_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();
        let secondary_events_ident = name_generator.get_space_secondary_index_events_ident();

        quote! {
            pub async fn insert(&self, row: #row_type) -> core::result::Result<#primary_key_type, WorkTableError> {
                self.1.ensure_running()?;
                let (op, res) = self.0.insert_cdc::<#secondary_events_ident>(row);
                if let Some(op) = op {
                    self.1.apply_operation(op)?;
                }
                res
            }
        }
    }

    fn gen_table_insert_many_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();
        let secondary_events_ident = name_generator.get_space_secondary_index_events_ident();

        quote! {
            /// Inserts every row of `rows`, all or nothing.
            ///
            /// Any rejected row rejects the whole batch, unwinds it without
            /// concurrent readers ever observing a value, and the error names
            /// the offending row and index. After `Ok`, every row is visible
            /// to reads; persisted durability follows the same `wait_for_ops`
            /// contract as single inserts, with the whole batch coalesced
            /// into one persistence engine application.
            pub async fn insert_many(&self, rows: Vec<#row_type>) -> core::result::Result<Vec<#primary_key_type>, BatchInsertError> {
                if let core::result::Result::Err(e) = self.1.ensure_running() {
                    return core::result::Result::Err(BatchInsertError::Table(WorkTableError::PersistenceError(e)));
                }
                let (ops, res) = self.0.insert_many_cdc::<#secondary_events_ident>(rows);
                // A rejected batch still produces an Acknowledge operation
                // that keeps the CDC event-id stream gap-free, so operations
                // are enqueued regardless of `res`.
                if let core::result::Result::Err(e) = self.1.apply_operations(ops) {
                    return core::result::Result::Err(BatchInsertError::Table(WorkTableError::PersistenceError(e)));
                }
                res
            }
        }
    }

    fn gen_table_delete_many_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let primary_key_type = name_generator.get_primary_key_type_ident();

        quote! {
            /// Deletes every row named by `pks`, in the order given.
            ///
            /// Returns the keys actually deleted. A key that is not present is
            /// skipped rather than failing the batch.
            ///
            /// Unlike `insert_many` this is **not** all-or-nothing. A delete
            /// that fails partway has already removed earlier rows and they
            /// are genuinely gone, so the error reports how many succeeded
            /// rather than pretending to rewind.
            ///
            /// On a persisted table each row is deleted through the same path
            /// as a single `delete`, so every removal produces its own
            /// persistence operation. That costs the batching the in-memory
            /// generator gets, and it is not optional: routing these straight
            /// at the in-memory batch left the rows gone from memory and
            /// present on disk, so they came back on the next load. Durability
            /// is not something to trade for a per-row constant.
            pub async fn delete_many<Pk>(&self, pks: Vec<Pk>)
                -> core::result::Result<Vec<#primary_key_type>, BatchDeleteError<#primary_key_type>>
            where #primary_key_type: From<Pk>
            {
                let pks: Vec<#primary_key_type> = pks.into_iter().map(core::convert::Into::into).collect();
                if pks.is_empty() {
                    return core::result::Result::Ok(Vec::new());
                }
                // Persisted deletes run one row at a time for durability, so
                // keep a cheap operation-wide activity signal across the gaps
                // between those row mutations. It holds no row or stripe lock.
                let _bulk_mutation = self.0.lock_manager.bulk_mutation_guard();
                let mut deleted = Vec::with_capacity(pks.len());
                for pk in pks {
                    match self.delete::<#primary_key_type>(pk.clone()).await {
                        core::result::Result::Ok(()) => deleted.push(pk),
                        // Absent keys are skipped, matching the in-memory
                        // contract; anything else stops the batch and reports
                        // the prefix that did land.
                        core::result::Result::Err(WorkTableError::NotFound) => {}
                        core::result::Result::Err(source) => {
                            return core::result::Result::Err(BatchDeleteError::Key {
                                key: pk,
                                deleted: deleted.len(),
                                source,
                            });
                        }
                    }
                }
                core::result::Result::Ok(deleted)
            }

            /// Deletes every row whose primary key falls in `range`.
            ///
            /// The shape bulk eviction has: a caller dropping a generation
            /// knows the span it wants gone rather than the individual keys.
            /// The span is collected from the primary index in one ordered
            /// walk, then deleted exactly as `delete_many` would, which on a
            /// persisted table means one persistence operation per row.
            ///
            /// Only the keys present when the range was walked are deleted. A
            /// key inserted into the span afterwards is left alone rather than
            /// removed without ever having been seen.
            pub async fn delete_range<R>(&self, range: R)
                -> core::result::Result<Vec<#primary_key_type>, BatchDeleteError<#primary_key_type>>
            where R: core::ops::RangeBounds<#primary_key_type>
            {
                // Cover the range walk as well as the per-row deletes below.
                let _bulk_mutation = self.0.lock_manager.bulk_mutation_guard();
                let start = range.start_bound().cloned();
                let end = range.end_bound().cloned();
                let keys: Vec<#primary_key_type> = self.0
                    .primary_index
                    .pk_map
                    .range_values((start, end))
                    .map(|(key, _)| key)
                    .collect();
                self.delete_many::<#primary_key_type>(keys).await
            }
        }
    }

    fn gen_table_reinsert_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();
        let secondary_events_ident = name_generator.get_space_secondary_index_events_ident();

        quote! {
            pub async fn reinsert(&self, row_old: #row_type, row_new: #row_type) -> core::result::Result<#primary_key_type, WorkTableError> {
                self.1.ensure_running()?;
                let (op, res) = self.0.reinsert_cdc::<#secondary_events_ident>(row_old, row_new);
                if let Some(op) = op {
                    self.1.apply_operation(op)?;
                }
                res
            }
        }
    }

    fn gen_table_upsert_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let full_row_lock = self.gen_full_lock_for_update();

        quote! {
            /// Inserts the row if its primary key is absent, updates it
            /// otherwise.
            ///
            /// A definitely absent key takes the same optimistic synchronous
            /// insert path as `insert`. Insert and generated locked mutations
            /// share a per-key mutation gate; existing keys and insertion
            /// collisions also acquire the full-row lock across the repeated
            /// existence check and selected mutation.
            pub async fn upsert(&self, row: #row_type) -> core::result::Result<(), WorkTableError> {
                let pk = row.get_primary_key();
                if !self.0.primary_index.pk_map.contains_key(&pk) {
                    match self.insert(row.clone()).await {
                        core::result::Result::Ok(_) => return core::result::Result::Ok(()),
                        core::result::Result::Err(WorkTableError::PrimaryAlreadyExists) => {}
                        core::result::Result::Err(e) => return core::result::Result::Err(e),
                    }
                }
                // Retries only fire when an existence flip invalidated the
                // optimistic decision (NotFound / row-absent). The FIFO
                // per-key mutation gate guarantees forward progress; retain a
                // bounded scheduler backoff for repeated decision races.
                let mut backoff_spins: u32 = 0;
                loop {
                    let pending_lock = { #full_row_lock };
                    let guard = pending_lock.into_guard_with_mutation();

                    let result = if self.0.primary_index.pk_map.contains_key(&pk) {
                        self.update_with_guard(row.clone(), guard).await
                    } else {
                        // `insert` acquires the same per-key mutation gate as
                        // `guard`; release the row operation before entering
                        // the synchronous insertion protocol, then retry the
                        // locked decision if another writer won the race.
                        drop(guard);
                        match self.insert(row.clone()).await {
                            core::result::Result::Ok(_) => core::result::Result::Ok(()),
                            core::result::Result::Err(WorkTableError::PrimaryAlreadyExists) =>
                                core::result::Result::Err(WorkTableError::NotFound),
                            core::result::Result::Err(e) => core::result::Result::Err(e),
                        }
                    };

                    match result {
                        core::result::Result::Err(WorkTableError::NotFound) => {}
                        core::result::Result::Err(WorkTableError::PagesError(e)) if e.is_row_absent() => {}
                        other => return other,
                    }
                    if backoff_spins < 8 {
                        backoff_spins = backoff_spins.saturating_add(1);
                        tokio::task::yield_now().await;
                    } else {
                        // Cap the exponent BEFORE shifting: `1u64 << 64` panics
                        // (overflow) in debug/test builds. Clamp the shift to a
                        // 256µs ceiling and saturate the counter so a long
                        // starvation streak can never overflow.
                        let exponent = core::cmp::min(backoff_spins - 8, 8);
                        let micros = core::cmp::min(1u64 << exponent, 256);
                        backoff_spins = backoff_spins.saturating_add(1);
                        tokio::time::sleep(std::time::Duration::from_micros(micros)).await;
                    }
                }
            }
        }
    }

    fn gen_table_get_next_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let primary_key_type = name_generator.get_primary_key_type_ident();

        match self.columns.generator_type {
            GeneratorType::Custom | GeneratorType::Autoincrement => {
                quote! {
                    pub fn get_next_pk(&self) -> #primary_key_type {
                        self.0.get_next_pk()
                    }
                }
            }
            GeneratorType::None => {
                quote! {}
            }
        }
    }

    fn gen_table_reserve_pks_fn(&self) -> TokenStream {
        if !matches!(self.columns.generator_type, GeneratorType::Autoincrement) || self.columns.primary_keys.len() != 1
        {
            return quote! {};
        }
        let pk_inner_type = self
            .columns
            .columns_map
            .get(&self.columns.primary_keys[0])
            .expect("primary key column should exist");

        quote! {
            /// Reserves `count` consecutive primary keys so a batch can be
            /// assigned contiguous keys before `insert_many`. Interleaved
            /// `get_next_pk` calls keep working and never overlap a
            /// reservation.
            pub fn reserve_pks(&self, count: usize) -> std::ops::Range<#pk_inner_type> {
                self.0.reserve_pks(count)
            }
        }
    }

    fn gen_table_pk_gen_state_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let primary_key_type = name_generator.get_primary_key_type_ident();

        quote! {
            pub fn pk_gen_state(&self) -> <<#primary_key_type as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State {
                self.0.pk_gen.get_state()
            }
        }
    }

    fn gen_table_iter_with_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let inner = self.gen_table_iter_inner(quote! {
            f(data)?;
        });

        quote! {
            pub fn iter_with<
                F: Fn(#row_type) -> core::result::Result<(), WorkTableError>
            >(&self, f: F) -> core::result::Result<(), WorkTableError> {
                #inner
            }
        }
    }

    fn gen_table_iter_with_async_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let inner = self.gen_table_iter_inner(quote! {
             f(data).await?;
        });

        quote! {
            pub async fn iter_with_async<
                F: Fn(#row_type) -> Fut,
                Fut: std::future::Future<Output = core::result::Result<(), WorkTableError>>
            >(&self, f: F) -> core::result::Result<(), WorkTableError> {
                #inner
            }
        }
    }

    fn gen_table_iter_inner(&self, func: TokenStream) -> TokenStream {
        quote! {
            let _read_guard = self.0.data.read_guard();
            // Snapshot the ordered links once. Re-starting a range at every
            // key turns materializing backends into quadratic full scans and
            // can retain backend node guards across an async callback.
            let links = self.0.primary_index.pk_map
                .iter_values()
                .map(|(_, link)| link.0)
                .collect::<Vec<_>>();
            for link in links {
                let data = self.0.data
                    .select_non_ghosted(link)
                    .map_err(WorkTableError::PagesError)?;
                #func
            }

            core::result::Result::Ok(())
        }
    }

    fn gen_table_count_fn(&self) -> TokenStream {
        quote! {
            pub fn count(&self) -> usize {
                let count = self.0.primary_index.pk_map.len();
                count
            }
        }
    }

    fn gen_system_info_fn(&self) -> TokenStream {
        quote! {
            pub fn system_info(&self) -> SystemInfo {
                self.0.system_info()
            }

            /// Rows currently in the table, without building a `SystemInfo`.
            pub fn row_count(&self) -> usize {
                self.0.row_count()
            }

            /// Row bytes plus index bytes, without building a `SystemInfo`.
            pub fn used_bytes(&self) -> u64 {
                self.0.used_bytes()
            }
        }
    }

    fn gen_table_vacuum_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let table_name = name_generator.get_work_table_literal_name();
        let secondary_index_events = name_generator.get_space_secondary_index_events_ident();
        let lock_type = name_generator.get_lock_type_ident();

        quote! {
            pub fn vacuum(&self) -> std::sync::Arc<dyn WorkTableVacuum + std::marker::Send + Sync> {
                std::sync::Arc::new(EmptyDataVacuum::<
                    _,
                    _,
                    _,
                    _,
                    _,
                    _,
                    #lock_type,
                    _,
                    #secondary_index_events
                >::new(
                    #table_name,
                    std::sync::Arc::clone(&self.0.data),
                    std::sync::Arc::clone(&self.0.lock_manager),
                    std::sync::Arc::clone(&self.0.primary_index),
                    std::sync::Arc::clone(&self.0.indexes),
                ).with_persistence(self.1.vacuum_sink()))
            }
        }
    }
}
