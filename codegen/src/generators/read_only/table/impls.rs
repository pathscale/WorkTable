use convert_case::{Case, Casing};
use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;

use crate::common::name_generator::{WorktableNameGenerator, is_float, is_unsized_vec};
use crate::generators::read_only::ReadOnlyGenerator;

impl ReadOnlyGenerator {
    pub fn gen_table_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();

        let persisted_impl = self.gen_table_new_fn();
        let name_fn = self.gen_table_name_fn();
        let version_fn = self.gen_table_version_fn();
        let select_fn = self.gen_table_select_fn();
        let select_range_fn = self.gen_table_select_range_fn();
        let insert_fn = self.gen_table_insert_fn();
        let reinsert_fn = self.gen_table_reinsert_fn();
        let upsert_fn = self.gen_table_upsert_fn();
        let get_next_fn = self.gen_table_get_next_fn();
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
                #reinsert_fn
                #upsert_fn
                #count_fn
                #get_next_fn
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
                    quote! {
                        for (indexed_key, offset_link) in self.0.indexes.#index_field.iter() {
                            let row = self.0.data.select_non_ghosted_checked(offset_link.0).map_err(|error| {
                                PersistenceLoadError::corrupt(
                                    path,
                                    format!("secondary index {} references an invalid row: {error}", #index_name),
                                )
                            })?;
                            let expected_key = #expected_key;
                            if indexed_key != &expected_key {
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
        let pk_map = match self.columns.primary_index_backend {
            crate::common::model::IndexBackend::Indexset => quote! { UpstreamIndexMap },
            _ => quote! { IndexMap },
        };

        let index_setup = if pk_types_unsized {
            quote! {
                inner.primary_index = std::sync::Arc::new(PrimaryIndex {
                    pk_map: IndexMap::<#pk_type, OffsetEqLink<#const_name>, UnsizedNode<_>>::with_maximum_node_size(#const_name),
                    reverse_pk_map: IndexMap::new(),
                });
            }
        } else {
            quote! {
                let size = get_index_page_size_from_data_length::<#pk_type>(#const_name);
                inner.primary_index = std::sync::Arc::new(PrimaryIndex {
                    pk_map: #pk_map::<_, OffsetEqLink<#const_name>>::with_maximum_node_size(size),
                    reverse_pk_map: IndexMap::new(),
                });
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
                async fn new(engine: E) -> eyre::Result<Self> {
                    let mut inner = WorkTable::default();
                    inner.table_name = #table_name;
                    #index_setup
                    core::result::Result::Ok(Self(inner))
                }

                async fn load(engine: E) -> eyre::Result<Self> {
                    Self::load_with(engine, LoadMode::Strict).await
                }

                async fn load_with(engine: E, mode: LoadMode) -> eyre::Result<Self> {
                    let table_path = engine.config().table_path().to_owned();
                    if !std::path::Path::new(&table_path).exists() {
                        return Self::new(engine).await;
                    };
                    let table = load_persisted_state(&table_path, async {
                        let space = #space_ident::parse_file(&table_path).await?;
                        Ok::<_, eyre::Report>(space.into_worktable_with_mode(&table_path, mode)?)
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
        quote! {}
    }

    fn gen_table_reinsert_fn(&self) -> TokenStream {
        quote! {}
    }

    fn gen_table_upsert_fn(&self) -> TokenStream {
        quote! {}
    }

    fn gen_table_get_next_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let primary_key_type = name_generator.get_primary_key_type_ident();

        match self.columns.generator_type {
            crate::common::model::GeneratorType::Custom | crate::common::model::GeneratorType::Autoincrement => {
                quote! {
                    pub fn get_next_pk(&self) -> #primary_key_type {
                        self.0.get_next_pk()
                    }
                }
            }
            crate::common::model::GeneratorType::None => {
                quote! {}
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
        quote! {}
    }
}
