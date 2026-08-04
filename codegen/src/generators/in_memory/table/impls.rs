use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::common::model::GeneratorType;
use crate::common::name_generator::WorktableNameGenerator;
use crate::generators::in_memory::InMemoryGenerator;

impl InMemoryGenerator {
    pub fn gen_table_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();

        let persisted_impl = self.gen_table_new_fn();
        let name_fn = self.gen_table_name_fn();
        let select_fn = self.gen_table_select_fn();
        let select_range_fn = self.gen_table_select_range_fn();
        let insert_fn = self.gen_table_insert_fn();
        let reinsert_fn = self.gen_table_reinsert_fn();
        let upsert_fn = self.gen_table_upsert_fn();
        let get_next_fn = self.gen_table_get_next_fn();
        let iter_with_fn = self.gen_table_iter_with_fn();
        let iter_with_async_fn = self.gen_table_iter_with_async_fn();
        let count_fn = self.gen_table_count_fn();
        let system_info_fn = self.gen_system_info_fn();
        let vacuum_fn = self.gen_table_vacuum_fn();

        quote! {
            #persisted_impl
            impl #ident {
                #name_fn
                #select_fn
                #select_range_fn
                #insert_fn
                #reinsert_fn
                #upsert_fn
                #count_fn
                #get_next_fn
                #iter_with_fn
                #iter_with_async_fn
                #system_info_fn
                #vacuum_fn
            }
        }
    }

    fn gen_table_new_fn(&self) -> TokenStream {
        // InMemory tables don't have PersistedWorkTable impl
        quote! {}
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

        quote! {
            pub fn insert(&self, row: #row_type) -> core::result::Result<#primary_key_type, WorkTableError> {
                self.0.insert(row)
            }
        }
    }

    fn gen_table_reinsert_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();

        quote! {
            pub async fn reinsert(&self, row_old: #row_type, row_new: #row_type) -> core::result::Result<#primary_key_type, WorkTableError> {
                self.0.reinsert(row_old, row_new).await
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
            /// A definitely absent key takes the same optimistic lock-free
            /// insert path as `insert`. Existing keys and insertion collisions
            /// acquire one full-row lock across the repeated existence check
            /// and selected mutation, so upserts, updates, and deletes on the
            /// same key cannot invalidate that decision.
            pub async fn upsert(&self, row: #row_type) -> core::result::Result<(), WorkTableError> {
                let pk = row.get_primary_key();
                if !self.0.primary_index.pk_map.contains_key(&pk) {
                    match self.insert(row.clone()) {
                        core::result::Result::Ok(_) => return core::result::Result::Ok(()),
                        core::result::Result::Err(WorkTableError::PrimaryAlreadyExists) => {}
                        core::result::Result::Err(e) => return core::result::Result::Err(e),
                    }
                }
                // Retries only fire when a racing unlocked insert/delete moved
                // the row out from under a locked decision (NotFound /
                // row-absent). A raw insert/delete pair does not join this row
                // lock, so a hot `yield_now` spin can livelock the upsert
                // against sustained same-key churn. Escalate the backoff so the
                // racing mutation's publication settles and the upsert makes
                // forward progress.
                let mut backoff_spins: u32 = 0;
                loop {
                    let op_lock = { #full_row_lock };
                    let guard = LockGuard::new(
                        op_lock,
                        self.0.lock_manager.clone(),
                        pk.clone(),
                    );

                    let result = if self.0.primary_index.pk_map.contains_key(&pk) {
                        self.update_with_guard(row.clone(), guard).await
                    } else {
                        match self.insert(row.clone()) {
                            core::result::Result::Ok(_) => core::result::Result::Ok(()),
                            core::result::Result::Err(WorkTableError::PrimaryAlreadyExists) => {
                                self.update_with_guard(row.clone(), guard).await
                            }
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
        }
    }

    fn gen_table_vacuum_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let table_name = name_generator.get_work_table_literal_name();
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
                    _
                >::new(
                    #table_name,
                    std::sync::Arc::clone(&self.0.data),
                    std::sync::Arc::clone(&self.0.lock_manager),
                    std::sync::Arc::clone(&self.0.primary_index),
                    std::sync::Arc::clone(&self.0.indexes),
                ))
            }
        }
    }
}
