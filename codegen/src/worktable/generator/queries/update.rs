use proc_macro2::Literal;
use std::collections::HashMap;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use crate::worktable::model::Operation;
use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

impl Generator {
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
        let avt_type_ident = name_generator.get_available_type_ident();
        let lock_ident = name_generator.get_lock_type_ident();

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

        let idents: Vec<_> = self
            .columns
            .indexes
            .values()
            .map(|idx| idx.field.clone())
            .collect();

        let diff_container = quote! {
            let row_old = self.select(pk.clone()).unwrap();
            let row_new = row.clone();
            let updated_bytes: Vec<u8> = vec![];
            let mut diffs: std::collections::HashMap<&str, Difference<#avt_type_ident>> = std::collections::HashMap::new();
        };
        let diff_process = self.gen_process_diffs_on_index(idents.as_slice(), idents.as_slice());
        let persist_call = if self.is_persist {
            quote! {
                if let Operation::Update(op) = &mut op {
                     op.bytes = self.0.data.select_raw(link)?;
                } else {
                    unreachable!("")
                };
                self.2.apply_operation(op);
            }
        } else {
            quote! {}
        };

        quote! {
            pub async fn update(&self, row: #row_ident) -> core::result::Result<(), WorkTableError> {
                let pk = row.get_primary_key();
                if let Some(lock) = self.0.lock_map.get(&pk) {
                    lock.lock_await().await;   // Waiting for all locks released
                }

                let lock_id = self.0.lock_map.next_id();
                let lock = std::sync::Arc::new(#lock_ident::with_lock(lock_id.into()));   //Creates new LockType with Locks
                self.0.lock_map.insert(pk.clone(), lock.clone()); // adds LockType to LockMap

                let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row).map_err(|_| WorkTableError::SerializeError)?;
                let mut archived_row = unsafe { rkyv::access_unchecked_mut::<<#row_ident as rkyv::Archive>::Archived>(&mut bytes[..]).unseal_unchecked() };
                let link = self.0
                    .pk_map
                    .get(&pk)
                    .map(|v| v.get().value)
                    .ok_or(WorkTableError::NotFound)?;

                #diff_container
                #diff_process

                unsafe { self.0.data.with_mut_ref(link, move |archived| {
                    #(#row_updates)*
                }).map_err(WorkTableError::PagesError)? };

                lock.unlock();  // Releases locks
                self.0.lock_map.remove(&pk); // Removes locks

                #persist_call

                core::result::Result::Ok(())
            }
        }
    }

    fn gen_custom_updates(&mut self, updates: HashMap<Ident, Operation>) -> TokenStream {
        let defs = updates
            .iter()
            .map(|(name, op)| {
                let snake_case_name = name
                    .to_string()
                    .from_case(Case::Pascal)
                    .to_case(Case::Snake);
                let index = self.columns.indexes.values().find(|idx| idx.field == op.by);

                let indexes_columns: Option<Vec<_>> = {
                    let columns: Vec<_> = self
                        .columns
                        .indexes
                        .values()
                        .filter(|idx| op.columns.contains(&idx.field))
                        .map(|idx| idx.field.clone())
                        .collect();

                    if columns.is_empty() {
                        None
                    } else {
                        Some(columns)
                    }
                };

                let idents = &op.columns;
                if let Some(index) = index {
                    let index_name = &index.name;

                    if index.is_unique {
                        self.gen_unique_update(snake_case_name, name, index_name, idents)
                    } else {
                        self.gen_non_unique_update(snake_case_name, name, index_name, idents)
                    }
                } else if self.columns.primary_keys.len() == 1 {
                    if *self.columns.primary_keys.first().unwrap() == op.by {
                        self.gen_pk_update(snake_case_name, name, idents, indexes_columns.as_ref())
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

    fn gen_process_diffs_on_index(&self, idents: &[Ident], idx_idents: &[Ident]) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_type_ident = name_generator.get_available_type_ident();
        let secondary_events_ident = name_generator.get_space_secondary_index_events_ident();
        let primary_key_ident = name_generator.get_primary_key_type_ident();

        let diff = idents
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
            .collect::<Vec<_>>();

        let process_difference = if self.is_persist {
            quote! {
                let secondary_keys_events = self.0.indexes.process_difference_cdc(link, diffs)?;
                let mut op: Operation<
                    <<#primary_key_ident as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
                    #primary_key_ident,
                    #secondary_events_ident
                > = Operation::Update(UpdateOperation {
                    id: Default::default(),
                    secondary_keys_events,
                    bytes: updated_bytes,
                    link,
                });
            }
        } else {
            quote! {
                self.0.indexes.process_difference(link, diffs)?;
            }
        };

        quote! {
            #(#diff)*
            #process_difference
        }
    }

    fn gen_pk_update(
        &self,
        snake_case_name: String,
        name: &Ident,
        idents: &[Ident],
        idx_idents: Option<&Vec<Ident>>,
    ) -> TokenStream {
        let pk_ident = &self.pk.as_ref().unwrap().ident;
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let lock_type_ident = name_generator.get_lock_type_ident();

        let method_ident = Ident::new(
            format!("update_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );
        let query_ident = Ident::new(format!("{name}Query").as_str(), Span::mixed_site());

        let lock_await_ident = Ident::new(
            format!("lock_await_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );
        let unlock_ident = Ident::new(
            format!("unlock_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );
        let lock_ident = Ident::new(
            format!("lock_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );

        let row_updates = idents
            .iter()
            .map(|i| {
                quote! {
                    std::mem::swap(&mut archived.inner.#i, &mut archived_row.#i);
                }
            })
            .collect::<Vec<_>>();

        let diff_process = if let Some(idx_idents) = idx_idents {
            let avt_type_ident = name_generator.get_available_type_ident();
            let diff_container = quote! {
                let row_old = self.select(by.clone()).unwrap();
                let row_new = row.clone();
                let updated_bytes: Vec<u8> = vec![];
                let mut diffs: std::collections::HashMap<&str, Difference<#avt_type_ident>> = std::collections::HashMap::new();
            };

            let process = self.gen_process_diffs_on_index(idents, idx_idents.as_slice());
            quote! {
                #diff_container
                #process
            }
        } else {
            quote! {}
        };
        let persist_call = if self.is_persist {
            quote! {
                if let Operation::Update(op) = &mut op {
                     op.bytes = self.0.data.select_raw(link)?;
                } else {
                    unreachable!("")
                };
                self.2.apply_operation(op);
            }
        } else {
            quote! {}
        };

        quote! {
            pub async fn #method_ident(&self, row: #query_ident, by: #pk_ident) -> core::result::Result<(), WorkTableError> {
                if let Some(lock) = self.0.lock_map.get(&by) {
                    lock.#lock_await_ident().await;   // Waiting for all locks released
                }
                let lock_id = self.0.lock_map.next_id();
                let mut lock = #lock_type_ident::new(lock_id.into());   //Creates new LockType with None
                lock.#lock_ident();

                self.0.lock_map.insert(by.clone(), std::sync::Arc::new(lock.clone()));

                let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row).map_err(|_| WorkTableError::SerializeError)?;
                let mut archived_row = unsafe { rkyv::access_unchecked_mut::<<#query_ident as rkyv::Archive>::Archived>(&mut bytes[..]).unseal_unchecked() };
                let link = self.0
                        .pk_map
                        .get(&by)
                        .map(|v| v.get().value)
                        .ok_or(WorkTableError::NotFound)?;

                #diff_process

                unsafe { self.0.data.with_mut_ref(link, |archived| {
                    #(#row_updates)*
                }).map_err(WorkTableError::PagesError)? };

                lock.#unlock_ident();
                self.0.lock_map.remove(&by);

                #persist_call

                core::result::Result::Ok(())
            }
        }
    }

    fn gen_non_unique_update(
        &self,
        snake_case_name: String,
        name: &Ident,
        index: &Ident,
        idents: &[Ident],
    ) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let lock_type_ident = name_generator.get_lock_type_ident();

        let method_ident = Ident::new(
            format!("update_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );

        let query_ident = Ident::new(format!("{name}Query").as_str(), Span::mixed_site());
        let by_ident = Ident::new(format!("{name}By").as_str(), Span::mixed_site());

        let lock_await_ident = Ident::new(
            format!("lock_await_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );
        let lock_ident = Ident::new(
            format!("lock_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );
        let unlock_ident = Ident::new(
            format!("unlock_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );

        let row_updates = idents
            .iter()
            .map(|i| {
                quote! {
                    std::mem::swap(&mut archived.inner.#i, &mut row.#i);
                }
            })
            .collect::<Vec<_>>();

        quote! {
            pub async fn #method_ident(&self, row: #query_ident, by: #by_ident) -> core::result::Result<(), WorkTableError> {
                for (_, link) in self.0.indexes.#index.get(&by) {
                    let pk = self.0.data.select(*link)?.get_primary_key();
                    if let Some(lock) = self.0.lock_map.get(&pk) {
                        lock.#lock_await_ident().await;
                    }
                }

                for (_, link) in self.0.indexes.#index.get(&by) {
                    let pk = self.0.data.select(*link)?.get_primary_key();
                    let lock_id = self.0.lock_map.next_id();
                    let mut lock = #lock_type_ident::new(lock_id.into());
                    lock.#lock_ident();
                    self.0.lock_map.insert(pk.clone(), std::sync::Arc::new(lock.clone()));
                }

                for (_, link) in self.0.indexes.#index.get(&by) {
                    let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row)
                    .map_err(|_| WorkTableError::SerializeError)?;

                    let mut row = unsafe {
                    rkyv::access_unchecked_mut::<<#query_ident as rkyv::Archive>::Archived>(&mut bytes[..])
                        .unseal_unchecked()
                };
                    unsafe {
                        self.0.data.with_mut_ref(*link, |archived| {
                            #(#row_updates)*
                        }).map_err(WorkTableError::PagesError)?;
                    }
                }
                for (_, link) in self.0.indexes.#index.get(&by) {
                    let pk = self.0.data.select(*link)?.get_primary_key();
                    if let Some(lock) = self.0.lock_map.get(&pk) {
                        lock.#unlock_ident();
                        self.0.lock_map.remove(&pk);
                    }
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
    ) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let lock_type_ident = name_generator.get_lock_type_ident();

        let method_ident = Ident::new(
            format!("update_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );

        let query_ident = Ident::new(format!("{name}Query").as_str(), Span::mixed_site());
        let by_ident = Ident::new(format!("{name}By").as_str(), Span::mixed_site());

        let lock_await_ident = Ident::new(
            format!("lock_await_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );
        let lock_ident = Ident::new(
            format!("lock_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );
        let unlock_ident = Ident::new(
            format!("unlock_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );

        let row_updates = idents
            .iter()
            .map(|i| {
                quote! {
                    std::mem::swap(&mut archived.inner.#i, &mut row.#i);
                }
            })
            .collect::<Vec<_>>();

        quote! {
            pub async fn #method_ident(&self, row: #query_ident, by: #by_ident) -> core::result::Result<(), WorkTableError> {
                 let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row)
                    .map_err(|_| WorkTableError::SerializeError)?;

                let mut row = unsafe {
                    rkyv::access_unchecked_mut::<<#query_ident as rkyv::Archive>::Archived>(&mut bytes[..])
                        .unseal_unchecked()
                };

                let link = self.0.indexes.#index
                    .get(&by)
                    .map(|kv| kv.get().value)
                    .ok_or(WorkTableError::NotFound)?;

                let pk = self.0.data.select(link)?.get_primary_key();

                if let Some(lock) = self.0.lock_map.get(&pk) {
                    lock.#lock_await_ident().await;
                }
                let lock_id = self.0.lock_map.next_id();
                let mut lock = #lock_type_ident::new(lock_id.into());
                lock.#lock_ident();
                self.0.lock_map.insert(pk.clone(), std::sync::Arc::new(lock.clone()));

                unsafe {
                    self.0.data.with_mut_ref(link, |archived| {
                        #(#row_updates)*
                    }).map_err(WorkTableError::PagesError)?;
                }

                lock.#unlock_ident();
                self.0.lock_map.remove(&pk);

                core::result::Result::Ok(())
            }
        }
    }
}
