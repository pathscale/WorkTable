use std::collections::HashMap;

use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use crate::worktable::model::Operation;

impl Generator {
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

        Ok(quote! {
            impl #table_ident {
                #full_row_delete
                #custom_deletes
            }
        })
    }

    fn gen_full_row_delete(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let pk_ident = name_generator.get_primary_key_type_ident();
        let secondary_events_ident = name_generator.get_space_secondary_index_events_ident();

        let delete_logic = if self.is_persist {
            quote! {
                let secondary_keys_events = self.0.indexes.delete_row_cdc(row, link)?;
                let (_, primary_key_events) = TableIndexCdc::remove_cdc(&self.0.pk_map, pk.clone(), link);
                self.0.data.delete(link).map_err(WorkTableError::PagesError)?;
                let mut op: Operation<
                    <<#pk_ident as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
                    #pk_ident,
                    #secondary_events_ident
                > = Operation::Delete(DeleteOperation {
                    id: Default::default(),
                    secondary_keys_events,
                    primary_key_events,
                });
                self.2.apply_operation(op);
            }
        } else {
            quote! {
                self.0.indexes.delete_row(row, link)?;
                self.0.pk_map.remove(&pk);
                self.0.data.delete(link).map_err(WorkTableError::PagesError)?;
            }
        };

        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let lock_ident = name_generator.get_lock_type_ident();

        quote! {
            pub async fn delete(&self, pk: #pk_ident) -> core::result::Result<(), WorkTableError> {

                if let Some(lock) = self.0.lock_map.get(&pk) {
                    lock.lock_await().await;   // Waiting for all locks released
                }

                let lock_id = self.0.lock_map.next_id();
                let lock = std::sync::Arc::new(#lock_ident::with_lock(lock_id.into()));   //Creates new LockType with None
                self.0.lock_map.insert(pk.clone(), lock.clone()); // adds LockType to LockMap

                let link = self.0
                    .pk_map
                    .get(&pk)
                    .map(|v| v.get().value)
                    .ok_or(WorkTableError::NotFound)?;

                let row = self.select(pk.clone()).unwrap();
                #delete_logic

                lock.unlock();  // Releases locks
                self.0.lock_map.remove(&pk); // Removes locks

                core::result::Result::Ok(())
            }
        }
    }

    fn gen_custom_deletes(&mut self, deleted: HashMap<Ident, Operation>) -> TokenStream {
        let defs = deleted
            .iter()
            .map(|(name, op)| {
                let snake_case_name = name
                    .to_string()
                    .from_case(Case::Pascal)
                    .to_case(Case::Snake);
                let method_ident = Ident::new(
                    format!("delete_{snake_case_name}").as_str(),
                    Span::mixed_site(),
                );
                let index = self.columns.indexes.values().find(|idx| idx.field == op.by);
                let type_ = self.columns.columns_map.get(&op.by).unwrap();
                if let Some(index) = index {
                    let index_name = &index.name;

                    if index.is_unique {
                        Self::gen_unique_delete(type_, &method_ident, index_name)
                    } else {
                        Self::gen_non_unique_delete(type_, &method_ident, index_name)
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

    fn gen_brute_force_delete_field(
        field: &Ident,
        type_: &TokenStream,
        name: &Ident,
    ) -> TokenStream {
        quote! {
            pub async fn #name(&self, by: #type_) -> core::result::Result<(), WorkTableError> {
                self.iter_with_async(|row| {
                    if row.#field == by {
                        futures::future::Either::Left(async move {
                            self.delete(row.id.into()).await
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

    fn gen_non_unique_delete(type_: &TokenStream, name: &Ident, index: &Ident) -> TokenStream {
        quote! {
            pub async fn #name(&self, by: #type_) -> core::result::Result<(), WorkTableError> {
                let rows_to_update = self.0.indexes.#index.get(&by).map(|kv| kv.1).collect::<Vec<_>>();
                for link in rows_to_update {
                    let row = self.0.data.select(*link).map_err(WorkTableError::PagesError)?;
                    self.delete(row.id.into()).await?;
                }
                core::result::Result::Ok(())
            }
        }
    }

    fn gen_unique_delete(type_: &TokenStream, name: &Ident, index: &Ident) -> TokenStream {
        quote! {
            pub async fn #name(&self, by: #type_) -> core::result::Result<(), WorkTableError> {
                let row_to_update = self.0.indexes.#index.get(&by).map(|v| v.get().value);
                if let Some(link) = row_to_update {
                    let row = self.0.data.select(link).map_err(WorkTableError::PagesError)?;
                    self.delete(row.id.into()).await?;
                }
                core::result::Result::Ok(())
            }
        }
    }
}
