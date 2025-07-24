use std::collections::HashMap;

use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use crate::worktable::model::Operation;

impl WorktableNameGenerator {
    pub fn get_update_query_lock_await_ident(snake_case_name: &String) -> Ident {
        Ident::new(
            format!("lock_await_update_{snake_case_name}").as_str(),
            Span::mixed_site(),
        )
    }

    pub fn get_update_query_lock_ident(snake_case_name: &String) -> Ident {
        Ident::new(
            format!("lock_update_{snake_case_name}").as_str(),
            Span::mixed_site(),
        )
    }

    pub fn get_update_query_unlock_ident(snake_case_name: &String) -> Ident {
        Ident::new(
            format!("unlock_update_{snake_case_name}").as_str(),
            Span::mixed_site(),
        )
    }

    pub fn get_update_in_place_query_lock_await_ident(snake_case_name: &String) -> Ident {
        Ident::new(
            format!("lock_await_update_in_place_{snake_case_name}").as_str(),
            Span::mixed_site(),
        )
    }

    pub fn get_update_in_place_query_lock_ident(snake_case_name: &String) -> Ident {
        Ident::new(
            format!("lock_update_in_place_{snake_case_name}").as_str(),
            Span::mixed_site(),
        )
    }

    pub fn get_update_in_place_query_unlock_ident(snake_case_name: &String) -> Ident {
        Ident::new(
            format!("unlock_update_in_place_{snake_case_name}").as_str(),
            Span::mixed_site(),
        )
    }
}

impl Generator {
    pub fn gen_query_locks_impl(&mut self) -> syn::Result<TokenStream> {
        if let Some(q) = &self.queries {
            let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
            let lock_type_ident = name_generator.get_lock_type_ident();

            let update_fns = Self::gen_update_query_locks(&q.updates);
            let update_in_place_fns = Self::gen_in_place_update_query_locks(&q.in_place);

            Ok(quote! {
                impl #lock_type_ident {
                    #update_fns
                    #update_in_place_fns
                }
            })
        } else {
            Ok(quote! {})
        }
    }

    fn gen_in_place_update_query_locks(updates: &HashMap<Ident, Operation>) -> TokenStream {
        let fns = updates
            .keys()
            .map(|name| {
                let snake_case_name = name
                    .to_string()
                    .from_case(Case::Pascal)
                    .to_case(Case::Snake);

                let lock_await_ident =
                    WorktableNameGenerator::get_update_in_place_query_lock_await_ident(
                        &snake_case_name,
                    );
                let lock_ident =
                    WorktableNameGenerator::get_update_in_place_query_lock_ident(&snake_case_name);
                let unlock_ident = WorktableNameGenerator::get_update_in_place_query_unlock_ident(
                    &snake_case_name,
                );

                let columns = &updates.get(name).as_ref().expect("exists").columns;
                let lock_await_fn = Self::gen_rows_lock_await_fn(columns, lock_await_ident);
                let lock_fn = Self::gen_rows_lock_fn(columns, lock_ident);
                let unlock_fn = Self::gen_rows_unlock_fn(columns, unlock_ident);

                quote! {
                    #lock_fn
                    #lock_await_fn
                    #unlock_fn
                }
            })
            .collect::<Vec<_>>();

        quote! {
            #(#fns)*
        }
    }

    fn gen_update_query_locks(updates: &HashMap<Ident, Operation>) -> TokenStream {
        let fns = updates
            .keys()
            .map(|name| {
                let snake_case_name = name
                    .to_string()
                    .from_case(Case::Pascal)
                    .to_case(Case::Snake);

                let lock_await_ident =
                    WorktableNameGenerator::get_update_query_lock_await_ident(&snake_case_name);
                let lock_ident =
                    WorktableNameGenerator::get_update_query_lock_ident(&snake_case_name);
                let unlock_ident =
                    WorktableNameGenerator::get_update_query_unlock_ident(&snake_case_name);

                let columns = &updates.get(name).as_ref().expect("exists").columns;
                let lock_await_fn = Self::gen_rows_lock_await_fn(columns, lock_await_ident);
                let lock_fn = Self::gen_rows_lock_fn(columns, lock_ident);
                let unlock_fn = Self::gen_rows_unlock_fn(columns, unlock_ident);

                quote! {
                    #lock_fn
                    #lock_await_fn
                    #unlock_fn
                }
            })
            .collect::<Vec<_>>();

        quote! {
            #(#fns)*
        }
    }

    fn gen_rows_unlock_fn(columns: &[Ident], ident: Ident) -> TokenStream {
        let inner = columns
            .iter()
            .map(|col| {
                let col = Ident::new(format!("{col}_lock").as_str(), Span::mixed_site());
                quote! {
                    if let Some(#col) = &self.#col {
                        #col.unlock();
                    }
                }
            })
            .collect::<Vec<_>>();

        quote! {
            pub fn #ident(&self) {
                #(#inner)*
            }
        }
    }

    fn gen_rows_lock_fn(columns: &[Ident], ident: Ident) -> TokenStream {
        let inner = columns
            .iter()
            .map(|col| {
                let col = Ident::new(format!("{col}_lock").as_str(), Span::mixed_site());
                quote! {
                    if let Some(lock) = &self.#col {
                        set.insert(lock.clone());
                    }
                    self.#col = Some(new_lock.clone());
                }
            })
            .collect::<Vec<_>>();

        quote! {
            pub fn #ident(&mut self, id: u16) -> (std::collections::HashSet< std::sync::Arc<Lock>>,  std::sync::Arc<Lock>) {
                let mut set = std::collections::HashSet::new();
                let new_lock = std::sync::Arc::new(Lock::new(id));
                #(#inner)*
                (set, new_lock)
            }
        }
    }

    fn gen_rows_lock_await_fn(columns: &[Ident], ident: Ident) -> TokenStream {
        let inner = columns
            .iter()
            .map(|col| {
                let col = Ident::new(format!("{col}_lock").as_str(), Span::mixed_site());
                quote! {
                    if let Some(lock) = &self.#col {
                        futures.push(lock.as_ref());
                    }
                }
            })
            .collect::<Vec<_>>();

        quote! {
            pub async fn #ident(&self) {
                let mut futures = Vec::new();

                #(#inner)*
                futures::future::join_all(futures).await;
            }
        }
    }

    pub fn gen_full_lock_for_update(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let lock_ident = name_generator.get_lock_type_ident();

        quote! {
            let lock_id = self.0.lock_map.next_id();
            if let Some(lock) = self.0.lock_map.get(&link) {
                let mut lock_guard = lock.write();
                let (locks, op_lock) = lock_guard.lock(lock_id);
                drop(lock_guard);
                futures::future::join_all(locks.iter().map(|l| l.as_ref()).collect::<Vec<_>>()).await;

                op_lock
            } else {
                let (lock, op_lock) = #lock_ident::with_lock(lock_id);
                let mut lock = std::sync::Arc::new(ParkingRwLock::new(lock));
                let mut guard = lock.write();
                if let Some(old_lock) = self.0.lock_map.insert(link, lock.clone()) {
                    let old_lock_guard = old_lock.read();
                    guard.merge(&*old_lock_guard);
                }

                op_lock
            }
        }
    }

    pub fn gen_custom_lock_for_update(&self, ident: Ident) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let lock_ident = name_generator.get_lock_type_ident();

        quote! {
            let lock_id = self.0.lock_map.next_id();
            if let Some(lock) = self.0.lock_map.get(&link) {
                let mut lock_guard = lock.write();
                let (locks, op_lock) = lock_guard.#ident(lock_id);
                drop(lock_guard);
                futures::future::join_all(locks.iter().map(|l| l.as_ref()).collect::<Vec<_>>()).await;

                op_lock
            } else {
                let mut lock = #lock_ident::new();
                let (_, op_lock) = lock.#ident(lock_id);
                let lock = std::sync::Arc::new(ParkingRwLock::new(lock));
                let mut guard = lock.write();
                if let Some(old_lock) = self.0.lock_map.insert(link, lock.clone()) {
                    let old_lock_guard = old_lock.read();
                    let locks = guard.merge(&*old_lock_guard);
                    drop(old_lock_guard);
                    futures::future::join_all(locks.iter().map(|l| l.as_ref()).collect::<Vec<_>>()).await;
                }

                op_lock
            }
        }
    }
}
