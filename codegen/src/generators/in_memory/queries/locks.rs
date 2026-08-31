use std::collections::HashMap;

use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::common::model::Operation;
use crate::common::name_generator::WorktableNameGenerator;
use crate::generators::in_memory::InMemoryGenerator;

impl InMemoryGenerator {
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
                let snake_case_name = name.to_string().from_case(Case::Pascal).to_case(Case::Snake);

                let lock_ident = WorktableNameGenerator::get_update_in_place_query_lock_ident(&snake_case_name);

                let columns = &updates.get(name).as_ref().expect("exists").columns;
                let lock_fn = Self::gen_rows_lock_fn(columns, lock_ident);

                quote! {
                    #lock_fn
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
                let snake_case_name = name.to_string().from_case(Case::Pascal).to_case(Case::Snake);

                let lock_ident = WorktableNameGenerator::get_update_query_lock_ident(&snake_case_name);

                let op = updates.get(name).expect("exists");
                // The lock set covers the updated columns AND the predicate
                // (`by`) column: multi-row updates re-validate the predicate
                // under this lock, which is only sound if no concurrent
                // operation can rewrite that column while it is held.
                let mut columns = op.columns.clone();
                if !columns.contains(&op.by) {
                    columns.push(op.by.clone());
                }
                let lock_fn = Self::gen_rows_lock_fn(&columns, lock_ident);

                quote! {
                    #lock_fn
                }
            })
            .collect::<Vec<_>>();

        quote! {
            #(#fns)*
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
            #[allow(clippy::mutable_key_type)]
            pub fn #ident(&mut self, id: u16) -> (std::collections::HashSet<std::sync::Arc<Lock>>,  std::sync::Arc<Lock>) {
                let mut set = std::collections::HashSet::new();
                let new_lock = std::sync::Arc::new(Lock::new(id));
                #(#inner)*
                (set, new_lock)
            }
        }
    }

    pub fn gen_full_lock_for_update(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let lock_ident = name_generator.get_lock_type_ident();

        quote! {
            let lock_id = self.0.lock_manager.next_id();
            // Same atomic acquire as the per-column path: see LockMap::get_or_insert_with.
            let lock = self
                .0
                .lock_manager
                .get_or_insert_with(pk.clone(), #lock_ident::new);
            let mut lock_guard = lock.write().await;
            #[allow(clippy::mutable_key_type)]
            let (locks, op_lock) = lock_guard.lock(lock_id);
            drop(lock_guard);
            // The registered lock must be cancellation-covered BEFORE the
            // predecessor wait below: a future dropped at that await (tokio
            // timeout, task abort) would otherwise leave the registered lock
            // held forever and hang every later operation on this key.
            let pending_lock = PendingLock::new(op_lock, self.0.lock_manager.clone(), pk.clone());
            futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>()).await;
            pending_lock
        }
    }

    pub fn gen_custom_lock_for_update(&self, ident: Ident) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let lock_ident = name_generator.get_lock_type_ident();

        quote! {
            let lock_id = self.0.lock_manager.next_id();
            // One atomic acquire, no check-then-act. Splitting this into `get`
            // then `insert` let two tasks both miss, both build a lock and both
            // enter the row: the loser merged into the winner's lock, but the
            // winner had already registered its operation on a lock that was no
            // longer the map's, so it never waited for the loser.
            let lock = self
                .0
                .lock_manager
                .get_or_insert_with(pk.clone(), #lock_ident::new);
            let mut lock_guard = lock.write().await;
            #[allow(clippy::mutable_key_type)]
            let (locks, op_lock) = lock_guard.#ident(lock_id);
            drop(lock_guard);
            // The registered lock must be cancellation-covered BEFORE the
            // predecessor wait below: a future dropped at that await (tokio
            // timeout, task abort) would otherwise leave the registered lock
            // held forever and hang every later operation on this key.
            let pending_lock = PendingLock::new(op_lock, self.0.lock_manager.clone(), pk.clone());
            futures::future::join_all(locks.iter().map(|l| l.wait()).collect::<Vec<_>>()).await;
            pending_lock
        }
    }
}
