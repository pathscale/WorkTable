use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

impl Generator {
    pub fn gen_locks_def(&self) -> TokenStream {
        let type_def = self.gen_locks_type();
        let impl_def = self.gen_locks_impl();

        quote! {
            #type_def
            #impl_def
        }
    }

    fn gen_locks_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let lock_ident = name_generator.get_lock_type_ident();
        let row_locks = self.gen_row_locks();

        quote! {
             #[derive(Debug, Clone)]
             pub struct #lock_ident {
                id: u16,
                lock: Option<std::sync::Arc<Lock>>,
                #(#row_locks)*
            }
        }
    }

    fn gen_locks_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let lock_ident = name_generator.get_lock_type_ident();

        let row_new = self.gen_row_new();
        let row_with_lock = self.gen_row_with_lock();
        let row_lock_await = self.gen_row_lock_await();
        let row_unlock = self.gen_row_unlock();

        quote! {
            impl #lock_ident {
                pub fn new(lock_id: u16) -> Self {
                    Self {
                        id: lock_id,
                        lock: None,
                        #(#row_new),*
                    }
                }

                pub fn with_lock(lock_id: u16) -> Self {
                    Self {
                        id: lock_id,
                        lock: Some(std::sync::Arc::new(Lock::new())),
                        #(#row_with_lock),*
                    }
                }

                pub fn unlock(&self) {
                    if let Some(lock) = &self.lock {
                        lock.unlock();
                    }
                    #(#row_unlock)*
                }

                pub async fn lock_await(&self) {
                    let mut futures = Vec::new();

                    if let Some(lock) = &self.lock {
                        futures.push(lock.as_ref());
                    }
                    #(#row_lock_await)*
                    futures::future::join_all(futures).await;
                }
            }
        }
    }

    fn gen_row_locks(&self) -> Vec<TokenStream> {
        self.columns
            .columns_map
            .keys()
            .map(|i| {
                let name = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! { #name: Option<std::sync::Arc<Lock>>, }
            })
            .collect()
    }

    fn gen_row_new(&self) -> Vec<TokenStream> {
        self.columns
            .columns_map
            .keys()
            .map(|i| {
                let col = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! { #col: None }
            })
            .collect()
    }

    fn gen_row_with_lock(&self) -> Vec<TokenStream> {
        self.columns
            .columns_map
            .keys()
            .map(|i| {
                let col = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! { #col: Some(std::sync::Arc::new(Lock::new())) }
            })
            .collect()
    }

    fn gen_row_lock_await(&self) -> Vec<TokenStream> {
        self.columns
            .columns_map
            .keys()
            .map(|col| {
                let col = Ident::new(format!("{}_lock", col).as_str(), Span::mixed_site());
                quote! {
                   if let Some(lock) = &self.#col {
                        futures.push(lock.as_ref());
                   }
                }
            })
            .collect()
    }

    fn gen_row_unlock(&self) -> Vec<TokenStream> {
        self.columns
            .columns_map
            .keys()
            .map(|col| {
                let col = Ident::new(format!("{}_lock", col).as_str(), Span::mixed_site());
                quote! {
                     if let Some(#col) = &self.#col {
                        #col.unlock();
                     }
                }
            })
            .collect()
    }
}
