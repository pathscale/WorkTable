use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

impl Generator {
    pub fn gen_locks_def(&self) -> TokenStream {
        let type_ = self.gen_locks_type();
        let impl_ = self.gen_locks_impl();

        println!("!TYPE {}", type_);
        println!("!IMPL {}", impl_);

        quote! {
            #type_
            #impl_
        }
    }

    pub fn gen_locks_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let lock_ident = name_generator.get_lock_type_ident();

        let row_locks = self
            .columns
            .columns_map
            .keys()
            .map(|i| {
                let name = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! {
                    #name: Option<std::sync::Arc<Lock>>,
                }
            })
            .collect::<Vec<_>>();

        quote! {
             #[derive(Debug, Clone)]
             pub struct #lock_ident {
                lock: Option<std::sync::Arc<Lock>>,
                #(#row_locks)*
            }
        }
    }

    fn gen_locks_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let lock_ident = name_generator.get_lock_type_ident();

        let row_new = self
            .columns
            .columns_map
            .keys()
            .map(|i| {
                let col = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! {
                    #col: None
                }
            })
            .collect::<Vec<_>>();

        let row_with_lock = self
            .columns
            .columns_map
            .keys()
            .map(|i| {
                let col = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! {
                    #col: Some(std::sync::Arc::new(Lock::new()))
                }
            })
            .collect::<Vec<_>>();

        let row_lock_await = self
            .columns
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
            .collect::<Vec<_>>();

        let row_unlock = self
            .columns
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
            .collect::<Vec<_>>();

        let row_lock = self
            .columns
            .columns_map
            .keys()
            .map(|col| {
                let col = Ident::new(format!("{}_lock", col).as_str(), Span::mixed_site());
                quote! {
                     if let Some(#col) = &self.#col {
                        #col.lock();
                     }
                }
            })
            .collect::<Vec<_>>();

        quote! {

            impl #lock_ident {
                pub fn new() -> Self {
                    Self {
                        lock: None,
                        #(#row_new),*
                    }
                }

                pub fn with_lock() -> Self {
                    Self {
                        lock:  Some(std::sync::Arc::new(Lock::new())),
                        #(#row_with_lock),*

                    }
                }

                pub fn lock(&self) {
                    if let Some(lock) = &self.lock {
                        lock.lock();
                    }
                    #(#row_lock)*

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
}
