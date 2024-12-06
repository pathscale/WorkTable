use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

impl Generator {
    /// Generates index type and it's impls.
    pub fn gen_index_def(&mut self) -> TokenStream {
        let type_def = self.gen_type_def();
        let impl_def = self.gen_impl_def();

        quote! {
            #type_def

            #impl_def
        }
    }

    /// Generates table's secondary index struct definition. It has fields with index names and types varying on index
    /// uniqueness. For unique index it's `TreeIndex<T, Link`, for non-unique `TreeIndex<T, Arc<LockFreeSet<Link>>>`.
    /// Index also derives `PersistIndex` macro.
    fn gen_type_def(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_index_type_ident();
        let index_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                (
                    idx.is_unique,
                    &idx.name,
                    self.columns.columns_map.get(&i).clone(),
                )
            })
            .map(|(unique, i, t)| {
                if unique {
                    quote! {#i: TreeIndex<#t, Link>}
                } else {
                    quote! {#i: TreeIndex<#t, std::sync::Arc<LockFreeSet<Link>>>}
                }
            })
            .collect::<Vec<_>>();

        quote! {
            #[derive(Debug, Default, Clone, PersistIndex)]
            pub struct #ident {
                #(#index_rows),*
            }
        }
    }

    /// Generates implementation of `TableIndex` trait for index.
    fn gen_impl_def(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();
        let index_type_ident = name_generator.get_index_type_ident();

        let save_row_fn = self.gen_save_row_index_fn();
        let delete_row_fn = self.gen_save_row_index_fn();

        quote! {
            impl TableIndex<#row_type_ident> for #index_type_ident {
                #save_row_fn
                #delete_row_fn
            }
        }
    }

    /// Generates `save_row` function of `TableIndex` trait for index. It saves `Link` to all secondary indexes. Logic
    /// varies on index uniqueness. For unique index we can just insert `Link` in index, but for non-unique we need to
    /// get set from index first and then insert `Link` in set.
    fn gen_save_row_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();

        let save_rows = self.columns.indexes
            .iter()
            .map(|(i, idx)| {
                let index_field_name = &idx.name;
                if idx.is_unique {
                    quote! {
                        self.#index_field_name.insert(row.#i, link).map_err(|_| WorkTableError::AlreadyExists)?;
                    }
                } else {
                    quote! {
                        let guard = Guard::new();
                        if let Some(set) = self.#index_field_name.peek(&row.#i, &guard) {
                            set.insert(link).expect("`Link` should not be already in set");
                        } else {
                            let set = LockFreeSet::new();
                            set.insert(link).expect("`Link` should not be already in set");
                            self.#index_field_name
                                .insert(row.#i, std::sync::Arc::new(set))
                                .map_err(|_| WorkTableError::AlreadyExists)?;
                        }
                    }
                }
            }).collect::<Vec<_>>();

        quote! {
            fn save_row(&self, row: #row_type_ident, link: Link) -> core::result::Result<(), WorkTableError> {
                #(#save_rows)*
                core::result::Result::Ok(())
            }
        }
    }

    /// Generates `delete_row` function of `TableIndex` trait for index. It removes `Link` from all secondary indexes.
    /// Logic varies on index uniqueness. For unique index we can just delete `Link` from index, but for non-unique we
    /// need to get set from index first and then delete `Link` from set.
    fn gen_delete_row_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();

        let delete_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let index_field_name = &idx.name;
                if idx.is_unique {
                    quote! {
                        self.#index_field_name.remove(&row.#i);
                    }
                } else {
                    quote! {
                        let guard = Guard::new();
                        if let Some(set) = self.#index_field_name.peek(&row.#i, &guard) {
                            set.remove(&link);
                        }
                    }
                }
            })
            .collect::<Vec<_>>();

        quote! {
            fn delete_row(&self, row: #row_type_ident, link: Link) -> core::result::Result<(), WorkTableError> {
                #(#delete_rows)*
                core::result::Result::Ok(())
            }
        }
    }
}

// TODO: tests...
