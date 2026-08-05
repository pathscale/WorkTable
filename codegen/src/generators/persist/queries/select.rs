use crate::common::name_generator::WorktableNameGenerator;
use crate::generators::persist::PersistGenerator;
use proc_macro2::TokenStream;
use quote::quote;

impl PersistGenerator {
    pub fn gen_query_select_impl(&mut self) -> syn::Result<TokenStream> {
        let select_all = self.gen_select_all();

        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let table_ident = name_generator.get_work_table_ident();

        Ok(quote! {
            impl #table_ident {
                #select_all
            }
        })
    }

    fn gen_select_all(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_ident = name_generator.get_row_type_ident();
        let column_range_type = name_generator.get_column_range_type_ident();
        let row_fields_ident = name_generator.get_row_fields_enum_ident();

        quote! {
            pub fn select_all(&self) -> SelectQueryBuilder<#row_ident,
                                                           impl DoubleEndedIterator<Item = #row_ident> + '_ + Sized,
                                                           #column_range_type,
                                                           #row_fields_ident>
            {
                // Acquire the grace-period guard only when iteration starts.
                // Merely constructing and retaining a query builder must not
                // stall retired-link reclamation.
                let iter = std::iter::once_with(move || {
                    let read_guard = self.0.data.read_guard();
                    self.0.primary_index.pk_map
                        .iter_values()
                        .filter_map(move |(primary_key, link)| {
                            let _read_guard = &read_guard;
                            let mut current_link = link.0;
                            for _ in 0..64 {
                                if let Ok(row) = self.0.data.select_non_ghosted(current_link) {
                                    return Some(row);
                                }

                                // A reinsert publishes the replacement link
                                // before retiring the captured one. Follow that
                                // replacement instead of silently omitting the
                                // row from a concurrent full-table scan.
                                let replacement: Link = self.0.primary_index.pk_map
                                    .lookup_for_select(&primary_key)
                                    .map(Into::into)?;
                                if replacement == current_link {
                                    return None;
                                }
                                current_link = replacement;
                                std::hint::spin_loop();
                            }
                            None
                        })
                }).flatten();

                SelectQueryBuilder::new(iter)
            }
        }
    }
}
