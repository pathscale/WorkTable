use std::collections::HashMap;

use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::common::model::Index;
use crate::common::name_generator::{WorktableNameGenerator, is_float};
use crate::generators::read_only::ReadOnlyGenerator;

impl ReadOnlyGenerator {
    pub fn gen_table_index_fns(&self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();
        let row_ident = name_generator.get_row_type_ident();
        let column_range_type = name_generator.get_column_range_type_ident();
        let row_fields_ident = name_generator.get_row_fields_enum_ident();

        let fn_defs = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let point_fn = if idx.is_unique {
                    Self::gen_unique_index_fn(i, idx, &self.columns.columns_map, row_ident.clone())?
                } else {
                    Self::gen_non_unique_index_fn(
                        i,
                        idx,
                        &self.columns.columns_map,
                        row_ident.clone(),
                        &column_range_type,
                        &row_fields_ident,
                    )?
                };

                let range_fn = Self::gen_range_index_fn(
                    i,
                    idx,
                    &self.columns.columns_map,
                    row_ident.clone(),
                    &column_range_type,
                    &row_fields_ident,
                )?;

                Ok(quote! { #point_fn #range_fn })
            })
            .collect::<Result<Vec<_>, syn::Error>>()?;

        Ok(quote! {
            impl #ident {
                #(#fn_defs)*
            }
        })
    }

    fn gen_unique_index_fn(
        i: &Ident,
        idx: &Index,
        columns_map: &HashMap<Ident, TokenStream>,
        row_ident: Ident,
    ) -> syn::Result<TokenStream> {
        let type_ = columns_map.get(i).ok_or(syn::Error::new(i.span(), "Row not found"))?;
        let fn_name = Ident::new(format!("select_by_{i}").as_str(), Span::mixed_site());
        let field_ident = &idx.name;
        let row_field_ident = &idx.field;
        let is_float = is_float(type_.to_string().as_str());
        let by = if is_float {
            quote! {
                &OrderedFloat(by)
            }
        } else {
            quote! {
                &by
            }
        };
        let predicate_matches = if is_float {
            quote! {
                OrderedFloat(row.#row_field_ident).eq(&OrderedFloat(by))
            }
        } else {
            quote! {
                row.#row_field_ident.eq(&by)
            }
        };
        let select = quote! {
            for _ in 0..64 {
                let link: Link = self.0.indexes.#field_ident
                    .lookup_for_select(#by)
                    .map(Into::into)?;
                if let Ok(row) = self.0.data.select_non_ghosted(link) {
                    if #predicate_matches {
                        return Some(row);
                    }
                }

                let current_link: Option<Link> = self.0.indexes.#field_ident
                    .lookup_for_select(#by)
                    .map(Into::into);
                if current_link == Some(link) {
                    return None;
                }
                std::hint::spin_loop();
            }
            None
        };

        Ok(quote! {
            pub fn #fn_name(&self, by: #type_) -> Option<#row_ident> {
                let _read_guard = self.0.data.read_guard();
                #select
            }
        })
    }

    fn gen_non_unique_index_fn(
        i: &Ident,
        idx: &Index,
        columns_map: &HashMap<Ident, TokenStream>,
        row_ident: Ident,
        column_range_type: &Ident,
        row_fields_ident: &Ident,
    ) -> syn::Result<TokenStream> {
        let type_ = columns_map.get(i).ok_or(syn::Error::new(i.span(), "Row not found"))?;
        let fn_name = Ident::new(format!("select_by_{i}").as_str(), Span::mixed_site());
        let field_ident = &idx.name;
        let row_field_ident = &idx.field;
        let by = if is_float(type_.to_string().as_str()) {
            quote! {
                &OrderedFloat(by)
            }
        } else {
            quote! {
                &by
            }
        };

        Ok(quote! {
            pub fn #fn_name(&self, by: #type_) -> SelectQueryBuilder<#row_ident,
                                                                     impl DoubleEndedIterator<Item = #row_ident> + '_,
                                                                     #column_range_type,
                                                                     #row_fields_ident>
            {
                let rows = std::iter::once_with(move || {
                    let read_guard = self.0.data.read_guard();
                    self.0.indexes.#field_ident
                        .get(#by)
                        .into_iter()
                        .filter_map(move |(_, link)| {
                            let _read_guard = &read_guard;
                            self.0.data.select_non_ghosted(link.0).ok()
                        })
                        .filter(move |r| &r.#row_field_ident == &by)
                }).flatten();

                SelectQueryBuilder::new(rows)
            }
        })
    }

    fn gen_range_index_fn(
        i: &Ident,
        idx: &Index,
        columns_map: &HashMap<Ident, TokenStream>,
        row_ident: Ident,
        column_range_type: &Ident,
        row_fields_ident: &Ident,
    ) -> syn::Result<TokenStream> {
        let type_ = columns_map.get(i).ok_or(syn::Error::new(i.span(), "Row not found"))?;
        let fn_name = Ident::new(format!("select_by_{i}_range").as_str(), Span::mixed_site());
        let field_ident = &idx.name;
        let row_field_ident = &idx.field;
        let column_pascal = Ident::new(&i.to_string().to_case(Case::Pascal), Span::mixed_site());

        let (range_bounds, range_arg) = if is_float(type_.to_string().as_str()) {
            (
                quote! { std::ops::RangeBounds<#type_> },
                quote! {
                    (
                        predicate_range.0.as_ref().map(|v| OrderedFloat(*v)),
                        predicate_range.1.as_ref().map(|v| OrderedFloat(*v)),
                    )
                },
            )
        } else {
            (
                quote! { std::ops::RangeBounds<#type_> },
                quote! { predicate_range.clone() },
            )
        };
        let (index_range, select_row) = if idx.is_unique {
            (
                quote! { self.0.indexes.#field_ident.range_links(#range_arg) },
                quote! {
                    move |link| {
                        let _read_guard = &read_guard;
                        self.0.data.select_non_ghosted(link.0).ok()
                    }
                },
            )
        } else {
            (
                quote! { self.0.indexes.#field_ident.range(#range_arg) },
                quote! {
                    move |(_, link)| {
                        let _read_guard = &read_guard;
                        self.0.data.select_non_ghosted(link.0).ok()
                    }
                },
            )
        };
        let predicate_setup = quote! {
            let predicate_range = (
                range.start_bound().cloned(),
                range.end_bound().cloned(),
            );
        };
        let predicate_filter = quote! {
            .filter(move |row| {
                std::ops::RangeBounds::contains(&predicate_range, &row.#row_field_ident)
            })
        };

        Ok(quote! {
            pub fn #fn_name<'a, R>(&'a self, range: R) -> SelectQueryBuilder<#row_ident,
                                                                     impl DoubleEndedIterator<Item = #row_ident> + 'a,
                                                                     #column_range_type,
                                                                     #row_fields_ident>
            where
                R: #range_bounds + 'a
            {
                #predicate_setup
                // Query construction is not an active read. Pin the grace
                // period on the first row lookup instead.
                let rows = std::iter::once_with(move || {
                    let read_guard = self.0.data.read_guard();
                    #index_range
                        .filter_map(#select_row)
                        #predicate_filter
                }).flatten();

                SelectQueryBuilder::new_sorted(rows, #row_fields_ident::#column_pascal)
            }
        })
    }
}
