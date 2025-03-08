use convert_case::{Case, Casing};
use proc_macro2::Ident;
use proc_macro2::Span;
use proc_macro2::{Literal, TokenStream};
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;

impl Generator {
    pub fn gen_table_select_query_executor_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let column_range_type = name_generator.get_column_range_type_ident();

        let columns = self.columns.columns_map.iter().map(|(column, _)| {
            let col_lit = Literal::string(&column.to_string());
            let col_ident = Ident::new(&column.to_string(), Span::call_site());
            quote! {
                #col_lit => Box::new(move |left, right| match order {
                    Order::Asc => left.#col_ident.partial_cmp(&right.#col_ident).unwrap(),
                    Order::Desc => right.#col_ident.partial_cmp(&left.#col_ident).unwrap(),
                }),
            }
        });

        let column_range_variants = self
            .columns
            .columns_map
            .iter()
            .filter(|(_, ty)| ty.to_string() != "String")
            .map(|(column, ty)| {
                let variant_ident =
                    Ident::new(&column.to_string().to_case(Case::Pascal), Span::call_site());
                let ty_ident = Ident::new(&ty.to_string(), Span::call_site());
                quote! {
                    #variant_ident(std::ops::RangeInclusive<#ty_ident>),
                }
            });

        let from_impls = self
            .columns
            .columns_map
            .iter()
            .filter(|(_, ty)| ty.to_string() != "String")
            .map(|(column, ty)| {
                let variant_ident =
                    Ident::new(&column.to_string().to_case(Case::Pascal), Span::call_site());
                let type_ident = Ident::new(&ty.to_string(), Span::call_site());

                quote! {
                    impl From<std::ops::RangeInclusive<#type_ident>> for #column_range_type {
                        fn from(range: std::ops::RangeInclusive<#type_ident>) -> Self {
                            Self::#variant_ident(range)
                        }
                    }
                }
            });

        let range_matches = self
            .columns
            .columns_map
            .iter()
            .filter(|(_, ty)| ty.to_string() != "String")
            .map(|(column, _)| {
                let col_lit = Literal::string(column.to_string().as_str());
                let col_ident = Ident::new(&column.to_string(), Span::call_site());
                let variant_ident =
                    Ident::new(&column.to_string().to_case(Case::Pascal), Span::call_site());
                quote! {
                    (#col_lit, #column_range_type::#variant_ident(range)) => vals
                        .into_iter()
                        .filter(|row| range.contains(&row.#col_ident))
                        .collect(),
                }
            });

        quote! {
            #[derive(Debug, Clone)]
            pub enum #column_range_type {
                #(#column_range_variants)*
            }

            #(#from_impls)*

            impl<I> SelectQueryExecutor<#row_type, I, #column_range_type>
                for SelectQueryBuilder<#row_type, I, #column_range_type>
            where
                I: Iterator<Item = #row_type>,
            {
                fn execute(self) -> Result<Vec<#row_type>, WorkTableError> {
                    let mut vals: Vec<#row_type> = self.iter.collect();

                    if let Some((range, column)) = self.params.range {
                        vals = match (column.as_str(), range) {
                            #(#range_matches)*
                            _ => unreachable!(),
                        };
                    }

                    if !self.params.orders.is_empty() {
                        for (order, col) in self.params.orders.iter() {
                            let cmp: Box<dyn Fn(&#row_type, &#row_type) -> std::cmp::Ordering> = match col.as_str() {
                                #(#columns)*
                                _ => unreachable!(),
                            };
                            vals.sort_by(|a, b| cmp(a, b));
                        }
                    }

                    let iter = vals.into_iter();
                    let iter = if let Some(offset) = self.params.offset {
                        Box::new(iter.skip(offset)) as Box<dyn Iterator<Item = #row_type>>
                    } else {
                        Box::new(iter)
                    };

                    let iter = if let Some(limit) = self.params.limit {
                        Box::new(iter.take(limit)) as Box<dyn Iterator<Item = #row_type>>
                    } else {
                        iter
                    };
                    core::result::Result::Ok(iter.collect())
                }
            }
        }
    }
}
