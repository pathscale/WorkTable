use convert_case::{Case, Casing};
use proc_macro2::Ident;
use proc_macro2::Span;
use proc_macro2::{Literal, TokenStream};
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;

impl Generator {
    pub fn gen_table_column_range_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let column_range_type = name_generator.get_column_range_type_ident();

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
                    impl From<std::ops::Range<#type_ident>> for #column_range_type {
                        fn from(range: std::ops::Range<#type_ident>) -> Self {
                            let end = range.end.saturating_sub(1);
                            Self::#variant_ident(range.start..=end)
                        }
                    }
                }
            });

        quote! {
            #[derive(Debug, Clone)]
            pub enum #column_range_type {
                #(#column_range_variants)*
            }

            #(#from_impls)*
        }
    }

    pub fn gen_table_select_query_executor_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let column_range_type = name_generator.get_column_range_type_ident();

        let order_matches = self.columns.columns_map.iter().map(|(column, _)| {
        let col_lit = Literal::string(&column.to_string());
        let col_ident = Ident::new(&column.to_string(), Span::call_site());
        quote! {
            #col_lit => |a: &#row_type, b: &#row_type| a.#col_ident.partial_cmp(&b.#col_ident).unwrap(),
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
                    (#col_lit, #column_range_type::#variant_ident(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.#col_ident)))
                            as Box<dyn DoubleEndedIterator<Item = #row_type>>
                    },
                }
            });

        quote! {
            impl<I> SelectQueryExecutor<#row_type, I, #column_range_type>
            for SelectQueryBuilder<#row_type, I, #column_range_type>
            where
                I: DoubleEndedIterator<Item = #row_type> + Sized,
            {
                fn execute(self) -> Result<Vec<#row_type>, WorkTableError> {
                    let mut iter: Box<dyn DoubleEndedIterator<Item = #row_type>> = Box::new(self.iter);
                    if !self.params.range.is_empty() {
                        for (range, column) in &self.params.range {
                            iter = match (column.as_str(), range.clone().into()) {
                                #(#range_matches)*
                                _ => unreachable!(),
                            };
                        }
                    }
                    if let Some((order, col)) = &self.params.order {
                        let cmp = match col.as_str() {
                            #(#order_matches)*
                            _ => unreachable!(),
                        };

                        // We cannot sort Iterator itself without Vec
                        let mut items: Vec<#row_type> = iter.collect();
                        items.sort_by(cmp);

                        iter = Box::new(items.into_iter());

                        if *order == Order::Desc {
                            iter = Box::new(iter.rev());
                        }
                    }

                    let iter_result: Box<dyn Iterator<Item = #row_type>> = if let Some(offset) = self.params.offset {
                        Box::new(iter.skip(offset))
                    } else {
                        Box::new(iter)
                    };

                    let iter_result: Box<dyn Iterator<Item = #row_type>> = if let Some(limit) = self.params.limit {
                         Box::new(iter_result.take(limit))
                    } else {
                        Box::new(iter_result)
                    };
                    Ok(iter_result.collect())
                }
            }
        }
    }
}
