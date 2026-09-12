use indexmap::IndexSet;
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::common::name_generator::WorktableNameGenerator;
use crate::generators::persist::PersistGenerator;

pub fn map_to_uppercase(str: &str) -> String {
    if str.contains("OrderedFloat") {
        let mut split = str.split("<");
        let _ = split.next();
        let inner_type = split
            .next()
            .expect("OrderedFloat def contains inner type")
            .replace(">", "");
        format!("Ordered{}", inner_type.to_uppercase().trim())
    } else if str.contains("Option") {
        let mut split = str.split("<");
        let _ = split.next();
        let inner_type = split.next().expect("Option def contains inner type").replace(">", "");
        format!("Option{}", inner_type.to_uppercase().trim())
    } else {
        str.to_uppercase()
    }
}

impl PersistGenerator {
    pub fn gen_available_types_def(&mut self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_type_ident = name_generator.get_available_type_ident();

        let unique_types: IndexSet<String> = self
            .columns
            .indexes
            .iter()
            .filter_map(|(_, idx)| self.columns.columns_map.get(&idx.field))
            .map(|ty| ty.to_string())
            .collect();

        let rows: Vec<_> = unique_types
            .iter()
            .map(|s| {
                let type_ident: TokenStream = s
                    .to_string()
                    .parse()
                    .expect("should be valid because parsed from declaration");
                let type_upper = map_to_uppercase(s);
                let type_upper = Ident::new(type_upper.as_str(), Span::mixed_site());
                Some((
                    quote! {
                        #type_upper(#type_ident),
                    },
                    // Written out rather than derived. `derive_more::From`
                    // generates `::derive_more::` paths inside its expansion,
                    // which makes that crate part of this macro's contract:
                    // a consumer who never wrote `derive_more` still had to
                    // declare it to compile a table. One newtype variant per
                    // type is a two-line impl, so the dependency bought
                    // nothing that could not be spelled here.
                    quote! {
                        impl From<#type_ident> for #avt_type_ident {
                            fn from(value: #type_ident) -> Self {
                                Self::#type_upper(value)
                            }
                        }
                    },
                ))
            })
            .collect();
        let (rows, from_impls): (Vec<_>, Vec<_>) = rows.into_iter().flatten().unzip();

        if !rows.is_empty() {
            Ok(quote! {
                #[derive(Clone, Debug, PartialEq)]
                #[non_exhaustive]
                pub enum #avt_type_ident {
                    #(#rows)*
                }

                #(#from_impls)*
            })
        } else {
            Ok(quote! {
                type #avt_type_ident = ();
            })
        }
    }

    pub fn gen_result_types_def(&mut self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_ident = name_generator.get_row_type_ident();

        if let Some(queries) = &self.queries {
            let query_defs = queries
                .updates
                .keys()
                .map(|v| {
                    let ident = Ident::new(format!("{v}Query").as_str(), Span::mixed_site());
                    let (rows, updates): (Vec<_>, Vec<_>) = queries
                        .updates
                        .get(v)
                        .expect("exists")
                        .columns
                        .iter()
                        .map(|i| {
                            let type_ = self
                                .columns
                                .columns_map
                                .get(i)
                                .ok_or(syn::Error::new(i.span(), "Unexpected column name"))?;

                            let def = if type_.to_string().contains("OrderedFloat") {
                                let inner_type = type_.to_string();
                                let mut split = inner_type.split("<");
                                let _ = split.next();
                                let inner_type = split
                                    .next()
                                    .expect("OrderedFloat def contains inner type")
                                    .to_uppercase()
                                    .replace(">", "");
                                let ident =
                                    Ident::new(format!("Ordered{}Def", inner_type.trim()).as_str(), Span::call_site());
                                quote! {
                                    #[rkyv(with = #ident)]
                                    pub #i: #type_,
                                }
                            } else {
                                quote! {pub #i: #type_,}
                            };

                            let update = quote! {
                                row.#i = self.#i;
                            };

                            Ok::<_, syn::Error>((def, update))
                        })
                        .collect::<Result<Vec<(_, _)>, _>>()?
                        .into_iter()
                        .unzip();

                    Ok::<_, syn::Error>(quote! {

                        #[derive(worktable::prelude::rkyv::Archive, Debug, worktable::prelude::rkyv::Deserialize, Clone, worktable::prelude::rkyv::Serialize)]
                        #[rkyv(crate = worktable::prelude::rkyv)]
                        #[repr(C)]
                        pub struct #ident {
                            #(#rows)*
                        }

                        impl Query<#row_ident> for #ident {
                            fn merge(self, mut row: #row_ident) -> #row_ident {
                                #(#updates)*

                                row
                            }
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let by_defs = queries
                .updates
                .values()
                .map(|op| {
                    let ident = Ident::new(format!("{}By", op.name).as_str(), Span::mixed_site());
                    let field_type = self
                        .columns
                        .columns_map
                        .get(&op.by)
                        .ok_or(syn::Error::new(op.by.span(), "Unexpected column name"))?;

                    Ok::<_, syn::Error>(quote! {
                        pub type #ident = #field_type;
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(quote! {
                #(#query_defs)*
                #(#by_defs)*
            })
        } else {
            Ok(quote! {})
        }
    }
}
