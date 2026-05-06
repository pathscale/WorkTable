use std::collections::HashSet;

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::common::name_generator::WorktableNameGenerator;
use crate::generators::read_only::ReadOnlyGenerator;

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

impl ReadOnlyGenerator {
    pub fn gen_available_types_def(&mut self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_type_ident = name_generator.get_available_type_ident();

        let unique_types: HashSet<String> = self
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
                Some(quote! {
                    #[from]
                    #type_upper(#type_ident),
                })
            })
            .collect();

        if !rows.is_empty() {
            Ok(quote! {
                #[derive(Clone, Debug, From,  PartialEq)]
                #[non_exhaustive]
                pub enum #avt_type_ident {
                    #(#rows)*
                }
            })
        } else {
            Ok(quote! {
                type #avt_type_ident = ();
            })
        }
    }
}
