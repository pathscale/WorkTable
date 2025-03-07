use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use proc_macro2::TokenStream;
use quote::quote;

impl Generator {
    pub fn gen_query_select_impl(&mut self) -> syn::Result<TokenStream> {
        let select_all = self.gen_select_all();
        let select_all2 = self.gen_select_all2();

        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let table_ident = name_generator.get_work_table_ident();

        println!("{}", select_all2);

        Ok(quote! {
            impl #table_ident {
                #select_all
                #select_all2
            }
        })
    }

    fn gen_select_all(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_ident = name_generator.get_row_type_ident();

        quote! {
            pub fn select_all<'a>(&'a self) -> SelectQueryBuilder<'a, #row_ident, Self> {
                SelectQueryBuilder::new(&self)
            }
        }
    }

    fn gen_select_all2(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_ident = name_generator.get_row_type_ident();
        let column_range_type = name_generator.get_column_range_type_ident();

        quote! {
            pub fn select_all2(&self) -> SelectQueryBuilder2<#row_ident, impl Iterator<Item = #row_ident> + '_, #column_range_type > {
                let iter = self.0.pk_map.iter().filter_map(|(_, link)| {
                     self.0.data.select(*link).ok()
                });

                SelectQueryBuilder2::new(iter)
            }
        }
    }
}
