use proc_macro2::TokenStream;
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::persist_table::generator::Generator;

impl Generator {
    pub fn gen_space_deserialize_impls(&self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let space_ident = name_generator.get_space_ident();

        let space_into_table = self.gen_space_into_table()?;
        let parse_space = self.gen_parse_space()?;

        Ok(quote! {
            impl #space_ident {
                #space_into_table
                #parse_space
            }
        })
    }

    fn gen_space_into_table(&self) -> syn::Result<TokenStream> {
        let wt_ident = &self.struct_def.ident;
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let index_ident = name_generator.get_index_type_ident();
        let index_type_ident = &self.index_type_ident;

        Ok(quote! {
            pub fn into_worktable(self, db_manager: std::sync::Arc<DatabaseManager>) -> #wt_ident {
                let mut page_id = 0;
                let data = self.data.into_iter().map(|p| {
                    let mut data = Data::from_data_page(p);
                    data.set_page_id(page_id.into());
                    page_id += 1;

                    std::sync::Arc::new(data)
                })
                    .collect();
                let data = DataPages::from_data(data)
                    .with_empty_links(self.info.inner.empty_links_list);
                let indexes = #index_ident::from_persisted(self.indexes);

                let pk_map = #index_type_ident::new();
                for page in self.primary_index {
                    for val in page.inner.index_values {
                        TableIndex::insert(&pk_map, val.key, val.link)
                            .expect("index is unique");
                    }
                }

                let table = WorkTable {
                    data,
                    pk_map,
                    indexes,
                    pk_gen: PrimaryKeyGeneratorState::from_state(self.info.inner.pk_gen_state),
                    lock_map: LockMap::new(),
                    table_name: "",
                    pk_phantom: std::marker::PhantomData
                };

                #wt_ident(
                    table,
                    db_manager
                )
            }
        })
    }

    fn gen_parse_space(&self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let pk_type = name_generator.get_primary_key_type_ident();
        let page_const_name = name_generator.get_page_size_const_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();
        let persisted_index_name = name_generator.get_persisted_index_ident();

        Ok(quote! {
            pub fn parse_file(file: &mut std::fs::File) -> eyre::Result<Self> {
                let info = parse_page::<SpaceInfoData<<<#pk_type as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State>, { #page_const_name as u32 }>(file, 0)?;

                let mut primary_index = vec![];
                for interval in &info.inner.primary_key_intervals {
                    for page_id in interval.0..interval.1 {
                        let index = parse_page::<IndexData<#pk_type>, { #page_const_name as u32 }>(file, page_id as u32)?;
                        primary_index.push(index);
                    }
                    let index = parse_page::<IndexData<#pk_type>, { #page_const_name as u32 }>(file, interval.1 as u32)?;
                    primary_index.push(index);
                }
                let indexes = #persisted_index_name::parse_from_file(file, &info.inner.secondary_index_intervals)?;
                let mut data = vec![];
                for interval in &info.inner.data_intervals {
                    for page_id in interval.0..interval.1 {
                        let index = parse_data_page::<{ #page_const_name }, { #inner_const_name }>(file, page_id as u32)?;
                        data.push(index);
                    }
                    let index = parse_data_page::<{ #page_const_name }, { #inner_const_name }>(file, interval.1 as u32)?;
                    data.push(index);
                }

                Ok(Self {
                    path: "".to_string(),
                    info,
                    primary_index,
                    indexes,
                    data
                })
            }
        })
    }
}
