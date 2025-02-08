mod worktable_impls;

use proc_macro2::{Literal, TokenStream};
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::persist_table::generator::Generator;

pub const WT_INDEX_EXTENSION: &str = ".wt.idx";
pub const WT_DATA_EXTENSION: &str = ".wt.data";

impl Generator {
    pub fn gen_space_file_def(&self) -> TokenStream {
        let type_ = self.gen_space_file_type();
        let impls = self.gen_space_file_impls();
        let worktable_impl = self.gen_space_file_worktable_impl();
        let space_persist_impl = self.gen_space_persist_impl();

        quote! {
            #type_
            #impls
            #worktable_impl
            #space_persist_impl
        }
    }

    fn gen_space_file_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let index_persisted_ident = name_generator.get_persisted_index_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();
        let pk_type = name_generator.get_primary_key_type_ident();
        let space_file_ident = name_generator.get_space_file_ident();

        quote! {
            #[derive(Debug)]
            pub struct #space_file_ident {
                pub path: String,
                pub primary_index: Vec<GeneralPage<NewIndexPage<#pk_type>>>,
                pub indexes: #index_persisted_ident,
                pub data: Vec<GeneralPage<DataPage<#inner_const_name>>>,
                pub data_info: GeneralPage<SpaceInfoData>,
            }
        }
    }

    fn gen_space_file_get_primary_index_info_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let ident = name_generator.get_work_table_ident();
        let pk_type = name_generator.get_primary_key_type_ident();

        quote! {
            fn get_primary_index_info(&self) -> eyre::Result<GeneralPage<SpaceInfoData<<<#pk_type as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State>>> {
                let mut info = #ident::space_info_default();
                info.inner.page_count = self.primary_index.len() as u32;
                Ok(info)
            }
        }
    }

    fn gen_space_persist_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let space_ident = name_generator.get_space_file_ident();
        let index_extension = Literal::string(WT_INDEX_EXTENSION);
        let data_extension = Literal::string(WT_DATA_EXTENSION);

        quote! {
            impl #space_ident {
                pub fn persist(&mut self) -> eyre::Result<()> {
                    let prefix = &self.path;
                    std::fs::create_dir_all(prefix)?;

                    {
                        let mut primary_index_file = std::fs::File::create(format!("{}/primary{}", &self.path, #index_extension))?;
                        let mut info = self.get_primary_index_info()?;
                        persist_page(&mut info, &mut primary_index_file)?;
                        for mut primary_index_page in &mut self.primary_index {
                            persist_page(&mut primary_index_page, &mut primary_index_file)?;
                        }
                    }

                    self.indexes.persist(&prefix)?;

                    {
                        let mut data_file = std::fs::File::create(format!("{}/{}", &self.path, #data_extension))?;
                        persist_page(&mut self.data_info, &mut data_file)?;
                        for mut data_page in &mut self.data {
                            persist_page(&mut data_page, &mut data_file)?;
                        }
                    }

                    Ok(())
                }
            }
        }
    }

    pub fn gen_space_file_impls(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let space_ident = name_generator.get_space_file_ident();

        let into_worktable_fn = self.gen_space_file_into_worktable_fn();
        let parse_file_fn = self.gen_space_file_parse_file_fn();
        let get_primary_index_info_fn = self.gen_space_file_get_primary_index_info_fn();

        quote! {
            impl #space_ident {
                #into_worktable_fn
                #parse_file_fn
                #get_primary_index_info_fn
            }
        }
    }

    fn gen_space_file_into_worktable_fn(&self) -> TokenStream {
        let wt_ident = &self.struct_def.ident;
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let index_ident = name_generator.get_index_type_ident();

        quote! {
            pub fn into_worktable(self, db_manager: std::sync::Arc<DatabaseManager>) -> #wt_ident {
                let mut page_id = 1;
                let data = self.data.into_iter().map(|p| {
                    let mut data = Data::from_data_page(p);
                    data.set_page_id(page_id.into());
                    page_id += 1;

                    std::sync::Arc::new(data)
                })
                    .collect();
                let data = DataPages::from_data(data)
                    .with_empty_links(self.data_info.inner.empty_links_list);
                let indexes = #index_ident::from_persisted(self.indexes);

                let pk_map = IndexMap::new();
                for page in self.primary_index {
                    let node = page.inner.get_node();
                    pk_map.attach_node(node);
                }

                let table = WorkTable {
                    data,
                    pk_map,
                    indexes,
                    pk_gen: PrimaryKeyGeneratorState::from_state(self.data_info.inner.pk_gen_state),
                    lock_map: LockMap::new(),
                    table_name: "",
                    pk_phantom: std::marker::PhantomData
                };

                #wt_ident(
                    table,
                    db_manager
                )
            }
        }
    }

    fn gen_space_file_parse_file_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let pk_type = name_generator.get_primary_key_type_ident();
        let page_const_name = name_generator.get_page_size_const_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();
        let persisted_index_name = name_generator.get_persisted_index_ident();
        let index_extension = Literal::string(WT_INDEX_EXTENSION);
        let data_extension = Literal::string(WT_DATA_EXTENSION);

        quote! {
            pub fn parse_file(path: &String) -> eyre::Result<Self> {
                let mut primary_index = {
                    let mut primary_index = vec![];
                    let mut primary_file = std::fs::File::open(format!("{}/primary{}", path, #index_extension))?;
                    let info = parse_page::<SpaceInfoData<<<#pk_type as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State>, { #page_const_name as u32 }>(&mut primary_file, 0)?;
                    for page_id in 1..=info.inner.page_count {
                        let index = parse_page::<NewIndexPage<#pk_type>, { #page_const_name as u32 }>(&mut primary_file, page_id as u32)?;
                        primary_index.push(index);
                    }
                    primary_index
                };

                let indexes = #persisted_index_name::parse_from_file(path)?;
                let (data, data_info) = {
                    let mut data = vec![];
                    let mut data_file = std::fs::File::open(format!("{}/{}", path, #data_extension))?;
                    let info = parse_page::<SpaceInfoData<<<#pk_type as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State>, { #page_const_name as u32 }>(&mut data_file, 0)?;
                    for page_id in 1..=info.inner.page_count {
                        let index = parse_data_page::<{ #page_const_name }, { #inner_const_name }>(&mut data_file, page_id as u32)?;
                        data.push(index);
                    }
                    (data, info)
                };

                Ok(Self {
                    path: "".to_string(),
                    primary_index,
                    indexes,
                    data,
                    data_info
                })
            }
        }
    }
}
