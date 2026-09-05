mod worktable_impls;

use proc_macro2::{Literal, TokenStream};
use quote::quote;

use crate::common::name_generator::WorktableNameGenerator;
use crate::persist_table::generator::Generator;

pub const WT_INDEX_EXTENSION: &str = ".wt.idx";
pub const WT_DATA_EXTENSION: &str = ".wt.data";

impl Generator {
    pub fn gen_space_file_def(&self) -> TokenStream {
        let type_ = self.gen_space_file_type();
        let impls = self.gen_space_file_impls();
        let worktable_impl = self.gen_space_file_worktable_impl();

        quote! {
            #type_
            #impls
            #worktable_impl
        }
    }

    fn gen_space_file_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let index_persisted_ident = name_generator.get_persisted_index_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();
        let pk_type = name_generator.get_primary_key_type_ident();
        let space_file_ident = name_generator.get_space_file_ident();
        let primary_index = if self.attributes.pk_unsized {
            quote! {
                pub primary_index: (Vec<GeneralPage<TableOfContentsPage<(#pk_type, Link)>>>, Vec<GeneralPage<UnsizedIndexPage<#pk_type, {#inner_const_name as u32}>>>),
            }
        } else if self.attributes.pk_congee {
            quote! {
                pub primary_index: PersistentCongeeIndex<#pk_type, OffsetEqLink<#inner_const_name>>,
            }
        } else {
            quote! {
                pub primary_index: (Vec<GeneralPage<TableOfContentsPage<(#pk_type, Link)>>>, Vec<GeneralPage<IndexPage<#pk_type>>>),
            }
        };

        quote! {
            #[derive(Debug)]
            pub struct #space_file_ident {
                #primary_index
                pub indexes: #index_persisted_ident,
                pub data: Vec<GeneralPage<DataPage<#inner_const_name>>>,
                pub data_info: GeneralPage<SpaceInfoPage<<<#pk_type as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State>>,
            }
        }
    }

    fn gen_space_file_get_primary_index_info_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let literal_name = name_generator.get_work_table_literal_name();
        let version_const = name_generator.get_version_const_ident();
        let primary_page_count = if self.attributes.pk_congee {
            quote! { 1 }
        } else {
            quote! { self.primary_index.0.len() as u32 + self.primary_index.1.len() as u32 }
        };
        let row_schema = self.attributes.row_schema.iter().map(|(name, type_name)| {
            quote! { (#name.to_string(), #type_name.to_string()) }
        });
        let primary_key_fields = self
            .attributes
            .primary_key_fields
            .iter()
            .map(|name| quote! { #name.to_string() });
        let secondary_index_types = self.attributes.secondary_index_types.iter().map(|(name, type_name)| {
            quote! { (#name.to_string(), #type_name.to_string()) }
        });

        quote! {
            fn get_primary_index_info(&self) -> eyre::Result<GeneralPage<SpaceInfoPage<()>>> {
                let mut info = {
                    let inner = SpaceInfoPage {
                        id: 0.into(),
                        version: #version_const,
                        page_count: 0,
                        name: #literal_name.to_string(),
                        pk_gen_state: (),
                        empty_links_list: vec![],
                        primary_key_fields: vec![#(#primary_key_fields),*],
                        row_schema: vec![#(#row_schema),*],
                        secondary_index_types: vec![#(#secondary_index_types),*],
                    };
                let header = GeneralHeader {
                    data_version: DATA_VERSION,
                    page_id: 0.into(),
                    previous_id: 0.into(),
                    next_id: 0.into(),
                    page_type: PageType::SpaceInfo,
                    space_id: 0.into(),
                    data_length: 0,
                };
                GeneralPage {
                    header,
                    inner
                }
                };
                info.inner.page_count = #primary_page_count;
                Ok(info)
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
        let task_ident = name_generator.get_persistence_task_ident();
        let const_name = name_generator.get_page_inner_size_const_ident();
        let pk_type = name_generator.get_primary_key_type_ident();
        let lock_type = name_generator.get_lock_type_ident();
        let table_name = name_generator.get_work_table_literal_name();
        let secondary_index_events = name_generator.get_space_secondary_index_events_ident();
        let avt_index_ident = name_generator.get_available_indexes_ident();

        let primary_index_init = if self.attributes.pk_arctic || self.attributes.pk_arctic_string {
            let map_type = if self.attributes.read_only {
                quote! { ArcticIndex }
            } else {
                quote! { PersistentArcticIndex }
            };
            quote! {
                let pk_map = #map_type::<#pk_type, OffsetEqLink<#const_name>>::default();
                for page in self.primary_index.1 {
                    for pair in page.inner.get_node() {
                        validate_arctic_link(pair.value)
                            .map_err(|error| PersistenceLoadError::corrupt(path, error))?;
                        pk_map.insert_value(pair.key, OffsetEqLink(pair.value));
                    }
                }
                let primary_index = PrimaryIndex::from_map(pk_map);
            }
        } else if self.attributes.pk_unsized {
            let pk_ident = &self.pk_ident;
            let map_type = if self.attributes.pk_wti_logical {
                quote! { PersistentWtiIndex }
            } else {
                quote! { IndexMap }
            };
            quote! {
                let pk_map = #map_type::<#pk_ident, OffsetEqLink<#const_name>, UnsizedNode<_>>::with_maximum_node_size(#const_name);
                let nodes = self.primary_index.1.into_iter().map(|page| {
                    let node = page
                        .inner
                        .get_node()
                        .into_iter()
                        .map(|p| IndexPair {
                            key: p.key,
                            value: p.value.into(),
                        })
                        .collect();
                    UnsizedNode::from_inner(node, #const_name)
                });
                pk_map.attach_nodes(nodes);
                let primary_index = PrimaryIndex::from_map(pk_map);
            }
        } else if self.attributes.pk_congee {
            quote! {
                let pk_map = self.primary_index;
                let primary_index = PrimaryIndex::from_map(pk_map);
            }
        } else {
            let map_type = if self.attributes.pk_wti_logical {
                quote! { PersistentWtiIndex }
            } else if self.pk_upstream {
                quote! { UpstreamIndexMap }
            } else {
                quote! { IndexMap }
            };
            let pair_type = if self.pk_upstream {
                quote! { UpstreamIndexPair }
            } else {
                quote! { IndexPair }
            };
            let attach_nodes = if self.pk_upstream {
                quote! {
                    for node in nodes {
                        pk_map.attach_node(node);
                    }
                }
            } else {
                quote! { pk_map.attach_nodes(nodes); }
            };
            quote! {
                let size = get_index_page_size_from_data_length::<#pk_type>(#const_name);
                let pk_map = #map_type::<_, OffsetEqLink<#const_name>>::with_maximum_node_size(size);
                let nodes = self.primary_index.1.into_iter().map(|page| {
                    page
                        .inner
                        .get_node()
                        .into_iter()
                        .map(|p| #pair_type {
                            key: p.key,
                            value: p.value.into(),
                        })
                        .collect()
                });
                #attach_nodes
                let primary_index = PrimaryIndex::from_map(pk_map);
            }
        };

        if self.attributes.read_only {
            quote! {
                pub fn into_worktable(self, path: &str) -> Result<#wt_ident, PersistenceLoadError> {
                    self.into_worktable_with_mode(path, LoadMode::Strict)
                }

                pub fn into_worktable_with_mode(
                    self,
                    path: &str,
                    mode: LoadMode,
                ) -> Result<#wt_ident, PersistenceLoadError> {
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

                    #primary_index_init

                    let table = WorkTable {
                        data: std::sync::Arc::new(data),
                        primary_index: std::sync::Arc::new(primary_index),
                        indexes: std::sync::Arc::new(indexes),
                        pk_gen: PrimaryKeyGeneratorState::from_state(self.data_info.inner.pk_gen_state),
                        lock_manager: std::sync::Arc::new(LockMap::<#lock_type, #pk_type>::default()),
                        table_name: #table_name,
                        pk_phantom: std::marker::PhantomData,
                    };

                    table.validate_persisted_state(path)?;
                    let worktable = #wt_ident(table);
                    worktable.validate_loaded_secondary_state(path, mode)?;
                    Ok(worktable)
                }
            }
        } else {
            quote! {
                pub async fn into_worktable<E, C>(
                    self,
                    engine: E,
                    path: &str,
                ) -> Result<#wt_ident, PersistenceLoadError>
                where
                    E: PersistenceEngine<
                        <<#pk_type as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
                        #pk_type,
                        #secondary_index_events,
                        #avt_index_ident,
                        Config=C
                    > + Send
                        + 'static,
                    C: Clone + PersistenceConfig,
                {
                    self.into_worktable_with_mode(engine, path, LoadMode::Strict).await
                }

                pub async fn into_worktable_with_mode<E, C>(
                    self,
                    engine: E,
                    path: &str,
                    mode: LoadMode,
                ) -> Result<#wt_ident, PersistenceLoadError>
                where
                    E: PersistenceEngine<
                        <<#pk_type as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
                        #pk_type,
                        #secondary_index_events,
                        #avt_index_ident,
                        Config=C
                    > + Send
                        + 'static,
                    C: Clone + PersistenceConfig,
                {
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

                    #primary_index_init

                    let table = WorkTable {
                        data: std::sync::Arc::new(data),
                        primary_index: std::sync::Arc::new(primary_index),
                        indexes: std::sync::Arc::new(indexes),
                        pk_gen: PrimaryKeyGeneratorState::from_state(self.data_info.inner.pk_gen_state),
                        lock_manager: std::sync::Arc::new(LockMap::<#lock_type, #pk_type>::default()),
                        table_name: #table_name,
                        pk_phantom: std::marker::PhantomData,
                    };

                    table.validate_persisted_state(path)?;
                    let worktable = #wt_ident(
                        table,
                        #task_ident::run_engine(engine)
                    );
                    worktable.validate_loaded_secondary_state(path, mode)?;
                    Ok(worktable)
                }
            }
        }
    }

    fn gen_space_file_parse_file_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let pk_type = name_generator.get_primary_key_type_ident();
        let page_const_name = name_generator.get_page_size_const_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();
        let persisted_index_name = name_generator.get_persisted_index_ident();
        let version_const_name = name_generator.get_version_const_ident();
        let index_extension = Literal::string(WT_INDEX_EXTENSION);
        let data_extension = Literal::string(WT_DATA_EXTENSION);

        let parse_pk_page = if self.attributes.pk_unsized {
            quote! {
                let index = parse_page::<UnsizedIndexPage<#pk_type, {#inner_const_name as u32}>, { #page_const_name as u32 }>(&mut primary_file, (*page_id).into()).await?;
            }
        } else {
            quote! {
                let index = parse_page::<IndexPage<#pk_type>, { #page_const_name as u32 }>(&mut primary_file, (*page_id).into()).await?;
            }
        };

        let parse_primary = if self.attributes.pk_congee {
            quote! {
                SpaceCongeeIndex::<#pk_type, { #inner_const_name as u32 }>::load_index::<#inner_const_name>(
                    format!("{}/primary{}", path, #index_extension),
                    #version_const_name,
                ).await?
            }
        } else {
            quote! {
                {
                    let mut primary_index = vec![];
                    let mut primary_file = tokio::fs::File::open(format!("{}/primary{}", path, #index_extension)).await?;
                    let info = parse_page::<SpaceInfoPage<()>, { #page_const_name as u32 }>(&mut primary_file, 0).await?;
                    let file_length = primary_file.metadata().await?.len();
                    // Pages sit at a fixed #page_const_name stride with the
                    // general header inside the slot, so the next free page id
                    // is ceil(len / stride). The previous divisor added the
                    // header on top of the full stride and lagged one page
                    // behind roughly every 512 pages.
                    let count = file_length.div_ceil(#page_const_name as u64);
                    let next_page_id = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(count as u32));
                    let toc = IndexTableOfContents::<_, { #page_const_name as u32 }>::parse_from_file(&mut primary_file, 0.into(), next_page_id.clone()).await?;
                    for page_id in toc.iter().map(|(_, page_id)| page_id) {
                        #parse_pk_page
                        primary_index.push(index);
                    }
                    (toc.pages, primary_index)
                }
            }
        };

        quote! {
            pub async fn parse_file(path: &str) -> eyre::Result<Self> {
                let primary_index = #parse_primary;

                let indexes = #persisted_index_name::parse_from_file(path).await?;
                let (data, data_info) = {
                    let mut data = vec![];
                    let mut data_file = tokio::fs::File::open(format!("{}/{}", path, #data_extension)).await?;
                    let info = parse_page::<SpaceInfoPage<<<#pk_type as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State>, { #page_const_name as u32 }>(&mut data_file, 0).await?;
                    let file_length = data_file.metadata().await?.len();
                    // ceil(len / stride) counts every occupied page slot,
                    // including the info page at id 0, whether or not the last
                    // page fills its slot. The previous floor + inclusive
                    // range parsed one page past EOF whenever the file ended
                    // exactly on a page boundary (a final data page that
                    // exactly fills its slot), failing the whole load.
                    let count = file_length.div_ceil(#page_const_name as u64);
                    for page_id in 1..count {
                        let index = parse_data_page::<{ #page_const_name as u32}, { #inner_const_name as usize }>(&mut data_file, page_id as u32).await?;
                        data.push(index);
                    }
                    (data, info)
                };

                Ok(Self {
                    primary_index,
                    indexes,
                    data,
                    data_info
                })
            }
        }
    }
}
