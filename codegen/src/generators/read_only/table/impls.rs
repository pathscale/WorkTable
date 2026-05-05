use proc_macro2::TokenStream;
use quote::quote;

use crate::common::name_generator::{WorktableNameGenerator, is_unsized_vec};
use crate::generators::read_only::ReadOnlyGenerator;

impl ReadOnlyGenerator {
    pub fn gen_table_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();

        let persisted_impl = self.gen_table_new_fn();
        let name_fn = self.gen_table_name_fn();
        let version_fn = self.gen_table_version_fn();
        let select_fn = self.gen_table_select_fn();
        let select_range_fn = self.gen_table_select_range_fn();
        let insert_fn = self.gen_table_insert_fn();
        let reinsert_fn = self.gen_table_reinsert_fn();
        let upsert_fn = self.gen_table_upsert_fn();
        let get_next_fn = self.gen_table_get_next_fn();
        let iter_with_fn = self.gen_table_iter_with_fn();
        let iter_with_async_fn = self.gen_table_iter_with_async_fn();
        let count_fn = self.gen_table_count_fn();
        let system_info_fn = self.gen_system_info_fn();
        let vacuum_fn = self.gen_table_vacuum_fn();

        quote! {
            #persisted_impl
            impl #ident {
                #name_fn
                #version_fn
                #select_fn
                #select_range_fn
                #insert_fn
                #reinsert_fn
                #upsert_fn
                #count_fn
                #get_next_fn
                #iter_with_fn
                #iter_with_async_fn
                #system_info_fn
                #vacuum_fn
            }
        }
    }

    fn gen_table_new_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();
        let table_name = name_generator.get_work_table_literal_name();
        let space_ident = name_generator.get_space_file_ident();
        let pk_type = name_generator.get_primary_key_type_ident();
        let const_name = name_generator.get_page_inner_size_const_ident();
        let secondary_index_events = name_generator.get_space_secondary_index_events_ident();
        let avt_index_ident = name_generator.get_available_indexes_ident();

        let pk_types = &self
            .columns
            .primary_keys
            .iter()
            .map(|i| {
                self.columns
                    .columns_map
                    .get(i)
                    .expect("should exist as got from definition")
                    .to_string()
            })
            .collect::<Vec<_>>();
        let pk_types_unsized = is_unsized_vec(pk_types);

        let index_setup = if pk_types_unsized {
            quote! {
                inner.primary_index = std::sync::Arc::new(PrimaryIndex {
                    pk_map: IndexMap::<#pk_type, OffsetEqLink<#const_name>, UnsizedNode<_>>::with_maximum_node_size(#const_name),
                    reverse_pk_map: IndexMap::new(),
                });
            }
        } else {
            quote! {
                let size = get_index_page_size_from_data_length::<#pk_type>(#const_name);
                inner.primary_index = std::sync::Arc::new(PrimaryIndex {
                    pk_map: IndexMap::<_, OffsetEqLink<#const_name>>::with_maximum_node_size(size),
                    reverse_pk_map: IndexMap::new(),
                });
            }
        };

        quote! {
            impl<E, C> PersistedWorkTable<E> for #ident
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
                async fn new(engine: E) -> eyre::Result<Self> {
                    let mut inner = WorkTable::default();
                    inner.table_name = #table_name;
                    #index_setup
                    core::result::Result::Ok(Self(inner))
                }

                async fn load(engine: E) -> eyre::Result<Self> {
                    let table_path = engine.config().table_path();
                    if !std::path::Path::new(table_path).exists() {
                        return Self::new(engine).await;
                    };
                    let space = #space_ident::parse_file(table_path).await?;
                    let table = space.into_worktable();
                    Ok(table)
                }
            }
        }
    }

    fn gen_table_name_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let dir_name = name_generator.get_dir_name();

        quote! {
            pub fn name(&self) -> &'static str {
                &self.0.table_name
            }

            pub fn name_snake_case() -> &'static str {
                #dir_name
            }
        }
    }

    fn gen_table_version_fn(&self) -> TokenStream {
        let version = self.version;

        quote! {
            pub fn version() -> u32 {
                #version
            }
        }
    }

    fn gen_table_select_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();

        quote! {
            pub fn select<Pk>(&self, pk: Pk) -> Option<#row_type>
            where #primary_key_type: From<Pk> {
                self.0.select(pk.into())
            }
        }
    }

    fn gen_table_select_range_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();
        let column_range_type = name_generator.get_column_range_type_ident();
        let row_fields_ident = name_generator.get_row_fields_enum_ident();

        quote! {
            pub fn select_by_pk_range<R>(&self, range: R) -> SelectQueryBuilder<#row_type,
                                                                     impl DoubleEndedIterator<Item = #row_type> + '_,
                                                                     #column_range_type,
                                                                     #row_fields_ident>
            where
                R: std::ops::RangeBounds<#primary_key_type>
            {
                let rows = self.0.primary_index.pk_map
                    .range(range)
                    .filter_map(|(_, link)| self.0.data.select_non_ghosted(link.0).ok());

                SelectQueryBuilder::new(rows)
            }
        }
    }

    fn gen_table_insert_fn(&self) -> TokenStream {
        quote! {}
    }

    fn gen_table_reinsert_fn(&self) -> TokenStream {
        quote! {}
    }

    fn gen_table_upsert_fn(&self) -> TokenStream {
        quote! {}
    }

    fn gen_table_get_next_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let primary_key_type = name_generator.get_primary_key_type_ident();

        match self.columns.generator_type {
            crate::common::model::GeneratorType::Custom | crate::common::model::GeneratorType::Autoincrement => {
                quote! {
                    pub fn get_next_pk(&self) -> #primary_key_type {
                        self.0.get_next_pk()
                    }
                }
            }
            crate::common::model::GeneratorType::None => {
                quote! {}
            }
        }
    }

    fn gen_table_iter_with_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let inner = self.gen_table_iter_inner(quote! {
            f(data)?;
        });

        quote! {
            pub fn iter_with<
                F: Fn(#row_type) -> core::result::Result<(), WorkTableError>
            >(&self, f: F) -> core::result::Result<(), WorkTableError> {
                #inner
            }
        }
    }

    fn gen_table_iter_with_async_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let inner = self.gen_table_iter_inner(quote! {
             f(data).await?;
        });

        quote! {
            pub async fn iter_with_async<
                F: Fn(#row_type) -> Fut,
                Fut: std::future::Future<Output = core::result::Result<(), WorkTableError>>
            >(&self, f: F) -> core::result::Result<(), WorkTableError> {
                #inner
            }
        }
    }

    fn gen_table_iter_inner(&self, func: TokenStream) -> TokenStream {
        quote! {
            let first = self.0.primary_index.pk_map.iter().next().map(|(k, v)| (k.clone(), v.0));
            let Some((mut k, link)) = first else {
                return Ok(())
            };

            let data = self.0.data.select_non_ghosted(link).map_err(WorkTableError::PagesError)?;
            #func

            let mut ind = false;
            while !ind {
                let next = {
                    let mut iter = self.0.primary_index.pk_map.range(k.clone()..);
                    let next = iter.next().map(|(k, v)| (k.clone(), v.0)).filter(|(key, _)| key != &k);
                    if next.is_some() {
                        next
                    } else {
                        iter.next().map(|(k, v)| (k.clone(), v.0))
                    }
                };
                if let Some((key, link)) = next {
                    let data = self.0.data.select_non_ghosted(link).map_err(WorkTableError::PagesError)?;
                   #func
                    k = key
                } else {
                    ind = true;
                };
            }

            core::result::Result::Ok(())
        }
    }

    fn gen_table_count_fn(&self) -> TokenStream {
        quote! {
            pub fn count(&self) -> usize {
                let count = self.0.primary_index.pk_map.len();
                count
            }
        }
    }

    fn gen_system_info_fn(&self) -> TokenStream {
        quote! {
            pub fn system_info(&self) -> SystemInfo {
                self.0.system_info()
            }
        }
    }

    fn gen_table_vacuum_fn(&self) -> TokenStream {
        quote! {}
    }
}