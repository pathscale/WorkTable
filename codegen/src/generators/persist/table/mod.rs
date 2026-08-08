mod impls;
mod index_fns;
mod select_executor;

use proc_macro2::{Literal, TokenStream};
use quote::quote;

use crate::common::name_generator::{WorktableNameGenerator, is_unsized_vec};
use crate::generators::index_backend::persistent_unique_index_type;
use crate::generators::persist::PersistGenerator;

impl PersistGenerator {
    pub fn gen_table_def(&mut self) -> syn::Result<TokenStream> {
        let page_size_consts = self.gen_page_size_consts();
        let version_const = self.gen_version_const();
        let type_ = self.gen_table_type()?;
        let impl_ = self.gen_table_impl();
        let index_fns = self.gen_table_index_fns()?;
        let select_query_executor_impl = self.gen_table_select_query_executor_impl();
        let column_range_type = self.gen_table_column_range_type();
        let columnar_methods = crate::generators::columnar::table_methods(&self.name, &self.columns);
        let table_ident = WorktableNameGenerator::from_table_name(self.name.to_string()).get_work_table_ident();

        Ok(quote! {
            #page_size_consts
            #version_const
            #type_
            #impl_
            #index_fns
            #select_query_executor_impl
            #column_range_type
            impl #table_ident {
                #columnar_methods
            }
        })
    }

    fn gen_page_size_consts(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let page_const_name = name_generator.get_page_size_const_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();

        if let Some(page_size) = &self.config.as_ref().and_then(|c| c.page_size) {
            let page_size = Literal::usize_unsuffixed(*page_size as usize);
            quote! {
                const #page_const_name: usize = #page_size;
                const #inner_const_name: usize = #page_size - GENERAL_HEADER_SIZE;
            }
        } else {
            quote! {
                const #page_const_name: usize = PAGE_SIZE;
                const #inner_const_name: usize = #page_const_name - GENERAL_HEADER_SIZE;
            }
        }
    }

    fn gen_version_const(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let version_const_name = name_generator.get_version_const_ident();
        let version = self.version;

        quote! {
            const #version_const_name: u32 = #version;
        }
    }

    fn gen_table_type(&self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();
        let index_type = name_generator.get_index_type_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();
        let avt_type_ident = name_generator.get_available_type_ident();
        let avt_index_ident = name_generator.get_available_indexes_ident();
        let persistence_task = name_generator.get_persistence_task_ident();
        let lock_ident = name_generator.get_lock_type_ident();

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
        let derive = match (pk_types_unsized, self.columns.primary_index_backend) {
            (true, crate::common::model::IndexBackend::Indexset) => quote! {
                #[derive(Debug, PersistTable)]
                #[table(pk_unsized, pk_upstream)]
            },
            (true, crate::common::model::IndexBackend::WorktablesIndex)
                if cfg!(feature = "logical-index-persistence") =>
            {
                quote! {
                    #[derive(Debug, PersistTable)]
                    #[table(pk_unsized, pk_wti_logical)]
                }
            }
            (true, _) => quote! {
                #[derive(Debug, PersistTable)]
                #[table(pk_unsized)]
            },
            (false, crate::common::model::IndexBackend::Indexset) => quote! {
                #[derive(Debug, PersistTable)]
                #[table(pk_upstream)]
            },
            (false, crate::common::model::IndexBackend::Arctic) => quote! {
                #[derive(Debug, PersistTable)]
                #[table(pk_arctic)]
            },
            (false, crate::common::model::IndexBackend::Congee) => quote! {
                #[derive(Debug, PersistTable)]
                #[table(pk_congee)]
            },
            (false, crate::common::model::IndexBackend::WorktablesIndex)
                if cfg!(feature = "logical-index-persistence") =>
            {
                quote! {
                    #[derive(Debug, PersistTable)]
                    #[table(pk_wti_logical)]
                }
            }
            (false, crate::common::model::IndexBackend::WorktablesIndex) => quote! {
                #[derive(Debug, PersistTable)]
            },
        };

        let key_type = quote! { #primary_key_type };
        let value_type = quote! { OffsetEqLink<#inner_const_name> };
        let worktables_node = if pk_types_unsized {
            Some(quote! {
                UnsizedNode<IndexPair<#primary_key_type, OffsetEqLink<#inner_const_name>>>
            })
        } else {
            None
        };
        let node_type = persistent_unique_index_type(
            self.columns.primary_index_backend,
            &key_type,
            &value_type,
            worktables_node,
        )?;

        let mut row_schema = self
            .columns
            .field_positions
            .iter()
            .map(|(name, position)| {
                let type_name = self.columns.columns_map.get(name).expect("column exists").to_string();
                (*position, name, Literal::string(&type_name))
            })
            .collect::<Vec<_>>();
        row_schema.sort_by_key(|(position, _, _)| *position);
        let row_schema_names = row_schema.iter().map(|(_, name, _)| *name).collect::<Vec<_>>();
        let row_schema_types = row_schema.iter().map(|(_, _, type_name)| type_name).collect::<Vec<_>>();
        let primary_key_fields = &self.columns.primary_keys;
        let secondary_indexes = self.columns.indexes.values().collect::<Vec<_>>();
        let secondary_index_names = secondary_indexes.iter().map(|index| &index.name).collect::<Vec<_>>();
        let secondary_index_types = secondary_indexes
            .iter()
            .map(|index| {
                let type_name = self
                    .columns
                    .columns_map
                    .get(&index.field)
                    .expect("indexed column exists")
                    .to_string();
                Literal::string(&type_name)
            })
            .collect::<Vec<_>>();
        let schema_attribute = quote! {
            #[table(
                row_schema(#(#row_schema_names = #row_schema_types),*),
                primary_key_fields(#(#primary_key_fields),*)
            )]
        };
        let secondary_schema_attribute = (!secondary_indexes.is_empty()).then(|| {
            quote! {
                #[table(secondary_index_types(#(#secondary_index_names = #secondary_index_types),*))]
            }
        });

        Ok(if self.config.as_ref().and_then(|c| c.page_size).is_some() {
            quote! {
                #derive
                #schema_attribute
                #secondary_schema_attribute
                pub struct #ident(
                    WorkTable<
                        #row_type,
                        #primary_key_type,
                        #avt_type_ident,
                        #avt_index_ident,
                        #index_type,
                        #lock_ident,
                        <#primary_key_type as TablePrimaryKey>::Generator,
                        #inner_const_name,
                        #node_type
                    >
                    , #persistence_task
                );
            }
        } else {
            quote! {
                #derive
                #schema_attribute
                #secondary_schema_attribute
                pub struct #ident(
                    WorkTable<
                        #row_type,
                        #primary_key_type,
                        #avt_type_ident,
                        #avt_index_ident,
                        #index_type,
                        #lock_ident,
                        <#primary_key_type as TablePrimaryKey>::Generator,
                        { INNER_PAGE_SIZE },
                        #node_type
                    >
                    , #persistence_task
                );
            }
        })
    }
}
