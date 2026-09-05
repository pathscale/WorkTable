use proc_macro2::TokenStream;
use quote::quote;

use crate::common::name_generator::WorktableNameGenerator;
use crate::persist_table::generator::Generator;

impl Generator {
    pub fn gen_space_file_worktable_impl(&self) -> TokenStream {
        let ident = &self.struct_def.ident;
        let space_info_fn = self.gen_worktable_space_info_fn();
        let persisted_pk_fn = self.gen_worktable_persisted_primary_key_fn();
        let wait_for_ops_fn = self.gen_worktable_wait_for_ops_fn();
        let persistence_monitor_fn = self.gen_worktable_persistence_monitor_fn();
        let close_fn = self.gen_worktable_close_fn();
        let unload_fn = self.gen_worktable_unload_fn();
        let persisted_data_file_size_fn = self.gen_persisted_data_file_size_fn();

        quote! {
            impl #ident {
                #space_info_fn
                #persisted_pk_fn
                #wait_for_ops_fn
                #persistence_monitor_fn
                #close_fn
                #unload_fn
                #persisted_data_file_size_fn
            }
        }
    }

    fn gen_worktable_unload_fn(&self) -> TokenStream {
        quote! {
            /// Retires an Arc-owned table generation after the caller's
            /// quiesce barrier has stopped new leases and drained old ones.
            pub async fn unload_gracefully<F, Fut>(
                self: std::sync::Arc<Self>,
                timeout: std::time::Duration,
                quiesce: F,
            ) -> Result<UnloadReport, UnloadFailure<Self>>
            where
                F: FnOnce() -> Fut,
                Fut: std::future::Future<Output = ()>,
            {
                // Attribute the generation at the retirement request. The
                // quiesce callback can give background maintenance time to
                // shrink or rearrange live structures before the final drop;
                // measuring afterwards would make the report depend on how
                // long the reader barrier happened to take.
                let estimated_released_bytes = self.heap_size();
                if tokio::time::timeout(timeout, quiesce()).await.is_err() {
                    return Err(UnloadFailure::retained(
                        self,
                        eyre::eyre!("timed out waiting for generation leases to quiesce"),
                    ));
                }

                let owned = match std::sync::Arc::try_unwrap(self) {
                    Ok(owned) => owned,
                    Err(arc) => {
                        let outstanding = std::sync::Arc::strong_count(&arc).saturating_sub(1);
                        return Err(UnloadFailure::retained(
                            arc,
                            eyre::eyre!("cannot unload generation: {outstanding} Arc lease(s) remain"),
                        ));
                    }
                };
                owned.close().await.map_err(|error| {
                    UnloadFailure::after_close(eyre::Report::new(error))
                })?;
                Ok(UnloadReport { estimated_released_bytes })
            }
        }
    }

    fn gen_persisted_data_file_size_fn(&self) -> TokenStream {
        if self.attributes.read_only {
            quote! {}
        } else {
            quote! {
                /// Returns the physical size of this table's `.wt.data` file.
                /// Persisted vacuum makes freed pages reusable across reloads,
                /// but does not truncate this file.
                pub async fn persisted_data_file_size_bytes(&self) -> std::io::Result<u64> {
                    self.1.persisted_data_file_size_bytes().await
                }
            }
        }
    }

    fn gen_worktable_wait_for_ops_fn(&self) -> TokenStream {
        if self.attributes.read_only {
            quote! {
                pub async fn wait_for_ops(&self) -> PersistenceResult {
                    Ok(())
                }
            }
        } else {
            quote! {
                pub async fn wait_for_ops(&self) -> PersistenceResult {
                   self.1.wait_for_ops().await
                }
            }
        }
    }

    fn gen_worktable_persistence_monitor_fn(&self) -> TokenStream {
        if self.attributes.read_only {
            quote! {}
        } else {
            quote! {
                /// Returns a cloneable terminal-state monitor that does not
                /// borrow the table and can therefore observe `close()`.
                pub fn persistence_monitor(&self) -> PersistenceMonitor {
                    self.1.monitor()
                }
            }
        }
    }

    fn gen_worktable_close_fn(&self) -> TokenStream {
        if self.attributes.read_only {
            quote! {
                pub async fn close(self) -> PersistenceResult {
                    Ok(())
                }
            }
        } else {
            quote! {
                pub async fn close(self) -> PersistenceResult {
                    self.1.close().await
                }
            }
        }
    }

    fn gen_worktable_space_info_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let pk = name_generator.get_primary_key_type_ident();
        let literal_name = name_generator.get_work_table_literal_name();
        let version_const = name_generator.get_version_const_ident();
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
            pub fn space_info_default() -> GeneralPage<SpaceInfoPage<<<#pk as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State>> {
                let inner = SpaceInfoPage {
                    version: #version_const,
                    id: 0.into(),
                    page_count: 0,
                    name: #literal_name.to_string(),
                    pk_gen_state: <<#pk as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State::default(),
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
            }
        }
    }

    fn gen_worktable_persisted_primary_key_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let pk_type = name_generator.get_primary_key_type_ident();
        let const_name = name_generator.get_page_inner_size_const_ident();
        if self.attributes.pk_congee {
            // Congee durability is maintained by its native checkpoint/WAL.
            quote! {}
        } else if self.attributes.pk_arctic_string {
            quote! {
                pub fn get_peristed_primary_key_with_toc(&self) -> (Vec<GeneralPage<TableOfContentsPage<(#pk_type, Link)>>>, Vec<GeneralPage<UnsizedIndexPage<#pk_type, {#const_name as u32}>>>) {
                    let shadow = IndexMap::<#pk_type, OffsetEqLink<#const_name>, UnsizedNode<_>>::with_maximum_node_size(#const_name);
                    for (key, value) in self.0.primary_index.pk_map.iter_values() {
                        shadow.insert(key, value);
                    }
                    let mut pages = vec![];
                    for node in shadow.snapshot_nodes() {
                        pages.push(UnsizedIndexPage::from_node(node.as_ref()));
                    }
                    let (toc, pages) = map_unsized_index_pages_to_toc_and_general::<_, { #const_name as u32 }>(pages);
                    (toc.pages, pages)
                }
            }
        } else if self.attributes.pk_arctic {
            quote! {
                pub fn get_peristed_primary_key_with_toc(&self) -> (Vec<GeneralPage<TableOfContentsPage<(#pk_type, Link)>>>, Vec<GeneralPage<IndexPage<#pk_type>>>) {
                    let size = get_index_page_size_from_data_length::<#pk_type>(#const_name);
                    let shadow = IndexMap::<#pk_type, OffsetEqLink<#const_name>>::with_maximum_node_size(size);
                    for (key, value) in self.0.primary_index.pk_map.iter_values() {
                        shadow.insert(key, value);
                    }
                    let mut pages = vec![];
                    for node in shadow.snapshot_nodes() {
                        pages.push(IndexPage::from_node(&node, size));
                    }
                    let (toc, pages) = map_index_pages_to_toc_and_general::<_, { #const_name as u32 }>(pages);
                    (toc.pages, pages)
                }
            }
        } else if self.attributes.pk_unsized {
            quote! {
                pub fn get_peristed_primary_key_with_toc(&self) -> (Vec<GeneralPage<TableOfContentsPage<(#pk_type, Link)>>>, Vec<GeneralPage<UnsizedIndexPage<#pk_type, {#const_name as u32}>>>) {
                    let mut pages = vec![];
                    for node in self.0.primary_index.pk_map.snapshot_nodes() {
                        let page = UnsizedIndexPage::from_node(node.as_ref());
                        pages.push(page);
                    }
                    let (toc, pages) = map_unsized_index_pages_to_toc_and_general::<_, { #const_name as u32 }>(pages);
                    (toc.pages, pages)
                }
            }
        } else {
            let collect_pages = if self.pk_upstream {
                quote! {
                    for node in self.0.primary_index.pk_map.iter_nodes() {
                        let node: Vec<IndexPair<#pk_type, OffsetEqLink<#const_name>>> = node
                            .lock_arc()
                            .iter()
                            .map(|pair| IndexPair {
                                key: pair.key.clone(),
                                value: pair.value,
                            })
                            .collect();
                        pages.push(IndexPage::from_node(&node, size));
                    }
                }
            } else {
                quote! {
                    for node in self.0.primary_index.pk_map.snapshot_nodes() {
                        pages.push(IndexPage::from_node(&node, size));
                    }
                }
            };
            quote! {
                pub fn get_peristed_primary_key_with_toc(&self) -> (Vec<GeneralPage<TableOfContentsPage<(#pk_type, Link)>>>, Vec<GeneralPage<IndexPage<#pk_type>>>) {
                    let size = get_index_page_size_from_data_length::<#pk_type>(#const_name);
                    let mut pages = vec![];
                    #collect_pages
                    let (toc, pages) = map_index_pages_to_toc_and_general::<_, { #const_name as u32 }>(pages);
                    (toc.pages, pages)
                }
            }
        }
    }
}
