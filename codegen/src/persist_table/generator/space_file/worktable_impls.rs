use proc_macro2::TokenStream;
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::persist_table::generator::Generator;

impl Generator {
    pub fn gen_space_file_worktable_impl(&self) -> TokenStream {
        let ident = &self.struct_def.ident;
        let space_info_fn = self.gen_worktable_space_info_fn();
        let persisted_pk_fn = self.gen_worktable_persisted_primary_key_fn();
        let wait_for_ops_fn = self.gen_worktable_wait_for_ops_fn();

        quote! {
            impl #ident {
                #space_info_fn
                #persisted_pk_fn
                #wait_for_ops_fn
            }
        }
    }

    fn gen_worktable_wait_for_ops_fn(&self) -> TokenStream {
        quote! {
            pub async fn wait_for_ops(&self) {
               self.1.wait_for_ops().await
            }
        }
    }

    fn gen_worktable_space_info_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let pk = name_generator.get_primary_key_type_ident();
        let literal_name = name_generator.get_work_table_literal_name();

        quote! {
            pub fn space_info_default() -> GeneralPage<SpaceInfoPage<<<#pk as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State>> {
                let inner = SpaceInfoPage {
                    id: 0.into(),
                    page_count: 0,
                    name: #literal_name.to_string(),
                    pk_gen_state: <<#pk as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State::default(),
                    empty_links_list: vec![],
                    primary_key_fields: vec![],
                    row_schema: vec![],
                    secondary_index_types: vec![],
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
        if self.attributes.pk_unsized {
            quote! {
                pub fn get_peristed_primary_key_with_toc(&self) -> (Vec<GeneralPage<TableOfContentsPage<(#pk_type, Link)>>>, Vec<GeneralPage<UnsizedIndexPage<#pk_type, {#const_name as u32}>>>) {
                    let mut pages = vec![];
                    for node in self.0.primary_index.pk_map.iter_nodes() {
                        let page = UnsizedIndexPage::from_node(node.lock_arc().as_ref());
                        pages.push(page);
                    }
                    let (toc, pages) = map_unsized_index_pages_to_toc_and_general::<_, { #const_name as u32 }>(pages);
                    (toc.pages, pages)
                }
            }
        } else {
            quote! {
                pub fn get_peristed_primary_key_with_toc(&self) -> (Vec<GeneralPage<TableOfContentsPage<(#pk_type, Link)>>>, Vec<GeneralPage<IndexPage<#pk_type>>>) {
                    let size = get_index_page_size_from_data_length::<#pk_type>(#const_name);
                    let mut pages = vec![];
                    for node in self.0.primary_index.pk_map.iter_nodes() {
                        let page = IndexPage::from_node(node.lock_arc().as_ref(), size);
                        pages.push(page);
                    }
                    let (toc, pages) = map_index_pages_to_toc_and_general::<_, { #const_name as u32 }>(pages);
                    (toc.pages, pages)
                }
            }
        }
    }
}
