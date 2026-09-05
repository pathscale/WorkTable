use proc_macro2::{Literal, TokenStream};
use quote::quote;

use crate::common::name_generator::{WorktableNameGenerator, is_unsized};
use crate::persist_index::generator::{ArtBackend, Generator, index_layout};

impl Generator {
    pub fn gen_space_secondary_index_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let ident = name_generator.get_space_secondary_index_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();

        let fields: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|field| {
                let layout = index_layout(field)?;
                let i = field.ident.as_ref().expect("index fields should be named");
                let t = self.field_types.get(i).expect("field type was collected");
                Ok(match layout.art_backend {
                    Some(ArtBackend::Arctic) if is_unsized(&t.to_string()) => quote! {
                        #i: SpaceLogicalIndexUnsized<#t, { #inner_const_name as u32}>,
                    },
                    Some(ArtBackend::Arctic) => quote! {
                        #i: SpaceLogicalIndex<#t, { #inner_const_name as u32}>,
                    },
                    Some(ArtBackend::ArcticMulti) if is_unsized(&t.to_string()) => quote! {
                        #i: SpaceLogicalMultiIndexUnsized<#t, { #inner_const_name as u32}>,
                    },
                    Some(ArtBackend::ArcticMulti) => quote! {
                        #i: SpaceLogicalMultiIndex<#t, { #inner_const_name as u32}>,
                    },
                    Some(ArtBackend::Congee) => quote! {
                        #i: SpaceCongeeIndex<#t, { #inner_const_name as u32}>,
                    },
                    None if layout.logical_wti && is_unsized(&t.to_string()) => quote! {
                        #i: SpaceLogicalIndexUnsized<#t, { #inner_const_name as u32}>,
                    },
                    None if layout.logical_wti => quote! {
                        #i: SpaceLogicalIndex<#t, { #inner_const_name as u32}>,
                    },
                    None if is_unsized(&t.to_string()) => quote! {
                        #i: SpaceIndexUnsized<#t, { #inner_const_name as u32}>,
                    },
                    None => quote! {
                        #i: SpaceIndex<#t, { #inner_const_name as u32}>,
                    },
                })
            })
            .collect::<syn::Result<Vec<_>>>()
            .expect("generated index layouts were validated");

        quote! {
            #[derive(Debug)]
            pub struct #ident {
                #(#fields)*
            }
        }
    }

    pub fn gen_space_secondary_index_impl_space_index(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let events_ident = name_generator.get_space_secondary_index_events_ident();
        let ident = name_generator.get_space_secondary_index_ident();

        let from_table_files_path_fn = self.gen_space_secondary_index_from_table_files_path_fn();
        let index_process_change_events_fn = self.gen_space_secondary_index_process_change_events_fn();
        let index_process_change_event_batch_fn = self.gen_space_secondary_index_process_change_event_batch_fn();

        quote! {
            impl SpaceSecondaryIndexOps<#events_ident> for #ident {
                #from_table_files_path_fn
                #index_process_change_events_fn
                #index_process_change_event_batch_fn
            }
        }
    }

    fn gen_space_secondary_index_from_table_files_path_fn(&self) -> TokenStream {
        let fields: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|field| {
                let layout = index_layout(field)?;
                let i = field.ident.as_ref().expect("index fields should be named");
                let t = self.field_types.get(i).expect("field type was collected");
                let literal_name = Literal::string(i.to_string().as_str());
                Ok(match layout.art_backend {
                    Some(ArtBackend::Arctic) if is_unsized(&t.to_string()) => quote! {
                        #i: SpaceLogicalIndexUnsized::secondary_from_table_files_path(path, #literal_name, version).await?,
                    },
                    Some(ArtBackend::Arctic) => quote! {
                        #i: SpaceLogicalIndex::secondary_from_table_files_path(path, #literal_name, version).await?,
                    },
                    Some(ArtBackend::ArcticMulti) if is_unsized(&t.to_string()) => quote! {
                        #i: SpaceLogicalMultiIndexUnsized::secondary_from_table_files_path(path, #literal_name, version).await?,
                    },
                    Some(ArtBackend::ArcticMulti) => quote! {
                        #i: SpaceLogicalMultiIndex::secondary_from_table_files_path(path, #literal_name, version).await?,
                    },
                    Some(ArtBackend::Congee) => quote! {
                        #i: SpaceCongeeIndex::secondary_from_table_files_path(path, #literal_name, version).await?,
                    },
                    None if layout.logical_wti && is_unsized(&t.to_string()) => quote! {
                        #i: SpaceLogicalIndexUnsized::secondary_from_table_files_path(path, #literal_name, version).await?,
                    },
                    None if layout.logical_wti => quote! {
                        #i: SpaceLogicalIndex::secondary_from_table_files_path(path, #literal_name, version).await?,
                    },
                    None if is_unsized(&t.to_string()) => quote! {
                        #i: SpaceIndexUnsized::secondary_from_table_files_path(path, #literal_name, version).await?,
                    },
                    None => quote! {
                        #i: SpaceIndex::secondary_from_table_files_path(path, #literal_name, version).await?,
                    },
                })
            })
            .collect::<syn::Result<Vec<_>>>()
            .expect("generated index layouts were validated");

        quote! {
            async fn from_table_files_path<S: AsRef<str>>(path: S, version: u32) -> eyre::Result<Self> {
                let path = path.as_ref();
                Ok(Self {
                    #(#fields)*
                })
            }
        }
    }

    fn gen_space_secondary_index_process_change_events_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let events_ident = name_generator.get_space_secondary_index_events_ident();

        let process: Vec<_> = self
            .field_types
            .keys()
            .map(|i| {
                quote! {
                    for event in events.#i {
                        self.#i.process_change_event(event).await?;
                    }
                }
            })
            .collect();

        quote! {
            async fn process_change_events(&mut self, events: #events_ident) -> eyre::Result<()> {
                #(#process)*
                core::result::Result::Ok(())
            }
        }
    }

    fn gen_space_secondary_index_process_change_event_batch_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let events_ident = name_generator.get_space_secondary_index_events_ident();

        let process: Vec<_> = self
            .field_types
            .keys()
            .map(|i| {
                quote! {
                    self.#i.process_change_event_batch(events.#i).await?;
                }
            })
            .collect();

        quote! {
            async fn process_change_event_batch(&mut self, events: #events_ident) -> eyre::Result<()> {
                #(#process)*
                core::result::Result::Ok(())
            }
        }
    }
}
