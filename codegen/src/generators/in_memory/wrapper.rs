use crate::common::name_generator::WorktableNameGenerator;
use crate::generators::in_memory::InMemoryGenerator;
use proc_macro2::TokenStream;
use quote::quote;

impl InMemoryGenerator {
    pub fn gen_wrapper_def(&self) -> TokenStream {
        let type_ = self.gen_wrapper_type();
        let impl_ = self.gen_wrapper_impl();
        let storable_impl = self.get_wrapper_storable_impl();
        let archived_wrapper_impl = self.get_archived_wrapper_impl();

        quote! {
            #type_
            #impl_
            #storable_impl
            #archived_wrapper_impl
        }
    }

    fn gen_wrapper_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_ident = name_generator.get_row_type_ident();
        let wrapper_ident = name_generator.get_wrapper_type_ident();

        quote! {
            #[derive(rkyv::Archive, Debug, rkyv::Deserialize, rkyv::Serialize)]
            #[rkyv(attr(repr(C)))]
            #[repr(C)]
            pub struct #wrapper_ident {
                inner: #row_ident,
                publication_flags: u8,
                cell_state: CellState,
            }
        }
    }

    pub fn gen_wrapper_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let wrapper_ident = name_generator.get_wrapper_type_ident();
        let row_ident = name_generator.get_row_type_ident();

        quote! {

            impl RowWrapper<#row_ident> for #wrapper_ident {
                fn get_inner(self) -> #row_ident {
                    self.inner
                }

                fn is_ghosted(&self) -> bool {
                    self.publication_flags & 1 != 0
                }

                fn is_vacuumed(&self) -> bool {
                    self.publication_flags & 4 != 0
                }

                fn is_deleted(&self) -> bool {
                    self.publication_flags & 2 != 0
                }

                fn from_inner(inner: #row_ident) -> Self {
                    Self {
                        inner,
                        publication_flags: 1,
                        cell_state: CellState,
                    }
                }
            }
        }
    }

    fn get_wrapper_storable_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_ident = name_generator.get_row_type_ident();
        let wrapper_ident = name_generator.get_wrapper_type_ident();

        quote! {
            impl StorableRow for #row_ident {
                type WrappedRow = #wrapper_ident;
            }
        }
    }

    fn get_archived_wrapper_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_ident = name_generator.get_archived_wrapper_type_ident();

        quote! {
            impl ArchivedRowWrapper for #row_ident {
                unsafe fn cell_state_ptr(this: *mut Self) -> *mut std::sync::atomic::AtomicU8 {
                    unsafe { std::ptr::addr_of_mut!((*this).cell_state).cast() }
                }
                fn unghost(&mut self) {
                    self.publication_flags &= !1;
                }
                fn set_in_vacuum_process(&mut self) {
                    self.publication_flags |= 4;
                }
                fn delete(&mut self) {
                    self.publication_flags |= 2;
                }
                fn is_deleted(&self) -> bool {
                    self.publication_flags & 2 != 0
                }
            }
        }
    }
}
