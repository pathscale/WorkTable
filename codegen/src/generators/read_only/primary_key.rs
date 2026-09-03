use indexmap::IndexMap;

use crate::common::model::{GeneratorType, PrimaryKey};
use crate::common::name_generator::{WorktableNameGenerator, is_unsized_vec};
use crate::generators::index_backend::primary_key_backend_impl;
use crate::generators::primary_key::gen_borrowed_primary_key_impl;
use crate::generators::read_only::ReadOnlyGenerator;

use proc_macro2::{Ident, TokenStream};
use quote::quote;

impl ReadOnlyGenerator {
    pub fn gen_primary_key_def(&mut self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_primary_key_type_ident();
        let values = self
            .columns
            .primary_keys
            .iter()
            .map(|i| {
                (
                    i.clone(),
                    self.columns
                        .columns_map
                        .get(i)
                        .expect("should exist as got from definition")
                        .clone(),
                )
            })
            .collect::<IndexMap<_, _>>();

        let def = self.gen_primary_key_type()?;
        let impl_ = self.gen_table_primary_key_impl()?;

        self.pk = Some(PrimaryKey { ident, values });

        Ok(quote! {
            #def
            #impl_
        })
    }

    fn gen_primary_key_type(&self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_primary_key_type_ident();

        let types = &self
            .columns
            .primary_keys
            .iter()
            .map(|i| {
                self.columns
                    .columns_map
                    .get(i)
                    .expect("should exist as got from definition")
            })
            .collect::<Vec<_>>();
        let unsized_derive = if is_unsized_vec(&types.iter().map(|v| v.to_string()).collect::<Vec<_>>()) {
            quote! {
                VariableSizeMeasure,
            }
        } else {
            quote! {}
        };
        let (backend_derive, backend_impl) =
            primary_key_backend_impl(self.columns.primary_index_backend, &ident, types)?;
        let borrowed_impl = gen_borrowed_primary_key_impl(&ident, types);

        Ok(quote! {
            #[derive(
                Clone,
                #backend_derive
                rkyv::Archive,
                Debug,
                Default,
                rkyv::Deserialize,
                Hash,
                rkyv::Serialize,
                From,
                Eq,
                Into,
                PartialEq,
                PartialOrd,
                Ord,
                SizeMeasure,
                MemStat,
                #unsized_derive
            )]
            #[rkyv(derive(PartialEq, Eq, PartialOrd, Ord, Debug))]
            pub struct #ident(#(#types),*);

            #borrowed_impl
            #backend_impl
        })
    }

    fn gen_table_primary_key_impl(&self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_primary_key_type_ident();

        Ok(match self.columns.generator_type {
            GeneratorType::None => {
                quote! {
                    impl TablePrimaryKey for #ident {
                        type Generator = ();
                    }
                }
            }
            GeneratorType::Autoincrement => {
                let i = self
                    .columns
                    .primary_keys
                    .first()
                    .expect("at least one primary key should exist if autoincrement");
                let type_ = self
                    .columns
                    .columns_map
                    .get(i)
                    .expect("primary key column name always exists if in primary keys list");

                let generator = Self::get_generator_from_type(type_, i)?;
                quote! {
                    impl TablePrimaryKey for #ident {
                        type Generator = #generator;
                    }
                }
            }
            GeneratorType::Custom => {
                quote! {}
            }
        })
    }

    fn get_generator_from_type(type_: &TokenStream, i: &Ident) -> syn::Result<TokenStream> {
        Ok(match type_.to_string().as_str() {
            "u8" => quote! { std::sync::atomic::AtomicU8 },
            "u16" => quote! { std::sync::atomic::AtomicU16 },
            "u32" => quote! { std::sync::atomic::AtomicU32 },
            "u64" => quote! { std::sync::atomic::AtomicU64 },
            "i8" => quote! { std::sync::atomic::AtomicI8 },
            "i16" => quote! { std::sync::atomic::AtomicI16 },
            "i32" => quote! { std::sync::atomic::AtomicI32 },
            "i64" => quote! { std::sync::atomic::AtomicI64 },
            // The accepted set is `worktable_dsl::AUTOINCREMENT_TYPES`, and the
            // arms above must stay equal to it. `check` uses that list to
            // answer "would the macro accept this", so a second copy drifting
            // shows up as `check` passing a declaration that then fails to
            // build, which is the failure it exists to prevent.
            //
            // Asserted rather than commented: the debug assertion below fires
            // during any codegen build if the two ever disagree.
            _ => {
                debug_assert!(
                    !worktable_dsl::AUTOINCREMENT_TYPES.contains(&type_.to_string().as_str()),
                    "`{}` is in worktable_dsl::AUTOINCREMENT_TYPES but has no atomic here; `check` \
                     will accept a declaration this refuses",
                    type_
                );
                return Err(syn::Error::new(
                    i.span(),
                    format!(
                        "type is not supported for autoincrement; supported types: {}",
                        worktable_dsl::AUTOINCREMENT_TYPES.join(", ")
                    ),
                ));
            }
        })
    }
}
