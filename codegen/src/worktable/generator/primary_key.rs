use std::collections::HashMap;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use crate::worktable::model::{GeneratorType, PrimaryKey};

use proc_macro2::{Ident, TokenStream};
use quote::quote;

impl Generator {
    pub fn gen_pk_def(&mut self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_primary_key_type_ident();
        let vals = self
            .columns
            .primary_keys
            .0
            .iter()
            .map(|i| (i.clone(), self.columns.columns_map.get(i).unwrap().clone()))
            .collect::<HashMap<_, _>>();

        let def = self.gen_pk_type();
        let impl_ = self.gen_pk_type();

        self.pk = Some(PrimaryKey { ident, vals });

        Ok(quote! {
            #def
            #impl_
        })
    }

    fn gen_pk_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_primary_key_type_ident();

        let types = &self.columns.primary_keys;

        quote! {
            quote! {
                #[derive(Clone, rkyv::Archive, Debug, rkyv::Deserialize, rkyv::Serialize, From, Eq, Into, PartialEq, PartialOrd, Ord)]
                pub struct #ident(#(#types),*);
            }
        }
    }

    fn gen_pk_impl(&self) -> syn::Result<TokenStream> {
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
                    .as_ref()
                    .expect("primary key column name always exists if in primary keys list");

                let gen = Self::gen_from_type(type_, i)?;
                quote! {
                    impl TablePrimaryKey for #ident {
                        type Generator = #gen;
                    }
                }
            }
            GeneratorType::Custom => {
                quote! {}
            }
        })
    }

    fn gen_from_type(type_: &TokenStream, i: &Ident) -> syn::Result<TokenStream> {
        Ok(match type_.to_string().as_str() {
            "u8" => quote! { std::sync::atomic::AtomicU8 },
            "u16" => quote! { std::sync::atomic::AtomicU16 },
            "u32" => quote! { std::sync::atomic::AtomicU32 },
            "u64" => quote! { std::sync::atomic::AtomicU64 },
            "i8" => quote! { std::sync::atomic::AtomicI8 },
            "i16" => quote! { std::sync::atomic::AtomicI16 },
            "i32" => quote! { std::sync::atomic::AtomicI32 },
            "i64" => quote! { std::sync::atomic::AtomicI64 },
            _ => {
                return Err(syn::Error::new(
                    i.span(),
                    "Type is not supported for autoincrement",
                ))
            }
        })
    }
}

// TODO: tests...
