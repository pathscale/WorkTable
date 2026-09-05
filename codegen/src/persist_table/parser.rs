use crate::persist_table::generator::PersistTableAttributes;
use proc_macro2::{Ident, Span, TokenStream};
use quote::ToTokens;
use syn::spanned::Spanned;
use syn::{Attribute, ItemStruct, LitStr};

pub struct Parser;

impl Parser {
    pub fn parse_struct(input: TokenStream) -> syn::Result<ItemStruct> {
        match syn::parse2::<ItemStruct>(input.clone()) {
            Ok(data) => Ok(data),
            Err(err) => Err(syn::Error::new(input.span(), err.to_string())),
        }
    }

    pub fn parse_pk_ident(item: &ItemStruct) -> Ident {
        // WorkTable<#row_type, #pk_type, <#pk_type as TablePrimaryKey>::Generator, #const_name>
        let type_str = item.fields.iter().next().unwrap().ty.to_token_stream().to_string();
        let mut split = type_str.split("<");
        split.next();
        let mut gens = split.next().unwrap().split(",");
        let pk_type = gens.nth(1).unwrap();

        Ident::new(pk_type.trim(), Span::mixed_site())
    }

    pub fn parse_attributes(attrs: &Vec<Attribute>) -> PersistTableAttributes {
        let mut res = PersistTableAttributes {
            pk_unsized: false,
            read_only: false,
            pk_upstream: false,
            pk_arctic: false,
            pk_arctic_string: false,
            pk_congee: false,
            pk_wti_logical: false,
            row_schema: vec![],
            primary_key_fields: vec![],
            secondary_index_types: vec![],
        };

        for attr in attrs {
            if attr.path().to_token_stream().to_string().as_str() == "table" {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("pk_unsized") {
                        res.pk_unsized = true;
                        return Ok(());
                    }
                    if meta.path.is_ident("read_only") {
                        res.read_only = true;
                        return Ok(());
                    }
                    if meta.path.is_ident("pk_upstream") {
                        res.pk_upstream = true;
                        return Ok(());
                    }
                    if meta.path.is_ident("pk_arctic") {
                        res.pk_arctic = true;
                        return Ok(());
                    }
                    if meta.path.is_ident("pk_arctic_string") {
                        res.pk_arctic_string = true;
                        res.pk_unsized = true;
                        return Ok(());
                    }
                    if meta.path.is_ident("pk_congee") {
                        res.pk_congee = true;
                        return Ok(());
                    }
                    if meta.path.is_ident("pk_wti_logical") {
                        res.pk_wti_logical = true;
                        return Ok(());
                    }
                    if meta.path.is_ident("row_schema") {
                        meta.parse_nested_meta(|field| {
                            let name = field
                                .path
                                .get_ident()
                                .ok_or_else(|| field.error("row schema field must be an identifier"))?
                                .to_string();
                            let type_name = field.value()?.parse::<LitStr>()?.value();
                            res.row_schema.push((name, type_name));
                            Ok(())
                        })?;
                        return Ok(());
                    }
                    if meta.path.is_ident("primary_key_fields") {
                        meta.parse_nested_meta(|field| {
                            let name = field
                                .path
                                .get_ident()
                                .ok_or_else(|| field.error("primary key field must be an identifier"))?
                                .to_string();
                            res.primary_key_fields.push(name);
                            Ok(())
                        })?;
                        return Ok(());
                    }
                    if meta.path.is_ident("secondary_index_types") {
                        meta.parse_nested_meta(|index| {
                            let name = index
                                .path
                                .get_ident()
                                .ok_or_else(|| index.error("secondary index name must be an identifier"))?
                                .to_string();
                            let type_name = index.value()?.parse::<LitStr>()?.value();
                            res.secondary_index_types.push((name, type_name));
                            Ok(())
                        })?;
                        return Ok(());
                    }
                    Ok(())
                })
                .expect("always ok even on unrecognized attrs");
            }
        }

        res
    }
}
