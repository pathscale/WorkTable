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

    pub fn parse_pk_ident(item: &ItemStruct) -> syn::Result<Ident> {
        // WorkTable<#row_type, #pk_type, <#pk_type as TablePrimaryKey>::Generator, #const_name>
        let first_field = item.fields.iter().next().ok_or_else(|| {
            syn::Error::new(
                item.ident.span(),
                format!(
                    "`{}` must have a field wrapping `WorkTable<Row, PrimaryKey, ...>`",
                    item.ident
                ),
            )
        })?;
        let syn::Type::Path(type_path) = &first_field.ty else {
            return Err(syn::Error::new_spanned(
                &first_field.ty,
                "`PersistTable` field type must be a path with generic arguments `WorkTable<Row, PrimaryKey, ...>`",
            ));
        };
        let last_segment = type_path.path.segments.last().ok_or_else(|| {
            syn::Error::new_spanned(&first_field.ty, "`PersistTable` field type path cannot be empty")
        })?;
        let syn::PathArguments::AngleBracketed(arguments) = &last_segment.arguments else {
            return Err(syn::Error::new_spanned(
                &first_field.ty,
                format!(
                    "`{}` must have generic type arguments `<Row, PrimaryKey, ...>`",
                    last_segment.ident
                ),
            ));
        };
        let Some(syn::GenericArgument::Type(pk_ty)) = arguments.args.iter().nth(1) else {
            return Err(syn::Error::new_spanned(
                &first_field.ty,
                format!(
                    "`{}` must have at least two generic type arguments `<Row, PrimaryKey, ...>`",
                    last_segment.ident
                ),
            ));
        };
        let syn::Type::Path(pk_path) = pk_ty else {
            return Err(syn::Error::new_spanned(
                pk_ty,
                "second generic argument of `PersistTable` field must be the primary key type identifier",
            ));
        };
        let pk_seg = pk_path
            .path
            .segments
            .last()
            .ok_or_else(|| syn::Error::new_spanned(pk_ty, "primary key type path cannot be empty"))?;

        Ok(Ident::new(&pk_seg.ident.to_string(), Span::mixed_site()))
    }

    pub fn parse_attributes(attrs: &[Attribute]) -> syn::Result<PersistTableAttributes> {
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
                    Err(meta.error("unsupported `#[table(...)]` attribute"))
                })?;
            }
        }

        Ok(res)
    }
}
