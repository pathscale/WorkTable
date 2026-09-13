use convert_case::{Case, Casing};
use proc_macro2::{Ident, Literal, TokenStream};
use quote::__private::Span;
use syn::{GenericArgument, PathArguments, Type};

pub fn is_unsized(ty_: &str) -> bool {
    matches!(ty_, "String")
}

pub fn is_unsized_vec(ty_: &[String]) -> bool {
    ty_.iter().any(|v| matches!(v.as_str(), "String"))
}

pub fn is_float(ty_: &str) -> bool {
    matches!(ty_, "f64" | "f32")
}

/// Whether moving only this field's archived bytes can leave a relative
/// pointer referring to the temporary query buffer.
///
/// The macro cannot inspect a user type's `Archive::Archived` layout. Keep the
/// field-swap fast path for Rust's known scalar shapes and make every opaque
/// user type take the full-row serialize/overwrite path. `String` is handled
/// by the existing variable-size path; wrapping it in `Option` still requires
/// the conservative full-row path because the option's archived payload can
/// contain the same relative pointer.
pub fn archived_field_requires_rebuild(ty: &TokenStream) -> bool {
    fn type_requires_rebuild(ty: &Type) -> bool {
        let Type::Path(type_path) = ty else {
            return true;
        };
        let Some(segment) = type_path.path.segments.last() else {
            return true;
        };

        match segment.ident.to_string().as_str() {
            "String" => false,
            "bool" | "char" | "u8" | "u16" | "u32" | "u64" | "u128" | "usize" | "i8" | "i16" | "i32" | "i64"
            | "i128" | "isize" | "f32" | "f64" | "Uuid" => false,
            "Option" => match &segment.arguments {
                PathArguments::AngleBracketed(arguments) => arguments
                    .args
                    .iter()
                    .find_map(|argument| match argument {
                        GenericArgument::Type(inner) => {
                            let is_string = matches!(inner, Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == "String"));
                            Some(is_string || type_requires_rebuild(inner))
                        }
                        _ => None,
                    })
                    .unwrap_or(true),
                _ => true,
            },
            _ => true,
        }
    }

    syn::parse2::<Type>(ty.clone())
        .map(|ty| type_requires_rebuild(&ty))
        .unwrap_or(true)
}

pub struct WorktableNameGenerator {
    pub(crate) name: String,
}

impl WorktableNameGenerator {
    pub fn from_struct_ident(struct_ident: &Ident) -> Self {
        Self {
            name: struct_ident
                .to_string()
                .strip_suffix("WorkTable")
                .expect("table type name should end on `WorkTable`")
                .to_string(),
        }
    }

    pub fn from_table_name(name: String) -> Self {
        Self { name }
    }

    pub fn get_dir_name(&self) -> String {
        self.name.from_case(Case::Pascal).to_case(Case::Snake)
    }

    pub fn get_update_partial_query_lock_ident(snake_case_name: &String) -> Ident {
        Ident::new(format!("lock_update_{snake_case_name}").as_str(), Span::mixed_site())
    }

    pub fn get_update_partial_in_place_query_lock_ident(snake_case_name: &String) -> Ident {
        Ident::new(
            format!("lock_update_in_place_{snake_case_name}").as_str(),
            Span::mixed_site(),
        )
    }

    pub fn get_work_table_literal_name(&self) -> Literal {
        Literal::string(self.name.as_str())
    }

    pub fn get_row_type_ident(&self) -> Ident {
        Ident::new(format!("{}Row", self.name).as_str(), Span::mixed_site())
    }

    pub fn get_row_fields_enum_ident(&self) -> Ident {
        Ident::new(format!("{}RowFields", self.name).as_str(), Span::mixed_site())
    }

    pub fn get_available_type_ident(&self) -> Ident {
        Ident::new(format!("{}AvaiableTypes", self.name).as_str(), Span::mixed_site())
    }

    pub fn get_available_indexes_ident(&self) -> Ident {
        Ident::new(format!("{}AvailableIndexes", self.name).as_str(), Span::mixed_site())
    }

    pub fn get_column_range_type_ident(&self) -> Ident {
        Ident::new(format!("{}ColumnRange", self.name).as_str(), Span::mixed_site())
    }

    pub fn get_work_table_ident(&self) -> Ident {
        Ident::new(format!("{}WorkTable", self.name).as_str(), Span::mixed_site())
    }

    pub fn get_primary_key_type_ident(&self) -> Ident {
        Ident::new(format!("{}PrimaryKey", self.name).as_str(), Span::mixed_site())
    }

    pub fn get_wrapper_type_ident(&self) -> Ident {
        Ident::new(format!("{}Wrapper", self.name).as_str(), Span::mixed_site())
    }

    pub fn get_archived_wrapper_type_ident(&self) -> Ident {
        Ident::new(format!("Archived{}Wrapper", self.name).as_str(), Span::mixed_site())
    }

    pub fn get_lock_type_ident(&self) -> Ident {
        Ident::new(format!("{}Lock", self.name).as_str(), Span::mixed_site())
    }

    pub fn get_index_type_ident(&self) -> Ident {
        Ident::new(format!("{}Index", self.name).as_str(), Span::mixed_site())
    }

    /// The alias the generated code names its runtime through.
    ///
    /// One name per table rather than the concrete type repeated at every site
    /// that needs it: the sync primitives, the timers and the vacuum spawn all
    /// have to agree, and a table that resolved its runtime twice could get two
    /// answers.
    pub fn get_runtime_type_ident(&self) -> Ident {
        Ident::new(format!("{}Runtime", self.name).as_str(), Span::mixed_site())
    }

    pub fn get_page_size_const_ident(&self) -> Ident {
        let upper_snake_case_name = self.name.from_case(Case::Pascal).to_case(Case::UpperSnake);
        Ident::new(
            format!("{}_PAGE_SIZE", upper_snake_case_name.to_uppercase()).as_str(),
            Span::mixed_site(),
        )
    }

    /// Payload budget for index and metadata pages, independent of row slots.
    pub fn get_disk_page_capacity(&self) -> proc_macro2::TokenStream {
        let page_size = self.get_page_size_const_ident();
        quote::quote! { (#page_size - worktable::prelude::GENERAL_HEADER_SIZE) }
    }

    pub fn get_page_inner_size_const_ident(&self) -> Ident {
        let upper_snake_case_name = self.name.from_case(Case::Pascal).to_case(Case::UpperSnake);
        Ident::new(
            format!("{}_INNER_SIZE", upper_snake_case_name.to_uppercase()).as_str(),
            Span::mixed_site(),
        )
    }

    pub fn get_version_const_ident(&self) -> Ident {
        let upper_snake_case_name = self.name.from_case(Case::Pascal).to_case(Case::UpperSnake);
        Ident::new(
            format!("{}_VERSION", upper_snake_case_name.to_uppercase()).as_str(),
            Span::mixed_site(),
        )
    }

    /// The name of the const carrying the table's own declaration.
    ///
    /// It follows `get_version_const_ident`'s shape because it answers the
    /// question next to it: the version says *which* schema, and this says
    /// *what* that schema is.
    pub fn get_schema_const_ident(&self) -> Ident {
        let upper_snake_case_name = self.name.from_case(Case::Pascal).to_case(Case::UpperSnake);
        Ident::new(
            format!("{}_SCHEMA", upper_snake_case_name.to_uppercase()).as_str(),
            Span::mixed_site(),
        )
    }
    pub fn get_space_secondary_index_ident(&self) -> Ident {
        Ident::new(format!("{}SpaceSecondaryIndex", self.name).as_str(), Span::mixed_site())
    }

    pub fn get_space_primary_index_ident(&self) -> Ident {
        Ident::new(format!("{}SpacePrimaryIndex", self.name).as_str(), Span::mixed_site())
    }

    pub fn get_space_secondary_index_events_ident(&self) -> Ident {
        Ident::new(
            format!("{}SpaceSecondaryIndexEvents", self.name).as_str(),
            Span::mixed_site(),
        )
    }

    #[cfg(feature = "s3-support")]
    pub fn get_s3_sync_persistence_engine_ident(&self) -> Ident {
        Ident::new(
            format!("{}S3SyncPersistenceEngine", self.name).as_str(),
            Span::mixed_site(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::archived_field_requires_rebuild;
    use quote::quote;

    #[test]
    fn archived_field_classification_is_conservative_for_opaque_types() {
        assert!(!archived_field_requires_rebuild(&quote!(u64)));
        assert!(!archived_field_requires_rebuild(&quote!(Option<u64>)));
        assert!(!archived_field_requires_rebuild(&quote!(String)));
        assert!(!archived_field_requires_rebuild(&quote!(Uuid)));
        assert!(!archived_field_requires_rebuild(&quote!(Option<Uuid>)));
        assert!(archived_field_requires_rebuild(&quote!(Option<String>)));
        assert!(archived_field_requires_rebuild(&quote!(EncryptedSecret)));
    }
}
