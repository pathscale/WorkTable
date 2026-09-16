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

/// Whether this column's archived form is a fixed-size scalar sitting inline
/// in the cell, with no relative pointer.
///
/// This is the safety condition for the zero-copy `select_with` path: a
/// concurrent writer can tear an inline scalar the reader's closure observes,
/// and the seqlock retry throws that away, but a torn *pointer* dereferenced
/// inside the closure is undefined behaviour.
///
/// Distinct from [`archived_field_requires_rebuild`], which answers a
/// different question and treats `String` as fine because the variable-size
/// path handles it. Here `String` is precisely what must be excluded.
///
/// Opaque user types are refused, because the macro cannot inspect a user
/// type's `Archive::Archived` layout. That is conservative in the safe
/// direction: an unrecognised column costs the table its zero-copy path, it
/// does not grant one unsoundly.
///
/// "No relative pointer" is necessary but not sufficient: the type must also
/// have no validity invariant, because a torn read must yield a wrong *value*
/// and not an invalid one. `char` is the type that fails that second test and
/// is why this list is an allowlist rather than "anything `Copy`". It archives
/// to `rend::char_le`, whose `to_native` transmutes its `u32` on the promise
/// that it holds a valid scalar value. Two valid chars can tear into one that
/// is not: `U+1D800` is `[00 D8 01 00]` and `U+0041` is `[41 00 00 00]`, so a
/// copy taking the low half of the first and the high half of the second reads
/// `0x0000D800`, a surrogate. The closure transmutes that before `still_stable`
/// ever runs, which is undefined behaviour and not a discarded wrong number.
/// `bool` stays: it is one byte, so it cannot tear into a third value.
///
/// A `char` column therefore costs its table `select_with` and nothing else.
/// This gate is only an optimisation.
pub fn archived_field_is_inline_scalar(ty: &TokenStream) -> bool {
    fn type_is_inline(ty: &Type) -> bool {
        let Type::Path(type_path) = ty else {
            return false;
        };
        let Some(segment) = type_path.path.segments.last() else {
            return false;
        };

        match segment.ident.to_string().as_str() {
            // `char` is deliberately absent: see the note above. Every type
            // here has every bit pattern valid, so a torn read is a wrong
            // number rather than an invalid value.
            "bool" | "u8" | "u16" | "u32" | "u64" | "u128" | "usize" | "i8" | "i16" | "i32" | "i64" | "i128"
            | "isize" | "f32" | "f64" => true,
            // An archived `Option<T>` of an inline `T` stays inline: rkyv
            // encodes the niche or a discriminant beside the payload.
            "Option" => match &segment.arguments {
                PathArguments::AngleBracketed(arguments) => arguments
                    .args
                    .iter()
                    .find_map(|argument| match argument {
                        GenericArgument::Type(inner) => Some(type_is_inline(inner)),
                        _ => None,
                    })
                    .unwrap_or(false),
                _ => false,
            },
            _ => false,
        }
    }

    syn::parse2::<Type>(ty.clone())
        .map(|ty| type_is_inline(&ty))
        .unwrap_or(false)
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

    pub fn get_update_query_lock_ident(snake_case_name: &String) -> Ident {
        Ident::new(format!("lock_update_{snake_case_name}").as_str(), Span::mixed_site())
    }

    pub fn get_update_in_place_query_lock_ident(snake_case_name: &String) -> Ident {
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

    /// Payload budget whose end is aligned for a tail-stored archived key.
    /// Variable-width index entries are written backwards from this boundary.
    ///
    /// # On-disk format
    ///
    /// This is a **format-affecting** value for unsized primary keys: it fixes
    /// how many entries an index node holds, so a change to it makes existing
    /// index files unreadable at the new capacity. The house convention is
    /// regenerate rather than migrate, but the change has to be stated rather
    /// than discovered.
    ///
    /// Rounding down to the archived key's alignment is what corrects the
    /// earlier under-budgeting (see the `u128_primary_index_capacity` test). A
    /// plain `String` key is unaffected, because `align_of::<ArchivedString>()`
    /// is 4 and the capacity was already a multiple of it; a composite key
    /// containing a `u128` aligns to 16 and does change.
    pub fn get_aligned_disk_page_capacity(&self, key_type: &proc_macro2::TokenStream) -> proc_macro2::TokenStream {
        let capacity = self.get_disk_page_capacity();
        quote::quote! {
            (#capacity - (#capacity % core::mem::align_of::<
                <#key_type as worktable::prelude::rkyv::Archive>::Archived
            >()))
        }
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
