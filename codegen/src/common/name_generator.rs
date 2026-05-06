use convert_case::{Case, Casing};
use proc_macro2::{Ident, Literal};
use quote::__private::Span;

pub fn is_unsized(ty_: &str) -> bool {
    matches!(ty_, "String")
}

pub fn is_unsized_vec(ty_: &[String]) -> bool {
    ty_.iter().any(|v| matches!(v.as_str(), "String"))
}

pub fn is_float(ty_: &str) -> bool {
    matches!(ty_, "f64" | "f32")
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

    pub fn get_page_size_const_ident(&self) -> Ident {
        let upper_snake_case_name = self.name.from_case(Case::Pascal).to_case(Case::UpperSnake);
        Ident::new(
            format!("{}_PAGE_SIZE", upper_snake_case_name.to_uppercase()).as_str(),
            Span::mixed_site(),
        )
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

    pub fn get_space_secondary_index_ident(&self) -> Ident {
        Ident::new(format!("{}SpaceSecondaryIndex", self.name).as_str(), Span::mixed_site())
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
