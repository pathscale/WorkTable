use proc_macro2::Ident;
use syn::ItemStruct;

mod size_measurable;
mod space_file_deserialize;
mod space_file_serialize;

pub struct Generator {
    pub struct_def: ItemStruct,
    pub pk_ident: Ident,
}
