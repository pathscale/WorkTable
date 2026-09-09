use indexmap::IndexMap;
use proc_macro2::Ident;

use crate::model::Operation;

#[derive(Debug, Default)]
pub struct Queries {
    pub updates: IndexMap<Ident, Operation>,
    pub deletes: IndexMap<Ident, Operation>,
    pub in_place: IndexMap<Ident, Operation>,
    /// The profile named by `update runtime <profile>:`, when the section was
    /// annotated. `None` is not a default: it means the section falls back to
    /// the table's `runtime`, and the table's own default only after that.
    ///
    /// The name is stored unresolved because the parser cannot resolve it. A
    /// profile is declared by `runtimes!` somewhere else in the crate, so
    /// whether it exists, and whether its backend matches the table's, is a
    /// question for code generation.
    pub update_runtime: Option<Ident>,
    /// The profile named by `delete runtime <profile>:`. See `update_runtime`.
    pub delete_runtime: Option<Ident>,
    /// The profile named by `in_place runtime <profile>:`. See `update_runtime`.
    pub in_place_runtime: Option<Ident>,
}
