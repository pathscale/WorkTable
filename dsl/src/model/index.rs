use proc_macro2::Ident;

/// Physical implementation selected for a generated index.
///
/// Arctic is the default runtime backend. Persisted tables retain the existing
/// WorkTablesIndex page format, so declarations without `using` can open files
/// created by earlier releases while getting Arctic for in-memory lookups.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum IndexBackend {
    WorktablesIndex,
    Indexset,
    Congee,
    /// A hash map. Point operations only: no ordered scan, no range, and no
    /// persisted page form. Accepted on `vec: true` and refused elsewhere.
    FxHash,
    #[default]
    Arctic,
}

impl IndexBackend {
    pub fn requires_explicit_persistence(self) -> bool {
        matches!(self, Self::Congee)
    }

    /// Can this backend answer an ordered scan?
    ///
    /// Every backend but `fxhash` is a tree, so this is false for exactly one
    /// of them today. It exists as a question about the backend rather than as
    /// a match on `FxHash` at each call site, because the next hash-shaped
    /// backend should not have to find them all.
    pub fn is_ordered(self) -> bool {
        !matches!(self, Self::FxHash)
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::WorktablesIndex => "worktables_index",
            Self::Indexset => "indexset",
            Self::Congee => "congee",
            Self::FxHash => "fxhash",
            Self::Arctic => "arctic",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Index {
    pub name: Ident,
    pub field: Ident,
    pub is_unique: bool,
    pub backend: IndexBackend,
}
