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
    #[default]
    Arctic,
}

impl IndexBackend {
    pub fn requires_explicit_persistence(self) -> bool {
        matches!(self, Self::Congee)
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::WorktablesIndex => "worktables_index",
            Self::Indexset => "indexset",
            Self::Congee => "congee",
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
