use proc_macro2::Ident;

/// Physical implementation selected for a generated index.
///
/// `WorktablesIndex` is deliberately the default so existing declarations keep
/// their current implementation and persistence semantics when `using` is
/// absent. Vanilla upstream IndexSet is an explicit, parallel backend.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum IndexBackend {
    #[default]
    WorktablesIndex,
    Indexset,
    Congee,
    Arctic,
}

impl IndexBackend {
    pub fn requires_explicit_persistence(self) -> bool {
        matches!(self, Self::Congee | Self::Arctic)
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
