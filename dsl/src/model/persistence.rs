/// Whether the table declaration explicitly selected persistence.
///
/// Keeping `Omitted` distinct from `MemoryOnly` lets the macro require an
/// explicit `persist: false` acknowledgement before selecting an index backend
/// that cannot participate in disk or S3 persistence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Persistence {
    #[default]
    Omitted,
    MemoryOnly,
    Persisted,
}

impl Persistence {
    pub fn is_persisted(self) -> bool {
        matches!(self, Self::Persisted)
    }
}
