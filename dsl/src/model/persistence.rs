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

/// What holds the rows.
///
/// The two are not variants of one table. A paged table is concurrent,
/// durable and async, bought with an archived row, links into pages, a
/// row-level lock map and change-data-capture. A `Vec` table is a contiguous
/// `Vec<Row>` and an index into it, single-writer and synchronous, and pays
/// for none of that.
///
/// It is a key on `worktable!` rather than a second macro because a second
/// macro means a second set of generated names: `worktable_vec!` shipped for
/// one release emitting `<Name>VecRow` and `<Name>VecTable`, which is a
/// parallel vocabulary to learn and a redefinition error when one table is
/// declared both ways. One macro means one `<Name>Row` and one
/// `<Name>WorkTable` whatever the storage is.
///
/// The grammar says `vec: true`, a flag, because that is the shape `persist:`
/// already has and needs no new noun. This enum exists anyway because
/// everything downstream crosses a boundary where two flags could disagree:
/// the schema is serialized, round-tripped and handed to a TypeScript
/// emitter, and serde enforces no cross-field invariant. One enum cannot say
/// two things.
///
/// The choice is still loud rather than silent: the two tables have different
/// method signatures, so moving a declaration between them fails to compile at
/// every call site instead of quietly weakening its guarantees.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Storage {
    /// Pages behind links, which is what a `worktable!` has always been.
    #[default]
    Paged,
    /// One contiguous `Vec<Row>` and an index of positions into it.
    Vec,
}

impl Storage {
    pub fn is_vec(self) -> bool {
        matches!(self, Self::Vec)
    }
}
