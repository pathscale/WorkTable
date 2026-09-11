use proc_macro2::Ident;

/// The routing key of a partitioned table.
///
/// The key is not a column. It identifies the partition rather than living in
/// a row, so it is stored once per partition rather than once per row and no
/// query can reference it. It is restricted to unsigned integers because
/// routing is an array index: see `docs/partitioned-tables-proposal.md` for
/// the measurements behind that restriction.
#[derive(Debug, Clone)]
pub struct PartitionKey {
    /// Name of the key, used for the generated argument names.
    pub name: Ident,
    /// Unsigned integer type of the key.
    pub ty: Ident,
    /// How many rows a single partition holds at most, declared as an index
    /// width. Required: see [`PartitionMaxSize`].
    pub max_size: PartitionMaxSize,
}

/// Key types routing accepts. Signed and floating types are rejected because
/// a routing coordinate is an index, and a `String` key is rejected because
/// hashing it costs more than every other part of the lookup combined.
pub const PARTITION_KEY_TYPES: [&str; 5] = ["u8", "u16", "u32", "u64", "usize"];

/// How large one partition gets, written as the width of its row index.
///
/// A **type**, not a count, because it is an index width and that is what the
/// generator needs. It matches `columnar_slot_id: ColumnSlotId16` in `config`,
/// which already means slot-width-as-a-type.
///
/// It is required beside `partition_by` because the declaration otherwise says
/// nothing about the shape being generated. A reader seeing
/// `exchange_id: u8 primary_key` in a partitioned table reads "big table with
/// a suspiciously tiny key", when the truth is "twenty thousand little tables,
/// each of which only needs a byte". Two declarations differing by 28 KB a
/// partition would look identical.
///
/// There is no `unbounded` keyword: the widths run out of smallness, so `u64`
/// is the escape and it generates exactly what a partitioned table generated
/// before this key existed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionMaxSize {
    /// Two rows.
    Bool,
    /// 256 rows.
    U8,
    /// 65,536 rows.
    U16,
    /// Unbounded in practice; a full generated table per partition.
    U32,
    /// Unbounded in practice; a full generated table per partition.
    U64,
}

/// Widths `partition_max_size` accepts, in the order they are offered in a
/// diagnostic.
pub const PARTITION_MAX_SIZE_TYPES: [&str; 5] = ["bool", "u8", "u16", "u32", "u64"];

impl PartitionMaxSize {
    /// The width as it is written in a declaration.
    pub fn type_name(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::U8 => "u8",
            Self::U16 => "u16",
            Self::U32 => "u32",
            Self::U64 => "u64",
        }
    }

    /// Parse the token a declaration wrote, or `None` if it is not a width.
    pub fn from_type_name(name: &str) -> Option<Self> {
        match name {
            "bool" => Some(Self::Bool),
            "u8" => Some(Self::U8),
            "u16" => Some(Self::U16),
            "u32" => Some(Self::U32),
            "u64" => Some(Self::U64),
            _ => None,
        }
    }

    /// Rows one partition holds, where that is a number worth having.
    ///
    /// `None` for `u32` and `u64`: four billion rows is not a cap anyone is
    /// declaring on purpose, and the generator treats those as "no cap" rather
    /// than allocating against them.
    pub fn rows(self) -> Option<u64> {
        match self {
            Self::Bool => Some(2),
            Self::U8 => Some(256),
            Self::U16 => Some(65_536),
            Self::U32 | Self::U64 => None,
        }
    }

    /// Whether this width selects the dense per-partition table.
    ///
    /// True exactly when [`Self::rows`] is `Some`. The two are one decision and
    /// are written as one so they cannot drift apart.
    pub fn is_dense(self) -> bool {
        self.rows().is_some()
    }

    /// The `ColumnSlotId*` type a columnar field in this partition would use.
    ///
    /// `bool` maps to the `u8` slot id: there is no narrower one, and a
    /// two-row partition does not need one.
    pub fn slot_id_type_name(self) -> &'static str {
        match self {
            Self::Bool | Self::U8 => "ColumnSlotId8",
            Self::U16 => "ColumnSlotId16",
            Self::U32 => "ColumnSlotId32",
            Self::U64 => "ColumnSlotId64",
        }
    }
}
