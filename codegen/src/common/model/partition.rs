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
}

/// Key types routing accepts. Signed and floating types are rejected because
/// a routing coordinate is an index, and a `String` key is rejected because
/// hashing it costs more than every other part of the lookup combined.
pub const PARTITION_KEY_TYPES: [&str; 5] = ["u8", "u16", "u32", "u64", "usize"];
