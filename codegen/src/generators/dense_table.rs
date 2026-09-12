//! The typed facade over [`worktable::partition::DenseRows`].
//!
//! Emitted instead of the full table as a partition payload when
//! `partition_max_size` is narrow. The storage lives in the library, so what is
//! generated here is the part that needs the row type: the key projection, the
//! per-column updates, and the three methods the router calls.
//!
//! It is *only* a partition payload. A dense table addressed by position is a
//! `Vec` with extra steps unless something upstream guarantees the keys are
//! dense and small, and `partition_by` is that guarantee: the routing key does
//! the spreading, and the inner key only has to separate the handful of rows
//! inside one partition.

use proc_macro2::{Ident, Literal, TokenStream};
use quote::{format_ident, quote};
use syn::Error;
use worktable_dsl::model::{Columns, Operation, PartitionMaxSize};

/// The `queries:` block, in the shape this generator needs it.
///
/// Lifted out of the model before the table generators consume it, because the
/// dense payload is emitted after them and `Queries` is not `Clone`.
#[derive(Debug, Default)]
pub struct DenseQueries {
    /// `update <Name>(columns) by <column>`.
    pub updates: Vec<(Ident, Operation)>,
    /// `delete <Name>() by <column>`.
    pub deletes: Vec<(Ident, Operation)>,
    /// `in_place <Name>(columns) by <column>`. Refused: see [`expand`].
    pub in_place: Vec<(Ident, Operation)>,
}

impl DenseQueries {
    /// Read what a dense payload needs out of a parsed `queries:` block.
    pub fn from_model(queries: Option<&worktable_dsl::model::Queries>) -> Self {
        let Some(queries) = queries else {
            return Self::default();
        };
        let lift = |map: &indexmap::IndexMap<Ident, Operation>| {
            map.iter().map(|(name, op)| (name.clone(), op.clone())).collect()
        };
        Self {
            updates: lift(&queries.updates),
            deletes: lift(&queries.deletes),
            in_place: lift(&queries.in_place),
        }
    }

    fn is_empty(&self) -> bool {
        self.updates.is_empty() && self.deletes.is_empty() && self.in_place.is_empty()
    }
}

/// Primary key types a position-addressed table can take.
///
/// Unsigned only, and for the same reason `partition_by`'s own key is: the key
/// *is* an index. A signed or floating key has no position to be, and a
/// `String` key would have to be hashed, which is the tree this shape exists to
/// delete.
const DENSE_KEY_TYPES: [&str; 5] = ["u8", "u16", "u32", "u64", "usize"];

/// The name of the generated payload type.
///
/// `Dense` and not `Micro`: "micro-partition" is Snowflake's word for a 50 to
/// 500 MB automatic columnar unit, and using it for a 23-row partition would
/// mean something different to everyone who has met the term before.
pub fn type_ident(name: &Ident) -> Ident {
    format_ident!("{}DenseTable", name)
}

/// Check that this declaration can be addressed by position.
///
/// Separate from [`expand`] because the router has to refuse before either
/// table is generated: a declaration that cannot be dense has to say so once,
/// naming the column, rather than failing inside an expansion.
pub fn validate(name: &Ident, columns: &Columns, max_size: PartitionMaxSize) -> syn::Result<(Ident, TokenStream)> {
    let rows = max_size.rows().expect("only a dense width reaches here");

    if let Some(indexed_column) = columns.indexes.keys().next() {
        return Err(Error::new(
            indexed_column.span(),
            format!(
                "`{indexed_column}` declares a secondary index, but `partition_max_size: {}` lowers each \
                 partition to a dense table without secondary indexes. Use `partition_max_size: u64` to \
                 retain declared indexes.",
                max_size.type_name()
            ),
        ));
    }

    if columns.primary_keys.len() != 1 {
        return Err(Error::new(
            name.span(),
            format!(
                "`partition_max_size: {}` addresses rows by position, so the primary key has to be \
                 one unsigned column. This table declares {} primary key columns. Use \
                 `partition_max_size: u64` for a full table per partition, which takes a composite \
                 key.",
                max_size.type_name(),
                columns.primary_keys.len()
            ),
        ));
    }

    let pk = columns.primary_keys.first().expect("checked above").clone();
    let pk_type = columns
        .columns_map
        .get(&pk)
        .expect("the primary key is a column")
        .clone();
    let pk_text = pk_type.to_string().replace(' ', "");

    if !DENSE_KEY_TYPES.contains(&pk_text.as_str()) {
        return Err(Error::new(
            pk.span(),
            format!(
                "`{pk}: {pk_text}` cannot address a row by position: `partition_max_size: {}` means \
                 the key indexes the partition directly, so it must be one of {}. Either give the \
                 partition an unsigned key, or use `partition_max_size: u64`, which keeps the full \
                 table and its index and takes a key of any type.",
                max_size.type_name(),
                DENSE_KEY_TYPES.join(", ")
            ),
        ));
    }

    // A key narrower than the cap cannot reach it, which is not an error but is
    // always a mistake worth naming: `partition_max_size: u16` beside a `u8`
    // key declares 65,536 rows and can hold 256.
    let key_span = key_bytes(&pk_text)
        .filter(|bytes| *bytes < 8)
        .map(|bytes| 1u64 << (8 * bytes));
    if let Some(key_span) = key_span.filter(|key_span| *key_span < rows) {
        return Err(Error::new(
            pk.span(),
            format!(
                "`partition_max_size: {}` declares {rows} rows a partition, but `{pk}: {pk_text}` \
                 only counts to {key_span}, so {} of those rows are unreachable. Declare \
                 `partition_max_size: {}` to match the key.",
                max_size.type_name(),
                rows - key_span,
                pk_text
            ),
        ));
    }

    Ok((pk, pk_type))
}

/// Bytes in a fixed-width unsigned type, or `None` for `usize`, whose width is
/// the target's rather than the declaration's.
fn key_bytes(name: &str) -> Option<u32> {
    match name {
        "u8" => Some(1),
        "u16" => Some(2),
        "u32" => Some(4),
        "u64" => Some(8),
        _ => None,
    }
}

/// Generate `<Name>DenseTable`.
///
/// `row_ident` is the row the paged or `Vec` generator already emitted: the
/// dense payload reuses it rather than declaring a parallel one, so a caller
/// carries one row type whichever shape the partition has.
pub fn expand(
    name: &Ident,
    columns: &Columns,
    max_size: PartitionMaxSize,
    queries: &DenseQueries,
) -> syn::Result<TokenStream> {
    let (pk, pk_type) = validate(name, columns, max_size)?;
    let rows = max_size.rows().expect("only a dense width reaches here");

    let row_ident = format_ident!("{}Row", name);
    let table = type_ident(name);
    let cap = Literal::usize_suffixed(usize::try_from(rows).expect("a cap is at most 65,536"));

    let per_column = columns
        .columns_map
        .iter()
        .filter(|(column, _)| **column != pk)
        .map(|(column, ty)| {
            let setter = format_ident!("update_{}", column);
            let doc = format!(
                "Set `{column}` on the row at `{pk}`, in place.\n\n\
                 Returns the previous value, or `None` if that key holds no row. \
                 The row is never cloned: at a wide row that is the difference \
                 between touching one field and copying the row twice."
            );
            quote! {
                #[doc = #doc]
                pub fn #setter(&self, #pk: &#pk_type, value: #ty) -> Option<#ty> {
                    let at = Self::at(#pk)?;
                    self.inner.update(at, |row| core::mem::replace(&mut row.#column, value))
                }
            }
        })
        .collect::<Vec<_>>();

    let query_methods = gen_queries(name, columns, &pk, &pk_type, queries)?;

    let table_doc = format!(
        "One partition of [`{name}Partitions`], addressed by position.\n\n\
         `{pk}` is not looked up, it *is* the row's position, so this table has no \
         primary index at all. `partition_max_size` declares {rows} rows here; the row \
         vector still grows only to the highest key used, so the declared width is a \
         bound rather than a reservation.\n\n\
         Every method takes `&self`, because `partition_or_create` hands out an `Arc`. \
         Writes serialise per partition. See `worktable::partition::DenseRows` for what \
         this drops relative to a full table and why each is safe to drop at this size."
    );

    Ok(quote! {
        #[doc = #table_doc]
        #[derive(Debug)]
        pub struct #table {
            inner: worktable::partition::DenseRows<#row_ident>,
        }

        impl Default for #table {
            fn default() -> Self {
                Self { inner: worktable::partition::DenseRows::new(#cap) }
            }
        }

        impl #table {
            /// Rows one partition holds, as `partition_max_size` declared it.
            pub const MAX_ROWS: usize = #cap;

            #[must_use]
            pub fn new() -> Self {
                Self::default()
            }

            /// The key as a position, or `None` if it does not fit one.
            ///
            /// Only a 32-bit target with a key above `u32::MAX` fails here, and
            /// a cap is at most 65,536, so such a key is out of range anyway.
            #[inline]
            fn at(#pk: &#pk_type) -> Option<usize> {
                usize::try_from(*#pk).ok()
            }

            /// The position a key names, refusing one that does not fit.
            #[inline]
            fn at_checked(#pk: &#pk_type) -> Result<usize, worktable::partition::DenseError> {
                Self::at(#pk).ok_or_else(|| {
                    worktable::partition::DenseError::out_of_range(*#pk as u64, Self::MAX_ROWS)
                })
            }

            /// Insert, refusing a key that is occupied or out of range.
            pub fn insert(&self, row: #row_ident) -> Result<(), worktable::partition::DenseError> {
                let at = Self::at_checked(&row.#pk)?;
                self.inner.insert(at, row)
            }

            /// Insert, or replace the row this key already names.
            pub fn upsert(
                &self,
                row: #row_ident,
            ) -> Result<Option<#row_ident>, worktable::partition::DenseError> {
                let at = Self::at_checked(&row.#pk)?;
                self.inner.upsert(at, row)
            }

            /// The row this key names, cloned out.
            ///
            /// One bounds check and one load. There is no tree to descend and
            /// nothing to hash.
            #[must_use]
            pub fn select(&self, #pk: &#pk_type) -> Option<#row_ident> {
                self.inner.get(Self::at(#pk)?)
            }

            /// Whether this key holds a row.
            #[must_use]
            pub fn contains(&self, #pk: &#pk_type) -> bool {
                Self::at(#pk).is_some_and(|at| self.inner.contains(at))
            }

            /// Replace the whole row this key names, returning the old one.
            ///
            /// `None` means the key held nothing, and nothing was written: this
            /// updates, it does not insert. `upsert` is the one that does both.
            pub fn update(
                &self,
                row: #row_ident,
            ) -> Result<Option<#row_ident>, worktable::partition::DenseError> {
                let at = Self::at_checked(&row.#pk)?;
                Ok(self.inner.update(at, |slot| core::mem::replace(slot, row)))
            }

            #(#per_column)*

            #(#query_methods)*

            /// Take the row this key names out.
            ///
            /// Nothing shifts. A position is a key, so compacting would
            /// renumber every row above it.
            pub fn delete(&self, #pk: &#pk_type) -> Option<#row_ident> {
                self.inner.remove(Self::at(#pk)?)
            }

            /// Every row present, ascending by key.
            #[must_use]
            pub fn select_all(&self) -> worktable::prelude::Vec<#row_ident> {
                self.inner.iter().into_iter().map(|(_, row)| row).collect()
            }

            /// Rows present. Does not take the lock.
            #[must_use]
            pub fn row_count(&self) -> usize {
                self.inner.row_count()
            }

            /// Rows present, under the name every other table uses.
            #[must_use]
            pub fn len(&self) -> usize {
                self.inner.row_count()
            }

            #[must_use]
            pub fn is_empty(&self) -> bool {
                self.inner.is_empty()
            }

            /// Slots allocated, present or not.
            ///
            /// One past the highest key ever inserted, and **not** the declared
            /// cap. This is the figure that explains the shape's memory, so it
            /// is exposed rather than inferred.
            #[must_use]
            pub fn slots(&self) -> usize {
                self.inner.slots()
            }

            /// Row bytes, which here is the whole table.
            ///
            /// `slots * size_of::<Option<Row>>()`. There is no index to add,
            /// which is the point: at 23 rows the index is most of what a full
            /// table costs. A column owning a heap allocation is not counted,
            /// the same gap the paged table's figure has.
            #[must_use]
            pub fn used_bytes(&self) -> u64 {
                (self.inner.slots() * core::mem::size_of::<Option<#row_ident>>()) as u64
            }
        }
    })
}

/// Generate one method per `queries:` entry.
///
/// The method names and the `<Name>Query` argument structs are the paged
/// table's: a partitioned declaration still generates the full table beside the
/// dense payload, so the query structs already exist and a caller keeps the
/// same call. What differs is the signature, deliberately, the same way every
/// other pair of shapes in this crate differs: there is no `.await` and no
/// `WorkTableError`, so moving a call between them fails to compile rather than
/// quietly changing what it guarantees.
fn gen_queries(
    name: &Ident,
    columns: &Columns,
    pk: &Ident,
    pk_type: &TokenStream,
    queries: &DenseQueries,
) -> syn::Result<Vec<TokenStream>> {
    if queries.is_empty() {
        return Ok(Vec::new());
    }

    // `in_place` exists on the paged table because a write there is async and
    // has to hold a column across a suspension point. Nothing here is async and
    // `update` is already in place, so generating both would be two names for
    // one method.
    if let Some((query, _)) = queries.in_place.first() {
        return Err(Error::new(
            query.span(),
            format!(
                "`in_place {query}` has no meaning on a dense partition: every update here is \
                 already in place, because there is no page to rewrite and no await to hold a \
                 column across. Declare it as `update {query}`, or use `partition_max_size: u64` \
                 for the full table."
            ),
        ));
    }

    let mut out = Vec::new();

    for (query, op) in &queries.updates {
        by_must_be_the_key(pk, query, op, "update")?;
        let method = format_ident!("update_{}", snake(query));
        let query_ty = format_ident!("{}Query", query);
        let fields = &op.columns;
        for column in fields {
            if !columns.columns_map.contains_key(column) {
                return Err(Error::new(column.span(), format!("no column `{column}`")));
            }
            if column == pk {
                return Err(Error::new(
                    column.span(),
                    format!(
                        "`update {query}` cannot update primary key `{pk}` in a dense partition: the key is \
                         the row's physical position. Remove `{pk}` from the update, or delete and insert the row \
                         at its new key."
                    ),
                ));
            }
        }
        let doc = format!(
            "`update {query}`, by position.\n\n\
             Edits {} in place on the row at `{pk}`, without cloning the row. \
             `None` means that key holds no row and nothing was written.\n\n\
             The paged table's method of this name is `async` and returns \
             `Result<(), WorkTableError>`. This one is neither, so a call does not \
             move silently between the two shapes.",
            fields.iter().map(|f| format!("`{f}`")).collect::<Vec<_>>().join(", ")
        );
        out.push(quote! {
            #[doc = #doc]
            pub fn #method(&self, row: #query_ty, #pk: &#pk_type) -> Option<()> {
                let at = Self::at(#pk)?;
                self.inner.update(at, |target| {
                    #(target.#fields = row.#fields;)*
                })
            }
        });
    }

    for (query, op) in &queries.deletes {
        by_must_be_the_key(pk, query, op, "delete")?;
        let method = format_ident!("delete_{}", snake(query));
        let row_ident = format_ident!("{}Row", name);
        let doc = format!(
            "`delete {query}`, by position.\n\n\
             Takes the row at `{pk}` out and returns it. Nothing shifts: a position \
             is a key, so compacting would renumber every row above it."
        );
        out.push(quote! {
            #[doc = #doc]
            pub fn #method(&self, #pk: &#pk_type) -> Option<#row_ident> {
                self.inner.remove(Self::at(#pk)?)
            }
        });
    }

    Ok(out)
}

/// A dense partition has no secondary index, so a query can only be keyed by
/// the position.
///
/// Refused rather than scanned. A scan of at most 65,536 rows would work and
/// would be the wrong thing to generate silently: the declaration asks for a
/// keyed operation and would get a linear one, which is the sort of quiet
/// downgrade the rest of this crate refuses.
fn by_must_be_the_key(pk: &Ident, query: &Ident, op: &Operation, kind: &str) -> syn::Result<()> {
    if op.by == *pk {
        return Ok(());
    }
    Err(Error::new(
        op.by.span(),
        format!(
            "`{kind} {query} ... by {}` needs an index on `{}`, and a dense partition has none: \
             the only key it can address a row by is its position, which is `{pk}`. Key the query \
             by `{pk}`, or use `partition_max_size: u64` for the full table and its indexes.",
            op.by, op.by
        ),
    ))
}

/// `TopPrice` to `top_price`, the same casing the paged table's methods use.
fn snake(name: &Ident) -> String {
    use convert_case::{Case, Casing as _};
    name.to_string().from_case(Case::Pascal).to_case(Case::Snake)
}
