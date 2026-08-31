use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

use crate::common::model::{PartitionKey, Persistence};

/// Generate the router for a partitioned table.
///
/// Only a typed facade is generated. The storage lives in
/// `worktable::partition::PartitionSet`, so the code emitted per partitioned
/// table stays small: one `worktable!` already expands to roughly 1,940 lines,
/// and a router that grew with it would be paid for by every table.
pub fn expand(name: &Ident, key: &PartitionKey, persistence: Persistence) -> TokenStream {
    let table = format_ident!("{}WorkTable", name);
    let partitions = format_ident!("{}Partitions", name);
    let key_name = &key.name;
    let key_ty = &key.ty;
    let doc = format!(
        "Partitions of [`{table}`], routed by `{key_name}`.\n\n\
         One table type, many instances. `{key_name}` identifies the partition \
         rather than living in a row, so it costs nothing per row and no query \
         can reference it.\n\n\
         Note that the primary key, `autoincrement` and any `unique` index are \
         unique *within* a partition, not across the set."
    );

    // A persisted table has no `Default`: it needs a `DatabaseManager`. The bound here
    // names a concrete type rather than a generic parameter, so it is a trivial bound,
    // and rustc rejects those outright instead of treating them as conditional. Emitting
    // it for a persisted table failed the whole macro. Such callers use
    // `partition_or_insert_with`, which is the right API for a table that needs one.
    let or_create = if persistence.is_persisted() {
        quote! {}
    } else {
        quote! {
            /// The partition routed to by `#key_name`, creating an empty one
            /// if absent. Racing callers on one key create a single table.
            pub fn partition_or_create(
                &self,
                #key_name: #key_ty,
            ) -> Result<std::sync::Arc<#table>, worktable::partition::PartitionError> {
                self.inner.get_or_create(#key_name as u64, <#table as Default>::default)
            }
        }
    };

    quote! {
        #[doc = #doc]
        #[derive(Debug, Default)]
        pub struct #partitions {
            inner: worktable::partition::PartitionSet<#table>,
        }

        impl #partitions {
            pub fn new() -> Self {
                Self { inner: worktable::partition::PartitionSet::new() }
            }

            /// The partition routed to by `#key_name`, if it exists.
            #[inline]
            pub fn partition(&self, #key_name: #key_ty) -> Option<std::sync::Arc<#table>> {
                self.inner.partition(#key_name as u64)
            }

            #or_create

            /// The partition routed to by `#key_name`, creating one with
            /// `make` if absent. `make` runs at most once per key.
            pub fn partition_or_insert_with<F>(
                &self,
                #key_name: #key_ty,
                make: F,
            ) -> Result<std::sync::Arc<#table>, worktable::partition::PartitionError>
            where
                F: FnOnce() -> #table,
            {
                self.inner.get_or_create(#key_name as u64, make)
            }

            /// The partition routed to by `#key_name`, borrowed rather than
            /// reference counted.
            ///
            /// `partition` costs two atomic read-modify-writes per call, which
            /// contend when several threads route to the same key. This costs
            /// none. Prefer it per tick; prefer `partition` when the handle has
            /// to outlive the borrow or be sent elsewhere.
            #[inline]
            pub fn partition_ref(&self, #key_name: #key_ty) -> Option<&#table> {
                self.inner.partition_ref(#key_name as u64)
            }

            /// Whether a partition exists for `#key_name`.
            pub fn contains(&self, #key_name: #key_ty) -> bool {
                self.inner.contains(#key_name as u64)
            }

            /// Drop a partition. Callers already holding it keep their handle.
            ///
            /// The partition is retired, not freed, and only `gc` frees it.
            /// `gc` needs `&mut self`, so a router shared behind an `Arc`
            /// never reclaims: treat it as append-only and watch
            /// `retired_len`.
            pub fn remove(&self, #key_name: #key_ty) -> Option<std::sync::Arc<#table>> {
                self.inner.remove(#key_name as u64)
            }

            /// Keys that currently hold a partition, ascending.
            pub fn keys(&self) -> Vec<#key_ty> {
                self.inner.keys().into_iter().map(|k| k as #key_ty).collect()
            }

            /// Every live partition with its key.
            pub fn iter(&self) -> Vec<(#key_ty, std::sync::Arc<#table>)> {
                self.inner
                    .iter()
                    .into_iter()
                    .map(|(k, t)| (k as #key_ty, t))
                    .collect()
            }

            /// Number of live partitions.
            pub fn len(&self) -> usize {
                self.inner.len()
            }

            pub fn is_empty(&self) -> bool {
                self.inner.is_empty()
            }

            /// Free partitions removed earlier.
            ///
            /// Returns how many were reclaimed. Takes `&mut self`, so a router
            /// shared behind an `Arc` can never call it: treat such a router as
            /// append-only and watch `retired_len` and `retired_bytes`.
            pub fn gc(&mut self) -> usize {
                self.inner.gc()
            }

            /// How many removed partitions are still awaiting `gc`.
            pub fn retired_len(&self) -> usize {
                self.inner.retired_len()
            }

            /// Row bytes plus index bytes, per partition, ascending by key.
            ///
            /// This is *used* bytes. It excludes the table's fixed floor, its
            /// reserved-but-unused page capacity, the router spine, and
            /// anything on the retire list, so it is not resident memory and
            /// must not be used as one. See `retired_bytes`.
            pub fn memory_by_key(&self) -> Vec<(#key_ty, u64)> {
                let mut out = Vec::with_capacity(self.inner.len());
                self.inner.for_each(|k, table| out.push((k as #key_ty, table.used_bytes())));
                out
            }

            /// Row bytes plus index bytes across every live partition.
            ///
            /// Folded directly: it does not build `memory_by_key` first.
            pub fn memory_total(&self) -> u64 {
                let mut total = 0u64;
                self.inner.for_each(|_, table| total += table.used_bytes());
                total
            }

            /// Bytes held by partitions that were removed but not reclaimed.
            ///
            /// `remove` retires rather than frees, and `gc` needs `&mut self`,
            /// so through a shared router this only grows. Without it a total
            /// falls after a removal that freed nothing.
            pub fn retired_bytes(&self) -> u64 {
                let mut total = 0u64;
                self.inner.for_each_retired(|table| total += table.used_bytes());
                total
            }

            /// Rows held per partition, ascending by key.
            pub fn rows_by_key(&self) -> Vec<(#key_ty, usize)> {
                let mut out = Vec::with_capacity(self.inner.len());
                self.inner.for_each(|k, table| out.push((k as #key_ty, table.row_count())));
                out
            }
        }
    }
}
