use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

use crate::common::model::PartitionKey;

/// Generate the router for a partitioned table.
///
/// Only a typed facade is generated. The storage lives in
/// `worktable::partition::PartitionSet`, so the code emitted per partitioned
/// table stays small: one `worktable!` already expands to roughly 1,940 lines,
/// and a router that grew with it would be paid for by every table.
pub fn expand(name: &Ident, key: &PartitionKey) -> TokenStream {
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

            /// The partition routed to by `#key_name`, creating an empty one
            /// if absent. Racing callers on one key create a single table.
            pub fn partition_or_create(
                &self,
                #key_name: #key_ty,
            ) -> Result<std::sync::Arc<#table>, worktable::partition::PartitionError>
            where
                #table: Default,
            {
                self.inner.get_or_create(#key_name as u64, <#table as Default>::default)
            }

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

            /// Memory held per partition: row bytes plus index bytes.
            ///
            /// Sourced from `system_info()` rather than `MemStat`, because the
            /// generated table reports its footprint there. "Which key is
            /// costing me" is the question a partitioned store gets asked, and
            /// a single total cannot answer it.
            pub fn memory_by_key(&self) -> Vec<(#key_ty, u64)> {
                self.inner
                    .iter()
                    .into_iter()
                    .map(|(k, table)| {
                        let info = table.system_info();
                        (k as #key_ty, info.memory_usage_bytes + info.idx_size as u64)
                    })
                    .collect()
            }

            /// Memory held across every live partition.
            pub fn memory_total(&self) -> u64 {
                self.memory_by_key().into_iter().map(|(_, bytes)| bytes).sum()
            }

            /// Free partitions removed earlier.
            ///
            /// [`Self::remove`] retires a partition rather than freeing it,
            /// because a reader running without a lock may have loaded its
            /// pointer a moment before. This takes `&mut self`: exclusive
            /// access is the proof that no such reader is in flight.
            pub fn gc(&mut self) -> usize {
                self.inner.gc()
            }

            /// How many removed partitions are still awaiting [`Self::gc`].
            pub fn retired_len(&self) -> usize {
                self.inner.retired_len()
            }

            /// Rows held per partition.
            pub fn rows_by_key(&self) -> Vec<(#key_ty, usize)> {
                self.inner
                    .iter()
                    .into_iter()
                    .map(|(k, table)| (k as #key_ty, table.system_info().row_count))
                    .collect()
            }
        }
    }
}
