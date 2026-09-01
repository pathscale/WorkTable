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
    let pinned = format_ident!("{}Pinned", name);
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

    let pinned_doc = format!(
        "A held pin on [`{partitions}`], so lookups inside it cost no fence.\n\n\
         See `{partitions}::pinned`."
    );

    quote! {
        #[doc = #pinned_doc]
        pub struct #pinned<'a> {
            inner: worktable::partition::Pinned<'a, #table>,
        }

        impl #pinned<'_> {
            /// The partition routed to by the key, if it exists.
            #[inline]
            pub fn get(&self, #key_name: #key_ty) -> Option<&#table> {
                self.inner.get(#key_name as u64)
            }

            /// Whether a partition exists for the key.
            #[inline]
            pub fn contains(&self, #key_name: #key_ty) -> bool {
                self.inner.contains(#key_name as u64)
            }
        }

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
            /// to outlive the borrow or be sent elsewhere. The borrow carries
            /// an epoch pin, so hold it for the access, not for a tick loop:
            /// while it lives, nothing removed after it was taken can be
            /// reclaimed.
            #[inline]
            pub fn partition_ref(
                &self,
                #key_name: #key_ty,
            ) -> Option<worktable::partition::PartRef<'_, #table>> {
                self.inner.partition_ref(#key_name as u64)
            }

            /// Pin once, then look up many times.
            ///
            /// `partition_ref` pins per call, and a pin ends in a fence the
            /// slot loads must wait on: 0.71 ns of lookup becomes 3.4 ns, and
            /// no reclamation scheme avoids it. A tick loop should pin once
            /// and read many times instead.
            ///
            /// The pin is held for the whole scope, so nothing retired during
            /// it is reclaimed until it drops. Hold it for a batch, not a
            /// session.
            #[inline]
            pub fn pinned(&self) -> #pinned<'_> {
                #pinned { inner: self.inner.pinned() }
            }

            /// Whether a partition exists for `#key_name`.
            pub fn contains(&self, #key_name: #key_ty) -> bool {
                self.inner.contains(#key_name as u64)
            }

            /// Drop a partition. Callers already holding it keep their handle.
            ///
            /// The partition is retired and freed once every reader that
            /// could have been touching it mid-query has finished (an epoch
            /// grace period). Removal and creation reclaim opportunistically,
            /// so a router shared behind an `Arc` does not accumulate removed
            /// partitions; `collect` is available for removal-only phases.
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

            /// Free removed partitions whose reader grace period has expired.
            ///
            /// Returns how many were freed. Works through `&self`, so it is
            /// callable on a router shared behind an `Arc`; `remove` and the
            /// creation paths already call it opportunistically.
            pub fn collect(&self) -> usize {
                self.inner.collect()
            }

            /// Exhaustively free every removed partition, driving the grace
            /// period as needed. Needs `&mut self`; the shared-router path
            /// reclaims through `collect` instead.
            pub fn gc(&mut self) -> usize {
                self.inner.gc()
            }

            /// How many removed partitions are still awaiting reclamation.
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

            /// Bytes held by partitions that were removed but not yet
            /// reclaimed (readers may still be inside their grace period).
            /// Without it a total falls after a removal that has not freed
            /// anything yet.
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
