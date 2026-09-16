//! The opaque-field rebuild arm of `gen_size_check`, shared by the in-memory
//! and persisted update generators.
//!
//! A column whose archived form the macro cannot inspect may contain relative
//! pointers, so an in-place update of one cannot be done field-by-field: the
//! whole row has to be rebuilt under its full row lock, so that every pointer
//! is based in the destination slot. The two generators emit exactly the same
//! code for that, differing only in whether the successful in-place write is
//! also published as a CDC operation.
//!
//! It lived as two verbatim copies, and this is a correctness-critical path:
//! a fix applied to one copy and not the other is a table whose opaque-field
//! update is right in memory and wrong on disk, or the reverse. Taking the
//! difference as a parameter is what keeps them in step.

use proc_macro2::{Ident, TokenStream};
use quote::quote;

/// Emits the `requires_rebuild` arm.
///
/// - `row_updates`: per-column assignments onto the rebuilt row.
/// - `full_row_lock`: the generator's full-row lock acquisition.
/// - `page_inner_size_const`: the table's page-inner-size constant.
/// - `touches_index`: whether any updated column is indexed. An indexed column
///   must keep the index-maintaining reinsert path and its unique-constraint
///   check, so the in-place attempt is skipped entirely.
/// - `on_in_place_success`: emitted inside the `in_place_ok` branch before it
///   returns. Empty for the in-memory generator; the persisted one publishes
///   the same-slot write there as an event-less data operation.
pub(crate) fn gen_rebuild_arm(
    row_updates: &[TokenStream],
    full_row_lock: &TokenStream,
    page_inner_size_const: &Ident,
    touches_index: bool,
    on_in_place_success: &TokenStream,
) -> TokenStream {
    if touches_index {
        return quote! {
            {
                drop(_guard);
                let pending_lock = { #full_row_lock };
                let _guard = pending_lock.into_guard_with_mutation();

                let row_old = self.0.select(pk.clone()).ok_or(WorkTableError::NotFound)?;
                let mut row_new = row_old.clone();
                #(#row_updates)*
                self.reinsert(row_old, row_new).await?;
                return core::result::Result::Ok(());
            }
        };
    }

    quote! {
        {
            // An opaque archived field may contain relative pointers.
            // Rebuild the complete row under its full lock so every
            // pointer is based in the destination slot, then retain the
            // current link when the serialized length still fits.
            drop(_guard);
            let pending_lock = { #full_row_lock };
            let _guard = pending_lock.into_guard_with_mutation();

            let row_old = self.0.select(pk.clone()).ok_or(WorkTableError::NotFound)?;
            let mut row_new = row_old.clone();
            #(#row_updates)*
            let current_link: Link = self.0
                .primary_index
                .pk_map
                .get_value(&pk)
                .map(Into::into)
                .ok_or(WorkTableError::NotFound)?;
            let in_place_ok = unsafe {
                self.0.data.update_in_place::<{ #page_inner_size_const }>(row_new.clone(), current_link).is_ok()
            };
            if in_place_ok {
                #on_in_place_success
                return core::result::Result::Ok(());
            }

            self.reinsert(row_old, row_new).await?;
            return core::result::Result::Ok(());
        }
    }
}
