//! A `Vec`-backed table with the same shape as a `worktable!` and none of its machinery.
//!
//! # What this is for
//!
//! `worktable!` buys concurrency and durability with an archived row, paged
//! storage behind links, a row-level lock map and change-data-capture. A
//! single-threaded table that never persists pays all of that for nothing, and
//! what applications otherwise grow by hand is a `Vec` plus a map from key to
//! position.
//!
//! So this generates that, from the same declaration, so the two can sit side
//! by side and be compared on identical rows.
//!
//! # What it drops, deliberately
//!
//! Each of these is the reason a `worktable!` costs what it does, and dropping
//! them is the point rather than an omission:
//!
//! - **The archived row.** Rows are stored as themselves. No `rkyv`, no
//!   serialize on write, no `Archived` type on read.
//! - **Paging and links.** One contiguous `Vec<Row>` and an index into it, so
//!   no page ids, no offsets, no empty-link registry and nothing for vacuum to
//!   do.
//! - **The lock map.** Mutation takes `&mut self`. That is what makes it
//!   single-writer, and what makes it as fast as the `Vec` it is.
//! - **Change-data-capture.** CDC exists to feed persistence and vacuum. With
//!   neither, it is pure cost.
//! - **The async surface.** `insert` and friends are synchronous, because
//!   nothing here can queue. That also takes the executor off the hot path.
//!
//! # What it keeps, and what it deliberately does not
//!
//! It keeps the declaration and the method names. `insert`, `upsert`,
//! `select`, `select_all` and `delete` are the same words doing the same job,
//! so the two tables read alike and a reader carries one vocabulary.
//!
//! It does **not** keep the signatures, and that is the safety property here
//! rather than an omission. This comment used to claim a table "can be moved
//! between the two by changing which macro is called", which is false and was
//! advertising the one hazard worth avoiding: a swap that changes a table's
//! concurrency and durability guarantees while every call site still compiles.
//!
//! Every call site breaks instead:
//!
//! | | `worktable!` | `worktable!` with `vec: true` |
//! |---|---|---|
//! | `insert` | `async fn(&self, Row) -> Result<Pk, WorkTableError>` | `fn(&mut self, Row) -> Result<(), Row>` |
//! | `upsert` | `async fn(&self, Row) -> Result<(), WorkTableError>` | `fn(&mut self, Row)` |
//! | `delete` | `async fn(&self, Pk) -> Result<(), WorkTableError>` | `fn(&mut self, &Pk) -> Option<Row>` |
//! | `select` | `fn(&self, Pk) -> Option<Row>`, cloned out | `fn(&self, &Pk) -> Option<&Row>`, borrowed |
//!
//! A missing `.await`, `&self` against `&mut self`, an owned row against a
//! borrowed one: the compiler rejects the swap four different ways before it
//! can silently weaken anything. The guarantees differ, so the types differ.
//! That is what makes the difference safe to live with, not the fact that this
//! is a separate macro. A second macro, or a second crate, would relabel the
//! divergence without catching it.
//!
//! **And the index backend.** This is not a detail. The first version of this
//! generator hardcoded `BTreeMap` and accepted `using arctic` without
//! honouring it, which is the worst of both: a stated choice silently dropped,
//! and the slower structure chosen on the caller's behalf. `worktable-vec`
//! measures the same two arms over a five-field row and one million point
//! lookups and reports 32.34 ns/query for `Vec + BTreeMap` against 5.25 for
//! `Vec + Arctic`. Defaulting to `BTreeMap` gave away roughly six times the
//! lookup, for a macro whose entire claim is that it costs what a `Vec` costs.
//!
//! So the default here is Arctic, which is `worktable!`'s default, and `using`
//! selects as it does there:
//!
//! | clause | this macro emits | non-unique |
//! |---|---|---|
//! | absent, or `using arctic` | `ArcticIndex` | `ArcticMultiIndex` |
//! | `using worktables_index` | WTI's `IndexMap` | refused, no shared multimap trait |
//! | `using congee` | `CongeeIndex` | refused, congee has no multimap |
//! | `using indexset` | `BTreeMap`, the plain ordered map | `BTreeMap<K, Vec<usize>>` |
//!
//! `worktable!` additionally demands an explicit `persist` before it accepts
//! congee, because congee behaves differently persisted and the author has to
//! say which they meant. This macro has no persistence at all, so the question
//! is already answered and the rule does not carry over. Congee was refused
//! here for a while on the strength of that rule's name rather than its
//! reason.
//!
//! `using indexset` is the way to ask for `BTreeMap` deliberately, and there
//! is one reason to: `delete` shifts every position above the hole, and a
//! `BTreeMap` shifts them in place while an ART has to reinsert each one. A
//! delete-heavy table should measure both.

use proc_macro2::TokenStream;
use quote::quote;
use syn::Ident;
use worktable_dsl::{Columns, IndexBackend};

use crate::generators::index_backend::primitive_name;

// Paths are written through `worktable::prelude`, never as bare `alloc::` or
// `std::`. The macro expands in the consumer's crate, so anything it names has
// to resolve there: emitting `alloc::` requires the consumer to have declared
// `extern crate alloc`, and emitting a crate name makes that crate part of this
// macro's contract. The same mistake has been made here with `tokio::`,
// `futures::` and `worktable::prelude::rkyv::`.

/// What a resolved backend actually stores.
///
/// Arctic and WTI collapse into one arm for unique indexes because both
/// implement `UniqueIndex`, so the emitted calls are identical and only the
/// type name differs. They separate again for non-unique ones, where the two
/// multimaps do not share a trait.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Repr {
    Arctic,
    Wti,
    Congee,
    Ordered,
}

impl Repr {
    /// Does this store positions through the `UniqueIndex` trait rather than
    /// through inherent `BTreeMap` methods?
    fn is_trait_backed(self) -> bool {
        matches!(self, Repr::Arctic | Repr::Wti | Repr::Congee)
    }

    /// Has this backend a multimap for a non-unique index?
    fn has_multimap(self) -> bool {
        matches!(self, Repr::Arctic | Repr::Ordered)
    }

    /// The `using` spelling, for error messages.
    fn name(self) -> &'static str {
        match self {
            Repr::Arctic => "arctic",
            Repr::Wti => "worktables_index",
            Repr::Congee => "congee",
            Repr::Ordered => "indexset",
        }
    }
}

/// Resolve a declared backend, refusing what this table cannot honour.
///
/// `what` names the index in the error, because a table with four of them
/// otherwise reports a refusal with nothing to attach it to.
fn resolve(backend: IndexBackend, ty: &TokenStream, span: proc_macro2::Span, what: &str) -> syn::Result<Repr> {
    let repr = match backend {
        IndexBackend::Arctic => Repr::Arctic,
        IndexBackend::WorktablesIndex => Repr::Wti,
        IndexBackend::Congee => Repr::Congee,
        IndexBackend::Indexset => Repr::Ordered,
    };
    // `worktable!` additionally requires `persist` to be stated before it will
    // accept congee, because congee behaves differently persisted and the
    // author has to say which they meant. This macro has no persistence at
    // all, so that question is already answered and the rule does not carry
    // over. It was refused here for a while on the strength of the rule's
    // name rather than its reason.
    let Some(supported) = worktable_dsl::validate::supported_key_types(backend) else {
        return Ok(repr);
    };
    if primitive_name(ty)
        .as_deref()
        .is_some_and(|name| supported.contains(&name))
    {
        return Ok(repr);
    }
    Err(syn::Error::new(
        span,
        format!(
            "`using {}` indexes {what} on one of {}, and `{ty}` is not one of them. \
             Use `using worktables_index` to index it, or `using indexset` for a plain \
             ordered map. (Type aliases cannot be resolved by the macro.)",
            repr.name(),
            supported.join(", ")
        ),
    ))
}

/// The stored type for a unique key-to-position map.
fn unique_type(repr: Repr, ty: &TokenStream) -> TokenStream {
    match repr {
        Repr::Arctic => quote! { worktable::prelude::ArcticIndex<#ty, u64> },
        Repr::Wti => quote! { worktable::prelude::IndexMap<#ty, u64> },
        Repr::Congee => quote! { worktable::prelude::CongeeIndex<#ty, u64> },
        Repr::Ordered => quote! { worktable::prelude::BTreeMap<#ty, usize> },
    }
}

/// The stored type for a non-unique key-to-positions map.
///
/// Only two backends reach here; `has_multimap` refuses the others first.
fn multi_type(repr: Repr, ty: &TokenStream) -> TokenStream {
    match repr {
        Repr::Arctic => quote! { worktable::prelude::ArcticMultiIndex<#ty, u64> },
        Repr::Ordered => quote! { worktable::prelude::BTreeMap<#ty, worktable::prelude::Vec<usize>> },
        Repr::Wti | Repr::Congee => {
            quote! { compile_error!("unreachable: this backend has no multimap and was refused during resolution") }
        }
    }
}

/// Congee packs a key into one `usize`, so a `u64` key needs a 64-bit target.
///
/// `impl CongeeKey for u64` is itself behind that cfg, so without this the
/// failure on a 32-bit target is an unsatisfied trait bound on a type the
/// author never wrote. `worktable!` emits the same guard for the same reason.
fn congee_width_guard(repr: Repr, ty: &TokenStream) -> TokenStream {
    if repr == Repr::Congee && primitive_name(ty).as_deref() == Some("u64") {
        quote! {
            #[cfg(not(target_pointer_width = "64"))]
            compile_error!("`using congee` with a `u64` key requires a 64-bit target");
        }
    } else {
        quote! {}
    }
}

// The five operations a unique map has to answer, emitted for whichever
// representation was resolved. `map` is the field access, already qualified.

fn unique_contains(repr: Repr, map: &TokenStream, key: &TokenStream) -> TokenStream {
    if repr.is_trait_backed() {
        quote! { worktable::prelude::UniqueIndex::contains_key(&#map, #key) }
    } else {
        quote! { #map.contains_key(#key) }
    }
}

fn unique_get(repr: Repr, map: &TokenStream, key: &TokenStream) -> TokenStream {
    if repr.is_trait_backed() {
        quote! { worktable::prelude::UniqueIndex::get_value(&#map, #key).map(|at| at as usize) }
    } else {
        quote! { #map.get(#key).copied() }
    }
}

fn unique_insert(repr: Repr, map: &TokenStream, key: &TokenStream, at: &TokenStream) -> TokenStream {
    if repr.is_trait_backed() {
        quote! { let _ = worktable::prelude::UniqueIndex::insert_value(&#map, #key, #at as u64); }
    } else {
        quote! { #map.insert(#key, #at); }
    }
}

/// Insert unless the key is already there, in one traversal. True means it was.
///
/// `insert` used to ask `contains_key` and then `insert_value`, which is two
/// full traversals of the index on every single insert, and it was the whole
/// of the macro's overhead: a hand-written `Vec` plus `ArcticIndex` ran 5.9 ms
/// over 200,000 rows, the same code with a `contains_key` guard added ran
/// 7.7 ms, and the generated table ran 7.7 ms. Both backends can answer the
/// question and do the work at once, so they do.
fn unique_insert_checked(repr: Repr, map: &TokenStream, key: &TokenStream, at: &TokenStream) -> TokenStream {
    if repr.is_trait_backed() {
        quote! {
            worktable::prelude::UniqueIndex::insert_value_checked(&#map, #key, #at as u64).is_none()
        }
    } else {
        quote! {
            match #map.entry(#key) {
                worktable::prelude::BTreeMapEntry::Occupied(_) => true,
                worktable::prelude::BTreeMapEntry::Vacant(slot) => {
                    slot.insert(#at);
                    false
                }
            }
        }
    }
}

fn unique_remove(repr: Repr, map: &TokenStream, key: &TokenStream) -> TokenStream {
    if repr.is_trait_backed() {
        quote! { worktable::prelude::UniqueIndex::remove_value(&#map, #key).map(|(_, at)| at as usize) }
    } else {
        quote! { #map.remove(#key) }
    }
}

/// Positions whose keys fall inside `bounds`, in key order.
///
/// Every backend this macro can resolve is an ordered tree — the two ARTs,
/// WTI's B-tree and a plain `BTreeMap` — so this is not a capability some of
/// them have and others emulate. `UniqueIndex` already requires
/// `range_links`, which means the operation was always there and only the
/// generated table declined to expose it.
///
/// What a range costs that a point lookup does not is the row fetch: the
/// positions come out in key order and the rows they name are scattered
/// through the vector, so a long range is a walk of random accesses rather
/// than a sequential read.
fn unique_range(repr: Repr, map: &TokenStream, bounds: &TokenStream) -> TokenStream {
    if repr.is_trait_backed() {
        quote! {
            worktable::prelude::UniqueIndex::range_links(&#map, #bounds).map(|at| at as usize)
        }
    } else {
        quote! { #map.range(#bounds).map(|(_, at)| *at) }
    }
}

/// Point every entry at where its row moved to, after a compaction.
///
/// Compaction is the only thing that moves a row, and it never removes an
/// index entry: a ghosted row left its indexes at the moment it was deleted,
/// so every entry still here names a row that survives. That is why this is a
/// rewrite of values and not a rebuild, and why it can keep the maps
/// themselves — replacing them with `Default::default()` would silently
/// discard a `with_node_size` the caller asked for.
fn unique_renumber(repr: Repr, map: &TokenStream, moved: &TokenStream) -> TokenStream {
    if repr.is_trait_backed() {
        quote! {
            let entries: worktable::prelude::Vec<_> =
                worktable::prelude::UniqueIndex::iter_values(&#map).collect();
            for (key, position) in entries {
                let to = #moved[position as usize];
                if to != position {
                    let _ = worktable::prelude::UniqueIndex::insert_value(&#map, key, to);
                }
            }
        }
    } else {
        quote! {
            for position in #map.values_mut() {
                *position = #moved[*position] as usize;
            }
        }
    }
}

pub fn expand(name: Ident, columns: Columns) -> syn::Result<TokenStream> {
    if columns.primary_keys.len() != 1 {
        return Err(syn::Error::new(
            name.span(),
            "`vec: true` takes a single-column primary key. A composite key needs a tuple key \
             type, which is the machinery this storage exists to avoid.",
        ));
    }
    if !columns.columnar_fields.is_empty() || !columns.columnar_indexes.is_empty() {
        return Err(syn::Error::new(
            name.span(),
            "`vec: true` does not support columnar fields. Columnar storage is a paging \
             feature and this table has no pages.",
        ));
    }

    // `{Name}Row` and `{Name}WorkTable`, the same names the paged table gets.
    //
    // This was `{Name}VecRow` and `{Name}VecTable` while a second macro
    // generated it, because two macros naming one table collided on the row.
    // One macro and a `storage:` key removes the collision at the source, so
    // there is no reason left to make a caller learn a parallel vocabulary:
    // the storage is a property of the declaration, not of every identifier
    // that comes out of it.
    let row_ident = Ident::new(&format!("{name}Row"), name.span());
    let table_ident = Ident::new(&format!("{name}WorkTable"), name.span());

    let pk = columns.primary_keys.first().expect("checked above").clone();
    let pk_type = columns
        .columns_map
        .get(&pk)
        .expect("the primary key is a column")
        .clone();

    let pk_repr = resolve(columns.primary_index_backend, &pk_type, pk.span(), "the primary key")?;
    let pk_map_type = unique_type(pk_repr, &pk_type);
    let mut width_guards = vec![congee_width_guard(pk_repr, &pk_type)];

    // WTI is the only backend with a node-size knob; arctic and congee have no
    // node-size concept at all. A constructor that took one on a table with no
    // WTI index would be a silent no-op, which this crate refuses everywhere
    // else, so it is emitted only when there is something for it to set.
    let pk_is_wti = matches!(pk_repr, Repr::Wti);

    let field_names: Vec<_> = columns.columns_map.keys().cloned().collect();
    let field_types: Vec<_> = columns.columns_map.values().cloned().collect();

    // Secondary indexes, as positions. Unique ones keep the reject.
    let mut index_fields = Vec::new();
    let mut index_map_types = Vec::new();
    let mut index_columns = Vec::new();
    let mut index_reprs = Vec::new();
    let mut index_unique = Vec::new();
    for (index_name, index) in &columns.indexes {
        let column = &index.field;
        let ty = columns
            .columns_map
            .get(column)
            .ok_or_else(|| syn::Error::new(index_name.span(), format!("no column `{column}`")))?;
        // `columns.indexes` is keyed by the indexed *column*; the name the
        // author wrote is `index.name`. Errors quote that one, because it is
        // the token they can go and edit.
        let declared = &index.name;
        let repr = resolve(index.backend, ty, index_name.span(), &format!("`{declared}`"))?;
        if !index.is_unique && !repr.has_multimap() {
            return Err(syn::Error::new(
                index_name.span(),
                format!(
                    "the non-unique index `{declared}` cannot use `{}`: congee has no multimap at \
                     all, and WTI's does not share a trait with Arctic's, so this would be a second \
                     code path with no measurement behind it. Use arctic (the default), \
                     `using indexset`, or declare the index `unique`.",
                    repr.name()
                ),
            ));
        }
        width_guards.push(congee_width_guard(repr, ty));
        index_fields.push(Ident::new(&format!("{index_name}_map"), index_name.span()));
        index_map_types.push(if index.is_unique {
            unique_type(repr, ty)
        } else {
            multi_type(repr, ty)
        });
        index_columns.push(column.clone());
        index_reprs.push(repr);
        index_unique.push(index.is_unique);
    }

    // Per-index statement fragments, so the method bodies below stay readable.
    let mut index_reject_duplicate = Vec::new();
    let mut index_insert = Vec::new();
    let mut index_upsert_move = Vec::new();
    let mut index_delete_remove = Vec::new();
    let mut index_renumber = Vec::new();
    for ((field, (column, (repr, unique))), _) in index_fields
        .iter()
        .zip(
            index_columns
                .iter()
                .zip(index_reprs.iter().copied().zip(index_unique.iter().copied())),
        )
        .zip(0..)
    {
        let map = quote! { self.#field };
        let key = quote! { &row.#column };
        let owned = quote! { row.#column.clone() };
        let at = quote! { at };

        index_reject_duplicate.push(if unique {
            let contains = unique_contains(repr, &map, &key);
            quote! { if #contains { return Err(row); } }
        } else {
            quote! {}
        });

        index_insert.push(if unique {
            unique_insert(repr, &map, &owned, &at)
        } else {
            match repr {
                Repr::Arctic => quote! { #map.insert_pair(#owned, at as u64); },
                _ => quote! { #map.entry(#owned).or_default().push(at); },
            }
        });

        // On upsert the row keeps its position and only its key changes, so
        // the old pair comes out and the new one goes in at the same `at`.
        //
        // The old key is bound to a local first. Reading it inline would
        // borrow the whole table (`row_at` takes `&self`) while the map call
        // it feeds wants `&mut` on a field, and the two-phase borrow that let
        // `self.rows[at]` work here does not reach through a method.
        let was = Ident::new(&format!("was_{field}_key"), field.span());
        index_upsert_move.push(if unique {
            let remove = unique_remove(repr, &map, &quote! { &#was });
            let insert = unique_insert(repr, &map, &owned, &at);
            quote! {
                let #was = self.row_at(at).#column.clone();
                let _ = #remove;
                #insert
            }
        } else {
            match repr {
                Repr::Arctic => quote! {
                    let #was = self.row_at(at).#column.clone();
                    let _ = #map.remove_pair(&#was, &(at as u64));
                    #map.insert_pair(#owned, at as u64);
                },
                _ => quote! {
                    let #was = self.row_at(at).#column.clone();
                    if let Some(positions) = #map.get_mut(&#was) {
                        positions.retain(|p| *p != at);
                    }
                    #map.entry(#owned).or_default().push(at);
                },
            }
        });

        index_delete_remove.push(if unique {
            let key = quote! { &row.#column };
            let remove = unique_remove(repr, &map, &key);
            quote! { let _ = #remove; }
        } else {
            match repr {
                Repr::Arctic => quote! { let _ = #map.remove_pair(&row.#column, &(at as u64)); },
                _ => quote! {
                    #map.retain(|_, positions| {
                        positions.retain(|p| *p != at);
                        !positions.is_empty()
                    });
                },
            }
        });

        index_renumber.push(if unique {
            unique_renumber(repr, &map, &quote! { moved })
        } else {
            match repr {
                Repr::Arctic => quote! {
                    let pairs: worktable::prelude::Vec<_> = #map.iter().collect();
                    for (key, position) in pairs {
                        let to = moved[position as usize];
                        if to != position {
                            let _ = #map.remove_pair(&key, &position);
                            #map.insert_pair(key, to);
                        }
                    }
                },
                _ => quote! {
                    for positions in #map.values_mut() {
                        for position in positions.iter_mut() {
                            *position = moved[*position] as usize;
                        }
                    }
                },
            }
        });
    }

    let select_by: Vec<_> = columns
        .indexes
        .iter()
        .zip(index_reprs.iter().copied())
        .map(|((index_name, index), repr)| {
            let column = &index.field;
            let fn_name = Ident::new(&format!("select_by_{column}"), index_name.span());
            let field = Ident::new(&format!("{index_name}_map"), index_name.span());
            let map = quote! { self.#field };
            let ty = columns.columns_map.get(column).expect("checked above");
            if index.is_unique {
                let get = unique_get(repr, &map, &quote! { key });
                let range_fn = Ident::new(&format!("range_by_{column}"), index_name.span());
                let range = unique_range(repr, &map, &quote! { bounds });
                quote! {
                    /// The row this key indexes, if any.
                    pub fn #fn_name(&self, key: &#ty) -> Option<&#row_ident> {
                        #get.map(|at| self.row_at(at))
                    }

                    /// Every row whose indexed value falls inside `bounds`, in
                    /// that value's order.
                    ///
                    /// Free for the same reason the primary-key range is: this
                    /// index is an ordered tree and was already answering
                    /// ranges, so the walk is the index's own and the only
                    /// added work is the row fetch each position names.
                    pub fn #range_fn<'a, R>(
                        &'a self,
                        bounds: R,
                    ) -> impl DoubleEndedIterator<Item = &'a #row_ident> + 'a
                    where
                        R: core::ops::RangeBounds<#ty> + 'a,
                    {
                        #range.map(|at| self.row_at(at))
                    }
                }
            } else {
                let positions = match repr {
                    Repr::Arctic => quote! {
                        let mut positions: worktable::prelude::Vec<usize> =
                            #map.get(key).map(|(_, at)| at as usize).collect();
                        // Arctic orders pairs by value, which is position, which
                        // is insertion order. Sorting states that rather than
                        // relying on it.
                        positions.sort_unstable();
                    },
                    _ => quote! {
                        let positions: worktable::prelude::Vec<usize> =
                            #map.get(key).map(|found| found.clone()).unwrap_or_default();
                    },
                };
                quote! {
                    /// Every row this key indexes, in insertion order.
                    pub fn #fn_name(&self, key: &#ty) -> Vec<&#row_ident> {
                        #positions
                        positions.into_iter().map(|at| self.row_at(at)).collect()
                    }
                }
            }
        })
        .collect();

    // Per-index fragments for `update`: what the key was before the edit, and
    // the repair when it changed. A non-unique index moves one pair; a unique
    // one re-keys a single entry.
    let index_before: Vec<Ident> = index_fields
        .iter()
        .map(|field| Ident::new(&format!("was_{field}"), field.span()))
        .collect();
    let mut index_repair = Vec::new();
    for (((field, column), repr), unique) in index_fields
        .iter()
        .zip(index_columns.iter())
        .zip(index_reprs.iter().copied())
        .zip(index_unique.iter().copied())
    {
        let map = quote! { self.#field };
        let before = Ident::new(&format!("was_{field}"), field.span());
        // Bound to a local for the same borrow reason `index_upsert_move`
        // binds its old key: the map calls below take `&mut` on a field, and
        // `row_at` borrows the whole table.
        let now = quote! { now };
        let repair = if unique {
            let remove = unique_remove(repr, &map, &quote! { &#before });
            let insert = unique_insert(repr, &map, &now, &quote! { at });
            quote! { let _ = #remove; #insert }
        } else {
            match repr {
                Repr::Arctic => quote! {
                    let _ = #map.remove_pair(&#before, &(at as u64));
                    #map.insert_pair(#now, at as u64);
                },
                _ => quote! {
                    if let Some(positions) = #map.get_mut(&#before) {
                        positions.retain(|p| *p != at);
                    }
                    #map.entry(#now).or_default().push(at);
                },
            }
        };
        index_repair.push(quote! {
            if self.row_at(at).#column != #before {
                let now = self.row_at(at).#column.clone();
                #repair
            }
        });
    }

    let pk_map = quote! { self.by_pk };
    let at_expr = quote! { at };
    let pk_insert_checked = unique_insert_checked(pk_repr, &pk_map, &quote! { row.#pk.clone() }, &at_expr);
    let pk_get_for_select = unique_get(pk_repr, &pk_map, &quote! { key });
    let pk_get_for_upsert = unique_get(pk_repr, &pk_map, &quote! { &row.#pk });
    let pk_remove = unique_remove(pk_repr, &pk_map, &quote! { key });
    let pk_get_for_moved_row = unique_get(pk_repr, &pk_map, &quote! { &now_pk });
    let pk_remove_old = {
        let remove = unique_remove(pk_repr, &pk_map, &quote! { &was_pk });
        quote! { let _ = #remove; }
    };
    let pk_reinsert_moved = unique_insert(pk_repr, &pk_map, &quote! { now_pk }, &quote! { at });
    let pk_renumber = unique_renumber(pk_repr, &pk_map, &quote! { moved });
    let pk_range = unique_range(pk_repr, &pk_map, &quote! { bounds });

    // rkyv's derives only when the table can be written out. They are not free
    // to a caller who never persists: an `Archived` type per row, a resolver
    // per row, and the compile time to produce both.
    //
    // The crate path is `worktable::prelude::rkyv`, and `#[rkyv(crate = ..)]`
    // redirects the derive's own generated paths to it. Emitting a bare `rkyv`
    // would make the consumer's manifest part of this macro's contract, which
    // is the leak `worktable!` still has.
    // Always, not behind a flag. `persist` is refused on this table, so there
    // is nothing left to gate them with, and the alternative is a third key.
    // Measured at 20 tables of five columns: 305 ms without, 470 ms with, so
    // about 8 ms a table. Real, and not worth a key.
    // The node-size constructor. Emitted only when there is a WTI index to set
    // it on, so it can never be a knob that does nothing.
    let with_node_size = if pk_is_wti || index_reprs.iter().any(|r| matches!(r, Repr::Wti)) {
        let pk_init = if pk_is_wti {
            quote! { by_pk: <#pk_map_type>::with_maximum_node_size(node_size), }
        } else {
            quote! { by_pk: Default::default(), }
        };
        let index_inits: Vec<_> = index_fields
            .iter()
            .zip(index_map_types.iter())
            .zip(index_reprs.iter())
            .map(|((field, ty), repr)| {
                if matches!(repr, Repr::Wti) {
                    quote! { #field: <#ty>::with_maximum_node_size(node_size), }
                } else {
                    quote! { #field: Default::default(), }
                }
            })
            .collect();
        quote! {
            /// A table whose `worktables_index` indexes use `node_size` as
            /// their leaf width, instead of the default 1,024.
            ///
            /// The width is a call-site decision rather than a declaration one,
            /// because the right value depends on the workload and not on the
            /// schema: the same table read-mostly in one process and written
            /// hard in another wants different numbers, and a declaration can
            /// only say one thing.
            ///
            /// Measured at a million shuffled keys
            /// (`perf-benchmarks/benchmarks/wti-node-size.rs`):
            ///
            /// | width | insert | lookup | drop |
            /// |---:|---:|---:|---:|
            /// | 128 | 127.36 ns | 134.58 ns | 555.3 us |
            /// | 256 | 128.80 ns | 129.17 ns | 290.5 us |
            /// | 1,024 (default) | 200.34 ns | 124.58 ns | 85.7 us |
            /// | 16,384 | 1,445.89 ns | 116.67 ns | 9.9 us |
            ///
            /// Narrow is much better for writing, slightly worse for reading,
            /// and worse for teardown. 256 is the write-heavy pick; the default
            /// stays 1,024 because a wrong guess is worse than no guess, and
            /// only the call site knows which way this table leans.
            ///
            /// **Nothing is capped.** The width is the leaf size a node splits
            /// at, not a limit on rows: the tree grows by adding nodes exactly
            /// as it does at the default, so an undersized guess costs
            /// performance and never correctness.
            ///
            /// Emitted only for tables that have at least one
            /// `using worktables_index`, so it is never a knob with nothing to
            /// turn.
            #[must_use]
            pub fn with_node_size(node_size: usize) -> Self {
                Self {
                    rows: worktable::prelude::Vec::new(),
                    live: 0,
                    #pk_init
                    #(#index_inits)*
                }
            }

            /// Both knobs at once: rows sized for `capacity`, WTI leaves at
            /// `node_size`.
            #[must_use]
            pub fn with_capacity_and_node_size(capacity: usize, node_size: usize) -> Self {
                let mut table = Self::with_node_size(node_size);
                table.rows = worktable::prelude::Vec::with_capacity(capacity);
                table
            }
        }
    } else {
        quote! {}
    };

    let row_derives = {
        quote! {
            #[derive(
                Clone,
                Debug,
                PartialEq,
                worktable::prelude::rkyv::Archive,
                worktable::prelude::rkyv::Serialize,
                worktable::prelude::rkyv::Deserialize,
            )]
            #[rkyv(crate = worktable::prelude::rkyv)]
        }
    };

    let hydrate = {
        quote! {
            /// Every row as pages, ready to be written somewhere.
            ///
            /// A page stands alone, so damage is local to one page and an
            /// append does not rewrite the file.
            ///
            /// # Errors
            ///
            /// [`worktable::prelude::RowTooLarge`] when one row's archive does
            /// not fit a page body. Nothing is produced in that case, rather
            /// than a file that will not load.
            ///
            /// Ghosted slots are not written, so a file never carries a row
            /// that was deleted. Collecting the live rows to do that costs one
            /// clone each, which is real and is dwarfed by the archive write
            /// that follows it.
            pub fn unload(&self) -> Result<worktable::prelude::Vec<u8>, worktable::prelude::RowTooLarge> {
                let live: worktable::prelude::Vec<#row_ident> =
                    self.rows.iter().flatten().cloned().collect();
                worktable::prelude::to_pages(&live)
            }

            /// A table back from pages, with every index rebuilt.
            ///
            /// The indexes are not stored. They are positions into the row
            /// vector, so they are cheaper to rebuild on load than to write,
            /// validate and keep consistent with the rows on disk.
            ///
            /// # Errors
            ///
            /// [`worktable::prelude::LoadError`], naming the page that went
            /// wrong. A different row type's file is refused by its
            /// fingerprint rather than read as debris.
            pub fn load(bytes: &[u8]) -> Result<Self, worktable::prelude::LoadError> {
                let rows: worktable::prelude::Vec<#row_ident> = worktable::prelude::from_pages(bytes)?;
                let mut table = Self::with_capacity(rows.len());
                for row in rows {
                    // A duplicate key in a loaded file is a corrupt file, not a
                    // caller error, and `insert` is the only thing that builds
                    // every index. Refusing here would be better still, but
                    // `LoadError` describes bytes rather than rows and there is
                    // no variant that could honestly say this.
                    let _ = table.insert(row);
                }
                Ok(table)
            }
        }
    };

    Ok(quote! {
        #(#width_guards)*

        #row_derives
        pub struct #row_ident {
            #(pub #field_names: #field_types,)*
        }

        /// A `Vec`-backed table with the same surface as the generated `WorkTable`.
        ///
        /// Single-writer by construction: every mutation takes `&mut self`.
        ///
        /// # Ghosts
        ///
        /// A slot is `None` once its row is deleted. That is the paged table's
        /// model applied to a vector, and it is what makes `delete` O(1)
        /// instead of O(rows + index): closing the hole would mean a memmove
        /// of every row above it *and* a rewrite of every index entry above
        /// it, which measured 21 milliseconds per delete at a million rows.
        ///
        /// The cost is that ghosts accumulate and nothing reclaims them until
        /// [`Self::compact`] is called, exactly as a paged table accumulates
        /// them until vacuum runs. [`Self::ghost_count`] and [`Self::slots`]
        /// are there so a caller can decide when that is worth doing.
        #[derive(Debug, Default)]
        pub struct #table_ident {
            /// Slots. `None` is a ghost: a row that was deleted and whose
            /// position no index names any more.
            rows: worktable::prelude::Vec<Option<#row_ident>>,
            /// Live rows, so `len` does not walk the vector counting them.
            live: usize,
            /// Primary key to position. The lookup a bare `Vec` does linearly.
            by_pk: #pk_map_type,
            #(#index_fields: #index_map_types,)*
        }

        impl #table_ident {
            #[must_use]
            pub fn new() -> Self {
                Self::default()
            }

            /// A table whose row vector can hold `capacity` rows without
            /// reallocating.
            ///
            /// Only the rows are sized. The indexes are trees and have no
            /// equivalent knob, so an accurate capacity removes the row
            /// vector's growth entirely and leaves theirs alone.
            #[must_use]
            pub fn with_capacity(capacity: usize) -> Self {
                Self {
                    rows: worktable::prelude::Vec::with_capacity(capacity),
                    ..Self::default()
                }
            }

            #with_node_size

            /// How many rows fit before the row vector grows again.
            #[must_use]
            pub fn capacity(&self) -> usize {
                self.rows.capacity()
            }

            /// Make room for `additional` more rows.
            pub fn reserve(&mut self, additional: usize) {
                self.rows.reserve(additional);
            }

            /// Every live row, in insertion order.
            ///
            /// Ghosted slots are skipped, so this yields [`Self::len`] rows
            /// and not [`Self::slots`] of them.
            pub fn iter(&self) -> impl Iterator<Item = &#row_ident> {
                self.rows.iter().flatten()
            }

            /// The live rows, leaving the indexes and the ghosts behind.
            ///
            /// For handing the data to something that does not want a table.
            /// The indexes are positions into this vector and mean nothing
            /// without it, so they are dropped rather than returned.
            #[must_use]
            pub fn into_rows(self) -> worktable::prelude::Vec<#row_ident> {
                self.rows.into_iter().flatten().collect()
            }

            /// The row at a position an index gave us.
            ///
            /// # Panics
            ///
            /// If the slot is a ghost. Every index entry is removed the moment
            /// its row is deleted, so a position that came out of an index
            /// always names a live row; reaching this panic means an index and
            /// the vector disagree, which is a bug in this macro rather than
            /// in a caller.
            #[inline]
            fn row_at(&self, at: usize) -> &#row_ident {
                self.rows[at]
                    .as_ref()
                    .expect("an index position always names a live row")
            }

            #[inline]
            fn row_at_mut(&mut self, at: usize) -> &mut #row_ident {
                self.rows[at]
                    .as_mut()
                    .expect("an index position always names a live row")
            }

            /// Row bytes plus index bytes.
            ///
            /// The same name and the same intent as the paged table's
            /// `used_bytes`, so a partitioned router can total either payload
            /// without knowing which it holds.
            ///
            /// Rows are counted as `len * size_of::<Row>()`: the inline row
            /// only. A column that owns a heap allocation, a `String` most
            /// obviously, has its buffer counted by neither this nor the paged
            /// table's equivalent. The indexes are counted through `MemStat`,
            /// which is where most of the cost is at small row counts: arctic
            /// holds about 600 bytes per 24-byte row at 64 rows and does not
            /// settle until a thousand.
            ///
            /// Slots are counted, not rows: a ghost still occupies its slot
            /// until [`Self::compact`] runs, and an `Option<Row>` is what a
            /// slot costs. For a row with a spare bit pattern that is the same
            /// as the row; for one with none it is the row plus its alignment.
            #[must_use]
            pub fn used_bytes(&self) -> u64 {
                let rows = self.rows.len() * core::mem::size_of::<Option<#row_ident>>();
                let indexes = worktable::prelude::MemStat::heap_size(&self.by_pk)
                    #(+ worktable::prelude::MemStat::heap_size(&self.#index_fields))*;
                (rows + indexes) as u64
            }

            /// Live rows.
            #[must_use]
            pub fn len(&self) -> usize {
                self.live
            }

            /// Slots, live and ghosted together.
            ///
            /// The length of the underlying vector, which is what memory is
            /// proportional to and what a range or a scan walks.
            #[must_use]
            pub fn slots(&self) -> usize {
                self.rows.len()
            }

            /// Deleted rows whose slots are still held.
            ///
            /// `slots() - len()`. A caller watching this decides when
            /// [`Self::compact`] is worth its cost, the same judgement a
            /// paged table makes about vacuum.
            #[must_use]
            pub fn ghost_count(&self) -> usize {
                self.rows.len() - self.live
            }

            /// Rows currently in the table.
            ///
            /// The same figure as [`Self::len`], under the name the paged
            /// table uses, so a partitioned router reads either payload
            /// through one call. Neither counts ghosts; [`Self::slots`] is the
            /// figure that does.
            #[must_use]
            pub fn row_count(&self) -> usize {
                self.live
            }

            #[must_use]
            pub fn is_empty(&self) -> bool {
                self.live == 0
            }

            /// Insert, refusing a key that is already present.
            ///
            /// `Err` carries the row back rather than dropping it, so a caller
            /// that wants `upsert` semantics on failure still has the value.
            pub fn insert(&mut self, row: #row_ident) -> Result<(), #row_ident> {
                // The unique secondaries are checked first and separately,
                // because a rejection from one of them must not leave the
                // primary key inserted. They are the only reads here that are
                // not also writes.
                #(#index_reject_duplicate)*
                let at = self.rows.len();
                if #pk_insert_checked {
                    return Err(row);
                }
                #(#index_insert)*
                self.rows.push(Some(row));
                self.live += 1;
                Ok(())
            }

            /// Insert, or replace the row this key already names.
            pub fn upsert(&mut self, row: #row_ident) {
                if let Some(at) = #pk_get_for_upsert {
                    #(#index_upsert_move)*
                    self.rows[at] = Some(row);
                    return;
                }
                let _ = self.insert(row);
            }

            /// The row this key names, if any.
            #[must_use]
            pub fn select(&self, key: &#pk_type) -> Option<&#row_ident> {
                #pk_get_for_select.map(|at| self.row_at(at))
            }

            /// Every live row, in insertion order.
            ///
            /// An iterator rather than the `&[Row]` this returned before
            /// ghosting: a deleted row leaves a hole, so the live rows are no
            /// longer a contiguous slice and no slice could be handed back
            /// without first paying the compaction this design exists to
            /// defer. Call [`Self::compact`] and then [`Self::iter`] if a
            /// caller genuinely needs one.
            pub fn select_all(&self) -> impl Iterator<Item = &#row_ident> {
                self.rows.iter().flatten()
            }

            /// Every live row whose primary key falls inside `bounds`, in key
            /// order.
            ///
            /// This costs nothing to provide and was simply never exposed.
            /// Every backend the `using` clause can name is an ordered tree,
            /// `UniqueIndex` already requires `range_links`, and the index was
            /// answering ranges the whole time.
            ///
            /// What it is not is a sorted vector. The keys come out in order
            /// and the rows they name are wherever insertion put them, so a
            /// long range is a sequence of random accesses into the row
            /// vector. Ordered, correct, and not sequential.
            ///
            /// ```ignore
            /// for row in table.range(10..20) { .. }
            /// for row in table.range(..).rev() { .. }
            /// ```
            pub fn range<'a, R>(
                &'a self,
                bounds: R,
            ) -> impl DoubleEndedIterator<Item = &'a #row_ident> + 'a
            where
                R: core::ops::RangeBounds<#pk_type> + 'a,
            {
                #pk_range.map(|at| self.row_at(at))
            }

            #(#select_by)*

            /// Edit a row where it sits, then repair whatever indexes it moved
            /// under.
            ///
            /// `worktable-vec` hands out `&mut (K, V)` for this, but only from
            /// `LinearTable`, which has no indexes to invalidate. Doing that
            /// here would let a caller change an indexed column and leave the
            /// index pointing at a key the row no longer has, which is silent
            /// and unfindable. A closure lets the table see what changed.
            ///
            /// Returns `false` when no row has that key, leaving the table
            /// untouched.
            ///
            /// # Panics
            ///
            /// If the edit gives the row a primary key that another row
            /// already holds. The row is restored first, so the table is
            /// unchanged; this is a panic rather than an error because the
            /// alternative is a table with two rows under one key, and there
            /// is no return value a caller could sensibly ignore.
            pub fn update(&mut self, key: &#pk_type, edit: impl FnOnce(&mut #row_ident)) -> bool {
                let Some(at) = #pk_get_for_select else {
                    return false;
                };
                // Only the key columns are copied, not the row. They are what
                // the indexes are keyed on, so they are the only things whose
                // "before" the repair below needs.
                let was_pk = self.row_at(at).#pk.clone();
                #(let #index_before = self.row_at(at).#index_columns.clone();)*

                edit(self.row_at_mut(at));

                if self.row_at(at).#pk != was_pk {
                    let now_pk = self.row_at(at).#pk.clone();
                    let taken = #pk_get_for_moved_row;
                    if taken.is_some_and(|other| other != at) {
                        self.row_at_mut(at).#pk = was_pk;
                        panic!("update gave a row a primary key another row already holds");
                    }
                    #pk_remove_old
                    #pk_reinsert_moved
                }
                #(#index_repair)*
                true
            }

            #hydrate

            /// Remove the row this key names, returning it and leaving a ghost
            /// where it was.
            ///
            /// Constant time. The row comes out of its slot, its index entries
            /// come out of the indexes, and nothing else moves: no position
            /// changes, so no other index entry needs touching.
            ///
            /// This used to close the hole with `Vec::remove`, which meant a
            /// memmove of every row above it plus a rewrite of every index
            /// entry above it. On a `BTreeMap` that rewrite is an in-place
            /// walk; on an ART it is a read-and-reinsert of each affected
            /// entry. Measured on a million-row table it cost **21
            /// milliseconds a delete**, so two hundred deletes took four
            /// seconds.
            ///
            /// What it costs instead is a slot that stays allocated until
            /// [`Self::compact`] runs, and the row order that `select_all`
            /// walks getting sparser as ghosts accumulate.
            pub fn delete(&mut self, key: &#pk_type) -> Option<#row_ident> {
                let at = #pk_remove?;
                let row = self.rows[at].take()?;
                self.live -= 1;
                #(#index_delete_remove)*
                Some(row)
            }

            /// Reclaim every ghosted slot, moving the live rows down to close
            /// the holes and pointing the indexes at where they went.
            ///
            /// This is the vacuum a paged table runs, and it is the other half
            /// of what makes `delete` constant time: the expensive work exists,
            /// it is O(slots + index), and it happens once when a caller asks
            /// for it rather than on every delete.
            ///
            /// Insertion order is preserved. Returns the number of slots
            /// reclaimed, which is what [`Self::ghost_count`] read beforehand.
            ///
            /// The row vector keeps its capacity, so a table that churns does
            /// not give memory back to the allocator and then ask for it
            /// again. [`Self::shrink_to_fit`] is there for a caller that wants
            /// the memory back rather than the reuse.
            pub fn compact(&mut self) -> usize {
                let reclaimed = self.rows.len() - self.live;
                if reclaimed == 0 {
                    return 0;
                }

                // Where each old position ends up. Ghosted slots get a value
                // no index entry can name, because no index entry names them:
                // a delete takes its entries out at the time it ghosts the row.
                let mut moved = worktable::prelude::Vec::with_capacity(self.rows.len());
                let mut next = 0u64;
                for slot in &self.rows {
                    moved.push(next);
                    if slot.is_some() {
                        next += 1;
                    }
                }

                #pk_renumber
                #(#index_renumber)*

                self.rows.retain(Option::is_some);
                debug_assert_eq!(self.rows.len(), self.live);
                reclaimed
            }

            /// Give the row vector's spare capacity back to the allocator.
            ///
            /// Separate from [`Self::compact`] because they answer different
            /// questions: compaction is about ghosts, this is about capacity,
            /// and a table that compacts in order to keep inserting wants the
            /// capacity it already has.
            pub fn shrink_to_fit(&mut self) {
                self.rows.shrink_to_fit();
            }
        }
    })
}
