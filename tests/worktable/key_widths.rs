//! Every key width the macro says a backend supports, actually works.
//!
//! `validate_index_backends` rejects a declaration whose key type the backend
//! cannot serve, and the accepted sets are narrow and specific: congee takes
//! `u8`, `u16`, `u32`, `u64`, `usize`; arctic takes `u16`, `u32`, `u64`,
//! `u128`. That list is a promise the macro makes to a consumer.
//!
//! Nothing tested it. Before this file the suite instantiated `u64` with both
//! backends and `u128` once, so most of the advertised matrix had never been
//! generated, let alone exercised. The backends test their own key widths in
//! their own repositories, which is the right place for the data structure and
//! the wrong place for this: what is unverified here is not whether arctic
//! handles a `u16`, it is whether *a generated WorkTable over an arctic `u16`
//! index* stores, finds and removes a row.
//!
//! Deliberately shallow per cell. One row through insert, select by that index,
//! and delete is enough to catch a width that was never wired up; depth belongs
//! in the backend's own suite.

use worktable::prelude::*;
use worktable::worktable;

/// One table per (backend, key width). Separate modules because the generated
/// idents collide otherwise.
macro_rules! width_case {
    ($module:ident, $backend:ident, $key:ident, $label:literal) => {
        mod $module {
            use super::*;

            worktable!(
                name: Width,
                persist: false,
                columns: {
                    id: u64 primary_key,
                    key: $key,
                },
                indexes: { key_idx: key unique using $backend },
            );

            #[tokio::test]
            async fn the_advertised_key_width_round_trips() {
                let table = WidthWorkTable::default();

                // Three keys rather than one, so ordering has something to be
                // wrong about on a trie backend.
                for (id, key) in [(1u64, 7 as $key), (2, 42 as $key), (3, 5 as $key)] {
                    table.insert(WidthRow { id, key }).await.unwrap_or_else(|error| {
                        panic!("{}: insert failed for key type {}: {error}", $label, stringify!($key))
                    });
                }

                for (id, key) in [(1u64, 7 as $key), (2, 42 as $key), (3, 5 as $key)] {
                    let found = table
                        .select_by_key(key)
                        .unwrap_or_else(|| panic!("{}: {} key {key} not found by index", $label, stringify!($key)));
                    assert_eq!(found.id, id, "{}: {} index returned the wrong row", $label, stringify!($key));
                }

                // A key that was never inserted must not resolve.
                assert!(
                    table.select_by_key(99 as $key).is_none(),
                    "{}: {} index resolved a key that was never inserted",
                    $label,
                    stringify!($key)
                );

                // And the entry comes out again.
                futures::executor::block_on(table.delete(2u64)).expect("delete");
                assert!(
                    table.select_by_key(42 as $key).is_none(),
                    "{}: {} index still resolves a deleted row",
                    $label,
                    stringify!($key)
                );
                assert_eq!(table.count(), 2);
            }
        }
    };
}

// Congee: u8, u16, u32, u64, usize.
width_case!(congee_u8, congee, u8, "congee");
width_case!(congee_u16, congee, u16, "congee");
width_case!(congee_u32, congee, u32, "congee");
width_case!(congee_u64, congee, u64, "congee");
width_case!(congee_usize, congee, usize, "congee");

// Arctic: u16, u32, u64, u128.
width_case!(arctic_u16, arctic, u16, "arctic");
width_case!(arctic_u32, arctic, u32, "arctic");
width_case!(arctic_u64, arctic, u64, "arctic");
width_case!(arctic_u128, arctic, u128, "arctic");

// The default backend takes any ordered key, so it is the control: if a width
// fails here too, the problem is not the backend.
width_case!(wti_u16, worktables_index, u16, "wti");
width_case!(wti_u128, worktables_index, u128, "wti");
