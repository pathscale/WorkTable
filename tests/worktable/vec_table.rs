//! `worktable_vec!` behaves, and costs what a `Vec` costs.
//!
//! The second half is the point. A Vec-backed table that is materially slower
//! than the `Vec` it wraps has no reason to exist: the caller would write the
//! `Vec`. So the comparison is against the thing it replaces, not against
//! `worktable!`.

use std::collections::BTreeMap;
use std::time::Instant;

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: Point,
    vec: true,
    columns: {
        id: u64 primary_key,
        value: u64,
        tag: u64,
    },
    indexes: {
        tag_idx: tag,
    },
);

#[test]
fn it_behaves_like_a_table() {
    let mut table = PointWorkTable::new();

    table
        .insert(PointRow {
            id: 1,
            value: 10,
            tag: 7,
        })
        .expect("fresh");
    table
        .insert(PointRow {
            id: 2,
            value: 20,
            tag: 7,
        })
        .expect("fresh");
    assert!(
        table
            .insert(PointRow {
                id: 1,
                value: 99,
                tag: 9
            })
            .is_err(),
        "duplicate key"
    );

    assert_eq!(table.select(&1).expect("present").value, 10);
    assert_eq!(table.len(), 2);
    assert_eq!(table.select_all().count(), 2);

    // A non-unique index returns every row, in insertion order.
    let tagged = table.select_by_tag(&7);
    assert_eq!(tagged.len(), 2);
    assert_eq!(tagged[0].id, 1);

    table
        .upsert(PointRow {
            id: 1,
            value: 11,
            tag: 7,
        })
        .unwrap();
    assert_eq!(table.select(&1).expect("present").value, 11, "upsert replaces");
    assert_eq!(table.len(), 2, "upsert does not grow the table");

    let removed = table.delete(&1).expect("present");
    assert_eq!(removed.value, 11);
    assert_eq!(table.len(), 1);
    assert!(table.select(&1).is_none());
    // The survivor keeps its position; only the dead row left the indexes.
    assert_eq!(table.select(&2).expect("present").value, 20);
    assert_eq!(table.select_by_tag(&7).len(), 1);
}

/// The generated table must not be categorically slower than a plain `Vec`.
///
/// The baseline is what an application writes when it has no table: a `Vec` of
/// rows and a `BTreeMap` from key to position.
///
/// **This is not the parity measurement, and cannot be.** It used to be: the
/// generated table also held a `BTreeMap`, so the two were the same data
/// structures and a gap was the macro. The default backend is arctic now, so
/// the arms differ in the index as well, and the two disagree about which way.
/// Optimized, the generated table runs 0.64x the baseline. Unoptimized, which
/// is how `cargo test` runs it, it runs 1.77x, because an ART's generics are a
/// pile of uninlined calls until the optimizer sees them and `BTreeMap` suffers
/// far less. A tight bound here would encode whichever build happened to be
/// used to pick it.
///
/// Parity is measured in `perf-benchmarks`, in `benchmarks/wt-vec-generated.rs`,
/// against `worktable-vec`'s own `ArcticTable` and `IndexedTable`, optimized
/// and interleaved. What is left here is the check that survives a debug
/// build: that the table still does a map lookup and not a linear scan.
#[test]
#[ignore = "manual timing guard; run on a quiet host with an optimized benchmark for release evidence"]
fn it_costs_what_a_vec_costs() {
    const ROWS: u64 = 50_000;

    struct Baseline {
        rows: Vec<(u64, u64, u64)>,
        by_pk: BTreeMap<u64, usize>,
    }

    let started = Instant::now();
    let mut baseline = Baseline {
        rows: Vec::new(),
        by_pk: BTreeMap::new(),
    };
    for id in 0..ROWS {
        baseline.by_pk.insert(id, baseline.rows.len());
        baseline.rows.push((id, id * 2, id % 64));
    }
    let mut sum = 0u64;
    for id in 0..ROWS {
        if let Some(at) = baseline.by_pk.get(&id) {
            sum += baseline.rows[*at].1;
        }
    }
    let vec_time = started.elapsed();

    let started = Instant::now();
    let mut table = PointWorkTable::new();
    for id in 0..ROWS {
        table
            .insert(PointRow {
                id,
                value: id * 2,
                tag: id % 64,
            })
            .expect("fresh");
    }
    let mut table_sum = 0u64;
    for id in 0..ROWS {
        if let Some(row) = table.select(&id) {
            table_sum += row.value;
        }
    }
    let table_time = started.elapsed();

    assert_eq!(sum, table_sum, "the two must do the same work");

    let ratio = table_time.as_secs_f64() / vec_time.as_secs_f64();
    eprintln!("VEC-COST vec={:?} table={:?} ratio={ratio:.2}x", vec_time, table_time);
    assert!(
        ratio < 4.0,
        "the generated table took {ratio:.2}x the hand-written Vec plus BTreeMap. \
         It maintains one extra index and a different backend, so it is not \
         expected to tie in either direction, but this much means it is scanning \
         where the baseline looks up. See the doc comment for where parity is \
         actually measured."
    );
}

worktable!(
    name: Ordered,
    vec: true,
    columns: {
        id: u64 primary_key using indexset,
        value: u64,
        tag: u64,
    },
    indexes: {
        tag_idx: tag using indexset,
    },
);

worktable!(
    name: Named,
    vec: true,
    columns: {
        key: String primary_key,
        value: u64,
    },
);

/// Deleting from the middle leaves every other row where it was, on whichever
/// backend is holding the indexes.
///
/// A delete now ghosts the slot, so nothing above the hole moves and no index
/// entry but the dead row's is touched. That is the cheap half; the expensive
/// half is `compact`, which is tested next to this. What this covers is the
/// state in between, where the vector is sparse and every lookup still has to
/// be right: a non-unique index whose posting list straddles the hole is what
/// makes an off-by-one visible, because a row reachable by the wrong key
/// rather than by none is something `select_all` alone would not catch.
#[test]
fn deleting_from_the_middle_reindexes_both_backends() {
    macro_rules! check {
        ($table:ty, $row:ident) => {{
            let mut table = <$table>::new();
            for id in 0..6u64 {
                table
                    .insert($row {
                        id,
                        value: id * 10,
                        tag: id % 2,
                    })
                    .expect("fresh");
            }

            assert_eq!(table.delete(&2).expect("present").value, 20);

            // Every survivor still answers to its own key, with its own value.
            for id in [0u64, 1, 3, 4, 5] {
                let row = table.select(&id).unwrap_or_else(|| panic!("{id} should survive"));
                assert_eq!(row.value, id * 10, "{id} came back as another row");
            }
            assert!(table.select(&2).is_none());
            assert_eq!(table.len(), 5);

            // Insertion order survives the hole.
            let ids: Vec<u64> = table.select_all().map(|row| row.id).collect();
            assert_eq!(ids, vec![0, 1, 3, 4, 5]);

            // The non-unique index straddled the hole: tag 0 held 0, 2 and 4.
            let even: Vec<u64> = table.select_by_tag(&0).iter().map(|row| row.id).collect();
            assert_eq!(even, vec![0, 4], "tag 0 kept a deleted row or lost a live one");
            let odd: Vec<u64> = table.select_by_tag(&1).iter().map(|row| row.id).collect();
            assert_eq!(odd, vec![1, 3, 5]);

            // And the table still takes writes afterwards.
            table
                .insert($row {
                    id: 9,
                    value: 90,
                    tag: 1,
                })
                .expect("fresh");
            assert_eq!(table.select(&9).expect("present").value, 90);
            let odd: Vec<u64> = table.select_by_tag(&1).iter().map(|row| row.id).collect();
            assert_eq!(odd, vec![1, 3, 5, 9]);
        }};
    }

    check!(PointWorkTable, PointRow);
    check!(OrderedWorkTable, OrderedRow);
}

/// Arctic takes a `String` key, so the macro does not have to refuse one.
///
/// This is here because the refusal test next to it uses `bool`, and the two
/// together say where the line actually is. A reader who sees only the refusal
/// would reasonably assume every non-integer key is out.
#[test]
fn a_string_keyed_table_works() {
    let mut table = NamedWorkTable::new();
    table
        .insert(NamedRow {
            key: "beta".to_string(),
            value: 2,
        })
        .expect("fresh");
    table
        .insert(NamedRow {
            key: "alpha".to_string(),
            value: 1,
        })
        .expect("fresh");
    assert!(
        table
            .insert(NamedRow {
                key: "alpha".to_string(),
                value: 9
            })
            .is_err()
    );

    assert_eq!(table.select(&"alpha".to_string()).expect("present").value, 1);
    assert_eq!(table.delete(&"beta".to_string()).expect("present").value, 2);
    assert_eq!(table.select(&"alpha".to_string()).expect("present").value, 1);
    assert_eq!(table.len(), 1);
}

worktable!(
    name: Congeed,
    vec: true,
    columns: {
        id: u64 primary_key using congee,
        value: u64,
    },
);

worktable!(
    name: Wtid,
    vec: true,
    columns: {
        id: u64 primary_key using worktables_index,
        value: u64,
        code: u64,
    },
    indexes: {
        code_idx: code unique,
    },
);

/// The two backends without a multimap still index a table, and still delete.
///
/// Congee is here because it was refused outright for a while: `worktable!`
/// demands an explicit `persist` before accepting it, and this macro inherited
/// the rule without inheriting the reason. There is no persistence here for
/// the author to declare, so there was never a question to answer.
///
/// The delete goes through the middle for the same reason as the arctic test:
/// it is the reinsert-every-position path, which neither of these backends can
/// do in place.
#[test]
fn the_backends_without_a_multimap_still_work() {
    let mut congee = CongeedWorkTable::new();
    for id in 1..=5u64 {
        congee.insert(CongeedRow { id, value: id * 10 }).expect("fresh");
    }
    assert!(congee.insert(CongeedRow { id: 3, value: 99 }).is_err(), "duplicate key");
    assert_eq!(congee.delete(&3).expect("present").value, 30);
    for id in [1u64, 2, 4, 5] {
        assert_eq!(congee.select(&id).unwrap_or_else(|| panic!("{id} gone")).value, id * 10);
    }
    assert!(congee.select(&3).is_none());
    assert_eq!(
        congee.select_all().map(|row| row.id).collect::<Vec<_>>(),
        vec![1, 2, 4, 5]
    );

    let mut wti = WtidWorkTable::new();
    for id in 1..=5u64 {
        wti.insert(WtidRow {
            id,
            value: id * 10,
            code: id + 100,
        })
        .expect("fresh");
    }
    // The unique secondary refuses independently of the primary key.
    assert!(
        wti.insert(WtidRow {
            id: 6,
            value: 60,
            code: 103
        })
        .is_err(),
        "duplicate code should be refused even though the id is fresh"
    );
    // ...and refusing it must not have left the fresh id behind.
    assert!(wti.select(&6).is_none(), "a rejected insert half-landed");
    assert_eq!(wti.len(), 5);

    let rejected = wti
        .upsert(WtidRow {
            id: 7,
            value: 70,
            code: 103,
        })
        .expect_err("upsert of a fresh id must return a unique-secondary rejection");
    assert_eq!(rejected.id, 7);
    assert!(
        wti.select(&7).is_none(),
        "a rejected upsert dropped or inserted the row"
    );
    assert_eq!(wti.len(), 5);

    let rejected = wti
        .upsert(WtidRow {
            id: 1,
            value: 999,
            code: 104,
        })
        .expect_err("replacement must return a unique-secondary rejection");
    assert_eq!(rejected.id, 1);
    assert_eq!(wti.select(&1).expect("original row remains").code, 101);
    assert_eq!(wti.select_by_code(&104).expect("owner remains").id, 4);

    assert_eq!(wti.select_by_code(&103).expect("present").id, 3);
    assert_eq!(wti.delete(&3).expect("present").value, 30);
    assert!(wti.select_by_code(&103).is_none(), "the secondary kept a deleted row");
    assert_eq!(wti.select_by_code(&104).expect("present").value, 40);
}

worktable!(
    name: Saved,
    vec: true,
    columns: {
        id: u64 primary_key,
        label: String,
        tag: u64,
    },
    indexes: {
        tag_idx: tag,
    },
);

/// Rows out as pages and back, with the indexes rebuilt rather than stored.
///
/// The indexes are positions into the row vector, so they are cheaper to
/// rebuild on load than to write, validate and keep consistent with the rows.
/// This checks the rebuild rather than only the rows: a `load` that restored
/// `select_all` and left `select` empty would look correct to any assertion
/// that only walked the rows.
#[test]
fn a_table_survives_a_round_trip_through_pages() {
    let mut table = SavedWorkTable::new();
    for id in 0..200u64 {
        table
            .insert(SavedRow {
                id,
                label: format!("row-{id}"),
                tag: id % 8,
            })
            .expect("fresh");
    }
    table.delete(&7).expect("present");

    let bytes = table.unload().expect("rows fit a page");
    assert_eq!(bytes.len() % 16384, 0, "whole pages only");

    let loaded = SavedWorkTable::load(&bytes).expect("its own bytes");
    assert_eq!(loaded.len(), 199);
    assert_eq!(loaded.select_all().count(), 199);
    assert!(loaded.select(&7).is_none(), "the deleted row came back");

    // Every key still finds its own row through the rebuilt primary index.
    for id in (0..200u64).filter(|id| *id != 7) {
        let row = loaded.select(&id).unwrap_or_else(|| panic!("{id} missing after load"));
        assert_eq!(row.label, format!("row-{id}"));
    }
    // And the secondary index was rebuilt too, minus the deleted row.
    assert_eq!(
        loaded.select_by_tag(&7).len(),
        24,
        "tag 7 held 25 rows before the delete"
    );
    assert_eq!(loaded.select_by_tag(&0).len(), 25);

    // Insertion order survives, which is what makes `select_all` meaningful.
    let ids: Vec<u64> = loaded.select_all().map(|row| row.id).collect();
    let expected: Vec<u64> = (0..200u64).filter(|id| *id != 7).collect();
    assert_eq!(ids, expected);
}

/// An empty table still writes a page, and loads back empty.
///
/// A zero byte file is indistinguishable from a missing one, so a load has to
/// be able to tell "no rows" from "nothing landed".
#[test]
fn an_empty_table_round_trips_as_one_page() {
    let bytes = SavedWorkTable::new().unload().expect("nothing to overflow");
    assert_eq!(bytes.len(), 16384, "one page, not zero bytes");
    assert!(SavedWorkTable::load(&bytes).expect("its own bytes").is_empty());
}

/// A flipped bit inside a row is caught, which is the whole reason for the CRC.
///
/// rkyv validates that an archive is structurally sound. It cannot tell that a
/// `u64` holds a different number than the one written, because the altered
/// archive is still perfectly well formed. Only the checksum sees it.
#[test]
fn a_flipped_bit_is_refused_rather_than_read() {
    let mut table = SavedWorkTable::new();
    table
        .insert(SavedRow {
            id: 1,
            label: "one".into(),
            tag: 0,
        })
        .expect("fresh");
    let mut bytes = table.unload().expect("fits");

    // Into the body, which the header's last `u32` gives the length of. A
    // fixed offset is not good enough: one small row archives to well under a
    // hundred bytes, so byte 64 landed in the page's zero padding, outside
    // what the checksum covers, and the file loaded cleanly.
    let body = u32::from_le_bytes(bytes[24..28].try_into().expect("four bytes")) as usize;
    assert!(body > 0, "a one-row page has a body");
    bytes[28 + body / 2] ^= 0b0000_0001;

    match SavedWorkTable::load(&bytes) {
        Err(LoadError::Corrupt { page, .. }) => assert_eq!(page, 0),
        other => panic!("a corrupted page loaded or failed some other way: {other:?}"),
    }
}

/// A truncated file is refused before any page is read.
#[test]
fn a_partial_page_is_refused() {
    let mut table = SavedWorkTable::new();
    table
        .insert(SavedRow {
            id: 1,
            label: "one".into(),
            tag: 0,
        })
        .expect("fresh");
    let bytes = table.unload().expect("fits");

    match SavedWorkTable::load(&bytes[..bytes.len() - 1]) {
        Err(LoadError::NotWholePages { found }) => assert_eq!(found, bytes.len() - 1),
        other => panic!("a torn file loaded: {other:?}"),
    }
    match SavedWorkTable::load(&[]) {
        Err(LoadError::NotWholePages { found }) => assert_eq!(found, 0),
        other => panic!("an empty file loaded: {other:?}"),
    }
}

worktable!(
    name: Other,
    vec: true,
    columns: {
        id: u64 primary_key,
        label: String,
        tag: u64,
    },
);

/// Another row type's file is refused, not reinterpreted.
///
/// `OtherRow` has the same fields in the same order as `SavedRow`, so its
/// archive deserializes without complaint. Nothing but the fingerprint stands
/// between a caller and a table full of another table's rows.
#[test]
fn another_row_types_pages_are_refused() {
    let mut other = OtherWorkTable::new();
    other
        .insert(OtherRow {
            id: 1,
            label: "one".into(),
            tag: 0,
        })
        .expect("fresh");
    let bytes = other.unload().expect("fits");

    match SavedWorkTable::load(&bytes) {
        Err(LoadError::ForeignRows { found, expected }) => assert_ne!(found, expected),
        other => panic!("another row type's file loaded: {other:?}"),
    }
}

/// Rows spanning many pages come back in order.
///
/// One page holds 16 KiB, so this is several of them, and the page-boundary
/// arithmetic is what the test is for: a row dropped at a boundary, or a page
/// whose rows are appended twice, shows up as a length or an order mismatch.
#[test]
fn rows_across_many_pages_come_back_in_order() {
    let mut table = SavedWorkTable::new();
    for id in 0..5_000u64 {
        table
            .insert(SavedRow {
                id,
                label: format!("a fairly long label for row {id}"),
                tag: id % 8,
            })
            .expect("fresh");
    }
    let bytes = table.unload().expect("no single row is oversized");
    assert!(
        bytes.len() / 16384 > 1,
        "this needs to span pages to be testing anything"
    );

    let loaded = SavedWorkTable::load(&bytes).expect("its own bytes");
    assert_eq!(loaded.len(), 5_000);
    let ids: Vec<u64> = loaded.select_all().map(|row| row.id).collect();
    assert_eq!(ids, (0..5_000u64).collect::<Vec<_>>());
    assert_eq!(
        loaded.select(&4_999).expect("last row").label,
        "a fairly long label for row 4999"
    );
}

/// `update` edits in place and repairs every index the edit moved the row
/// under.
///
/// The reason it takes a closure rather than handing out `&mut Row`: a caller
/// with `&mut Row` can change an indexed column, and the index then points at
/// a key the row no longer has. That is silent, and the row is unfindable by
/// either key. The closure lets the table compare before against after.
#[test]
fn update_edits_in_place_and_repairs_the_indexes() {
    let mut table = PointWorkTable::new();
    for id in 0..4u64 {
        table
            .insert(PointRow {
                id,
                value: id * 10,
                tag: id % 2,
            })
            .expect("fresh");
    }

    // An unindexed column: nothing to repair, and nothing should move.
    assert!(table.update(&2, |row| row.value = 999));
    assert_eq!(table.select(&2).expect("present").value, 999);
    assert_eq!(table.select_by_tag(&0).len(), 2);

    // An indexed column: the row has to leave one posting list and join another.
    assert!(table.update(&2, |row| row.tag = 1));
    let evens: Vec<u64> = table.select_by_tag(&0).iter().map(|row| row.id).collect();
    assert_eq!(evens, vec![0], "row 2 stayed in its old posting list");
    let odds: Vec<u64> = table.select_by_tag(&1).iter().map(|row| row.id).collect();
    assert_eq!(odds, vec![1, 2, 3], "row 2 never joined its new one");

    // The primary key itself: findable under the new key, gone from the old.
    assert!(table.update(&2, |row| row.id = 42));
    assert!(table.select(&2).is_none(), "the old key still resolves");
    assert_eq!(table.select(&42).expect("present").value, 999);
    assert_eq!(table.len(), 4, "a re-key is not an insert");

    // A key that does not exist changes nothing.
    assert!(!table.update(&1000, |row| row.value = 1));
}

/// A re-key onto an occupied key is refused, and refused without damage.
#[test]
#[should_panic(expected = "primary key another row already holds")]
fn update_refuses_to_collide_two_rows_onto_one_key() {
    let mut table = PointWorkTable::new();
    table
        .insert(PointRow {
            id: 1,
            value: 10,
            tag: 0,
        })
        .expect("fresh");
    table
        .insert(PointRow {
            id: 2,
            value: 20,
            tag: 0,
        })
        .expect("fresh");
    table.update(&1, |row| row.id = 2);
}

/// Sizing the row vector up front, and handing the rows back out.
#[test]
fn capacity_and_into_rows() {
    let mut table = PointWorkTable::with_capacity(64);
    assert!(table.capacity() >= 64);
    table.reserve(256);
    assert!(table.capacity() >= 256);

    for id in 0..3u64 {
        table.insert(PointRow { id, value: id, tag: 0 }).expect("fresh");
    }
    assert_eq!(table.iter().count(), 3);
    assert_eq!(table.iter().map(|row| row.id).collect::<Vec<_>>(), vec![0, 1, 2]);

    let rows = table.into_rows();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2].id, 2);
}

// The WTI leaf width, set at the call site.
//
// It is a call-site parameter and not grammar because the right width depends
// on the workload rather than the schema: measured at a million shuffled keys,
// 256 is 1.56x faster than the 1,024 default on insert and 3.7% slower on
// lookup, so the same declaration wants different widths in a write-heavy
// process and a read-heavy one. A declaration can only say one thing.
worktable!(
    name: Tuned,
    vec: true,
    columns: {
        id: u64 primary_key using worktables_index,
        value: u64,
    }
);

// No WTI index anywhere, so no knob should be generated for it.
worktable!(
    name: Untuned,
    vec: true,
    columns: {
        id: u64 primary_key,
        value: u64,
    }
);

#[test]
fn the_node_size_is_a_call_site_parameter() {
    let mut table = TunedWorkTable::with_node_size(256);
    for id in 0..1_000u64 {
        table.insert(TunedRow { id, value: id * 2 }).expect("fresh key");
    }
    assert_eq!(table.len(), 1_000);
    assert_eq!(table.select(&500).expect("present").value, 1_000);
}

#[test]
fn a_narrow_node_size_caps_nothing() {
    // The width is the leaf size a node splits at, not a limit on rows. A tree
    // built with a width of 2 must hold thousands of rows by adding nodes,
    // exactly as it does at the default. This is the "what happens when it
    // needs to grow" question, answered: it grows.
    let mut table = TunedWorkTable::with_node_size(2);
    for id in 0..5_000u64 {
        table.insert(TunedRow { id, value: id }).expect("fresh key");
    }
    assert_eq!(table.len(), 5_000);
    for id in (0..5_000u64).step_by(97) {
        assert_eq!(table.select(&id).expect("present").value, id, "row {id} went missing");
    }

    // And the same table at an absurdly wide leaf holds exactly the same rows.
    let mut wide = TunedWorkTable::with_node_size(1 << 20);
    for id in 0..5_000u64 {
        wide.insert(TunedRow { id, value: id }).expect("fresh key");
    }
    assert_eq!(wide.len(), 5_000);
    assert_eq!(wide.select(&4_999).expect("present").value, 4_999);
}

#[test]
fn both_knobs_compose() {
    let table = TunedWorkTable::with_capacity_and_node_size(4_096, 256);
    assert!(table.capacity() >= 4_096, "the row vector was sized");
    assert_eq!(table.len(), 0);
}

#[test]
fn a_table_with_no_wti_index_gets_no_node_size_knob() {
    // Asserted by compiling: `UntunedWorkTable::with_node_size` does not exist,
    // because arctic has no node-size concept and a constructor that accepted
    // one would be a silent no-op. The table still works.
    let mut table = UntunedWorkTable::with_capacity(16);
    table.insert(UntunedRow { id: 1, value: 2 }).expect("fresh key");
    assert_eq!(table.select(&1).expect("present").value, 2);
}

// ---------------------------------------------------------------------------
// Ghosts, and the compaction that reclaims them.

/// A delete costs a bit and a slot, and moves nothing.
///
/// This is the whole claim, so it is asserted on structure rather than on
/// behaviour: `slots` does not fall, `len` does, and the surviving rows keep
/// the positions they had. A `delete` that quietly went back to closing the
/// hole would still pass every lookup assertion in this file, because closing
/// the hole correctly is what the old implementation did.
#[test]
fn a_delete_leaves_a_ghost_and_nothing_moves() {
    let mut table = PointWorkTable::new();
    for id in 0..6u64 {
        table
            .insert(PointRow {
                id,
                value: id * 10,
                tag: id % 2,
            })
            .expect("fresh");
    }
    assert_eq!(table.slots(), 6);
    assert_eq!(table.ghost_count(), 0);

    table.delete(&2).expect("present");

    assert_eq!(table.len(), 5, "one fewer live row");
    assert_eq!(table.slots(), 6, "the slot was kept, not closed");
    assert_eq!(table.ghost_count(), 1);
    assert!(!table.is_empty());

    // Deleting every row leaves six ghosts and an empty table.
    for id in [0u64, 1, 3, 4, 5] {
        table.delete(&id).expect("present");
    }
    assert!(table.is_empty());
    assert_eq!(table.len(), 0);
    assert_eq!(table.slots(), 6);
    assert_eq!(table.ghost_count(), 6);
    assert_eq!(table.select_all().count(), 0);

    // And an insert after that appends rather than reusing a ghost, which is
    // what keeps `select_all` in insertion order.
    table
        .insert(PointRow {
            id: 42,
            value: 420,
            tag: 0,
        })
        .expect("fresh");
    assert_eq!(table.slots(), 7);
    assert_eq!(table.select(&42).expect("present").value, 420);
}

/// Compaction closes every hole and leaves every index pointing at the row it
/// named before.
///
/// Run on each backend, because renumbering is the one operation whose
/// implementation genuinely differs between them: a `BTreeMap` rewrites values
/// in place, an ART cannot and has to reinsert, and the non-unique arm moves
/// pairs. Deleting from the middle of a straddling posting list is what makes
/// an off-by-one visible, for the same reason the delete test does it.
#[test]
fn compaction_reclaims_the_ghosts_and_repairs_every_index() {
    macro_rules! check {
        ($table:ty, $row:ident) => {{
            let mut table = <$table>::new();
            for id in 0..8u64 {
                table
                    .insert($row {
                        id,
                        value: id * 10,
                        tag: id % 2,
                    })
                    .expect("fresh");
            }
            for id in [1u64, 2, 5] {
                table.delete(&id).expect("present");
            }
            assert_eq!(table.ghost_count(), 3);

            assert_eq!(table.compact(), 3, "three slots were reclaimed");
            assert_eq!(table.ghost_count(), 0);
            assert_eq!(table.slots(), 5);
            assert_eq!(table.len(), 5);
            assert_eq!(table.compact(), 0, "a second pass has nothing to do");

            // Every survivor answers to its own key, with its own value. A
            // renumbering that was off by one would hand back a neighbour.
            for id in [0u64, 3, 4, 6, 7] {
                let row = table.select(&id).unwrap_or_else(|| panic!("{id} lost by compaction"));
                assert_eq!(row.value, id * 10, "{id} came back as another row");
            }
            assert!(table.select(&2).is_none());

            // Insertion order survived.
            let ids: Vec<u64> = table.select_all().map(|row| row.id).collect();
            assert_eq!(ids, vec![0, 3, 4, 6, 7]);

            // The non-unique index straddled all three holes.
            let even: Vec<u64> = table.select_by_tag(&0).iter().map(|row| row.id).collect();
            assert_eq!(even, vec![0, 4, 6], "tag 0 lost a row or kept a dead one");
            let odd: Vec<u64> = table.select_by_tag(&1).iter().map(|row| row.id).collect();
            assert_eq!(odd, vec![3, 7]);

            // And the table still takes writes at the new positions.
            table
                .insert($row {
                    id: 9,
                    value: 90,
                    tag: 1,
                })
                .expect("fresh");
            assert_eq!(table.select(&9).expect("present").value, 90);
            assert_eq!(table.select(&0).expect("present").value, 0);
            let odd: Vec<u64> = table.select_by_tag(&1).iter().map(|row| row.id).collect();
            assert_eq!(odd, vec![3, 7, 9]);
        }};
    }

    check!(PointWorkTable, PointRow);
    check!(OrderedWorkTable, OrderedRow);
}

/// The backends with no multimap compact too, including a unique secondary.
#[test]
fn compaction_repairs_the_multimapless_backends() {
    let mut congee = CongeedWorkTable::new();
    for id in 1..=6u64 {
        congee.insert(CongeedRow { id, value: id * 10 }).expect("fresh");
    }
    congee.delete(&2).expect("present");
    congee.delete(&3).expect("present");
    assert_eq!(congee.compact(), 2);
    for id in [1u64, 4, 5, 6] {
        assert_eq!(congee.select(&id).unwrap_or_else(|| panic!("{id} gone")).value, id * 10);
    }
    assert_eq!(congee.slots(), 4);

    let mut wti = WtidWorkTable::new();
    for id in 1..=6u64 {
        wti.insert(WtidRow {
            id,
            value: id * 10,
            code: id + 100,
        })
        .expect("fresh");
    }
    wti.delete(&2).expect("present");
    wti.delete(&5).expect("present");
    assert_eq!(wti.compact(), 2);
    for id in [1u64, 3, 4, 6] {
        assert_eq!(wti.select(&id).unwrap_or_else(|| panic!("{id} gone")).value, id * 10);
        // The unique secondary was renumbered alongside the primary.
        assert_eq!(
            wti.select_by_code(&(id + 100))
                .unwrap_or_else(|| panic!("{id} code gone"))
                .id,
            id,
            "the code index points at the wrong row after compaction"
        );
    }
    assert!(wti.select_by_code(&102).is_none(), "a deleted row kept its code entry");
}

/// Compaction keeps the leaf width the call site asked for.
///
/// Rebuilding the indexes from `Default::default()` would be the obvious way
/// to renumber and would silently throw away `with_node_size`, which is the
/// kind of failure nothing else here would catch: the table would still be
/// correct and only slower. Asserted by continuing to work at a width of 2,
/// where a reset to the 1,024 default changes the tree's shape entirely.
#[test]
fn compaction_keeps_the_node_size_the_caller_asked_for() {
    let mut table = TunedWorkTable::with_node_size(2);
    for id in 0..64u64 {
        table.insert(TunedRow { id, value: id }).expect("fresh");
    }
    for id in (0..64u64).step_by(2) {
        table.delete(&id).expect("present");
    }
    assert_eq!(table.compact(), 32);
    assert_eq!(table.slots(), 32);
    for id in (1..64u64).step_by(2) {
        assert_eq!(table.select(&id).unwrap_or_else(|| panic!("{id} gone")).value, id);
    }
}

/// Ghosts are slots, and `shrink_to_fit` is the only thing that hands them
/// back to the allocator.
#[test]
fn compaction_keeps_capacity_and_shrinking_gives_it_back() {
    let mut table = PointWorkTable::with_capacity(256);
    for id in 0..128u64 {
        table.insert(PointRow { id, value: id, tag: 0 }).expect("fresh");
    }
    for id in 0..120u64 {
        table.delete(&id).expect("present");
    }
    table.compact();
    assert!(table.capacity() >= 256, "compaction kept the capacity for reuse");
    table.shrink_to_fit();
    assert!(table.capacity() < 256, "shrinking did not give it back");
    assert_eq!(table.len(), 8);
}

/// A ghost is not written out, so a reload does not resurrect it.
#[test]
fn unload_does_not_write_a_ghost() {
    let mut table = SavedWorkTable::new();
    for id in 0..40u64 {
        table
            .insert(SavedRow {
                id,
                label: format!("row-{id}"),
                tag: id % 4,
            })
            .expect("fresh");
    }
    for id in [3u64, 11, 29] {
        table.delete(&id).expect("present");
    }
    assert_eq!(table.ghost_count(), 3, "unload is being asked to skip real ghosts");

    let loaded = SavedWorkTable::load(&table.unload().expect("rows fit")).expect("its own bytes");
    assert_eq!(loaded.len(), 37);
    assert_eq!(loaded.slots(), 37, "the ghosts were written as rows");
    for id in [3u64, 11, 29] {
        assert!(loaded.select(&id).is_none(), "{id} came back from the dead");
    }
}

// ---------------------------------------------------------------------------
// Ranges, which the index was always able to answer.

/// The primary-key range walks in key order, in both directions, on every
/// bound shape.
///
/// Keys are inserted out of order on purpose: an implementation that walked
/// the row vector instead of the index would return insertion order and pass
/// any test whose rows went in sorted.
#[test]
fn a_range_walks_the_keys_in_order() {
    let mut table = PointWorkTable::new();
    for id in [5u64, 1, 9, 3, 7, 2, 8, 4, 6] {
        table
            .insert(PointRow {
                id,
                value: id * 10,
                tag: id % 2,
            })
            .expect("fresh");
    }

    let ids = |rows: Vec<&PointRow>| rows.into_iter().map(|row| row.id).collect::<Vec<_>>();

    assert_eq!(ids(table.range(3..7).collect()), vec![3, 4, 5, 6]);
    assert_eq!(ids(table.range(3..=7).collect()), vec![3, 4, 5, 6, 7]);
    assert_eq!(ids(table.range(..3).collect()), vec![1, 2]);
    assert_eq!(ids(table.range(7..).collect()), vec![7, 8, 9]);
    assert_eq!(ids(table.range(..).collect()), vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    assert_eq!(ids(table.range(100..200).collect()), Vec::<u64>::new());

    // The rows are the right rows, not just the right keys.
    for row in table.range(..) {
        assert_eq!(row.value, row.id * 10);
    }

    // Backwards, which is what makes this a `DoubleEndedIterator` rather than
    // an iterator that happens to arrive sorted.
    assert_eq!(ids(table.range(..).rev().collect()), vec![9, 8, 7, 6, 5, 4, 3, 2, 1]);
    assert_eq!(ids(table.range(3..7).rev().collect()), vec![6, 5, 4, 3]);
}

/// A deleted row leaves no index entry, so a range never has to look at a
/// ghost.
///
/// This is what lets `range` call `row_at` and expect a row: if a delete left
/// its entry behind, the range would walk into an empty slot and panic, which
/// is a far better failure than silently returning a stale row and is still a
/// failure. Asserted so the invariant is checked rather than assumed.
#[test]
fn a_range_skips_a_deleted_row() {
    let mut table = PointWorkTable::new();
    for id in 0..10u64 {
        table
            .insert(PointRow {
                id,
                value: id * 10,
                tag: id % 2,
            })
            .expect("fresh");
    }
    for id in [4u64, 5, 6] {
        table.delete(&id).expect("present");
    }

    let ids: Vec<u64> = table.range(2..9).map(|row| row.id).collect();
    assert_eq!(ids, vec![2, 3, 7, 8], "a range walked into a ghost");
    assert_eq!(table.range(..).count(), 7);

    // And compaction does not change the answer, only where the rows live.
    table.compact();
    let ids: Vec<u64> = table.range(2..9).map(|row| row.id).collect();
    assert_eq!(ids, vec![2, 3, 7, 8]);
}

/// Every backend answers a range, because every backend the `using` clause can
/// name is an ordered tree.
///
/// Congee is the one worth naming: it is an adaptive radix tree with a native
/// range scan, and it was the backend most likely to have been given a range
/// that silently returned everything.
#[test]
fn every_backend_answers_a_range() {
    let mut ordered = OrderedWorkTable::new();
    let mut congee = CongeedWorkTable::new();
    let mut wti = WtidWorkTable::new();
    for id in [7u64, 2, 9, 4, 1, 6, 3, 8, 5] {
        ordered
            .insert(OrderedRow {
                id,
                value: id * 10,
                tag: id % 2,
            })
            .expect("fresh");
        congee.insert(CongeedRow { id, value: id * 10 }).expect("fresh");
        wti.insert(WtidRow {
            id,
            value: id * 10,
            code: id + 100,
        })
        .expect("fresh");
    }

    assert_eq!(
        ordered.range(3..7).map(|row| row.id).collect::<Vec<_>>(),
        vec![3, 4, 5, 6]
    );
    assert_eq!(
        congee.range(3..7).map(|row| row.id).collect::<Vec<_>>(),
        vec![3, 4, 5, 6]
    );
    assert_eq!(wti.range(3..7).map(|row| row.id).collect::<Vec<_>>(), vec![3, 4, 5, 6]);

    // A unique secondary index is an ordered tree too, and ranges on its own
    // column rather than on the primary key.
    assert_eq!(
        wti.range_by_code(&103..&106).map(|row| row.id).collect::<Vec<_>>(),
        vec![3, 4, 5],
        "the secondary range answered on the wrong column"
    );
}

/// A `String` key ranges lexicographically, which is the index's order and not
/// the vector's.
#[test]
fn a_string_key_ranges_lexicographically() {
    let mut table = NamedWorkTable::new();
    for key in ["delta", "alpha", "charlie", "bravo", "echo"] {
        table
            .insert(NamedRow {
                key: key.to_string(),
                value: key.len() as u64,
            })
            .expect("fresh");
    }
    let keys: Vec<&str> = table.range(..).map(|row| row.key.as_str()).collect();
    assert_eq!(keys, vec!["alpha", "bravo", "charlie", "delta", "echo"]);

    let keys: Vec<&str> = table
        .range("bravo".to_string().."delta".to_string())
        .map(|row| row.key.as_str())
        .collect();
    assert_eq!(keys, vec!["bravo", "charlie"]);
}

// ---------------------------------------------------------------------------
// `using fxhash`: point operations only, and the range methods are absent.

worktable!(
    name: Hashed,
    vec: true,
    columns: {
        id: u64 primary_key using fxhash,
        value: u64,
        tag: u64,
    },
    indexes: {
        tag_idx: tag using fxhash,
        code_idx: value unique using fxhash,
    },
);

/// A hash-backed table is a table: everything but ordering still works.
///
/// Deliberately exercises the whole surface rather than a lookup, because the
/// hash arm reaches a different branch in every one of `insert`, `upsert`,
/// `update`, `delete` and `compact` — it uses inherent map methods where the
/// ARTs use the `UniqueIndex` trait, and its entry type is `HashMapEntry`
/// rather than `BTreeMapEntry`.
#[test]
fn a_hash_backed_table_does_everything_but_order() {
    let mut table = HashedWorkTable::new();
    for id in 0..8u64 {
        table
            .insert(HashedRow {
                id,
                value: id * 10,
                tag: id % 2,
            })
            .expect("fresh");
    }

    // Duplicate primary key, refused in one traversal through the entry API.
    assert!(
        table
            .insert(HashedRow {
                id: 3,
                value: 999,
                tag: 0
            })
            .is_err(),
        "duplicate primary key"
    );
    // Duplicate unique secondary, refused independently of the primary key.
    assert!(
        table
            .insert(HashedRow {
                id: 99,
                value: 30,
                tag: 0
            })
            .is_err(),
        "duplicate unique secondary"
    );

    assert_eq!(table.select(&3).expect("present").value, 30);
    assert_eq!(table.select_by_value(&40).expect("present").id, 4);
    let even: Vec<u64> = table.select_by_tag(&0).iter().map(|row| row.id).collect();
    assert_eq!(even, vec![0, 2, 4, 6]);

    // Update, including a key move, which repairs three maps.
    assert!(table.update(&5, |row| {
        row.value = 555;
        row.tag = 0;
    }));
    assert_eq!(table.select(&5).expect("present").value, 555);
    assert_eq!(table.select_by_value(&555).expect("present").id, 5);
    assert!(table.select_by_value(&50).is_none(), "the old value kept its entry");

    // Upsert replaces in place.
    table
        .upsert(HashedRow {
            id: 5,
            value: 5_555,
            tag: 1,
        })
        .unwrap();
    assert_eq!(table.select(&5).expect("present").value, 5_555);
    assert_eq!(table.len(), 8, "upsert did not grow the table");

    // Delete ghosts, and compaction renumbers every hash map.
    assert_eq!(table.delete(&2).expect("present").value, 20);
    assert_eq!(table.delete(&6).expect("present").value, 60);
    assert_eq!(table.ghost_count(), 2);
    assert_eq!(table.compact(), 2);
    assert_eq!(table.slots(), 6);
    for id in [0u64, 1, 3, 4, 5, 7] {
        assert_eq!(
            table.select(&id).unwrap_or_else(|| panic!("{id} lost")).id,
            id,
            "compaction pointed the primary index at the wrong row"
        );
    }
    assert_eq!(
        table.select_by_value(&70).expect("present").id,
        7,
        "compaction pointed the unique secondary at the wrong row"
    );
    let even: Vec<u64> = table.select_by_tag(&0).iter().map(|row| row.id).collect();
    assert_eq!(even, vec![0, 4], "compaction lost or misplaced a posting list entry");
}

/// Ranges are not emitted for a hash backend, and that is checked by compiling.
///
/// `HashedWorkTable::range` and `range_by_value` do not exist. There is nothing
/// to call here, which is the assertion: a method that existed and panicked, or
/// returned insertion order and called it key order, is the failure mode this
/// design avoids. The ordered tables next to this one have both methods and are
/// tested for them.
#[test]
fn a_hash_backed_table_has_no_range() {
    let mut table = HashedWorkTable::new();
    table
        .insert(HashedRow {
            id: 1,
            value: 1,
            tag: 1,
        })
        .expect("fresh");
    // Still walkable in insertion order, which needs no index at all.
    assert_eq!(table.select_all().count(), 1);
    assert_eq!(table.iter().count(), 1);
}

worktable!(
    name: HashedSaved,
    vec: true,
    columns: {
        id: u64 primary_key using fxhash,
        label: String,
        code: u64,
    },
    indexes: {
        code_idx: code unique using fxhash,
        tag_idx: label using fxhash,
    },
);

/// A hash-backed table round-trips through pages, indexes and all.
///
/// This is the question `persist: true` makes people ask about `fxhash` and
/// answers wrongly. A **paged** table cannot take a hash index because a
/// persisted index's on-disk form *is* sorted pages, rebuilt with
/// `attach_nodes`. A `vec: true` table stores **no index at all**: `unload`
/// writes rows and `load` rebuilds every index by re-inserting them. So the
/// thing that blocks the paged table does not exist here, and manual
/// flush-and-hydrate works on a hash backend exactly as it does on a tree.
///
/// Asserted on the indexes rather than on the rows, because rows surviving is
/// the easy half: a `load` that restored `select_all` and left `select` empty
/// would pass any assertion that only walked the table.
#[test]
fn a_hash_backed_table_round_trips_through_pages() {
    let mut table = HashedSavedWorkTable::with_capacity(500);
    for id in 0..500u64 {
        table
            .insert(HashedSavedRow {
                id,
                label: format!("row-{}", id % 8),
                code: id + 10_000,
            })
            .expect("fresh");
    }
    // Ghosts too, so the round trip is exercised on a table that has deleted.
    for id in [3u64, 111, 499] {
        table.delete(&id).expect("present");
    }
    assert_eq!(table.ghost_count(), 3);

    let bytes = table.unload().expect("rows fit a page");
    assert_eq!(bytes.len() % 16_384, 0, "whole pages only");

    let loaded = HashedSavedWorkTable::load(&bytes).expect("its own bytes");
    assert_eq!(loaded.len(), 497);
    assert_eq!(loaded.slots(), 497, "a ghost was written as a row");

    // The primary hash index was rebuilt.
    for id in (0..500u64).filter(|id| ![3, 111, 499].contains(id)) {
        let row = loaded.select(&id).unwrap_or_else(|| panic!("{id} missing after load"));
        assert_eq!(row.code, id + 10_000, "{id} came back as another row");
    }
    for id in [3u64, 111, 499] {
        assert!(loaded.select(&id).is_none(), "{id} came back from the dead");
    }

    // And both secondary hash indexes, unique and non-unique.
    assert_eq!(
        loaded.select_by_code(&10_042).expect("present").id,
        42,
        "the unique secondary was not rebuilt"
    );
    assert!(loaded.select_by_code(&10_003).is_none(), "a deleted row kept its code");
    // The deleted ids are 3, 111 and 499, which are 3, 7 and 3 mod 8, so
    // lost two and  lost one.  lost none, which is why it is not
    // the tag asserted on: a posting list that never changed proves nothing
    // about whether a delete reached the index.
    let intact = loaded.select_by_label(&"row-0".to_string());
    assert_eq!(intact.len(), 63, "row-0 lost a row it never had deleted");
    let lost_two = loaded.select_by_label(&"row-3".to_string());
    assert_eq!(lost_two.len(), 61, "row-3 held 63 and lost ids 3 and 499");
    let lost_one = loaded.select_by_label(&"row-7".to_string());
    assert_eq!(lost_one.len(), 61, "row-7 held 62 (ids 7..=495 step 8) and lost id 111");

    // Insertion order survives, which is what makes `select_all` meaningful.
    let ids: Vec<u64> = loaded.select_all().map(|row| row.id).collect();
    let expected: Vec<u64> = (0..500u64).filter(|id| ![3, 111, 499].contains(id)).collect();
    assert_eq!(ids, expected);

    // The reloaded table still takes writes, which proves the rebuilt index is
    // a working map and not just a populated one.
    let mut loaded = loaded;
    assert!(
        loaded
            .insert(HashedSavedRow {
                id: 42,
                label: "dup".into(),
                code: 1
            })
            .is_err()
    );
    loaded
        .insert(HashedSavedRow {
            id: 3,
            label: "back".into(),
            code: 3,
        })
        .expect("the deleted key is free again");
    assert_eq!(loaded.select(&3).expect("present").code, 3);
}

/// Two unloads concatenated load as one table, so a flush can append.
///
/// `unload` writes the whole table, so writing it to a file is a clobber and
/// there is no incremental form of it. But a page is self-describing — its own
/// header, CRC, row directory and row-type fingerprint — and `from_pages` walks
/// `chunks_exact(PAGE_SIZE)` in order without any global header or trailer. So
/// the bytes of two unloads concatenated are a valid file, and a caller that
/// keeps new rows in a second table can append rather than rewrite.
///
/// What append cannot express is a delete. `load` applies rows in order and
/// keeps the first of any duplicate key, so a later segment cannot remove or
/// replace an earlier row. Both halves are asserted, because the second is the
/// one that decides whether this is a usable strategy or a trap.
#[test]
fn two_unloads_concatenate_into_one_table() {
    let mut first = HashedSavedWorkTable::with_capacity(64);
    for id in 0..64u64 {
        first
            .insert(HashedSavedRow {
                id,
                label: "a".into(),
                code: id,
            })
            .expect("fresh");
    }
    let mut second = HashedSavedWorkTable::with_capacity(64);
    for id in 64..128u64 {
        second
            .insert(HashedSavedRow {
                id,
                label: "b".into(),
                code: id,
            })
            .expect("fresh");
    }

    let mut appended = first.unload().expect("rows fit");
    appended.extend_from_slice(&second.unload().expect("rows fit"));
    assert_eq!(appended.len() % 16_384, 0, "still whole pages");

    let loaded = HashedSavedWorkTable::load(&appended).expect("a concatenation of its own pages");
    assert_eq!(loaded.len(), 128, "the append lost a segment");
    for id in 0..128u64 {
        assert_eq!(
            loaded.select(&id).unwrap_or_else(|| panic!("{id} missing")).code,
            id,
            "{id} came back as another row"
        );
    }
    // Order is segment order, which is what makes this an append rather than a
    // merge: the second file's rows follow the first file's.
    let ids: Vec<u64> = loaded.select_all().map(|row| row.id).collect();
    assert_eq!(ids, (0..128u64).collect::<Vec<_>>());

    // And the limit. A later segment cannot replace an earlier row: `load`
    // keeps the first of a duplicate key, so an append-only log of these needs
    // a full rewrite to express an update or a delete.
    let mut shadow = HashedSavedWorkTable::with_capacity(1);
    shadow
        .insert(HashedSavedRow {
            id: 7,
            label: "newer".into(),
            code: 9_999,
        })
        .expect("fresh");
    let mut with_shadow = first.unload().expect("rows fit");
    with_shadow.extend_from_slice(&shadow.unload().expect("rows fit"));
    let reloaded = HashedSavedWorkTable::load(&with_shadow).expect("valid pages");
    assert_eq!(
        reloaded.select(&7).expect("present").code,
        7,
        "a later segment overwrote an earlier row; it must not, and if this ever \
         changes then append becomes a way to silently lose the newer value \
         instead of the older one"
    );
    assert_eq!(reloaded.len(), 64, "the duplicate was counted as a new row");
}

#[test]
fn append_callsite_writes_only_new_live_rows_and_rebuilds_indexes() {
    let mut table = HashedSavedWorkTable::new();
    for id in 0..100u64 {
        table
            .insert(HashedSavedRow {
                id,
                label: "first".into(),
                code: id,
            })
            .unwrap();
    }
    table.delete(&3).unwrap();
    let first = table.len();
    let mut bytes = table.unload().unwrap();
    let before = bytes.clone();
    for id in 100..200u64 {
        table
            .insert(HashedSavedRow {
                id,
                label: "second".into(),
                code: id,
            })
            .unwrap();
    }
    let pages_before = u32::try_from(bytes.len() / worktable::vec_hydrate::PAGE_SIZE).unwrap();
    let appended = table.unload_appending(first, pages_before).unwrap();
    assert_eq!(HashedSavedWorkTable::load(&appended).unwrap().len(), 100);
    bytes.extend_from_slice(&appended);
    assert_eq!(&bytes[..before.len()], &before);
    let loaded = HashedSavedWorkTable::load(&bytes).unwrap();
    assert_eq!(loaded.len(), 199);
    assert!(loaded.select(&3).is_none());
    assert_eq!(loaded.select_by_label(&"second".into()).len(), 100);
    assert_eq!(loaded.select_by_code(&199).unwrap().id, 199);
}

worktable!(
    name: Level,
    vec: true,
    columns: {
        exchange: u64 primary_key,
        bid: f64,
        ask: f64,
        size: u64,
    },
);

worktable!(
    name: Labelled,
    vec: true,
    columns: {
        id: u64 primary_key,
        label: String,
    },
);

/// A fixed-width row's page layout does not move when a value changes.
///
/// This is the property the whole in-place persistence question turns on. An
/// orderbook updates a price: an `f64` becomes another `f64`, never an `f80`.
/// If the archive of a page is the same length before and after, then row K
/// lives at the same byte offset forever, a page can be written back in place,
/// and none of the append, segment, last-wins or tombstone machinery is needed
/// for that shape.
///
/// `rows_per_page` searches rather than computing, so stability is a property
/// of the data and not an obvious one: it is asserted here rather than assumed
/// anywhere that relies on it.
///
/// The `String` table is the control. Without it this test would pass on a
/// format that simply never varies, and prove nothing about the format's
/// ability to vary.
#[test]
fn a_fixed_width_rows_pages_are_byte_stable_under_update() {
    let mut table = LevelWorkTable::with_capacity(5_000);
    for exchange in 0..5_000u64 {
        table
            .insert(LevelRow {
                exchange,
                bid: 100.0,
                ask: 101.0,
                size: 10,
            })
            .expect("fresh key");
    }
    let before = table.unload().expect("rows fit");

    // Every value changes, and every value stays the same width.
    for exchange in 0..5_000u64 {
        assert!(table.update(&exchange, |row| {
            row.bid = (exchange % 997) as f64 + 0.5;
            row.ask = f64::MAX;
            row.size = u64::MAX;
        }));
    }
    let after = table.unload().expect("rows fit");

    assert_eq!(
        before.len(),
        after.len(),
        "a fixed-width row changed its page count by changing its values"
    );
    // Stronger than equal length: every page boundary is where it was, so the
    // row at a given offset is still the row that was there.
    assert_eq!(before.len() % 16_384, 0);
    let pages = before.len() / 16_384;
    for page in 0..pages {
        let at = page * 16_384;
        // The header carries the page index and the body length. Both must be
        // unchanged; only the body bytes may differ.
        assert_eq!(
            before[at..at + 32],
            after[at..at + 32],
            "page {page}'s header moved, so a row's home is not stable"
        );
    }
    assert_ne!(before, after, "the values did not actually change");

    // The control: a variable-width row is not stable, which is what makes the
    // assertion above a real property rather than a description of the format.
    let mut labelled = LabelledWorkTable::with_capacity(5_000);
    for id in 0..5_000u64 {
        labelled
            .insert(LabelledRow {
                id,
                label: "x".to_string(),
            })
            .expect("fresh key");
    }
    let short = labelled.unload().expect("rows fit");
    for id in 0..5_000u64 {
        assert!(labelled.update(&id, |row| {
            row.label = "x".repeat(64);
        }));
    }
    let long = labelled.unload().expect("rows fit");
    assert!(
        long.len() > short.len(),
        "a String column grew by 63 bytes a row and the file did not grow, so \
         this test is not measuring what it claims to"
    );
}

worktable!(
    name: Mixed,
    vec: true,
    columns: {
        id: u64 primary_key using fxhash,
        venue: u64,
        seq: u64,
    },
    indexes: {
        venue_idx: venue using arctic,
        seq_idx: seq unique using arctic,
    },
);

/// A backend is chosen per index, so a hash primary key does not cost the
/// secondaries their ordering.
///
/// The primary key here cannot answer a range and the secondaries can, which is
/// the whole point: `range` is absent from this table and `range_by_seq` is
/// present on it. If capability were decided per table rather than per index,
/// one of those two facts would be wrong.
#[test]
fn a_hash_primary_key_leaves_an_arctic_secondary_ordered() {
    let mut table = MixedWorkTable::with_capacity(64);
    // Inserted out of key order so an implementation that walked the row vector
    // instead of the index would return insertion order and be caught.
    for id in [5u64, 1, 9, 3, 7, 2, 8, 4, 6] {
        table
            .insert(MixedRow {
                id,
                venue: id % 3,
                seq: 100 + id,
            })
            .expect("fresh key");
    }

    // The hash primary key does point lookups, and refuses a duplicate.
    assert_eq!(table.select(&7).expect("present").seq, 107);
    assert!(
        table
            .insert(MixedRow {
                id: 7,
                venue: 0,
                seq: 999
            })
            .is_err()
    );

    // The unique arctic secondary ranges, in its own column's order.
    let ranged: Vec<u64> = table.range_by_seq(&103..&107).map(|row| row.id).collect();
    assert_eq!(ranged, vec![3, 4, 5, 6], "the arctic secondary lost its order");
    let backwards: Vec<u64> = table.range_by_seq(&103..&107).rev().map(|row| row.id).collect();
    assert_eq!(backwards, vec![6, 5, 4, 3]);

    // The non-unique arctic secondary still groups.
    let venue0: Vec<u64> = table.select_by_venue(&0).iter().map(|row| row.id).collect();
    assert_eq!(
        venue0,
        vec![9, 3, 6],
        "insertion order within a venue: 9, 3 and 6 are the ids with venue 0"
    );

    // And all of it survives a delete and a compaction, which renumber the
    // hash map and both ARTs by different code paths.
    table.delete(&5).expect("present");
    assert_eq!(table.compact(), 1);
    let ranged: Vec<u64> = table.range_by_seq(&103..&108).map(|row| row.id).collect();
    assert_eq!(ranged, vec![3, 4, 6, 7], "compaction broke the secondary range");
    assert_eq!(table.select(&7).expect("present").seq, 107);
}

worktable!(
    name: Ticket,
    vec: true,
    columns: {
        id: u64 primary_key using fxhash,
        owner: u64,
        state: u8,
        amount: u64,
    },
    indexes: {
        owner_idx: owner using fxhash,
        amount_idx: amount unique using arctic,
    },
    queries: {
        update: {
            StateById(state) by id,
            StateByOwner(state) by owner,
            AmountById(amount) by id,
        },
        delete: {
            ById() by id,
            ByOwner() by owner,
        },
        in_place: {
            Status(state) by id,
        },
    },
);

/// Declared queries work on a `vec: true` table, and a hash index serves them.
///
/// Every declared query is an *equality* lookup, which is the shape a hash
/// index is best at. That is why these are emitted whatever the `using` clause
/// says, while `range` and `range_by_` are not: the restriction is ordering,
/// not the query machinery.
///
/// This table deliberately mixes backends — a `fxhash` primary key, a `fxhash`
/// non-unique secondary, and an `arctic` unique secondary — so a query keyed by
/// each kind runs against a different implementation.
#[test]
fn declared_queries_run_on_a_vec_table() {
    let mut table = TicketWorkTable::new();
    for id in 0..6u64 {
        table
            .insert(TicketRow {
                id,
                owner: id % 2,
                state: 0,
                amount: 100 + id,
            })
            .expect("fresh");
    }

    // Keyed by the hash primary key: one row.
    assert_eq!(table.update_state_by_id(StateByIdQuery { state: 7 }, &3), 1);
    assert_eq!(table.select(&3).expect("present").state, 7);
    assert_eq!(table.select(&2).expect("present").state, 0, "only one row moved");

    // Keyed by a non-unique hash secondary: every row it names.
    assert_eq!(
        table.update_state_by_owner(StateByOwnerQuery { state: 5 }, &1),
        3,
        "owner 1 holds ids 1, 3 and 5"
    );
    for id in [1u64, 3, 5] {
        let row = table.select(&id).expect("present");
        assert_eq!(row.state, 5);
        assert_eq!(row.amount, 100 + id);
    }
    assert_eq!(table.select(&0).expect("present").amount, 100, "owner 0 untouched");

    assert_eq!(table.update_amount_by_id(AmountByIdQuery { amount: 999 }, &1), 1);
    // The unique arctic secondary was repaired without stealing another row's key.
    assert!(
        table.select_by_amount(&101).is_none(),
        "the old amount kept its entry after an update moved the row"
    );
    assert_eq!(
        table.select_by_amount(&999).expect("present").owner,
        1,
        "the updated row owns amount 999"
    );

    // in_place edits one column through a closure.
    assert_eq!(table.update_status_in_place(|s| *s = 42, &0), 1);
    assert_eq!(table.select(&0).expect("present").state, 42);

    // Deletes, by the key and by a non-unique secondary.
    assert_eq!(table.delete_by_id(&0), 1);
    assert!(table.select(&0).is_none());
    assert_eq!(table.delete_by_owner(&1), 3, "owner 1 had three rows left");
    assert_eq!(table.len(), 2, "ids 2 and 4 survive");
    assert_eq!(table.ghost_count(), 4, "deletes ghost rather than close the hole");
}

#[test]
fn vec_unique_collisions_and_failed_edits_leave_rows_and_indexes_unchanged() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let mut table = HashedSavedWorkTable::new();
    for id in 1..=3u64 {
        table
            .insert(HashedSavedRow {
                id,
                code: id * 10,
                label: format!("row-{id}"),
            })
            .unwrap();
    }
    let before = table.unload().unwrap();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            table.update(&1, |row| {
                row.id = 2;
                row.code = 99;
                row.label = "changed".into();
            });
        }))
        .is_err()
    );
    assert_eq!(table.unload().unwrap(), before);
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            table.update(&1, |row| {
                row.code = 20;
                row.label = "changed".into();
            });
        }))
        .is_err()
    );
    assert_eq!(table.unload().unwrap(), before);
    let rejected = table
        .upsert(HashedSavedRow {
            id: 1,
            code: 20,
            label: "changed".into(),
        })
        .expect_err("unique secondary collision");
    assert_eq!(rejected.id, 1);
    assert_eq!(rejected.code, 20);
    assert_eq!(table.unload().unwrap(), before);
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            table.update(&1, |row| {
                row.id = 4;
                row.code = 40;
                panic!("caller failed");
            });
        }))
        .is_err()
    );
    assert_eq!(table.unload().unwrap(), before);
    for id in 1..=3u64 {
        assert_eq!(table.select_by_code(&(id * 10)).unwrap().id, id);
        assert_eq!(table.select_by_label(&format!("row-{id}")).len(), 1);
    }
    table.delete(&1).unwrap();
    table.compact();
    assert_eq!(table.select_by_code(&20).unwrap().id, 2);
    assert_eq!(table.select_by_code(&30).unwrap().id, 3);
}

#[test]
fn vec_declared_query_cannot_steal_another_rows_unique_key() {
    let mut table = TicketWorkTable::new();
    for id in 0..2u64 {
        table
            .insert(TicketRow {
                id,
                owner: id,
                state: 0,
                amount: 100 + id,
            })
            .unwrap();
    }
    let before = table.unload().unwrap();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            table.update_amount_by_id(AmountByIdQuery { amount: 101 }, &0);
        }))
        .is_err()
    );
    assert_eq!(table.unload().unwrap(), before);
    assert_eq!(table.select_by_amount(&100).unwrap().id, 0);
    assert_eq!(table.select_by_amount(&101).unwrap().id, 1);
}

#[test]
fn vec_secondary_key_churn_does_not_retain_empty_posting_lists() {
    let mut table = HashedSavedWorkTable::new();
    table
        .insert(HashedSavedRow {
            id: 1,
            code: 1,
            label: "initial".into(),
        })
        .unwrap();
    table
        .insert(HashedSavedRow {
            id: 2,
            code: 2,
            label: "stable".into(),
        })
        .unwrap();
    for revision in 0..100 {
        assert!(table.update(&1, |row| row.label = format!("edited-{revision}")));
        table
            .upsert(HashedSavedRow {
                id: 1,
                code: 1,
                label: format!("replaced-{revision}"),
            })
            .unwrap();
        assert_eq!(table.label_map.len(), 2, "secondary index must contain only live keys");
    }
    table.delete(&1).unwrap();
    assert_eq!(table.label_map.len(), 1);
    assert_eq!(table.select_by_label(&"stable".into())[0].id, 2);
    table.delete(&2).unwrap();
    assert!(table.label_map.is_empty());
}
