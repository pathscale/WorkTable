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

    table.insert(PointRow { id: 1, value: 10, tag: 7 }).expect("fresh");
    table.insert(PointRow { id: 2, value: 20, tag: 7 }).expect("fresh");
    assert!(table.insert(PointRow { id: 1, value: 99, tag: 9 }).is_err(), "duplicate key");

    assert_eq!(table.select(&1).expect("present").value, 10);
    assert_eq!(table.len(), 2);
    assert_eq!(table.select_all().len(), 2);

    // A non-unique index returns every row, in insertion order.
    let tagged = table.select_by_tag(&7);
    assert_eq!(tagged.len(), 2);
    assert_eq!(tagged[0].id, 1);

    table.upsert(PointRow { id: 1, value: 11, tag: 7 });
    assert_eq!(table.select(&1).expect("present").value, 11, "upsert replaces");
    assert_eq!(table.len(), 2, "upsert does not grow the table");

    let removed = table.delete(&1).expect("present");
    assert_eq!(removed.value, 11);
    assert_eq!(table.len(), 1);
    assert!(table.select(&1).is_none());
    // The surviving row's position shifted, so its index entry had to shift too.
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
fn it_costs_what_a_vec_costs() {
    const ROWS: u64 = 50_000;

    struct Baseline {
        rows: Vec<(u64, u64, u64)>,
        by_pk: BTreeMap<u64, usize>,
    }

    let started = Instant::now();
    let mut baseline = Baseline { rows: Vec::new(), by_pk: BTreeMap::new() };
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
        table.insert(PointRow { id, value: id * 2, tag: id % 64 }).expect("fresh");
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
    eprintln!(
        "VEC-COST vec={:?} table={:?} ratio={ratio:.2}x",
        vec_time, table_time
    );
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

/// Deleting from the middle has to move every position above the hole, in
/// every index, on whichever backend is holding them.
///
/// The `BTreeMap` arm rewrites its values in place. The Arctic arm cannot, so
/// it reads the affected entries out and reinserts them, and that path is new
/// enough to be the one worth testing. Doing it three rows in, with a
/// non-unique index whose posting list straddles the hole, is what makes an
/// off-by-one visible: a shift that skips the boundary leaves a row reachable
/// by the wrong key rather than by none, which `select_all` alone would not
/// catch.
#[test]
fn deleting_from_the_middle_reindexes_both_backends() {
    macro_rules! check {
        ($table:ty, $row:ident) => {{
            let mut table = <$table>::new();
            for id in 0..6u64 {
                table.insert($row { id, value: id * 10, tag: id % 2 }).expect("fresh");
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
            let ids: Vec<u64> = table.select_all().iter().map(|row| row.id).collect();
            assert_eq!(ids, vec![0, 1, 3, 4, 5]);

            // The non-unique index straddled the hole: tag 0 held 0, 2 and 4.
            let even: Vec<u64> = table.select_by_tag(&0).iter().map(|row| row.id).collect();
            assert_eq!(even, vec![0, 4], "tag 0 kept a deleted row or lost a live one");
            let odd: Vec<u64> = table.select_by_tag(&1).iter().map(|row| row.id).collect();
            assert_eq!(odd, vec![1, 3, 5]);

            // And the table still takes writes afterwards.
            table.insert($row { id: 9, value: 90, tag: 1 }).expect("fresh");
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
    table.insert(NamedRow { key: "beta".to_string(), value: 2 }).expect("fresh");
    table.insert(NamedRow { key: "alpha".to_string(), value: 1 }).expect("fresh");
    assert!(table.insert(NamedRow { key: "alpha".to_string(), value: 9 }).is_err());

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
    assert_eq!(congee.select_all().iter().map(|row| row.id).collect::<Vec<_>>(), vec![1, 2, 4, 5]);

    let mut wti = WtidWorkTable::new();
    for id in 1..=5u64 {
        wti.insert(WtidRow { id, value: id * 10, code: id + 100 }).expect("fresh");
    }
    // The unique secondary refuses independently of the primary key.
    assert!(
        wti.insert(WtidRow { id: 6, value: 60, code: 103 }).is_err(),
        "duplicate code should be refused even though the id is fresh"
    );
    // ...and refusing it must not have left the fresh id behind.
    assert!(wti.select(&6).is_none(), "a rejected insert half-landed");
    assert_eq!(wti.len(), 5);

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
            .insert(SavedRow { id, label: format!("row-{id}"), tag: id % 8 })
            .expect("fresh");
    }
    table.delete(&7).expect("present");

    let bytes = table.unload().expect("rows fit a page");
    assert_eq!(bytes.len() % 16384, 0, "whole pages only");

    let loaded = SavedWorkTable::load(&bytes).expect("its own bytes");
    assert_eq!(loaded.len(), 199);
    assert_eq!(loaded.select_all().len(), 199);
    assert!(loaded.select(&7).is_none(), "the deleted row came back");

    // Every key still finds its own row through the rebuilt primary index.
    for id in (0..200u64).filter(|id| *id != 7) {
        let row = loaded.select(&id).unwrap_or_else(|| panic!("{id} missing after load"));
        assert_eq!(row.label, format!("row-{id}"));
    }
    // And the secondary index was rebuilt too, minus the deleted row.
    assert_eq!(loaded.select_by_tag(&7).len(), 24, "tag 7 held 25 rows before the delete");
    assert_eq!(loaded.select_by_tag(&0).len(), 25);

    // Insertion order survives, which is what makes `select_all` meaningful.
    let ids: Vec<u64> = loaded.select_all().iter().map(|row| row.id).collect();
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
    table.insert(SavedRow { id: 1, label: "one".into(), tag: 0 }).expect("fresh");
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
    table.insert(SavedRow { id: 1, label: "one".into(), tag: 0 }).expect("fresh");
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
    other.insert(OtherRow { id: 1, label: "one".into(), tag: 0 }).expect("fresh");
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
            .insert(SavedRow { id, label: format!("a fairly long label for row {id}"), tag: id % 8 })
            .expect("fresh");
    }
    let bytes = table.unload().expect("no single row is oversized");
    assert!(bytes.len() / 16384 > 1, "this needs to span pages to be testing anything");

    let loaded = SavedWorkTable::load(&bytes).expect("its own bytes");
    assert_eq!(loaded.len(), 5_000);
    let ids: Vec<u64> = loaded.select_all().iter().map(|row| row.id).collect();
    assert_eq!(ids, (0..5_000u64).collect::<Vec<_>>());
    assert_eq!(loaded.select(&4_999).expect("last row").label, "a fairly long label for row 4999");
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
        table.insert(PointRow { id, value: id * 10, tag: id % 2 }).expect("fresh");
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
    table.insert(PointRow { id: 1, value: 10, tag: 0 }).expect("fresh");
    table.insert(PointRow { id: 2, value: 20, tag: 0 }).expect("fresh");
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
