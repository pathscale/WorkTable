//! `worktable_vec!` behaves, and costs what a `Vec` costs.
//!
//! The second half is the point. A Vec-backed table that is materially slower
//! than the `Vec` it wraps has no reason to exist: the caller would write the
//! `Vec`. So the comparison is against the thing it replaces, not against
//! `worktable!`.

use std::collections::BTreeMap;
use std::time::Instant;

use worktable::worktable_vec;

worktable_vec!(
    name: Point,
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
    let mut table = PointVecTable::new();

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

/// The generated table must cost what the hand-written pattern costs.
///
/// The baseline is what an application writes when it has no table: a `Vec` of
/// rows and a `BTreeMap` from key to position. Identical data structures, so a
/// gap is overhead the macro added rather than a different algorithm.
///
/// The bound is loose because this is a wall clock on a shared machine. It is
/// here to catch a table that is *categorically* slower, a linear scan where
/// the baseline does a map lookup, not to police a few percent.
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
    let mut table = PointVecTable::new();
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
         It maintains one extra index here, so it is not expected to tie, but a \
         categorical gap means it is doing something the baseline is not."
    );
}

worktable_vec!(
    name: Ordered,
    columns: {
        id: u64 primary_key using indexset,
        value: u64,
        tag: u64,
    },
    indexes: {
        tag_idx: tag using indexset,
    },
);

worktable_vec!(
    name: Named,
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

    check!(PointVecTable, PointRow);
    check!(OrderedVecTable, OrderedRow);
}

/// Arctic takes a `String` key, so the macro does not have to refuse one.
///
/// This is here because the refusal test next to it uses `bool`, and the two
/// together say where the line actually is. A reader who sees only the refusal
/// would reasonably assume every non-integer key is out.
#[test]
fn a_string_keyed_table_works() {
    let mut table = NamedVecTable::new();
    table.insert(NamedRow { key: "beta".to_string(), value: 2 }).expect("fresh");
    table.insert(NamedRow { key: "alpha".to_string(), value: 1 }).expect("fresh");
    assert!(table.insert(NamedRow { key: "alpha".to_string(), value: 9 }).is_err());

    assert_eq!(table.select(&"alpha".to_string()).expect("present").value, 1);
    assert_eq!(table.delete(&"beta".to_string()).expect("present").value, 2);
    assert_eq!(table.select(&"alpha".to_string()).expect("present").value, 1);
    assert_eq!(table.len(), 1);
}
