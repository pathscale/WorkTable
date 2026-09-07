//! What a data page has to say about itself, for beta.20.
//!
//! # Where this comes from
//!
//! beta.19 changed the on-disk format and every existing `.wt.data` had to be
//! thrown away and rebuilt, because nothing could read the old shape. That is a
//! regeneration event, and it happened on 6 September 2026 across every store on
//! this machine.
//!
//! It does not have to happen again. The reason it did is that a data page
//! cannot be read without the index that points into it:
//!
//! ```ignore
//! pub struct DataPage<const DATA_LENGTH: usize> {
//!     pub length: u32,
//!     pub data: [u8; DATA_LENGTH],
//! }
//! ```
//!
//! Rows are bump allocated into `data` and `length` is a high water mark. There
//! are no delimiters, so nothing can tell where one row ends and the next
//! begins. `empty_links_list` in the `SpaceInfoPage` records freed ranges and is
//! explicitly lossy: `bound_empty_links_list` truncates it when it outgrows the
//! info page and logs "space leak, not corruption".
//!
//! **The schema is already there and this is not asking for it again.**
//! `SpaceInfoPage` carries `row_schema`, `primary_key_fields` and
//! `secondary_index_types`, and `ensure_schema` refuses a mismatch by name. A
//! reader already knows how to decode a row. What it cannot do is find one.
//!
//! # The two things, in order of how much they matter
//!
//! 1. **A row directory in the data page**, the usual slotted layout: an
//!    `(offset, length)` per row growing down from the end of the page, with a
//!    count. Then a page describes itself, a reader needs no index, and the CRC
//!    on that page validates the directory together with the rows it points at.
//!
//! 2. **A reader for the format beta.19 writes**, so beta.20 is an upgrade
//!    rather than another regeneration. One already exists and is switched off:
//!    `src/page/iterators.rs` in DataBucket, where `LinksIterator` walks index
//!    pages for links and `DataIterator` follows them, decoding through
//!    `row_schema`. It is 226 lines, commented out at `src/page/mod.rs:4`, and
//!    enabling it produces nine errors that are bit rot rather than design:
//!    `crate::IndexData` and `super::SpaceInfo` were renamed, and one call site
//!    predates the API going async.
//!
//! # How the two fit together
//!
//! `DATA_VERSION` is 2 today and lives in every page's `GeneralHeader`, so it is
//! per page rather than per file.
//!
//! - **beta.20 ships both.** It writes 3 and reads 2 and 3.
//! - **beta.21 ships neither of the old ones.** The v2 path is deleted.
//!
//! So v2 is a one way ramp rather than dual support: a store is loaded through
//! it once, written back as v3, and never read that way again. It does not need
//! to be fast and it never needs append, which is most of why it is cheap.
//!
//! Two things to settle rather than discover:
//!
//! - Once a page has a directory and an index, both know where a row is and they
//!   can disagree. One has to be authoritative. The directory is the better
//!   candidate: it is local to the page and validated by the same CRC, where the
//!   index is a separate structure with a different topology per backend. Under
//!   `validate-reads` a load can compare the two and name a disagreement instead
//!   of silently preferring one.
//! - Whether one file may hold both v2 and v3 pages. Per page versioning allows
//!   it, which makes migration an append rather than a rewrite, but then no
//!   reader may assume uniformity.
//!
//! # What is missing here, and is the next piece of work
//!
//! A committed `.wt.data` written by beta.19, so the ramp can be tested against
//! a real old file rather than against one this build just wrote. Until that
//! fixture exists, `a_store_reopens_without_being_rebuilt` below only proves the
//! current version reopens, which is the weaker half.

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: SlottedRow,
    version: 1,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        blob: String,
    },
);

/// Enough rows to fill more than one page, so a directory would be doing real
/// work rather than describing a single row.
const ROWS: u64 = 4_000;

async fn filled(dir: &str) -> SlottedRowWorkTable {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).expect("a directory");

    let engine = SlottedRowPersistenceEngine::new(DiskConfig::new_with_table_name(
        dir,
        SlottedRowWorkTable::name_snake_case(),
        SlottedRowWorkTable::version(),
    ))
    .await
    .expect("an engine");
    let table = SlottedRowWorkTable::load(engine).await.expect("a table");

    for n in 0..ROWS {
        table
            .insert(SlottedRowRow {
                id: table.get_next_pk().into(),
                blob: format!("row {n}, long enough to make the page boundaries interesting"),
            })
            .await
            .expect("a row");
    }
    table.wait_for_ops().await.expect("the queue drains");
    table
}

fn data_file(dir: &str) -> std::path::PathBuf {
    std::path::Path::new(dir)
        .join(SlottedRowWorkTable::name_snake_case())
        .join(".wt.data")
}

/// A data page should say where its rows are, without an index.
///
/// **This is the beta.20 requirement.** The check goes straight at the bytes on
/// purpose. Reading the page through the engine would prove only that the index
/// still works, and the index is exactly what a self describing page is supposed
/// to make unnecessary.
///
/// Written against bytes rather than against an API that does not exist yet, so
/// this file compiles today and fails on the missing behaviour rather than on a
/// missing symbol.
#[tokio::test]
#[ignore = "beta.20: a data page carries no row directory"]
async fn a_data_page_says_where_its_rows_are() {
    let dir = "tests/data/slotted_page/self_describing";
    let table = filled(dir).await;
    table.close().await.expect("the table closes");

    let bytes = std::fs::read(data_file(dir)).expect("the file");
    assert!(
        bytes.len() > PAGE_SIZE,
        "the fixture has to span pages: {} bytes",
        bytes.len()
    );

    // A slotted page keeps its directory at the end: a row count in the last
    // four bytes, then that many (offset, length) pairs growing back up. Any
    // layout would do; what matters is that something in the page delimits the
    // rows. Today the tail is write padding, so this reads zero.
    let mut described = 0usize;
    for page in bytes.chunks_exact(PAGE_SIZE).skip(1) {
        let mut tail = [0u8; 4];
        tail.copy_from_slice(&page[PAGE_SIZE - 4..]);
        described += u32::from_le_bytes(tail) as usize;
    }

    assert_eq!(
        described, ROWS as usize,
        "no page says how many rows it holds, so the {ROWS} rows in this file \
         cannot be found without the index. A row directory in the data page is \
         what makes a page readable on its own, and what makes the next format \
         change an upgrade instead of a regeneration."
    );
    let _ = std::fs::remove_dir_all(dir);
}

/// A store reopens without being deleted first.
///
/// **Not ignored, and passing.** It guards the property at the current version,
/// so a format change that breaks reopening trips here rather than in somebody's
/// deploy. It is the weaker half of the requirement: proving beta.20 can read
/// beta.19 needs a beta.19 file committed as a fixture, which does not exist
/// yet.
#[tokio::test]
async fn a_store_reopens_without_being_rebuilt() {
    let dir = "tests/data/slotted_page/reopen";
    let table = filled(dir).await;
    table.close().await.expect("the table closes");

    let engine = SlottedRowPersistenceEngine::new(DiskConfig::new_with_table_name(
        dir,
        SlottedRowWorkTable::name_snake_case(),
        SlottedRowWorkTable::version(),
    ))
    .await
    .expect("an engine");
    let reopened = SlottedRowWorkTable::load(engine)
        .await
        .expect("a store reopens rather than needing to be rebuilt");

    assert_eq!(
        reopened.select_all().execute().expect("a read").len(),
        ROWS as usize,
        "no rows are lost reopening a store"
    );
    reopened.close().await.expect("the table closes");
    let _ = std::fs::remove_dir_all(dir);
}
