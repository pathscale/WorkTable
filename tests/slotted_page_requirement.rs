//! V3 data pages locate their live rows independently of indexes.
//!
//! The release deliberately cuts over from v2. Most deployments recreate their
//! stores; this runtime refuses old bytes rather than carrying a v2 reader.
//! See docs/on-disk-v3-cutover.md for the release contract.

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

fn scan_rows(dir: &str) -> std::collections::BTreeMap<u64, String> {
    let bytes = std::fs::read(data_file(dir)).unwrap();
    assert_eq!(bytes.len() % PAGE_SIZE, 0);
    let mut rows = std::collections::BTreeMap::new();
    for (id, page) in bytes.as_chunks::<PAGE_SIZE>().0.iter().enumerate().skip(1) {
        assert_eq!(u32::from_le_bytes(page[..4].try_into().unwrap()), 3);
        assert_eq!(u32::from_le_bytes(page[8..12].try_into().unwrap()), id as u32);
        let length = u32::from_le_bytes(page[24..28].try_into().unwrap());
        let decoded = data_bucket::DataPage::<INNER_PAGE_SIZE>::decode(&page[GENERAL_HEADER_SIZE..], length).unwrap();
        for slot in &decoded.rows {
            // An aligned copy lets rkyv validate each independently located
            // archive without depending on its file offset's alignment.
            let mut archive = rkyv::util::AlignedVec::<16>::new();
            archive.extend_from_slice(&decoded.data[slot.offset as usize..][..slot.length as usize]);
            let wrapped =
                rkyv::from_bytes::<<SlottedRowRow as StorableRow>::WrappedRow, rkyv::rancor::Error>(&archive).unwrap();
            let row = wrapped.get_inner();
            assert!(rows.insert(row.id, row.blob).is_none(), "duplicate live primary key");
        }
    }
    rows
}

#[tokio::test]
async fn directory_survives_deletes_moves_reuse_and_reopen_without_index_access() {
    let dir = "tests/data/slotted_page/churn";
    let table = filled(dir).await;
    let mut expected = std::collections::BTreeMap::new();
    for id in 0..ROWS {
        if id % 3 == 0 {
            table.delete(id).await.unwrap();
        } else {
            let blob = "grown".repeat(10 + (id % 71) as usize);
            table.upsert(SlottedRowRow { id, blob: blob.clone() }).await.unwrap();
            expected.insert(id, blob);
        }
    }
    table.close().await.unwrap();
    assert_eq!(scan_rows(dir), expected);
    let engine = SlottedRowPersistenceEngine::new(DiskConfig::new_with_table_name(
        dir,
        SlottedRowWorkTable::name_snake_case(),
        SlottedRowWorkTable::version(),
    ))
    .await
    .unwrap();
    let table = SlottedRowWorkTable::load(engine).await.unwrap();
    for (id, blob) in &expected {
        assert_eq!(table.select(*id).unwrap().blob, *blob);
    }
    table.close().await.unwrap();
    assert_eq!(scan_rows(dir), expected);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn actual_v2_store_is_refused_without_modifying_it() {
    let dir = "tests/data/slotted_page/v2_refused";
    let path = data_file(dir);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let original = include_bytes!("fixtures/page-format/v2.wt.data");
    std::fs::write(&path, original).unwrap();
    let result = SlottedRowPersistenceEngine::new(DiskConfig::new_with_table_name(
        dir,
        SlottedRowWorkTable::name_snake_case(),
        SlottedRowWorkTable::version(),
    ))
    .await;
    let error = match result {
        Ok(_) => panic!("v2 unexpectedly opened"),
        Err(error) => error,
    };
    assert!(format!("{error:#}").contains("page format v2"), "{error:#}");
    assert_eq!(std::fs::read(&path).unwrap(), original);
    std::fs::remove_dir_all(dir).unwrap();
}

/// A data page should say where its rows are, without an index.
///
/// Read the bytes directly so an index cannot hide a missing directory.
#[tokio::test]
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

    // The count is the final u32; the preceding word is the CRC.
    let mut described = 0usize;
    let (pages, _) = bytes.as_chunks::<PAGE_SIZE>();
    for page in pages.iter().skip(1) {
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
    assert_eq!(scan_rows(dir).len(), ROWS as usize);
    let _ = std::fs::remove_dir_all(dir);
}

/// A store reopens without being deleted first.
///
/// The new writer and reader must agree after a clean close.
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
