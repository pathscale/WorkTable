use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use data_bucket::{INNER_PAGE_SIZE, Link};
use tokio::fs::OpenOptions;
use worktable::prelude::IndexTableOfContents;

#[tokio::test]
async fn test_index_table_of_contents_read() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/persist_index_table_of_contents.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(1));
    let toc =
        IndexTableOfContents::<u32, { INNER_PAGE_SIZE as u32 }>::parse_from_file(&mut file, 0.into(), next_id_gen)
            .await
            .unwrap();

    assert_eq!(toc.get(&13), Some(1.into()))
}

#[tokio::test]
async fn test_index_table_of_contents_read_from_space() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/test_persist/primary.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(1));
    let toc = IndexTableOfContents::<(u64, Link), { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();
    assert_eq!(
        toc.get(&(
            99,
            Link {
                page_id: 1.into(),
                offset: 2352,
                length: 24
            }
        )),
        Some(2.into())
    )
}

#[tokio::test]
async fn test_index_table_of_contents_read_from_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_create_node.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(2));
    let toc = IndexTableOfContents::<(u32, Link), { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    assert_eq!(
        toc.get(&(
            5,
            Link {
                page_id: 0.into(),
                offset: 0,
                length: 24
            }
        )),
        Some(2.into())
    )
}

#[tokio::test]
async fn test_index_table_of_contents_read_from_space_index_after_insert() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_insert_at.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(2));
    let toc = IndexTableOfContents::<(u32, Link), { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    assert_eq!(
        toc.get(&(
            5,
            Link {
                page_id: 0.into(),
                offset: 0,
                length: 24
            }
        )),
        Some(2.into())
    )
}

#[tokio::test]
async fn test_index_table_of_contents_read_from_space_index_with_updated_node_id() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_insert_at_with_node_id_update.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(2));
    let toc = IndexTableOfContents::<(u32, Link), { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    assert_eq!(
        toc.get(&(
            7,
            Link {
                page_id: 0.into(),
                offset: 24,
                length: 48
            }
        )),
        Some(2.into())
    )
}

#[tokio::test]
async fn test_index_table_of_contents_read_from_space_index_with_remove_at_node_id() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_remove_at_node_id.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(2));
    let toc = IndexTableOfContents::<(u32, Link), { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    assert_eq!(
        toc.get(&(
            3,
            Link {
                page_id: 0.into(),
                offset: 24,
                length: 48
            }
        )),
        Some(2.into())
    );
}

#[tokio::test]
async fn test_index_table_of_contents_read_from_space_index_with_remove_node() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_remove_node.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(2));
    let toc = IndexTableOfContents::<(u32, Link), { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    assert_eq!(
        toc.get(&(
            5,
            Link {
                page_id: 1.into(),
                offset: 0,
                length: 24
            }
        )),
        None
    );
    assert_eq!(
        toc.get(&(
            15,
            Link {
                page_id: 1.into(),
                offset: 0,
                length: 24
            }
        )),
        Some(3.into())
    );
}

#[tokio::test]
async fn test_index_table_of_contents_read_from_space_index_with_create_node_after_remove_node() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_create_node_after_remove.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(2));
    let toc = IndexTableOfContents::<(u32, Link), { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    assert_eq!(
        toc.get(&(
            10,
            Link {
                page_id: 0.into(),
                offset: 0,
                length: 24
            }
        )),
        Some(2.into())
    );
    assert_eq!(
        toc.get(&(
            15,
            Link {
                page_id: 1.into(),
                offset: 0,
                length: 24
            }
        )),
        Some(3.into())
    );
}

#[tokio::test]
async fn test_index_table_of_contents_read_from_space_index_after_split_node() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_split_node.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(2));
    let toc = IndexTableOfContents::<(u32, Link), { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    assert_eq!(
        toc.get(&(
            1000,
            Link {
                page_id: 0.into(),
                offset: 24,
                length: 24
            }
        )),
        Some(3.into())
    );
    assert_eq!(
        toc.get(&(
            457,
            Link {
                page_id: 0.into(),
                offset: 10968,
                length: 24
            }
        )),
        Some(2.into())
    );
}

#[tokio::test]
async fn test_truncated_table_of_contents_is_an_error_not_an_empty_index() {
    let path = std::env::temp_dir().join(format!("worktable-toc-truncated-{}.wt.idx", uuid::Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .await
        .unwrap();

    // A DATA_LENGTH of 20 forces the table of contents to span several pages.
    let mut toc = IndexTableOfContents::<u8, 20>::new(0.into(), Arc::new(AtomicU32::new(1)));
    for key in 0..10 {
        toc.insert(key, u32::from(key).into()).await;
    }
    assert!(toc.pages.len() > 1, "fixture must span TOC pages");
    toc.persist(&mut file).await.unwrap();
    file.sync_all().await.unwrap();

    // The intact file must round-trip.
    let reloaded = IndexTableOfContents::<u8, 20>::parse_from_file(&mut file, 0.into(), Arc::new(AtomicU32::new(1)))
        .await
        .unwrap();
    assert_eq!(reloaded.pages.len(), toc.pages.len());

    // Tear the first table-of-contents page: the file still extends into its
    // slot, so the load must fail loudly instead of yielding an empty index.
    file.set_len(data_bucket::PAGE_SIZE as u64 + 10).await.unwrap();
    let error = IndexTableOfContents::<u8, 20>::parse_from_file(&mut file, 0.into(), Arc::new(AtomicU32::new(1)))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("table of contents page 1 failed to parse"),
        "unexpected error: {error:#}"
    );

    // A file that never grew past page 0 is a real bootstrap and still loads
    // as a fresh empty table of contents.
    file.set_len(0).await.unwrap();
    let bootstrapped =
        IndexTableOfContents::<u8, 20>::parse_from_file(&mut file, 0.into(), Arc::new(AtomicU32::new(1)))
            .await
            .unwrap();
    assert_eq!(bootstrapped.pages.len(), 1);

    drop(file);
    tokio::fs::remove_file(&path).await.unwrap();
}
