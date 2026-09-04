//! Serving-process requirements for swapping one generation for the next.
//!
//! # Where this comes from
//!
//! `moe-pgo` serves model clusters and swaps them without restarting: attach
//! generation N+1, let generation N's readers drain, release N's memory, keep
//! answering throughout. It cannot do that with WorkTable, so it does it with a
//! private crate that hand-writes `data_bucket` pages, a `GeneralHeader` at a
//! time, with its own manifest and its own lease registry. That crate is 1,548
//! lines reimplementing a layer this one already owns, and the reason it exists
//! is the gap below rather than a preference.
//!
//! # What is missing, in order of how much it matters
//!
//! 1. **A lease-aware unload.** `close(self)` takes ownership. A server holds a
//!    generation behind an `Arc`, so the unload receiver owns the retiring Arc,
//!    waits for readers to drain under a caller-supplied barrier and timeout,
//!    then unwraps and drops the allocation while reporting its measured size:
//!
//!    ```ignore
//!    async fn unload_gracefully<F, Fut>(self: Arc<Self>, timeout: Duration, quiesce: F)
//!        -> eyre::Result<UnloadReport>;
//!    ```
//!
//!    `wait_for_ops` is not this. It drains queued *writes*; a swap has to
//!    drain *readers*, and there is no lease count to observe.
//!
//! 2. **`MemStat` on a generated persisted table.** `heap_size()` supplies the
//!    pre-drop measurement returned by unload.
//!
//! Two things that would help and are not blocking: a registry keyed by content
//! digest so generations are addressed by id rather than by path, and digest
//! verification on attach so a file whose bytes do not match its expected id is
//! refused.
//!
//! # What already works, so nobody spends a day on it
//!
//! Attaching two handles to one on-disk generation at the same time. That was
//! the first thing tested here on the assumption it was the gap, and it passed.
//! `attaching_a_second_handle_already_works` keeps that result so it stays true.

use std::sync::Arc;
use std::time::Duration;

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: GenerationSwap,
    version: 1,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        blob: String,
    },
);

const DIR: &str = "tests/data/generation_swap/persisted";

/// A generation big enough that releasing it is worth reporting.
const ROWS: u64 = 2_000;

/// What a caller would pass as the drain barrier.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

async fn attach(dir: &str) -> GenerationSwapWorkTable {
    let engine = GenerationSwapPersistenceEngine::new(DiskConfig::new_with_table_name(
        dir,
        GenerationSwapWorkTable::name_snake_case(),
        GenerationSwapWorkTable::version(),
    ))
    .await
    .expect("an engine");
    GenerationSwapWorkTable::load(engine).await.expect("a generation")
}

async fn fill(table: &GenerationSwapWorkTable) {
    for n in 0..ROWS {
        table
            .insert(GenerationSwapRow {
                id: table.get_next_pk().into(),
                blob: format!("row {n} with enough text to occupy real pages"),
            })
            .await
            .expect("a row");
    }
    table.wait_for_ops().await.expect("the queue drains");
}

#[tokio::test]
async fn a_retired_generation_releases_its_memory() {
    let _ = std::fs::remove_dir_all(DIR);
    std::fs::create_dir_all(DIR).expect("a directory");

    let generation = Arc::new(attach(DIR).await);
    fill(&generation).await;

    // A reader in flight, exactly as during a swap.
    let held = generation.heap_size();
    assert!(held > 0, "a filled generation reports memory");

    let reader = Arc::clone(&generation);
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let reader_task = tokio::spawn(async move {
        assert_eq!(
            reader.select_all().execute().expect("a read").len(),
            ROWS as usize,
            "the outgoing generation is still answering"
        );
        let _ = release_rx.await;
        drop(reader);
    });

    let report = generation
        .unload_gracefully(DRAIN_TIMEOUT, move || async move {
            let _ = release_tx.send(());
            reader_task.await.expect("reader drains");
        })
        .await
        .expect("the generation retires");
    assert_eq!(report.released_bytes, held);
    assert!(report.released_bytes > 0, "the memory came back");

    let _ = std::fs::remove_dir_all(DIR);
}

#[tokio::test]
async fn a_generation_can_report_what_it_holds() {
    let _ = std::fs::remove_dir_all(DIR);
    std::fs::create_dir_all(DIR).expect("a directory");

    let generation = attach(DIR).await;
    fill(&generation).await;

    let held = generation.heap_size();
    assert!(held > 0, "a filled generation holds memory: {held}");
    generation.close().await.expect("generation closes");
    let _ = std::fs::remove_dir_all(DIR);
}

/// **Already works.** Kept so nobody spends a day building it: two handles on
/// one on-disk generation coexist, and the outgoing one keeps answering.
#[tokio::test]
async fn attaching_a_second_handle_already_works() {
    let dir = "tests/data/generation_swap/second_handle";
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).expect("a directory");

    let live = attach(dir).await;
    live.insert(GenerationSwapRow {
        id: live.get_next_pk().into(),
        blob: "N".to_owned(),
    })
    .await
    .expect("a row");
    live.wait_for_ops().await.expect("the queue drains");

    let next = attach(dir).await;
    assert_eq!(next.select_all().execute().expect("a read").len(), 1);
    assert_eq!(
        live.select_all().execute().expect("a read").len(),
        1,
        "the outgoing generation still answers while the next is attached"
    );

    let _ = std::fs::remove_dir_all(dir);
}
