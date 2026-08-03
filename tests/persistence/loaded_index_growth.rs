use std::collections::BTreeSet;
use std::time::Duration;

use tokio::time::timeout;

use crate::remove_dir_if_exists;
use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable::worktable;

/*
 * Regression test: growing a LOADED table's primary index.
 *
 * Tables built up from empty grow their index files fine. Tables loaded from
 * disk are mapped at their on-disk size, and on 2026-08-01 an insert that
 * needed the primary index to grow past that size wrote past the mapping
 * instead of extending it or refusing: SIGBUS mid-write, torn table, every
 * subsequent open of the store dead. It killed two production tables the
 * same day with the same signature — `primary.wt.idx` frozen at exactly the
 * size it was loaded with (65536 in one table, 229376 in the other) while
 * `.wt.data` kept growing.
 *
 * The schema mirrors the table that died: String primary key of uuid shape,
 * a String secondary index with heavy duplication, and a ~1K payload so the
 * data:index ratio stays honest.
 */
worktable!(
    name: LoadedIndexGrowth,
    persist: true,
    columns: {
        id: String primary_key,
        project_id: String,
        body: String,
    },
    indexes: {
        project_idx: project_id,
    },
);

/// Enough rows that the primary index spans several growth steps: the table
/// that died at 65536 bytes of index held 442 rows of this key shape.
const ROWS_BEFORE_RELOAD: u64 = 1_500;
const ROWS_AFTER_RELOAD: u64 = 1_500;

fn key(i: u64) -> String {
    // Same length and shape as the uuid-suffixed ids the dead table held.
    format!("msg-00000000-0000-4000-8000-{i:012}")
}

fn row(i: u64) -> LoadedIndexGrowthRow {
    LoadedIndexGrowthRow {
        id: key(i),
        project_id: format!("proj-{:02}", i % 3),
        body: "x".repeat(1_000),
    }
}

fn primary_idx_size(dir: &str) -> u64 {
    let path = format!("{dir}/{}/primary.wt.idx", LoadedIndexGrowthWorkTable::name_snake_case());
    std::fs::metadata(&path)
        .unwrap_or_else(|error| panic!("no primary index at {path}: {error}"))
        .len()
}

/// Build a store, close it, LOAD it, and append until the primary index must
/// grow. The bug makes the append phase die of SIGBUS the moment the loaded
/// index's capacity is exhausted; fixed, the index file grows exactly as it
/// does for a fresh table and every row stays addressable across one more
/// reload.
#[test]
fn test_primary_index_grows_on_a_loaded_table() {
    let dir = "tests/data/loaded_index_growth/persisted";
    let config = DiskConfig::new_with_table_name(
        dir,
        LoadedIndexGrowthWorkTable::name_snake_case(),
        LoadedIndexGrowthWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists(dir.to_string()).await;

        // Phase 1: build from empty. Growth on this path has always worked;
        // this phase exists to leave a store whose index is well past its
        // initial allocation, so phase 2 cannot fit inside leftover headroom.
        {
            let engine = LoadedIndexGrowthPersistenceEngine::new(config.clone()).await.unwrap();
            let table = LoadedIndexGrowthWorkTable::load(engine).await.unwrap();
            for i in 0..ROWS_BEFORE_RELOAD {
                table.insert(row(i)).unwrap();
            }
            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence stalled building the initial store")
                .expect("persistence engine failed");
        }

        let idx_when_loaded = primary_idx_size(dir);

        // Phase 2: load the store and append the same volume again. The
        // primary index MUST grow past the size it was mapped at; the bug
        // kills the process right here instead.
        {
            let engine = LoadedIndexGrowthPersistenceEngine::new(config.clone()).await.unwrap();
            let table = LoadedIndexGrowthWorkTable::load(engine).await.unwrap();
            for i in ROWS_BEFORE_RELOAD..(ROWS_BEFORE_RELOAD + ROWS_AFTER_RELOAD) {
                table
                    .insert(row(i))
                    .unwrap_or_else(|error| panic!("insert {i} into the loaded table was refused: {error:?}"));
            }
            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence stalled appending to the loaded store")
                .expect("persistence engine failed");
        }

        let idx_after_appends = primary_idx_size(dir);
        assert!(
            idx_after_appends > idx_when_loaded,
            "the primary index never grew while loaded ({idx_when_loaded} -> \
             {idx_after_appends} bytes): the workload no longer exercises \
             growth-on-a-loaded-table, which is the whole regression"
        );

        // Phase 3: reload and hold the table to the exact id set, not a
        // count. A torn-but-openable store is the failure mode this bug
        // ships in production; every row must come back and stay
        // addressable through the secondary index too.
        {
            let engine = LoadedIndexGrowthPersistenceEngine::new(config.clone()).await.unwrap();
            let table = LoadedIndexGrowthWorkTable::load(engine).await.unwrap();

            let expected: BTreeSet<String> = (0..ROWS_BEFORE_RELOAD + ROWS_AFTER_RELOAD).map(key).collect();
            let got: BTreeSet<String> = table
                .select_all()
                .execute()
                .unwrap()
                .into_iter()
                .map(|r| r.id)
                .collect();
            assert_eq!(
                got, expected,
                "rows lost or duplicated across grow-while-loaded and reload"
            );

            for project in 0..3u64 {
                let per_project = table
                    .select_by_project_id(format!("proj-{project:02}"))
                    .execute()
                    .unwrap()
                    .len() as u64;
                assert_eq!(
                    per_project,
                    (ROWS_BEFORE_RELOAD + ROWS_AFTER_RELOAD) / 3,
                    "secondary index lost rows for proj-{project:02}"
                );
            }

            // And the grown, reloaded table must still be writable: the
            // production stores died on exactly this insert.
            table.insert(row(ROWS_BEFORE_RELOAD + ROWS_AFTER_RELOAD)).unwrap();
            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence stalled on the post-reload insert")
                .expect("persistence engine failed");
        }
    })
}
