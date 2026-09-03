//! Regression (review finding F2): vacuum must never lose a surviving row.
//!
//! In the compaction loop, `page_from` is `mark_page_empty`'d unconditionally
//! after its rows are moved, with no `page_from != page_to` guard. If the
//! destination search falls through to `allocate_new_or_pop_free()` and hands
//! back a page that ends up holding moved-in rows (e.g. the source page itself,
//! or a temp page later reclaimed), marking that page empty can drop live rows
//! once the read grace period ends.
//!
//! This test forces heavy cross-page compaction (large rows, half deleted from
//! many pages) while a concurrent reader repeatedly resolves rows, then audits
//! that EVERY surviving row — by primary key AND by secondary index — is still
//! present and correct after vacuum quiesces.

macro_rules! vacuum_no_loss_backend_suite {
    ($module:ident, $using:ident) => {
        mod $module {
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use worktable::prelude::*;
use worktable::vacuum::{VacuumManager, VacuumManagerConfig};
use worktable_codegen::worktable;

worktable!(
    name: VacuumLoss,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement using $using,
        value: i64,
        data: String
    },
    indexes: {
        value_idx: value unique,
        data_idx: data,
    }
);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vacuum_never_loses_surviving_rows() {
    let config = VacuumManagerConfig {
        // Wake on any freed byte. The reactive equivalent of "run vacuum
        // eagerly": there is no interval to turn down any more, because a
        // short one made the fallback timer win every wake and neither the
        // threshold nor the settle did anything.
        wake_threshold_bytes: 1,
        ..Default::default()
    };
    let vacuum_manager = Arc::new(VacuumManager::with_config(config));
    let table = Arc::new(VacuumLossWorkTable::default());

    // Large rows so only a couple fit per page -> deleting half fragments many
    // pages and forces the compaction loop to move rows across pages.
    let mut all: HashMap<u64, VacuumLossRow> = HashMap::new();
    for i in 0..400i64 {
        let row = VacuumLossRow {
            id: table.get_next_pk().into(),
            value: i,
            data: format!("{i:04}-{}", "d".repeat(4_000)),
        };
        table.insert(row.clone()).await.unwrap();
        all.insert(row.id, row);
    }

    // Delete every other row to leave fragmented source pages.
    let mut survivors: HashMap<u64, VacuumLossRow> = HashMap::new();
    let mut ids: Vec<u64> = all.keys().copied().collect();
    ids.sort_unstable();
    for (n, id) in ids.iter().enumerate() {
        if n % 2 == 0 {
            table.delete(*id).await.unwrap();
        } else {
            survivors.insert(*id, all[id].clone());
        }
    }

    let vacuum = table.vacuum();
    vacuum_manager.register(vacuum);
    let handle = vacuum_manager.run_vacuum_task();

    // Concurrent reader active across the whole vacuum: repeatedly resolve
    // survivors so a grace period is live while pages are being moved/retired.
    let reader_table = table.clone();
    let reader_survivors: Vec<u64> = survivors.keys().copied().collect();
    let reader = tokio::spawn(async move {
        for _ in 0..50 {
            for id in &reader_survivors {
                let _ = reader_table.select(*id);
            }
            tokio::task::yield_now().await;
        }
    });

    reader.await.unwrap();
    // Let vacuum run a few more cycles, then stop it and let grace periods drain.
    tokio::time::sleep(Duration::from_millis(200)).await;
    handle.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // FULL AUDIT: every survivor must still be present and correct, by pk and
    // by both secondary indexes. A lost row (None) is the data-loss bug.
    for (id, expected) in &survivors {
        let by_pk = table.select(*id);
        assert_eq!(
            by_pk.as_ref(),
            Some(expected),
            "row {id} lost or corrupted after vacuum (by primary key)"
        );
        let by_value = table.select_by_value(expected.value);
        assert_eq!(
            by_value.as_ref(),
            Some(expected),
            "row {id} unreachable via unique value index after vacuum"
        );
    }

    // Deleted rows must stay gone.
    for id in ids.iter().enumerate().filter(|(n, _)| n % 2 == 0).map(|(_, id)| *id) {
        assert_eq!(table.select(id), None, "deleted row {id} resurrected by vacuum");
    }
}

        }
    };
}

vacuum_no_loss_backend_suite!(wti, worktables_index);
vacuum_no_loss_backend_suite!(congee, congee);
vacuum_no_loss_backend_suite!(arctic, arctic);
