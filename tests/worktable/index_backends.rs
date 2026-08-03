use worktable::prelude::*;
use worktable::worktable;

use crate::remove_dir_if_exists;

worktable! {
    name: MixedBackend,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement using congee,
        wti_key: u64,
        upstream_key: u64,
        congee_key: u64,
        arctic_key: u64,
    },
    indexes: {
        wti_idx: wti_key unique using worktables_index,
        upstream_idx: upstream_key unique using indexset,
        congee_idx: congee_key unique using congee,
        arctic_idx: arctic_key unique using arctic,
    },
}

mod provider_switch_wti {
    use worktable::prelude::*;
    use worktable::worktable;

    worktable! {
        name: ProviderSwitch,
        persist: true,
        columns: {
            id: u64 primary_key autoincrement,
            unique_key: u64,
        },
        indexes: {
            unique_idx: unique_key unique,
        },
    }
}

mod provider_switch_upstream {
    use worktable::prelude::*;
    use worktable::worktable;

    worktable! {
        name: ProviderSwitch,
        persist: true,
        columns: {
            id: u64 primary_key autoincrement using indexset,
            unique_key: u64,
        },
        indexes: {
            unique_idx: unique_key unique using indexset,
        },
    }
}

worktable! {
    name: UpstreamPrimary,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement using indexset,
        value: u64,
    },
}

worktable! {
    name: ArcticPrimary,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement using arctic,
        value: u64,
    },
}

worktable! {
    name: CongeePrimary,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement using congee,
        value: u64,
    },
}

worktable! {
    name: PersistedUpstream,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement using indexset,
        unique_key: u64,
    },
    indexes: {
        unique_idx: unique_key unique using indexset,
    },
}

worktable! {
    name: PersistedArctic,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement using arctic,
        congee_key: u64,
    },
    indexes: {
        congee_idx: congee_key unique using congee,
    },
}

worktable! {
    name: PersistedCongee,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement using congee,
        arctic_key: u64,
    },
    indexes: {
        arctic_idx: arctic_key unique using arctic,
    },
}

#[tokio::test]
async fn all_unique_backends_support_crud_ranges_and_conflict_rollback() {
    let table = MixedBackendWorkTable::default();
    let row = MixedBackendRow {
        id: table.get_next_pk().into(),
        wti_key: 11,
        upstream_key: 12,
        congee_key: 13,
        arctic_key: 14,
    };

    let pk = table.insert(row.clone()).unwrap();
    let second = MixedBackendRow {
        id: table.get_next_pk().into(),
        wti_key: 31,
        upstream_key: 32,
        congee_key: 33,
        arctic_key: 34,
    };
    table.insert(second.clone()).unwrap();

    assert_eq!(table.select(pk), Some(row.clone()));
    assert_eq!(table.select_by_wti_key(11), Some(row.clone()));
    assert_eq!(table.select_by_upstream_key(12), Some(row.clone()));
    assert_eq!(table.select_by_congee_key(13), Some(row.clone()));
    assert_eq!(table.select_by_arctic_key(14), Some(row.clone()));

    for (attempt, duplicate_backend) in ["wti", "indexset", "congee", "arctic"].into_iter().enumerate() {
        let base = 100 + attempt as u64 * 10;
        let candidate = MixedBackendRow {
            id: table.get_next_pk().into(),
            wti_key: if duplicate_backend == "wti" { 11 } else { base + 1 },
            upstream_key: if duplicate_backend == "indexset" { 12 } else { base + 2 },
            congee_key: if duplicate_backend == "congee" { 13 } else { base + 3 },
            arctic_key: if duplicate_backend == "arctic" { 14 } else { base + 4 },
        };
        assert!(table.insert(candidate.clone()).is_err());
        assert!(table.select(candidate.id).is_none());
        if candidate.wti_key != 11 {
            assert!(table.select_by_wti_key(candidate.wti_key).is_none());
        }
        if candidate.upstream_key != 12 {
            assert!(table.select_by_upstream_key(candidate.upstream_key).is_none());
        }
        if candidate.congee_key != 13 {
            assert!(table.select_by_congee_key(candidate.congee_key).is_none());
        }
        if candidate.arctic_key != 14 {
            assert!(table.select_by_arctic_key(candidate.arctic_key).is_none());
        }
    }
    assert_eq!(table.count(), 2);

    let updated = MixedBackendRow {
        id: row.id,
        wti_key: 21,
        upstream_key: 22,
        congee_key: 23,
        arctic_key: 24,
    };
    table.update(updated.clone()).await.unwrap();
    assert_eq!(table.select(pk), Some(updated.clone()));
    assert!(table.select_by_wti_key(11).is_none());
    assert!(table.select_by_upstream_key(12).is_none());
    assert!(table.select_by_congee_key(13).is_none());
    assert!(table.select_by_arctic_key(14).is_none());
    assert_eq!(table.select_by_wti_key(21), Some(updated.clone()));
    assert_eq!(table.select_by_upstream_key(22), Some(updated.clone()));
    assert_eq!(table.select_by_congee_key(23), Some(updated.clone()));
    assert_eq!(table.select_by_arctic_key(24), Some(updated.clone()));

    for rows in [
        table.select_by_wti_key_range(20..=31).execute().unwrap(),
        table.select_by_upstream_key_range(20..=32).execute().unwrap(),
        table.select_by_congee_key_range(20..=33).execute().unwrap(),
        table.select_by_arctic_key_range(20..=34).execute().unwrap(),
    ] {
        assert_eq!(rows.len(), 2);
    }

    table.delete(row.id).await.unwrap();
    assert!(table.select(pk).is_none());
    assert!(table.select_by_wti_key(21).is_none());
    assert!(table.select_by_upstream_key(22).is_none());
    assert!(table.select_by_congee_key(23).is_none());
    assert!(table.select_by_arctic_key(24).is_none());
    assert_eq!(table.count(), 1);
}

#[tokio::test]
async fn alternative_primary_backends_support_point_crud() {
    macro_rules! assert_point_crud {
        ($table:ty, $row:ident) => {{
            let table = <$table>::default();
            let original = $row {
                id: table.get_next_pk().into(),
                value: 1,
            };
            let pk = table.insert(original.clone()).unwrap();
            assert_eq!(table.select(pk.clone()), Some(original.clone()));

            let updated = $row {
                id: original.id,
                value: 2,
            };
            table.update(updated.clone()).await.unwrap();
            assert_eq!(table.select(pk.clone()), Some(updated));

            table.delete(original.id).await.unwrap();
            assert!(table.select(pk).is_none());
            assert_eq!(table.count(), 0);
        }};
    }

    assert_point_crud!(UpstreamPrimaryWorkTable, UpstreamPrimaryRow);
    assert_point_crud!(CongeePrimaryWorkTable, CongeePrimaryRow);
    assert_point_crud!(ArcticPrimaryWorkTable, ArcticPrimaryRow);
}

#[tokio::test]
async fn upstream_indexset_survives_persist_reload_and_more_writes() {
    const ROOT: &str = "tests/data/index_backend_upstream_runtime";
    remove_dir_if_exists(ROOT.to_string()).await;

    let config = DiskConfig::new_with_table_name(
        ROOT,
        PersistedUpstreamWorkTable::name_snake_case(),
        PersistedUpstreamWorkTable::version(),
    );
    let engine = PersistedUpstreamPersistenceEngine::new(config.clone()).await.unwrap();
    let table = PersistedUpstreamWorkTable::load(engine).await.unwrap();

    for unique_key in 0..1_024 {
        table
            .insert(PersistedUpstreamRow {
                id: table.get_next_pk().into(),
                unique_key,
            })
            .unwrap();
    }
    table.wait_for_ops().await.unwrap();
    drop(table);

    let engine = PersistedUpstreamPersistenceEngine::new(config.clone()).await.unwrap();
    let table = PersistedUpstreamWorkTable::load(engine).await.unwrap();
    assert_eq!(table.count(), 1_024);
    assert_eq!(table.select_by_unique_key(777).unwrap().unique_key, 777);

    let added_pk = table
        .insert(PersistedUpstreamRow {
            id: table.get_next_pk().into(),
            unique_key: 2_000,
        })
        .unwrap();
    let added_id: u64 = added_pk.clone().into();
    table.delete(10).await.unwrap();
    table.wait_for_ops().await.unwrap();
    drop(table);

    let engine = PersistedUpstreamPersistenceEngine::new(config).await.unwrap();
    let table = PersistedUpstreamWorkTable::load(engine).await.unwrap();
    assert_eq!(table.count(), 1_024);
    assert!(table.select(10).is_none());
    assert_eq!(table.select(added_pk).unwrap().unique_key, 2_000);
    assert_eq!(table.select_by_unique_key(2_000).unwrap().id, added_id);
    table.wait_for_ops().await.unwrap();
    drop(table);

    remove_dir_if_exists(ROOT.to_string()).await;
}

#[tokio::test]
async fn native_art_backends_survive_wal_reload_and_further_mutation() {
    const ARCTIC_ROOT: &str = "tests/data/index_backend_arctic_persistence";
    const CONGEE_ROOT: &str = "tests/data/index_backend_congee_persistence";
    remove_dir_if_exists(ARCTIC_ROOT.to_string()).await;
    remove_dir_if_exists(CONGEE_ROOT.to_string()).await;

    let arctic_config = DiskConfig::new_with_table_name(
        ARCTIC_ROOT,
        PersistedArcticWorkTable::name_snake_case(),
        PersistedArcticWorkTable::version(),
    );
    let engine = PersistedArcticPersistenceEngine::new(arctic_config.clone())
        .await
        .unwrap();
    let table = PersistedArcticWorkTable::load(engine).await.unwrap();
    for congee_key in 0..256 {
        table
            .insert(PersistedArcticRow {
                id: table.get_next_pk().into(),
                congee_key,
            })
            .unwrap();
    }
    let rejected_id = table.get_next_pk().0;
    assert!(
        table
            .insert(PersistedArcticRow {
                id: rejected_id,
                congee_key: 77,
            })
            .is_err()
    );
    let accepted_id = table.get_next_pk().0;
    table
        .insert(PersistedArcticRow {
            id: accepted_id,
            congee_key: 300,
        })
        .unwrap();
    table.wait_for_ops().await.unwrap();
    drop(table);

    let engine = PersistedArcticPersistenceEngine::new(arctic_config.clone())
        .await
        .unwrap();
    let table = PersistedArcticWorkTable::load(engine).await.unwrap();
    assert_eq!(table.count(), 257);
    assert!(table.select(rejected_id).is_none());
    assert_eq!(table.select(accepted_id).unwrap().congee_key, 300);
    assert_eq!(table.select_by_congee_key(77).unwrap().congee_key, 77);
    table.delete(77).await.unwrap();
    table.wait_for_ops().await.unwrap();
    drop(table);

    let engine = PersistedArcticPersistenceEngine::new(arctic_config).await.unwrap();
    let table = PersistedArcticWorkTable::load(engine).await.unwrap();
    assert!(table.select(77).is_none());
    assert!(table.select_by_congee_key(77).is_none());
    table.wait_for_ops().await.unwrap();
    drop(table);

    let congee_config = DiskConfig::new_with_table_name(
        CONGEE_ROOT,
        PersistedCongeeWorkTable::name_snake_case(),
        PersistedCongeeWorkTable::version(),
    );
    let engine = PersistedCongeePersistenceEngine::new(congee_config.clone())
        .await
        .unwrap();
    let table = PersistedCongeeWorkTable::load(engine).await.unwrap();
    for arctic_key in 0..256 {
        table
            .insert(PersistedCongeeRow {
                id: table.get_next_pk().into(),
                arctic_key,
            })
            .unwrap();
    }
    table.wait_for_ops().await.unwrap();
    drop(table);

    let engine = PersistedCongeePersistenceEngine::new(congee_config.clone())
        .await
        .unwrap();
    let table = PersistedCongeeWorkTable::load(engine).await.unwrap();
    assert_eq!(table.count(), 256);
    assert_eq!(table.select_by_arctic_key(199).unwrap().arctic_key, 199);
    table.delete(199).await.unwrap();
    table.wait_for_ops().await.unwrap();
    drop(table);

    let engine = PersistedCongeePersistenceEngine::new(congee_config).await.unwrap();
    let table = PersistedCongeeWorkTable::load(engine).await.unwrap();
    assert!(table.select(199).is_none());
    assert!(table.select_by_arctic_key(199).is_none());
    table.wait_for_ops().await.unwrap();
    drop(table);

    remove_dir_if_exists(ARCTIC_ROOT.to_string()).await;
    remove_dir_if_exists(CONGEE_ROOT.to_string()).await;
}

#[tokio::test]
async fn native_art_backends_recover_concurrent_same_row_updates() {
    use std::sync::Arc;

    use tokio::sync::Barrier;

    const ROOT: &str = "tests/data/index_backend_art_concurrent_persistence";
    const WORKERS: u64 = 8;
    const UPDATES_PER_WORKER: u64 = 100;
    remove_dir_if_exists(ROOT.to_string()).await;

    let config = DiskConfig::new_with_table_name(
        ROOT,
        PersistedArcticWorkTable::name_snake_case(),
        PersistedArcticWorkTable::version(),
    );
    let engine = PersistedArcticPersistenceEngine::new(config.clone()).await.unwrap();
    let table = Arc::new(PersistedArcticWorkTable::load(engine).await.unwrap());
    let id = table.get_next_pk().0;
    table.insert(PersistedArcticRow { id, congee_key: 1 }).unwrap();

    let barrier = Arc::new(Barrier::new(WORKERS as usize + 1));
    let mut workers = Vec::new();
    for worker in 0..WORKERS {
        let table = Arc::clone(&table);
        let barrier = Arc::clone(&barrier);
        workers.push(tokio::spawn(async move {
            barrier.wait().await;
            for update in 0..UPDATES_PER_WORKER {
                table
                    .update(PersistedArcticRow {
                        id,
                        congee_key: 10_000 + worker * UPDATES_PER_WORKER + update,
                    })
                    .await
                    .unwrap();
            }
        }));
    }
    barrier.wait().await;
    for worker in workers {
        worker.await.unwrap();
    }

    let expected = table.select(id).unwrap();
    table.wait_for_ops().await.unwrap();
    drop(table);

    let engine = PersistedArcticPersistenceEngine::new(config).await.unwrap();
    let table = PersistedArcticWorkTable::load(engine).await.unwrap();
    assert_eq!(table.select(id), Some(expected.clone()));
    assert_eq!(table.select_by_congee_key(expected.congee_key), Some(expected.clone()));
    for key in 10_000..10_000 + WORKERS * UPDATES_PER_WORKER {
        if key != expected.congee_key {
            assert!(table.select_by_congee_key(key).is_none());
        }
    }
    table.wait_for_ops().await.unwrap();
    drop(table);

    remove_dir_if_exists(ROOT.to_string()).await;
}

#[tokio::test]
async fn persisted_tables_can_switch_between_wti_and_upstream_without_rebuild() {
    use provider_switch_upstream as upstream;
    use provider_switch_wti as wti;

    const ROOT: &str = "tests/data/index_backend_provider_switch_runtime";
    remove_dir_if_exists(ROOT.to_string()).await;

    let wti_config = DiskConfig::new_with_table_name(
        ROOT,
        wti::ProviderSwitchWorkTable::name_snake_case(),
        wti::ProviderSwitchWorkTable::version(),
    );
    let engine = wti::ProviderSwitchPersistenceEngine::new(wti_config.clone())
        .await
        .unwrap();
    let table = wti::ProviderSwitchWorkTable::load(engine).await.unwrap();
    for unique_key in 0..1_024 {
        table
            .insert(wti::ProviderSwitchRow {
                id: table.get_next_pk().into(),
                unique_key,
            })
            .unwrap();
    }
    table.wait_for_ops().await.unwrap();
    drop(table);

    let upstream_config = DiskConfig::new_with_table_name(
        ROOT,
        upstream::ProviderSwitchWorkTable::name_snake_case(),
        upstream::ProviderSwitchWorkTable::version(),
    );
    let engine = upstream::ProviderSwitchPersistenceEngine::new(upstream_config)
        .await
        .unwrap();
    let table = upstream::ProviderSwitchWorkTable::load(engine).await.unwrap();
    assert_eq!(table.count(), 1_024);
    assert_eq!(table.select_by_unique_key(600).unwrap().unique_key, 600);
    table.delete(10).await.unwrap();
    table
        .insert(upstream::ProviderSwitchRow {
            id: table.get_next_pk().into(),
            unique_key: 2_000,
        })
        .unwrap();
    table.wait_for_ops().await.unwrap();
    drop(table);

    let engine = wti::ProviderSwitchPersistenceEngine::new(wti_config).await.unwrap();
    let table = wti::ProviderSwitchWorkTable::load(engine).await.unwrap();
    assert_eq!(table.count(), 1_024);
    assert!(table.select(10).is_none());
    assert_eq!(table.select_by_unique_key(2_000).unwrap().unique_key, 2_000);
    table.wait_for_ops().await.unwrap();
    drop(table);

    remove_dir_if_exists(ROOT.to_string()).await;
}
