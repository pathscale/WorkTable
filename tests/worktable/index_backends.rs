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

#[test]
fn all_unique_backends_coexist_in_one_table() {
    let table = MixedBackendWorkTable::default();
    let row = MixedBackendRow {
        id: table.get_next_pk().into(),
        wti_key: 11,
        upstream_key: 12,
        congee_key: 13,
        arctic_key: 14,
    };

    let pk = table.insert(row.clone()).unwrap();
    assert_eq!(table.select(pk), Some(row.clone()));
    assert_eq!(table.select_by_wti_key(11), Some(row.clone()));
    assert_eq!(table.select_by_upstream_key(12), Some(row.clone()));
    assert_eq!(table.select_by_congee_key(13), Some(row.clone()));
    assert_eq!(table.select_by_arctic_key(14), Some(row));
}

#[test]
fn alternative_primary_backends_support_point_crud() {
    let upstream = UpstreamPrimaryWorkTable::default();
    let upstream_row = UpstreamPrimaryRow {
        id: upstream.get_next_pk().into(),
        value: 1,
    };
    let upstream_pk = upstream.insert(upstream_row.clone()).unwrap();
    assert_eq!(upstream.select(upstream_pk), Some(upstream_row));

    let arctic = ArcticPrimaryWorkTable::default();
    let arctic_row = ArcticPrimaryRow {
        id: arctic.get_next_pk().into(),
        value: 2,
    };
    let arctic_pk = arctic.insert(arctic_row.clone()).unwrap();
    assert_eq!(arctic.select(arctic_pk), Some(arctic_row));
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
    table.wait_for_ops().await;
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
    table.wait_for_ops().await;
    drop(table);

    let engine = PersistedUpstreamPersistenceEngine::new(config).await.unwrap();
    let table = PersistedUpstreamWorkTable::load(engine).await.unwrap();
    assert_eq!(table.count(), 1_024);
    assert!(table.select(10).is_none());
    assert_eq!(table.select(added_pk).unwrap().unique_key, 2_000);
    assert_eq!(table.select_by_unique_key(2_000).unwrap().id, added_id);
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
    table.wait_for_ops().await;
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
    table.wait_for_ops().await;
    drop(table);

    let engine = wti::ProviderSwitchPersistenceEngine::new(wti_config).await.unwrap();
    let table = wti::ProviderSwitchWorkTable::load(engine).await.unwrap();
    assert_eq!(table.count(), 1_024);
    assert!(table.select(10).is_none());
    assert_eq!(table.select_by_unique_key(2_000).unwrap().unique_key, 2_000);
    drop(table);

    remove_dir_if_exists(ROOT.to_string()).await;
}
