use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;
use worktable::prelude::*;
use worktable::worktable;

use crate::remove_dir_if_exists;

#[derive(
    Archive,
    Clone,
    Copy,
    Debug,
    Default,
    Deserialize,
    Eq,
    Hash,
    MemStat,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    SizeMeasure,
)]
#[rkyv(compare(PartialEq), derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord))]
struct PersistedPaymentId(Uuid);

#[derive(
    Archive,
    Clone,
    Copy,
    Debug,
    Default,
    Deserialize,
    Eq,
    Hash,
    MemStat,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    SizeMeasure,
)]
#[rkyv(compare(PartialEq), derive(Debug, PartialOrd, PartialEq, Eq, Ord))]
struct PersistedAppId(Uuid);

#[derive(Archive, Clone, Copy, Debug, Deserialize, Eq, Hash, MemStat, Ord, PartialEq, PartialOrd, Serialize)]
#[rkyv(compare(PartialEq), derive(Debug))]
#[repr(u8)]
enum PersistedPaymentStatus {
    SetUp,
    Confirmed,
}

worktable! {
    name: PersistedUuidPrimaryMutation,
    persist: true,
    columns: {
        id: PersistedPaymentId primary_key using worktables_index,
        app_id: PersistedAppId,
        endpoint_address: String,
        symbol: String,
        network: u64 optional,
        deposited_amount: String,
        goal_amount: String,
        goal_amount_usd: String,
        status: PersistedPaymentStatus,
        created_at: i64,
        deposit_until: i64 optional,
        completed_at: i64 optional,
    },
    indexes: {
        symbol_idx: symbol,
        endpoint_address_idx: endpoint_address,
        app_id_idx: app_id using worktables_index,
        created_at_idx: created_at,
    },
}

fn payment_id(index: usize) -> PersistedPaymentId {
    PersistedPaymentId(Uuid::from_u128(
        0x0199_45d2_0000_7000_8000_0000_0000_0000 | index as u128,
    ))
}

fn row(index: usize, status: PersistedPaymentStatus) -> PersistedUuidPrimaryMutationRow {
    PersistedUuidPrimaryMutationRow {
        id: payment_id(index),
        app_id: PersistedAppId(Uuid::from_u128(
            0x66c0_af2b_ef09_4255_86db_9c60_0000_0000 | (index % 32) as u128,
        )),
        endpoint_address: format!("0x{:040x}", index % 32),
        symbol: if index.is_multiple_of(2) {
            "usdc".to_owned()
        } else {
            "pol".to_owned()
        },
        network: Some(137),
        deposited_amount: format!("{index}.000001"),
        goal_amount: format!("{}.025", 100 + index),
        goal_amount_usd: format!("{}.50", 25 + index),
        status,
        created_at: 1_789_000_000 + index as i64,
        deposit_until: Some(1_789_000_900 + index as i64),
        completed_at: None,
    }
}

#[tokio::test]
async fn pays_shaped_uuid_primary_upserts_survive_batch_boundaries_and_reload() {
    const ROWS: usize = 256;

    let directory = std::env::temp_dir().join(format!("worktable-uuid-primary-upsert-{}", Uuid::new_v4()));
    let root = directory.to_string_lossy().into_owned();
    remove_dir_if_exists(root.clone()).await;
    let config = DiskConfig::new_with_table_name(
        &root,
        PersistedUuidPrimaryMutationWorkTable::name_snake_case(),
        PersistedUuidPrimaryMutationWorkTable::version(),
    );

    {
        let engine = PersistedUuidPrimaryMutationPersistenceEngine::new(config.clone())
            .await
            .unwrap();
        let table = PersistedUuidPrimaryMutationWorkTable::load(engine).await.unwrap();
        for index in 0..ROWS {
            table.insert(row(index, PersistedPaymentStatus::SetUp)).await.unwrap();
        }
        table.wait_for_ops().await.unwrap();

        for index in 0..ROWS {
            let permuted = (index * 129) & (ROWS - 1);
            table
                .upsert(row(permuted, PersistedPaymentStatus::Confirmed))
                .await
                .unwrap();
        }
        table.wait_for_ops().await.unwrap();
        table.close().await.unwrap();
    }

    let engine = PersistedUuidPrimaryMutationPersistenceEngine::new(config)
        .await
        .unwrap();
    let table = PersistedUuidPrimaryMutationWorkTable::load(engine).await.unwrap();
    assert_eq!(table.select_all().execute().unwrap().len(), ROWS);
    for index in [0, ROWS / 2, ROWS - 1] {
        assert_eq!(
            table.select(payment_id(index)).unwrap().status,
            PersistedPaymentStatus::Confirmed
        );
    }
    table.close().await.unwrap();
    remove_dir_if_exists(root).await;
}
