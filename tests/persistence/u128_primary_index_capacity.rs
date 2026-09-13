use worktable::prelude::*;
use worktable::worktable;

use crate::remove_dir_if_exists;

worktable!(
    name: U128PrimaryIndexCapacity,
    persist: true,
    columns: {
        id: u128 primary_key using worktables_index,
        value: u64,
    },
);

worktable!(
    name: U8ThenU128KeyLayout,
    columns: {
        shard: u8 primary_key using worktables_index,
        id: u128 primary_key using worktables_index,
        value: u64,
    },
);

worktable!(
    name: U128ThenU8KeyLayout,
    columns: {
        id: u128 primary_key using worktables_index,
        shard: u8 primary_key using worktables_index,
        value: u64,
    },
);

worktable!(
    name: StringKeyLayout,
    columns: {
        id: String primary_key using worktables_index,
        value: u64,
    },
);

#[test]
fn generated_key_size_measure_tracks_archived_alignment_and_dynamic_length() {
    use data_bucket::{IndexPage, IndexValue, Persistable};

    macro_rules! assert_default_index_page_fits {
        ($key:ty) => {{
            let capacity = get_index_page_size_from_data_length::<$key>(INNER_PAGE_SIZE);
            let page = IndexPage::<$key>::new(IndexValue::default(), capacity);
            assert!(page.as_bytes().as_ref().len() <= INNER_PAGE_SIZE);
        }};
    }

    assert_default_index_page_fits!(U128PrimaryIndexCapacityPrimaryKey);
    assert_default_index_page_fits!(U8ThenU128KeyLayoutPrimaryKey);
    assert_default_index_page_fits!(U128ThenU8KeyLayoutPrimaryKey);

    assert_eq!(U128PrimaryIndexCapacityPrimaryKey::align(), Some(16));
    assert_eq!(U8ThenU128KeyLayoutPrimaryKey::align(), Some(16));
    assert_eq!(U128ThenU8KeyLayoutPrimaryKey::align(), Some(16));

    let empty = StringKeyLayoutPrimaryKey(String::new()).aligned_size();
    let populated = StringKeyLayoutPrimaryKey("a dynamic primary key".to_string()).aligned_size();
    assert!(
        populated > empty,
        "dynamic key length was lost by the generated wrapper"
    );
}

/// The generated primary-key newtype must report its 16-byte archived
/// alignment. Under-reporting it makes the WTI node larger than its serialized
/// default page before the first batch is written.
#[test]
fn u128_primary_index_fits_and_survives_default_page_persistence() {
    let dir = "tests/data/u128_primary_index_capacity";
    let config = DiskConfig::new_with_table_name(
        dir,
        U128PrimaryIndexCapacityWorkTable::name_snake_case(),
        U128PrimaryIndexCapacityWorkTable::version(),
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists(dir.to_string()).await;
        {
            let engine = U128PrimaryIndexCapacityPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = U128PrimaryIndexCapacityWorkTable::load(engine).await.unwrap();
            for id in 0..64u128 {
                table
                    .insert(U128PrimaryIndexCapacityRow { id, value: id as u64 })
                    .await
                    .unwrap();
            }
            table.wait_for_ops().await.unwrap();
        }
        {
            let engine = U128PrimaryIndexCapacityPersistenceEngine::new(config.clone())
                .await
                .unwrap();
            let table = U128PrimaryIndexCapacityWorkTable::load(engine).await.unwrap();
            for id in 0..64u128 {
                assert_eq!(table.select(id).unwrap().value, id as u64);
            }
        }
        remove_dir_if_exists(dir.to_string()).await;
    });
}
