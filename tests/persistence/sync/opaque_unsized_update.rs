use crate::remove_dir_if_exists;

use worktable::prelude::*;
use worktable::worktable;

#[derive(
    worktable::prelude::rkyv::Archive,
    Clone,
    Debug,
    worktable::prelude::rkyv::Deserialize,
    MemStat,
    PartialEq,
    PartialOrd,
    worktable::prelude::rkyv::Serialize,
)]
#[rkyv(crate = worktable::prelude::rkyv)]
#[rkyv(derive(Debug, PartialEq, PartialOrd))]
struct WrappedSecret(String);

worktable!(
    name: OpaqueUnsizedUpdate,
    version: 1,
    persist: true,
    columns: {
        id: u64 primary_key,
        secret: WrappedSecret,
        untouched: u64,
    },
    queries: {
        update: {
            SecretById(secret) by id,
        }
    }
);

#[test]
fn targeted_update_of_string_wrapper_survives_read_and_reload() {
    const DIR: &str = "tests/data/sync/opaque_unsized_update";
    let config = DiskConfig::new_with_table_name(
        DIR,
        OpaqueUnsizedUpdateWorkTable::name_snake_case(),
        OpaqueUnsizedUpdateWorkTable::version(),
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists(DIR.to_string()).await;

        fn link_of(table: &OpaqueUnsizedUpdateWorkTable, id: u64) -> Link {
            table
                .0
                .primary_index
                .pk_map
                .get_value(&OpaqueUnsizedUpdatePrimaryKey::from(id))
                .map(Into::into)
                .expect("row must exist")
        }

        {
            let engine = OpaqueUnsizedUpdatePersistenceEngine::new(config.clone()).await.unwrap();
            let table = OpaqueUnsizedUpdateWorkTable::load(engine).await.unwrap();
            table
                .insert(OpaqueUnsizedUpdateRow {
                    id: 7,
                    secret: WrappedSecret("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()),
                    untouched: 42,
                })
                .await
                .unwrap();
            let link_before = link_of(&table, 7);

            table
                .update_by_id(
                    7,
                    OpaqueUnsizedUpdateColumns::SECRET,
                    WrappedSecret("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()),
                )
                .await
                .unwrap();
            let link_after = link_of(&table, 7);

            let row = table.select(7).unwrap();
            assert_eq!(
                row.secret,
                WrappedSecret("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string())
            );
            assert_eq!(row.untouched, 42);
            assert_eq!(link_before, link_after, "same-size opaque update must retain its slot");
            table.wait_for_ops().await.unwrap();
        }

        {
            let engine = OpaqueUnsizedUpdatePersistenceEngine::new(config.clone()).await.unwrap();
            let table = OpaqueUnsizedUpdateWorkTable::load(engine).await.unwrap();
            let row = table.select(7).unwrap();
            assert_eq!(
                row.secret,
                WrappedSecret("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string())
            );
            assert_eq!(row.untouched, 42);

            table
                .update_by_id(
                    7,
                    OpaqueUnsizedUpdateColumns::SECRET,
                    WrappedSecret("a replacement with a deliberately different serialized length".to_string()),
                )
                .await
                .unwrap();
            table.wait_for_ops().await.unwrap();
        }

        {
            let engine = OpaqueUnsizedUpdatePersistenceEngine::new(config.clone()).await.unwrap();
            let table = OpaqueUnsizedUpdateWorkTable::load(engine).await.unwrap();
            let row = table.select(7).unwrap();
            assert_eq!(
                row.secret,
                WrappedSecret("a replacement with a deliberately different serialized length".to_string())
            );
            assert_eq!(row.untouched, 42);

            table
                .replace(OpaqueUnsizedUpdateRow {
                    id: 7,
                    secret: WrappedSecret("full-row replacement after targeted updates".to_string()),
                    untouched: 84,
                })
                .await
                .unwrap();
            table.wait_for_ops().await.unwrap();
        }

        {
            let engine = OpaqueUnsizedUpdatePersistenceEngine::new(config).await.unwrap();
            let table = OpaqueUnsizedUpdateWorkTable::load(engine).await.unwrap();
            let row = table.select(7).unwrap();
            assert_eq!(
                row.secret,
                WrappedSecret("full-row replacement after targeted updates".to_string())
            );
            assert_eq!(row.untouched, 84);
        }
    });
}
