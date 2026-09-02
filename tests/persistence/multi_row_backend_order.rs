use worktable::prelude::*;
use worktable::worktable;

use crate::remove_dir_if_exists;

macro_rules! persisted_multi_row_backend_case {
    (
        $module:ident,
        $name:ident,
        $table:ident,
        $row:ident,
        $engine:ident,
        $backend:ident,
        $root:literal
    ) => {
        mod $module {
            use super::*;

            worktable! {
                name: $name,
                persist: true,
                columns: {
                    id: u64 primary_key autoincrement using $backend,
                    group_id: u64,
                    payload: String,
                },
                indexes: {
                    group_idx: group_id,
                },
                queries: {
                    update: {
                        PayloadByGroup(payload) by group_id,
                    }
                }
            }

            #[tokio::test]
            async fn multi_row_relocation_survives_persisted_reload() {
                const ROOT: &str = $root;
                remove_dir_if_exists(ROOT.to_string()).await;
                let config = DiskConfig::new_with_table_name(ROOT, $table::name_snake_case(), $table::version());

                let engine = $engine::new(config.clone()).await.unwrap();
                let table = $table::load(engine).await.unwrap();
                for length in 1..=128 {
                    table
                        .insert($row {
                            id: table.get_next_pk().into(),
                            group_id: 7,
                            payload: "x".repeat(length),
                        })
                        .await
                        .unwrap();
                }
                table.wait_for_ops().await.unwrap();

                let replacement = "new-payload".repeat(64);
                table
                    .update_payload_by_group(
                        PayloadByGroupQuery {
                            payload: replacement.clone(),
                        },
                        7,
                    )
                    .await
                    .unwrap();
                table.wait_for_ops().await.unwrap();
                drop(table);

                let engine = $engine::new(config).await.unwrap();
                let table = $table::load(engine).await.unwrap();
                let rows = table.select_by_group_id(7).execute().unwrap();
                assert_eq!(rows.len(), 128);
                assert!(rows.iter().all(|row| row.payload == replacement));
                table.wait_for_ops().await.unwrap();
                drop(table);
                remove_dir_if_exists(ROOT.to_string()).await;
            }
        }
    };
}

persisted_multi_row_backend_case!(
    wti,
    MultiRowWti,
    MultiRowWtiWorkTable,
    MultiRowWtiRow,
    MultiRowWtiPersistenceEngine,
    worktables_index,
    "tests/data/multi_row_backend_wti"
);
persisted_multi_row_backend_case!(
    congee,
    MultiRowCongee,
    MultiRowCongeeWorkTable,
    MultiRowCongeeRow,
    MultiRowCongeePersistenceEngine,
    congee,
    "tests/data/multi_row_backend_congee"
);
persisted_multi_row_backend_case!(
    arctic,
    MultiRowArctic,
    MultiRowArcticWorkTable,
    MultiRowArcticRow,
    MultiRowArcticPersistenceEngine,
    arctic,
    "tests/data/multi_row_backend_arctic"
);
