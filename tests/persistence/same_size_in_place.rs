use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable_codegen::worktable;

use crate::remove_dir_if_exists;

worktable!(
    name: SameSizeInPlace,
    persist: true,
    columns: {
        id: u64 primary_key,
        amount: u64,
        note: String,
    },
    queries: {
        update: {
            AmountById(amount) by id,
            NoteById(note) by id,
        }
    }
);

/// A String-bearing (unsized) persisted row updated without a size change
/// must keep its slot: the old dead size check (`need_to_reinsert`
/// initialized to true) forced a full delete-and-reinsert, moving the link on
/// every update. The same-slot write must also survive a reload.
#[test]
fn same_size_updates_keep_the_row_link() {
    let dir = "tests/data/same_size_in_place";
    let config = DiskConfig::new_with_table_name(
        dir,
        SameSizeInPlaceWorkTable::name_snake_case(),
        SameSizeInPlaceWorkTable::version(),
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
            let engine = SameSizeInPlacePersistenceEngine::new(config.clone()).await.unwrap();
            let table = SameSizeInPlaceWorkTable::load(engine).await.unwrap();
            table
                .insert(SameSizeInPlaceRow {
                    id: 1,
                    amount: 1,
                    note: "aaaa".to_string(),
                })
                .await
                .unwrap();
            let pk = SameSizeInPlacePrimaryKey(1);
            let link_before = table.0.primary_index.pk_map.get_value(&pk).unwrap().0;

            // Fixed-size column update: archived in-place swap, same slot.
            table
                .update_amount_by_id(AmountByIdQuery { amount: 2 }, 1)
                .await
                .unwrap();
            let link_after_amount = table.0.primary_index.pk_map.get_value(&pk).unwrap().0;
            assert_eq!(
                link_before, link_after_amount,
                "a fixed-size column update must not move the row"
            );

            // Same-length String update: same-size in-place path, same slot.
            table
                .update_note_by_id(
                    NoteByIdQuery {
                        note: "bbbb".to_string(),
                    },
                    1,
                )
                .await
                .unwrap();
            let link_after_note = table.0.primary_index.pk_map.get_value(&pk).unwrap().0;
            assert_eq!(
                link_before, link_after_note,
                "a same-size unsized update must write in place, not reinsert"
            );

            // A size-changing String update must still reinsert correctly.
            table
                .update_note_by_id(
                    NoteByIdQuery {
                        note: "a-considerably-longer-note".to_string(),
                    },
                    1,
                )
                .await
                .unwrap();
            assert_eq!(table.select(1).unwrap().note, "a-considerably-longer-note");

            table.wait_for_ops().await.unwrap();
        }
        {
            // The in-place writes must survive a reload.
            let engine = SameSizeInPlacePersistenceEngine::new(config.clone()).await.unwrap();
            let table = SameSizeInPlaceWorkTable::load(engine).await.unwrap();
            let row = table.select(1).expect("the row must survive reload");
            assert_eq!(row.amount, 2);
            assert_eq!(row.note, "a-considerably-longer-note");
        }

        remove_dir_if_exists(dir.to_string()).await;
    })
}
