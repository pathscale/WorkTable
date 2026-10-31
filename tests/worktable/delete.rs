use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: DeleteTest,
    columns: {
        token: String primary_key,
        val1: u64,
        val2: u64,
    },
    indexes: {
        val1_idx: val1,
        val2_idx: val2,
    },
    queries: {
        update: {
            Val1ByToken(val1) by token,
        },
        delete: {
            ByVal1() by val1,
            ByVal2() by val2,
            ByToken() by token,
        }
    }
);

#[test]
fn brute_force_delete_future_is_send() {
    fn assert_send<T: Send>(_: T) {}

    let table = DeleteTestWorkTable::default();
    assert_send(table.delete_by_val_2(1));
}

#[tokio::test]
async fn delete_by_primary_key_deletes_row_without_scan() {
    let table = DeleteTestWorkTable::default();
    table
        .insert(DeleteTestRow {
            token: "a".to_string(),
            val1: 1,
            val2: 10,
        })
        .await
        .unwrap();
    table
        .insert(DeleteTestRow {
            token: "b".to_string(),
            val1: 2,
            val2: 20,
        })
        .await
        .unwrap();

    table.delete_by_token("a".to_string()).await.unwrap();
    assert!(table.select("a".to_string()).is_none());
    assert!(table.select("b".to_string()).is_some());

    assert!(table.delete_by_token("missing".to_string()).await.is_ok());
    assert!(table.select("b".to_string()).is_some());
}
