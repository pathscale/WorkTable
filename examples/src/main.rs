use worktable::prelude::*;
use worktable::worktable;

// describe WorkTable
worktable!(
    name: My,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        value: i64,
        exchange: String,
        test2: u32,

    },
    indexes: {
        idx1: exchange,
        idx2: test2 unique,
    },
    queries: {
        delete: {
            ById() by id,
        }
        update: {
            Test2ById(test2) by id
        }
    }
);

#[tokio::main]
async fn main() {
    // Init Worktable
    let config = PersistenceConfig::new("mydata_dir", "mydata_dir").unwrap();
    let my_table = MyWorkTable::new(config).await.unwrap();
    // WT rows (has prefix My because of table name)
    let row = MyRow {
        value: 777,
        exchange: "Exchange".to_string(),
        test2: 345,
        id: 0,
    };

    // insert
    let pk: MyPrimaryKey = my_table.insert(row).expect("primary key");

    // Select ALL records from WT
    let select_all = my_table.select_all().execute();
    println!("Select All {:?}", select_all);

    // Select All records with attribute TEST
    let select_all = my_table.select_all().execute();
    println!("Select All {:?}", select_all);

    // Select by Idx
    let select_by_test1 = my_table
        .select_by_exchange("Attribute1".to_string())
        .execute()
        .unwrap();

    for row in select_by_test1 {
        println!("Select by idx, row {:?}", row);
    }

    // Update Value query
    let update = my_table.update_test_2_by_id(Test2ByIdQuery { test2: 1337 }, pk.clone());
    let _ = update.await;

    let select_all = my_table.select_all().execute();
    println!("Select after update val {:?}", select_all);

    let delete = my_table.delete(pk);
    let _ = delete.await;

    let select_all = my_table.select_all().execute();
    println!("Select after delete {:?}", select_all);

    let _ = my_table.persist().await;

    let _ = my_table.wait_for_ops().await;
}
