use futures::executor::block_on;
use worktable::prelude::*;
use worktable::worktable;

fn main() {
    // describe WorkTable
    worktable!(
        name: My,
        columns: {
            id: u64 primary_key autoincrement,
            val: i64,
            test: u8,
            attr: String,
            attr2: i16,

        },
        indexes: {
            idx1: attr,
            idx2: attr2 unique,
        },
        queries: {
            update: {
                ValById(val) by id,
                AllAttrById(attr, attr2) by id,
                UpdateOptionalById(test) by id,
            },
            delete: {
                ByAttr() by attr,
                ById() by id,
            }
        }
    );

    // Init Worktable
    let my_table = MyWorkTable::default();

    // WT rows (has prefix My because of table name)
    let row = MyRow {
        val: 777,
        attr: "Attribute1".to_string(),
        attr2: 1,
        test: 1,
        id: 0,
    };

    let row1 = MyRow {
        val: 777,
        attr: "Attribute1".to_string(),
        attr2: 2,
        test: 1,
        id: 2,
    };

    let row2 = MyRow {
        val: 777,
        attr: "Attribute1".to_string(),
        attr2: 3,
        test: 1,
        id: 3,
    };

    let row3 = MyRow {
        val: 777,
        attr: "Attribute1".to_string(),
        attr2: 7,
        test: 1,
        id: 4,
    };

    let row4 = MyRow {
        val: 777,
        attr: "Attribute1".to_string(),
        attr2: 11,
        test: 1,
        id: 5,
    };

    // insert
    let pk: MyPrimaryKey = my_table.insert(row).expect("primary key");
    let _pk: MyPrimaryKey = my_table.insert(row1).expect("primary key");
    let _pk: MyPrimaryKey = my_table.insert(row2).expect("primary key");
    let _pk: MyPrimaryKey = my_table.insert(row3).expect("primary key");
    let _pk: MyPrimaryKey = my_table.insert(row4).expect("primary key");

    // Select ALL records from WT
    let select_all = my_table.select_all().execute();
    println!("Select All {:?}", select_all);

    // Select All records with attribute TEST
    let select_all = my_table.select_all().execute();
    println!("Select All {:?}", select_all);

    // Select by Idx
    let select_by_attr = my_table.select_by_attr("Attribute1".to_string());
    println!("Select by idx {:?}", select_by_attr.unwrap().vals);

    // Update Value query
    let update = my_table.update_val_by_id(ValByIdQuery { val: 1337 }, pk.clone());
    let _ = block_on(update);

    let select_all = my_table.select_all().execute();
    println!("Select after update val {:?}", select_all);

    let delete = my_table.delete(pk);
    let _ = block_on(delete);

    let select_all = my_table.select_all().execute();
    println!("Select after delete {:?}", select_all);

    let _where1 = my_table.select_where_attr2(0..5);
    let _where2 = my_table.select_where_attr2(..10);
    let _where3 = my_table.select_where_attr2(10..);

    let _where4 = my_table.select_where_attr2(4..=10);
}
