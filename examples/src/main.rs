//use futures::executor::block_on;
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
            idx2: attr2,
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
        attr2: 0,
        test: 1,
        id: 0,
    };

    let row2 = MyRow {
        val: 777,
        attr: "Attribute2".to_string(),
        attr2: 1,
        test: 2,
        id: 2,
    };

    let row3 = MyRow {
        val: 777,
        attr: "Attribute3".to_string(),
        attr2: 2,
        test: 3,
        id: 3,
    };

    let row4 = MyRow {
        val: 777,
        attr: "Attribute4".to_string(),
        attr2: 3,
        test: 4,
        id: 4,
    };

    // insert
    let _pk: MyPrimaryKey = my_table.insert(row).expect("primary key");
    let _pk: MyPrimaryKey = my_table.insert(row2).expect("primary key");
    let _pk: MyPrimaryKey = my_table.insert(row3).expect("primary key");
    let _pk: MyPrimaryKey = my_table.insert(row4).expect("primary key");

    // Select ALL records from WT
    // let select_all = my_table.select_all().execute();
    // println!("Select All {:?}", select_all);
    //
    // // Select All records with attribute TEST
    // let select_all = my_table.select_all().execute();
    // println!("Select All {:?}", select_all);

    let select_all2 = my_table
        .select_all()
        .where_by(0..3i16, "attr2")
        .where_by(2..=4u8, "test")
        .order_by(Order::Asc, "attr2")
        .offset(0)
        .limit(10)
        .execute();

    for row in select_all2.unwrap() {
        println!("SELECT ALL {:?}", row);
    }

    let select_all2 = my_table
        .select_by_attr2(2)
        .expect("msg")
        .where_by(2..=5u8, "test")
        .order_by(Order::Desc, "test")
        .offset(0)
        .limit(10)
        .execute();

    for row in select_all2.unwrap() {
        println!("SELECT BY {:?}", row);
    }

    // Select by Idx
    // let select_by_attr = my_table.select_by_attr("Attribute1".to_string());
    // println!("Select by idx {:?}", select_by_attr.unwrap().vals);
    //
    // // Update Value query
    // let update = my_table.update_val_by_id(ValByIdQuery { val: 1337 }, pk.clone());
    // let _ = block_on(update);
    //
    // let select_all = my_table.select_all().execute();
    // println!("Select after update val {:?}", select_all);
    //
    // let delete = my_table.delete(pk);
    // let _ = block_on(delete);
    //
    // let select_all = my_table.select_all().execute();
    // println!("Select after delete {:?}", select_all);
}
