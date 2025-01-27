use std::fmt::Debug;

use async_std::task;
use worktable::prelude::*;
use worktable::worktable;

fn main() {
    // describe WorkTable

    worktable!(
        name: My,
        columns: {
            id: u64 primary_key autoincrement,
            val: i64,
            attribute: String,
            attribute2: String,

        },
        indexes: {
            attr_idx: attribute,
            attr2_idx: attribute2,
        }
        queries: {
            update: {
                AttrById(attribute) by id,
                Attr2ById(attribute2) by id,
                ValByAttr(val) by attribute,
            },
            delete: {
                ByAttr() by attribute,
                ByAttr2() by attribute2,
                ById() by id,
            }
        }
    );

    // Init Worktable
    let my_table = MyWorkTable::default();

    // WT rows (has prefix My because of table name)
    let row = MyRow {
        val: 1,
        attribute: "TEST".to_string(),
        attribute2: 145.to_string(),
        id: 0,
    };

    let row1 = MyRow {
        val: 2,
        attribute: "TEST2".to_string(),
        attribute2: 245.to_string(),
        id: 1,
    };

    let row2 = MyRow {
        val: 1337,
        attribute: "TEST2".to_string(),
        attribute2: 345.to_string(),
        id: 2,
    };

    let row3 = MyRow {
        val: 555,
        attribute: "TEST3".to_string(),
        attribute2: 445.to_string(),
        id: 3,
    };

    // insert
    let _ = my_table.insert(row);
    let _ = my_table.insert(row1);
    let _ = my_table.insert(row2);
    let _ = my_table.insert(row3);

    // Select ALL records from WT
    let select_all = my_table.select_all().execute();
    println!("Select All {:?}", select_all);

    // Update all recrods val by attr TEST2
    //let update_val = my_table.update_val_by_attr(ValByAttrQuery { val: 777 }, "TEST2".to_string());
    //let _ = task::block_on(update_val);
    //
    let select_updated = my_table.select_by_attribute("TEST2".to_string());
    println!(
        "Select updated by Attribute TEST2: {:?}",
        select_updated.unwrap().vals
    );

    // Update record val by attr
    //let update_exchange =
    //    my_table.update_val_by_attr(ValByAttrQuery { val: 7777 }, "TEST".to_string());
    //let _ = task::block_on(update_exchange);
    //
    //let select_all_after_update = my_table.select_all();
    //println!(
    //    "Select After Val Update by Attribute: {:?}",
    //    select_all_after_update.execute()
    //);

    println!("Update attr TEST3 -> TEST2");
    let update_attr = my_table.update_attr_by_id(
        AttrByIdQuery {
            attribute: "TEST2".to_string(),
        },
        MyPrimaryKey(3),
    );
    let _ = task::block_on(update_attr);

    let select_by_attr = my_table.select_by_attribute("TEST2".to_string());
    if let Ok(vals) = select_by_attr {
        println!("Select by Attribute TEST2: {:?}", vals.vals);
    }

    println!("Delete TEST2");

    let test_delete = my_table.delete_by_attr("TEST2".to_string());
    let _ = task::block_on(test_delete);

    // Select All records with attribute TEST2
    let select_by_attr = my_table.select_by_attribute("TEST3".to_string());
    if let Ok(ref res) = select_by_attr {
        println!("Select by Attribute TEST3: {:?}", res.vals);
    } else {
        println!("Select by Attribute TEST3: {:?}", "emp")
    }

    let select_by_attr = my_table.select_by_attribute("TEST2".to_string());
    println!(
        "Select by Attribute TEST2: {:?}",
        select_by_attr.unwrap().vals
    );

    println!(
        "Select after deleted TEST2 {:?}",
        my_table.select_all().execute()
    );

    println!(
        "2 Select after deleted TEST2 {:?}",
        my_table.select_all().execute()
    );
}
