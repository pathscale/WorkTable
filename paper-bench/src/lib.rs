pub mod dynamic;
pub mod util;

use worktable::prelude::*;
use worktable::worktable;

// The paper's benchmark table. `a` is indexed (non-unique) so we can measure
// indexed vs non-indexed update cost separately. `b`/`e` are the disjoint
// contention pair (both non-indexed). `d` keeps the table "unsized" (String),
// matching realistic schemas.
worktable!(
    name: Bench,
    columns: {
        id: u64 primary_key autoincrement,
        a: u64,
        b: u64,
        e: u64,
        c: f64,
        d: String,
    },
    indexes: {
        a_idx: a,
    },
    queries: {
        update: {
            UpdA(a) by id,
            UpdB(b) by id,
            UpdE(e) by id,
            UpdBE(b, e) by id,
        },
        in_place: {
            IncB(b) by id,
        }
    }
);

pub fn mk_row(table: &BenchWorkTable, v: u64) -> BenchRow {
    BenchRow {
        id: table.get_next_pk().into(),
        a: v,
        b: v,
        e: v,
        c: v as f64,
        d: "payloadpayload".to_string(), // fixed-length: updates stay in place
    }
}
