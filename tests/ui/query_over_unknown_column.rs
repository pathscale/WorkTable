// Rule: a query names columns of its own table. `missing` is not one, and the
// error has to say so at the column rather than somewhere inside the generated
// row type.
use worktable::worktable;

worktable! {
    name: UnknownQueryColumn,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement,
        value: u64,
    },
    queries: {
        update: {
            MissingById(missing) by id,
        }
    },
}

fn main() {}
