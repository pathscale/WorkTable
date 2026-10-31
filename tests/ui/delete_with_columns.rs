// Rule: a `delete` query takes no column list; columns inside `Name(...)` were
// previously ignored.
use worktable::worktable;

worktable! {
    name: DeleteWithColumns,
    persist: false,
    columns: {
        id: u64 primary_key,
        a: u64,
        b: u64,
    },
    indexes: {
        b_idx: b,
    },
    queries: {
        delete: {
            ByB(a) by b,
        }
    },
}

fn main() {}
