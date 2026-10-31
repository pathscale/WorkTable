// Rule: at most one secondary index may be declared per column, because only
// one `select_by_<col>` method is generated.
use worktable::worktable;

worktable! {
    name: DuplicateIndexOnCol,
    persist: false,
    columns: {
        id: u64 primary_key,
        a: u64,
    },
    indexes: {
        a1: a,
        a2: a unique,
    },
}

fn main() {}
