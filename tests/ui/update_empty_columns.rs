// Rule: an `update` (or `update_in_place`) query must declare at least one
// column to update.
use worktable::worktable;

worktable! {
    name: UpdateEmptyColumns,
    persist: false,
    columns: {
        id: u64 primary_key,
        value: u64,
    },
    queries: {
        update: {
            Nothing() by id,
        }
    },
}

fn main() {}
