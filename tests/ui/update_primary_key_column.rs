// Rule: a paged table `update` query cannot change a primary-key column,
// because rewriting the key in the row without re-keying the primary index
// leaves the row reachable only under its old key.
use worktable::worktable;

worktable! {
    name: UpdatePk,
    persist: false,
    columns: {
        id: u64 primary_key,
        value: u64,
    },
    queries: {
        update: {
            ReKey(id) by id,
        }
    },
}

fn main() {}
