// Rule: an `update_in_place` query cannot change a primary-key column, for the
// same reason an `update` query cannot: the primary index would keep the old
// key. The message must not send the user to an `update` query instead.
use worktable::worktable;

worktable! {
    name: InPlacePk,
    persist: false,
    columns: {
        id: u64 primary_key,
        value: u64,
    },
    queries: {
        update_in_place: {
            ReKey(id) by id,
        }
    }
}

fn main() {}
