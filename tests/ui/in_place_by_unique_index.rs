// Rule: an in-place update cannot select by a secondary unique index yet, because the index entry would have to move with the row.

use worktable::worktable;

worktable!(
    name: InPlaceByEmail,
    persist: false,
    columns: {
        id: u64 primary_key,
        email: String,
        amount: u64,
    },
    indexes: {
        email_idx: email unique,
    },
    queries: {
        update_in_place: {
            BumpByEmail(amount) by email,
        }
    }
);

fn main() {}
