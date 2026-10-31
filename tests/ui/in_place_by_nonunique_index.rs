// Rule: an in-place update cannot select by a secondary non-unique index yet, because the index entries would have to move with the rows.

use worktable::worktable;

worktable!(
    name: InPlaceByGroup,
    persist: false,
    columns: {
        id: u64 primary_key,
        group_id: u64,
        amount: u64,
    },
    indexes: {
        group_idx: group_id,
    },
    queries: {
        update_in_place: {
            BumpByGroup(amount) by group_id,
        }
    }
);

fn main() {}
